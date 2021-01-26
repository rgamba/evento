use crate::{
    state::{OperationExecutionData, State},
    OperationExecutor, WorkflowData, WorkflowError, WorkflowRegistry, WorkflowRunner,
    WorkflowStatus,
};
use anyhow::{bail, format_err, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use lazy_static::lazy_static;
#[cfg(not(test))]
use log::{error, info, warn};
#[cfg(test)]
use std::{println as info, println as warn, println as error};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    time::Duration,
    todo,
};
use thread::JoinHandle;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(i64::MAX, 0), Utc);
}

type WorkflowSender = Sender<(
    WorkflowData,
    Sender<anyhow::Result<WorkflowStatus, WorkflowError>>,
)>;
type WorkflowReceiver = Receiver<(
    WorkflowData,
    Sender<anyhow::Result<WorkflowStatus, WorkflowError>>,
)>;

/// Workflow runner implementation that tries to run the workflow definition
/// by injecting the available operation executions into the workflow.
/// If any operation or wait has not been completed, it delegates the execution and
/// stops. It relies on another component to actually execute the operation asynchronously
/// and populate the execution results.
struct AsyncWorkflowRunner {
    state: State,
    workflow_registry: Arc<dyn WorkflowRegistry>,
    handle: JoinHandle<()>,
    sender: Arc<Mutex<WorkflowSender>>,
}

impl WorkflowRunner for AsyncWorkflowRunner {
    fn run(&self, workflow_data: WorkflowData) -> Result<WorkflowStatus, WorkflowError> {
        let (result_sender, result_receiver) = mpsc::channel();
        self.sender
            .lock()
            .unwrap()
            .send((workflow_data, result_sender))
            .map_err(|e| format_err!("{:?}", e))?;
        result_receiver.recv().map_err(|e| format_err!("{:?}", e))?
    }
}

impl AsyncWorkflowRunner {
    fn new(state: State, workflow_registry: Arc<dyn WorkflowRegistry>) -> Self {
        let (sender, receiver): (WorkflowSender, WorkflowReceiver) = mpsc::channel();
        let state_clone = state.clone();
        let registry_clone = workflow_registry.clone();
        let rx = Arc::new(Mutex::new(receiver));
        let main_handle = thread::spawn(move || {
            let receiver_clone = rx.clone();
            loop {
                // Main loop - in case the runner thread crashes, we can restart and we don't block the main thread.
                let another_state_clone = state_clone.clone();
                let another_registry_clone = registry_clone.clone();
                let another_rx_clone = receiver_clone.clone();
                let handle = thread::spawn(move || {
                    // Runner loop thread
                    loop {
                        match another_rx_clone.lock().unwrap().recv() {
                            Ok((data, result_sender)) => {
                                info!("New request to process: {:?}", data);
                                match Self::run_internal(
                                    another_state_clone.clone(),
                                    data,
                                    another_registry_clone.clone(),
                                ) {
                                    Ok(result) => {
                                        result_sender.send(Ok(result));
                                        info!("Successfully ran workflow.");
                                    }
                                    Err(err) => {
                                        result_sender.send(Err(err.clone()));
                                        error!("Unexpected workflow run error. error={:?}", err);
                                    }
                                }
                            }
                            Err(_) => {}
                        };
                    }
                });
                if let Err(err) = handle.join() {
                    error!("Workflow Runner thread panicked: {:?}", err);
                    thread::sleep(Duration::from_secs(1));
                } else {
                    error!("Workflow Runner main thread has stopped");
                }
            }
        });
        Self {
            state: state.clone(),
            handle: main_handle,
            sender: Arc::new(Mutex::new(sender)),
            workflow_registry,
        }
    }

    fn run_internal(
        state: State,
        workflow_data: WorkflowData,
        workflow_registry: Arc<dyn WorkflowRegistry>,
    ) -> anyhow::Result<WorkflowStatus, WorkflowError> {
        let workflow_data = state
            .store
            .get_workflow(workflow_data.id)?
            .ok_or(format_err!(
                "Unable to find workflow with id {}",
                workflow_data.id,
            ))?;
        let operation_results = state.store.get_operation_results(workflow_data.id)?;
        let workflow = workflow_registry.create_workflow(
            workflow_data.name.clone(),
            workflow_data.id,
            workflow_data.correlation_id.clone(),
            workflow_data.context.clone(),
            operation_results,
        )?;
        let result = workflow.run();
        match &result {
            Ok(WorkflowStatus::Completed) => {
                info!("Workflow has been completed. id={}", workflow_data.id);
                state.store.complete_workflow(workflow_data.id)?;
            }
            Ok(WorkflowStatus::Error(error)) => {
                warn!(
                    "Workflow has returned error. id={}, error={:?}",
                    workflow_data.id, error
                );
                state
                    .store
                    .abort_workflow_with_error(workflow_data.id, error.clone())?;
            }
            Ok(WorkflowStatus::CompletedWithError(error)) => {
                info!(
                    "Workflow has completed with error. id={}, error={:?}",
                    workflow_data.id, error
                );
                state
                    .store
                    .complete_workflow_with_error(workflow_data.id, error.error.to_string())?;
            }
            Ok(WorkflowStatus::RunNext(inputs)) => {
                info!(
                    "Workflow has returned next operations. id={}, next_operations={:?}",
                    workflow_data.id, inputs
                );
                state.store.queue_all_operations(
                    inputs
                        .iter()
                        .map(|input| {
                            (
                                OperationExecutionData {
                                    workflow_id: workflow_data.id,
                                    correlation_id: workflow_data.correlation_id.clone(),
                                    retry_count: None,
                                    input: input.clone(),
                                },
                                Utc::now(),
                            )
                        })
                        .collect(),
                )?;
            }
            Ok(WorkflowStatus::WaitForExternal((input, timeout))) => {
                info!(
                    "Workflow has returned a wait. id={}, input={:?}, timeout={:?}",
                    workflow_data.id, input, timeout
                );
                state.store.queue_operation(
                    OperationExecutionData {
                        workflow_id: workflow_data.id,
                        correlation_id: workflow_data.correlation_id.clone(),
                        retry_count: None,
                        input: input.clone(),
                    },
                    timeout.map_or(*INFINITE_WAIT, |t| t),
                )?;
            }
            Ok(_) => {
                // All others are a no-op
            }
            Err(workflow_error) => {
                // Errors raised from workflow execution should not be expected and are
                // not retriable, hence we'll abort the workflow.
                error!(
                    "Workflow execution returned unexpected error. id={}, error={:?}",
                    workflow_data.id, workflow_error
                );
                state
                    .store
                    .abort_workflow_with_error(workflow_data.id, workflow_error.clone())?;
            }
        }
        result
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{MockWorkflow, MockWorkflowFactory, WorkflowError, WorkflowFactory, WorkflowId};
    use std::collections::HashMap;
    use uuid::Uuid;

    use super::*;
    use crate::{registry::SimpleWorkflowRegistry, state::tests::create_test_state, Workflow};

    #[test]
    fn test_runner() {
        let wf_id = Uuid::new_v4();
        let wf_name = "test".to_string();
        let corr_id = "123".to_string();
        let wf_context = serde_json::Value::String("test".to_string());
        let mut factories = HashMap::new();
        factories.insert(
            wf_name.clone(),
            create_test_workflow_factory(Ok(WorkflowStatus::Completed)),
        );
        let registry = SimpleWorkflowRegistry::new(factories);
        let state = create_test_state();
        state
            .store
            .create_workflow(wf_name.clone(), wf_id, corr_id.clone(), wf_context)
            .unwrap();
        let runner = AsyncWorkflowRunner::new(state.clone(), Arc::new(registry));

        let result = runner
            .run(WorkflowData {
                id: wf_id,
                name: wf_name.clone(),
                correlation_id: "test".to_string(),
                status: WorkflowStatus::Created,
                created_at: Utc::now(),
                context: serde_json::Value::String("test".to_string()),
            })
            .unwrap();
        assert!(matches!(result, WorkflowStatus::Completed));
        wait_for_workflow_to_complete(wf_id, state.clone(), Duration::from_secs(3)).unwrap();
    }

    fn create_test_workflow_factory(
        run_result: Result<WorkflowStatus, WorkflowError>,
    ) -> Arc<dyn WorkflowFactory> {
        let mut mock = MockWorkflowFactory::new();
        mock.expect_create().return_once(move |_, _, _, _| {
            let mut mock_wf = Box::new(MockWorkflow::new());
            mock_wf.expect_run().return_once(move || run_result);
            mock_wf
        });
        Arc::new(mock)
    }

    pub fn wait_for_workflow_to_complete(
        workflow_id: WorkflowId,
        state: State,
        timeout: Duration,
    ) -> Result<WorkflowData> {
        let time_timeout = Utc::now()
            .checked_add_signed(chrono::Duration::from_std(timeout).unwrap())
            .unwrap();
        loop {
            if Utc::now().ge(&time_timeout) {
                break Err(format_err!("Workflow failed to reach completed status"));
            }
            let wf = state.store.get_workflow(workflow_id).unwrap().unwrap();
            if let WorkflowStatus::Completed = wf.status {
                break Ok(wf);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}
