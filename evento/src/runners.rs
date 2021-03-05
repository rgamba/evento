use crate::{
    state::{OperationExecutionData, State},
    WorkflowData, WorkflowError, WorkflowId, WorkflowRegistry, WorkflowRunner, WorkflowStatus,
};
use anyhow::{bail, format_err, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use log::{error, info, warn};
use std::sync::atomic::{AtomicU8, Ordering};
use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
use thread::JoinHandle;
use uuid::Uuid;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(16725225600, 0), Utc);
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
pub struct AsyncWorkflowRunner {
    _state: State,
    _workflow_registry: Arc<dyn WorkflowRegistry>,
    _handle: JoinHandle<()>,
    sender: Arc<Mutex<WorkflowSender>>,
    stop_state: Arc<AtomicU8>, // 0=dont stop, 1=stopping, 2=stopped
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
    pub fn new(state: State, workflow_registry: Arc<dyn WorkflowRegistry>) -> Self {
        let stop = Arc::new(AtomicU8::new(0));
        let stop_clone = stop.clone();
        let (sender, receiver): (WorkflowSender, WorkflowReceiver) = mpsc::channel();
        let state_clone = state.clone();
        let registry_clone = workflow_registry.clone();
        let rx = Arc::new(Mutex::new(receiver));
        let main_handle = thread::spawn(move || {
            let receiver_clone = rx.clone();
            let stop_clone_clone = stop_clone.clone();
            while stop_clone_clone.load(Ordering::SeqCst) == 0 {
                // Main loop - in case the runner thread crashes, we can restart and we don't block the main thread.
                let another_state_clone = state_clone.clone();
                let another_registry_clone = registry_clone.clone();
                let another_rx_clone = receiver_clone.clone();
                let stop_clone_clone_clone = stop_clone_clone.clone();
                let handle = thread::spawn(move || {
                    // Runner loop thread
                    while stop_clone_clone_clone.load(Ordering::SeqCst) == 0 {
                        let recv = {
                            // Get and release the lock immediately, otherwise it could end up being
                            // poisoned if the runner panics.
                            another_rx_clone.lock().unwrap().recv()
                        };
                        if let Ok((data, result_sender)) = recv {
                            info!("New request to process workflow id: {:?}", data.id);
                            log::debug!(
                                "Workflow data to be processed:\n{}",
                                serde_json::to_string_pretty(&data).unwrap()
                            );
                            match Self::run_internal(
                                another_state_clone.clone(),
                                data,
                                another_registry_clone.clone(),
                            ) {
                                Ok(result) => {
                                    if let Err(e) = result_sender.send(Ok(result.clone())) {
                                        error!("Unable to send result back to caller. result={:?}, error={:?}", result, e);
                                    }
                                    log::debug!("Successfully ran workflow.");
                                }
                                Err(err) => {
                                    if let Err(e) = result_sender.send(Err(err.clone())) {
                                        error!("Unable to send response to caller. error={:?}", e);
                                    }
                                    error!("Unexpected workflow run error. error={:?}", err);
                                }
                            }
                        }
                    }
                });
                if let Err(err) = handle.join() {
                    if let Some(msg) = err.downcast_ref::<String>() {
                        error!("Workflow Runner thread panicked: {:?}", msg);
                    } else if let Some(msg) = err.downcast_ref::<&str>() {
                        error!("Workflow Runner thread panicked: {:?}", msg);
                    } else {
                        error!(
                            "Workflow Runner thread panicked with unexpected error: {:?}",
                            err
                        );
                    }
                    thread::sleep(Duration::from_secs(1));
                } else {
                    error!("Workflow Runner main thread has stopped");
                }
            }
            stop_clone.store(2, Ordering::SeqCst);
        });
        Self {
            _state: state,
            _handle: main_handle,
            sender: Arc::new(Mutex::new(sender)),
            _workflow_registry: workflow_registry,
            stop_state: stop,
        }
    }

    //TODO: this method should be ran on a separate thread to avoid poisoning the runner thread's receiver lock.
    fn run_internal(
        state: State,
        workflow_data: WorkflowData,
        workflow_registry: Arc<dyn WorkflowRegistry>,
    ) -> anyhow::Result<WorkflowStatus, WorkflowError> {
        let workflow_data = state
            .store
            .get_workflow(workflow_data.id)?
            .ok_or_else(|| format_err!("Unable to find workflow with id {}", workflow_data.id,))?;
        let operation_results = state.store.get_operation_results(workflow_data.id)?;
        log::info!("Running workflow with id: {}", workflow_data.id);
        log::debug!(
            "Injecting results: {}",
            serde_json::to_string_pretty(&operation_results).unwrap()
        );
        let workflow = workflow_registry.create_workflow(
            workflow_data.name.clone(),
            workflow_data.id,
            workflow_data.correlation_id.clone(),
            workflow_data.context.clone(),
            operation_results,
        )?;
        let result = workflow.run();
        log::debug!("Ran completed. result={:?}", result);
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
            Ok(WorkflowStatus::Active(inputs)) => {
                info!(
                    "Workflow has returned next operations. workflow_id={}, count={}",
                    workflow_data.id,
                    inputs.len()
                );
                log::debug!(
                    "Next operations. id={}, next_operations={:?}",
                    workflow_data.id,
                    inputs
                );
                state.store.mark_active(workflow_data.id, inputs.clone())?;
                state.store.queue_all_operations(
                    inputs
                        .iter()
                        .map(|input| {
                            let mut ext_input = input.input.clone();
                            if let Some(wait_params) = &input.wait_params {
                                ext_input.external_key = Some(wait_params.external_input_key);
                            }
                            (
                                // Execution data:
                                OperationExecutionData {
                                    workflow_id: workflow_data.id,
                                    correlation_id: workflow_data.correlation_id.clone(),
                                    retry_count: None,
                                    input: ext_input,
                                },
                                // Next run date:
                                input
                                    .wait_params
                                    .clone()
                                    .map(|wp| wp.timeout.unwrap_or_else(|| *INFINITE_WAIT))
                                    .unwrap_or_else(Utc::now),
                            )
                        })
                        .collect(),
                )?;
            }
            Ok(_) => {
                // All others are a no-op
            }
            Err(workflow_error) => {
                // Errors raised from workflow execution should not be expected and are
                // not retriable, hence we'll abort the workflow.
                log::debug!(
                    "Workflow execution returned unexpected error. id={}, error={:?}",
                    workflow_data.id,
                    workflow_error
                );
                state
                    .store
                    .abort_workflow_with_error(workflow_data.id, workflow_error.clone())?;
            }
        }
        result
    }

    #[allow(unused_must_use)]
    pub fn stop(&self) -> Result<()> {
        log::info!("Stopping runner");
        self.stop_state.store(1, Ordering::SeqCst);
        // TODO:we should be able to send a None workflow data to signal a stop
        self.run(WorkflowData {
            id: Uuid::nil(),
            name: "".to_string(),
            correlation_id: "".to_string(),
            status: WorkflowStatus::active(),
            created_at: Utc::now(),
            context: serde_json::Value::Null,
        });
        for _ in 0..1000 {
            if self.stop_state.load(Ordering::SeqCst) == 2 {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
        bail!("Runner thread did not stop")
    }
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

#[cfg(test)]
pub mod tests {
    use crate::{MockWorkflow, MockWorkflowFactory, WorkflowError, WorkflowFactory};
    use std::collections::HashMap;
    use uuid::Uuid;

    use super::*;
    use crate::{registry::SimpleWorkflowRegistry, state::tests::create_test_state};

    #[test]
    fn test_runner_ok() {
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
            .create_workflow(wf_name.clone(), wf_id, corr_id, wf_context)
            .unwrap();
        let runner = AsyncWorkflowRunner::new(state.clone(), Arc::new(registry));

        let result = runner
            .run(WorkflowData {
                id: wf_id,
                name: wf_name,
                correlation_id: "test".to_string(),
                status: WorkflowStatus::active(),
                created_at: Utc::now(),
                context: serde_json::Value::String("test".to_string()),
            })
            .unwrap();
        assert!(matches!(result, WorkflowStatus::Completed));
        wait_for_workflow_to_complete(wf_id, state, Duration::from_secs(3)).unwrap();
        runner.stop().unwrap();
    }

    #[test]
    fn test_runner_when_execution_panics() {
        let wf_id = Uuid::new_v4();
        let wf_name = "test".to_string();
        let corr_id = "123".to_string();
        let wf_context = serde_json::Value::String("test".to_string());

        let mut factory = MockWorkflowFactory::new();
        factory.expect_create().return_once(move |_, _, _, _| {
            panic!("something failed");
        });

        let mut factories: HashMap<String, Arc<dyn WorkflowFactory>> = HashMap::new();
        factories.insert(wf_name.clone(), Arc::new(factory));
        let registry = SimpleWorkflowRegistry::new(factories);
        let state = create_test_state();
        state
            .store
            .create_workflow(wf_name.clone(), wf_id, corr_id, wf_context)
            .unwrap();
        let runner = AsyncWorkflowRunner::new(state, Arc::new(registry));

        let result = runner.run(WorkflowData {
            id: wf_id,
            name: wf_name,
            correlation_id: "test".to_string(),
            status: WorkflowStatus::active(),
            created_at: Utc::now(),
            context: serde_json::Value::String("test".to_string()),
        });
        assert!(result.is_err());
        runner.stop().unwrap();
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
}
