use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use log::{error, info, warn};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use thread::JoinHandle;

use crate::{
    state::{OperationExecutionData, State},
    OperationExecutor, OperationResult, WorkflowData, WorkflowError, WorkflowErrorType,
    WorkflowRunner, WorkflowStatus,
};

lazy_static! {
    static ref STOP_POLLING: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

static MAX_RETRIES: u64 = 10;

pub fn start_polling(
    state: State,
    executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
) -> JoinHandle<()> {
    let state_clone = state.clone();
    let executor_clone = executor.clone();
    let runner_clone = workflow_runner.clone();
    thread::spawn(move || {
        while !STOP_POLLING.load(Ordering::SeqCst) {
            info!("Starting poller thread");
            // Main loop - in case the poller thread crashes, we can restart and we don't block the main thread.
            let another_state_clone = state_clone.clone();
            let another_executor_clone = executor_clone.clone();
            let another_runner_clone = runner_clone.clone();
            let handle = thread::spawn(move || {
                // Poller loop thread
                while !STOP_POLLING.load(Ordering::SeqCst) {
                    poll(
                        another_state_clone.clone(),
                        another_executor_clone.clone(),
                        another_runner_clone.clone(),
                    );
                }
            });
            if let Err(err) = handle.join() {
                error!("Poller thread panicked: {:?}", err);
                thread::sleep(Duration::from_secs(1));
            } else {
                error!("Polling has stopped");
            }
        }
    })
}

pub fn stop_polling() {
    STOP_POLLING.store(true, Ordering::SeqCst);
}

fn poll(
    state: State,
    executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
) {
    match state.store.fetch_operations(Utc::now()) {
        Ok(operations) => {
            if operations.is_empty() {
                thread::sleep(Duration::from_millis(1000));
                return;
            }
            info!(
                "Fetched {} new operations. operations={:?}",
                operations.len(),
                operations
            );
            for operation in operations {
                match executor.execute(operation.input.clone()) {
                    Ok(operation_result) => {
                        handle_execution_success(
                            state.clone(),
                            operation.clone(),
                            operation_result,
                            workflow_runner.clone(),
                        );
                    }
                    Err(error) => handle_execution_failure(state.clone(), operation.clone(), error),
                }
            }
        }
        Err(err) => {
            error!(
                "Unexpected error when trying to fetch operations from store: error={:?}",
                err
            );
            thread::sleep(Duration::from_millis(1000));
        }
    }
}

fn handle_execution_success(
    state: State,
    data: OperationExecutionData,
    operation_result: OperationResult,
    workflow_runner: Arc<dyn WorkflowRunner>,
) {
    let workflow_data = match state.store.get_workflow(data.workflow_id) {
        // This is OK, the operation will be retried.
        Err(_) | Ok(None) => {
            log::error!("Unable to get workflow data in order to complete successful execution");
            return;
        }
        Ok(Some(data)) => data,
    };
    if let Err(e) = state.store.store_execution_result(
        data.workflow_id,
        data.input.operation_name.clone(),
        Ok(operation_result),
    ) {
        // This is OK, the operation will be retried.
        log::error!("Unable to store execution result: {:?}", e);
        return;
    }
    // Run the workflow in order to get next inputs.
    if let Err(e) = workflow_runner.run(workflow_data) {
        if let WorkflowErrorType::InternalError = e.error_type {
            // This is OK, the operation will be retried. All other error types are domain
            // specific and can be safely ignored.
            log::error!("Workflow run returned an unexpected result: {:?}", e);
            return;
        }
    }
    if let Err(e) = state.store.dequeue_operation(
        data.workflow_id,
        data.input.operation_name.clone(),
        data.input.iteration,
    ) {
        // This is still OK, the operation will be retried.
        log::error!("Unable to dequeue operation: {:?}", e);
        return;
    }
}

fn handle_execution_failure(state: State, data: OperationExecutionData, error: WorkflowError) {
    info!("Operation execution failed. data={:?}", data);
    let count = data.retry_count.unwrap();
    let new_run_date = get_new_retry_date(count as u64);
    if count >= MAX_RETRIES as usize || !error.is_retriable {
        let wf_error = if !error.is_retriable {
            WorkflowError {
                is_retriable: false,
                error_type: WorkflowErrorType::OperationExecutionError,
                error: format!(
                    "operation execution reached the maximum number of retries. error={}",
                    error.error,
                ),
            }
        } else {
            error
        };
        if let Err(e) = state.store.store_execution_result(
            data.workflow_id,
            data.input.operation_name.clone(),
            Err(wf_error.clone()),
        ) {
            // This is OK, the operation will be retried.
            log::error!("Unable to store execution result: {:?}", e);
            return;
        }
        if let Err(e) = state.store.dequeue_operation(
            data.workflow_id,
            data.input.operation_name.clone(),
            data.input.iteration,
        ) {
            // This is still OK, the operation will be retried.
            log::error!("Unable to dequeue operation: {:?}", e);
            return;
        }
        if let Err(e) = state
            .store
            .abort_workflow_with_error(data.workflow_id, wf_error.clone())
        {
            // This will leave the operation in an inconsistent state.
            //TODO:raise alert
            log::error!("Unable to abort workflow: {:?}", e);
        }
    } else {
        if let Err(e) = state.store.store_execution_result(
            data.workflow_id,
            data.input.operation_name.clone(),
            Err(error),
        ) {
            // This is OK, the operation will be retried.
            log::error!("Unable to store execution result: {:?}", e);
            return;
        }
        if let Err(e) = state.store.queue_operation(data.clone(), new_run_date) {
            // This is a recoverable scenario, operation will be fetched again shortly and executed again.
            log::error!(
                "Unable to re-queue the failed operation. workflow_id={}, error={:?}",
                data.workflow_id,
                e
            );
        }
    }
}

fn get_new_retry_date(retry_count: u64) -> DateTime<Utc> {
    Utc::now()
        .checked_add_signed(chrono::Duration::seconds(1))
        .unwrap()
}

#[cfg(test)]
mod test {
    use crate::{
        MockOperationExecutor, MockWorkflowRunner, OperationInput, OperationResult, WorkflowId,
    };
    use std::collections::HashMap;

    use crate::{registry::SimpleOperationExecutor, state::tests::create_test_state};

    use super::*;
    use crate::runners::tests::wait_for_workflow_to_complete;
    use mockall::predicate::eq;
    use uuid::Uuid;

    #[test]
    fn test_poller_happy_path() {
        let operation_input = OperationInput::new(
            "test".to_string(),
            "operation_test".to_string(),
            1,
            serde_json::Value::String("testinput".to_string()),
        )
        .unwrap();
        let wf_id = Uuid::new_v4();
        let expected_result =
            OperationResult::new(String::from("Good"), 0, String::from("operation_test")).unwrap();
        let result_clone = expected_result.clone();
        let mut workflow_runner = MockWorkflowRunner::new();
        workflow_runner
            .expect_run()
            .times(1)
            .returning(|_| Ok(WorkflowStatus::Completed));
        let mut executor = MockOperationExecutor::new();
        executor
            .expect_execute()
            .with(eq(operation_input.clone()))
            .return_once(move |_| Ok(result_clone));
        let state = create_test_state();
        start_polling(state.clone(), Arc::new(executor), Arc::new(workflow_runner));
        state
            .store
            .create_workflow(
                "test".to_string(),
                wf_id,
                String::new(),
                serde_json::Value::Null,
            )
            .unwrap();
        state
            .store
            .queue_operation(
                OperationExecutionData {
                    workflow_id: wf_id,
                    correlation_id: "".to_string(),
                    retry_count: None,
                    input: operation_input.clone(),
                },
                Utc::now(),
            )
            .unwrap();
        wait_until_operations_queue_is_empty(state.clone());
        let results = state.store.get_operation_results(wf_id).unwrap();
        assert_eq!(1, results.len());
        assert_eq!(&expected_result, results.get(0).unwrap());
        stop_polling();
    }

    #[test]
    fn test_poller_when_operation_execution_fails_with_retriable_error() {
        let operation_input = OperationInput::new(
            "test".to_string(),
            "operation_test".to_string(),
            1,
            serde_json::Value::String("testinput".to_string()),
        )
        .unwrap();
        let wf_id = Uuid::new_v4();
        let mut workflow_runner = MockWorkflowRunner::new();
        workflow_runner
            .expect_run()
            .times(0)
            .returning(|_| Ok(WorkflowStatus::Completed));
        let mut executor = MockOperationExecutor::new();
        executor
            .expect_execute()
            .with(eq(operation_input.clone()))
            .returning(|_| {
                Err(WorkflowError {
                    error: "test error".to_string(),
                    is_retriable: true,
                    error_type: WorkflowErrorType::InternalError,
                })
            });
        let state = create_test_state();
        start_polling(state.clone(), Arc::new(executor), Arc::new(workflow_runner));

        state
            .store
            .queue_operation(
                OperationExecutionData {
                    workflow_id: wf_id,
                    correlation_id: "".to_string(),
                    retry_count: None,
                    input: operation_input.clone(),
                },
                Utc::now(),
            )
            .unwrap();
        wait_until_operations_queue_is_empty(state.clone());
        let results = state
            .store
            .get_operation_results_with_errors(wf_id)
            .unwrap();
        assert_eq!(10, results.len());
        stop_polling();
    }

    fn wait_until_operations_queue_is_empty(state: State) {
        for i in 1..100 {
            match state.store.fetch_operations(Utc::now()) {
                Ok(operations) => {
                    if operations.is_empty() {
                        return;
                    }
                }
                _ => {}
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}
