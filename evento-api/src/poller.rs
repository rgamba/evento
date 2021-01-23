use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
#[cfg(not(test))]
use log::{error, info, warn};
#[cfg(test)]
use std::{println as info, println as warn, println as error};
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
    OperationExecutor, OperationResult, WorkflowError,
};

lazy_static! {
    static ref STOP_POLLING: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

static MAX_RETRIES: u64 = 10;

pub fn start_polling(state: State, executor: Arc<dyn OperationExecutor>) -> JoinHandle<()> {
    let state_clone = state.clone();
    let executor_clone = executor.clone();
    thread::spawn(move || {
        while !STOP_POLLING.load(Ordering::SeqCst) {
            info!("Starting poller thread");
            // Main loop - in case the poller thread crashes, we can restart and we don't block the main thread.
            let another_state_clone = state_clone.clone();
            let another_executor_clone = executor_clone.clone();
            let handle = thread::spawn(move || {
                // Poller loop thread
                while !STOP_POLLING.load(Ordering::SeqCst) {
                    poll(another_state_clone.clone(), another_executor_clone.clone());
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

fn poll(state: State, executor: Arc<dyn OperationExecutor>) {
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
) {
    if let Err(e) = state.store.store_execution_result(
        data.workflow_id,
        data.input.operation_name.clone(),
        Ok(operation_result),
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
    if let Err(e) = state.store.complete_workflow(data.workflow_id) {
        // This will leave the operation in an inconsistent state.
        //TODO:raise alert
        log::error!(
            "Unable to mark workflow as completed: workflow_id={}",
            data.workflow_id
        );
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
    use crate::{MockOperationExecutor, OperationInput, OperationResult, WorkflowId};
    use std::collections::HashMap;

    use crate::{registry::SimpleOperationExecutor, state::tests::create_test_state};

    use super::*;
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
        let mut executor = MockOperationExecutor::new();
        executor
            .expect_execute()
            .with(eq(operation_input.clone()))
            .return_once(move |_| Ok(result_clone));
        let state = create_test_state();
        start_polling(state.clone(), Arc::new(executor));

        state
            .store
            .queue_operation(
                OperationExecutionData {
                    workflow_id: wf_id,
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
        let mut executor = MockOperationExecutor::new();
        executor
            .expect_execute()
            .with(eq(operation_input.clone()))
            .returning(|_| {
                Err(WorkflowError {
                    error: "test error".to_string(),
                    is_retriable: true,
                })
            });
        let state = create_test_state();
        start_polling(state.clone(), Arc::new(executor));

        state
            .store
            .queue_operation(
                OperationExecutionData {
                    workflow_id: wf_id,
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
