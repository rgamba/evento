use anyhow::{bail, Result};
use chrono::Utc;
use log::{error, info, warn};
use std::{
    sync::{atomic::Ordering, Arc},
    thread,
    time::Duration,
};

use crate::{
    state::{OperationExecutionData, State},
    OperationExecutor, OperationResult, WorkflowError, WorkflowErrorType, WorkflowRunner,
};
use std::sync::atomic::AtomicU8;

#[derive(Clone)]
pub struct Poller {
    stop_polling: Arc<AtomicU8>,
    retry_strategy: Arc<dyn RetryStrategy>,
}

impl Poller {
    pub fn start_polling(
        state: State,
        executor: Arc<dyn OperationExecutor>,
        workflow_runner: Arc<dyn WorkflowRunner>,
        retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        let stop: Arc<AtomicU8> = Arc::new(AtomicU8::new(0));
        let stop_clone = stop.clone();
        let state_clone = state;
        let executor_clone = executor.clone();
        let runner_clone = workflow_runner.clone();
        let retry_strategy_clone = retry_strategy.clone();
        thread::spawn(move || {
            while stop_clone.load(Ordering::SeqCst) == 0 {
                info!("Starting poller thread");
                // Main loop - in case the poller thread crashes, we can restart and we don't block the main thread.
                let another_state_clone = state_clone.clone();
                let another_executor_clone = executor_clone.clone();
                let another_runner_clone = runner_clone.clone();
                let stop_clone_clone = stop_clone.clone();
                let retry_strategy_clone_clone = retry_strategy_clone.clone();
                let handle = thread::spawn(move || {
                    // Poller loop thread
                    while stop_clone_clone.load(Ordering::SeqCst) == 0 {
                        poll(
                            another_state_clone.clone(),
                            another_executor_clone.clone(),
                            another_runner_clone.clone(),
                            retry_strategy_clone_clone.clone(),
                        );
                    }
                });

                if let Err(err) = handle.join() {
                    if let Some(msg) = err.downcast_ref::<String>() {
                        error!("Poller thread panicked: {:?}", msg);
                    } else {
                        error!("Poller thread panicked with unexpected error: {:?}", err);
                    }
                    thread::sleep(Duration::from_secs(1));
                } else {
                    error!("Polling has stopped");
                }
            }
            stop_clone.store(2, Ordering::SeqCst);
        });
        Self {
            stop_polling: stop,
            retry_strategy,
        }
    }

    pub fn stop_polling(&self) -> Result<()> {
        log::info!("Stopping poller");
        self.stop_polling.store(1, Ordering::SeqCst);
        for _ in 0..1000 {
            if self.stop_polling.load(Ordering::SeqCst) == 2 {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
        bail!("Poller thread did not stop")
    }
}

fn poll(
    state: State,
    executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
    retry_strategy: Arc<dyn RetryStrategy>,
) {
    match state.store.fetch_operations(Utc::now()) {
        Ok(operations) => {
            if operations.is_empty() {
                thread::sleep(Duration::from_millis(1000));
                return;
            }
            info!("Fetched {} new operations", operations.len(),);
            log::debug!("Fetched operations: {:?}", operations);
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
                    Err(error) => handle_execution_failure(
                        state.clone(),
                        operation.clone(),
                        error,
                        retry_strategy.clone(),
                    ),
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

fn handle_execution_failure(
    state: State,
    data: OperationExecutionData,
    error: WorkflowError,
    retry_strategy: Arc<dyn RetryStrategy>,
) {
    warn!("Operation execution failed. error={:?}", error);
    let count = data.retry_count.unwrap_or(1);
    let new_run_date = retry_strategy
        .next_retry_interval(count as u64)
        .map(|d| Utc::now().checked_add_signed(d).unwrap());
    if new_run_date.is_none() || !error.is_retriable {
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
        log::warn!(
            "Marking the workflow as aborted! workflow_id={}",
            data.workflow_id
        );
        if let Err(e) = state
            .store
            .abort_workflow_with_error(data.workflow_id, wf_error)
        {
            // This will leave the operation in an inconsistent state.
            //TODO:raise alert
            log::error!("Unable to abort workflow: {:?}", e);
        }
    } else if let Some(new_run_date) = new_run_date {
        if let Err(e) = state.store.store_execution_result(
            data.workflow_id,
            data.input.operation_name.clone(),
            Err(error),
        ) {
            // This is OK, the operation will be retried.
            log::error!("Unable to store execution result: {:?}", e);
            return;
        }
        let mut new_data = data.clone();
        new_data.retry_count = Some(count + 1);
        if let Err(e) = state.store.queue_operation(new_data, new_run_date) {
            // This is a recoverable scenario, operation will be fetched again shortly and executed again.
            log::error!(
                "Unable to re-queue the failed operation. workflow_id={}, error={:?}",
                data.workflow_id,
                e
            );
        }
    }
}

pub trait RetryStrategy: Send + Sync {
    fn next_retry_interval(&self, retry_count: u64) -> Option<chrono::Duration>;
}

pub struct FixedRetryStrategy {
    pub interval: chrono::Duration,
    pub max_retries: u64,
}

impl RetryStrategy for FixedRetryStrategy {
    fn next_retry_interval(&self, retry_count: u64) -> Option<chrono::Duration> {
        if retry_count >= self.max_retries {
            return None;
        }
        Some(self.interval)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        MockOperationExecutor, MockWorkflowRunner, OperationInput, OperationResult, WorkflowStatus,
    };

    use crate::state::tests::create_test_state;

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
        let test_input = OperationInput::new(
            "test".to_string(),
            "test".to_string(),
            0,
            serde_json::Value::Null,
        )
        .unwrap();
        let wf_id = Uuid::new_v4();
        let expected_result = OperationResult::new(
            String::from("Good"),
            0,
            String::from("operation_test"),
            test_input,
        )
        .unwrap();
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
        let poller = Poller::start_polling(
            state.clone(),
            Arc::new(executor),
            Arc::new(workflow_runner),
            Arc::new(FixedRetryStrategy {
                interval: chrono::Duration::seconds(0),
                max_retries: 10,
            }),
        );
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
                    input: operation_input,
                },
                Utc::now(),
            )
            .unwrap();
        wait_until_operations_queue_is_empty(state.clone());
        let results = state.store.get_operation_results(wf_id).unwrap();
        assert_eq!(1, results.len());
        assert_eq!(&expected_result, results.get(0).unwrap());
        poller.stop_polling().unwrap();
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
        let poller = Poller::start_polling(
            state.clone(),
            Arc::new(executor),
            Arc::new(workflow_runner),
            Arc::new(FixedRetryStrategy {
                interval: chrono::Duration::seconds(0),
                max_retries: 10,
            }),
        );

        state
            .store
            .queue_operation(
                OperationExecutionData {
                    workflow_id: wf_id,
                    correlation_id: "".to_string(),
                    retry_count: None,
                    input: operation_input,
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
        poller.stop_polling().unwrap();
    }

    fn wait_until_operations_queue_is_empty(state: State) {
        for _ in 1..100 {
            if let Ok(count) = state.store.count_queued_elements() {
                if count == 0 {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}
