use crate::registry::ExternalWorkflows;
use crate::state::{OperationExecutionData, State};
use anyhow::Result;
use chrono::{DateTime, Utc};
use evento_api::{Operation, Workflow, WorkflowError, WorkflowStatus};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct WorkflowEngine {
    state: Arc<State>,
    registry: ExternalWorkflows,
    retry_strategy: Box<dyn RetryStrategy>,
}

impl WorkflowEngine {
    pub async fn execute(&self, data: OperationExecutionData) -> Result<()> {
        let mut workflow = self.instantiate_workflow(&data.workflow_name, &data.workflow_id)?;
        match workflow.run() {
            Ok(WorkflowStatus::Completed) => {
                self.state.store.mark_completed(&data.workflow_id).await?;
            }
            Ok(WorkflowStatus::RunNext(next_operations)) => {
                self.state
                    .store
                    .queue_operations(
                        next_operations
                            .into_iter()
                            .map(|input| OperationExecutionData {
                                workflow_id: data.workflow_id,
                                retry_count: 0,
                                input,
                            })
                            .collect(),
                    )
                    .await?;
            }
            Err(error) => {
                if error.is_retriable {
                    self.handle_retry(data, error).await?;
                } else {
                    self.state
                        .store
                        .mark_errored(data.workflow_id, error)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn instantiate_workflow(
        &self,
        workflow_name: &str,
        workflow_id: &Uuid,
    ) -> Result<Box<dyn Workflow>> {
        let operation_results = self.state.store.get_operation_results(workflow_id).await?;
        Ok(self
            .registry
            .create_workflow(workflow_name, workflow_id.clone(), operation_results)?)
    }

    async fn handle_retry(&self, data: OperationExecutionData, error: WorkflowError) -> Result<()> {
        let run_date = self
            .retry_strategy
            .get_next_retry_date(data.retry_count, Utc::now());
        self.state.store.queue_operation(data, run_date).await?;
        Ok(())
    }
}

pub trait RetryStrategy {
    /// Given the queue element, get the next retry date based on the schedule.
    fn get_next_retry_date(&self, retries: usize, base_date: DateTime<Utc>) -> DateTime<Utc>;
}

// Simple exponential backoff variation that produces deterministic
// retry schedules.
pub struct ExponentialBackoffRetryStrategy {
    retry_interval_secs: u64,
}

impl Default for ExponentialBackoffRetryStrategy {
    fn default() -> Self {
        Self {
            retry_interval_secs: 30,
        }
    }
}

impl RetryStrategy for ExponentialBackoffRetryStrategy {
    fn get_next_retry_date(&self, retries: usize, base_date: DateTime<Utc>) -> DateTime<Utc> {
        let base = self.retry_interval_secs as i64;
        let multiplier: i64 = 2_i64.checked_pow(retries as u32).unwrap_or(i64::MAX);
        let secs: i64 = base.checked_mul(multiplier).unwrap_or(i64::MAX);
        base_date
            .next_run_at
            .checked_add_signed(chrono::Duration::seconds(secs))
            .unwrap_or(queue_element.next_run_at)
    }
}
