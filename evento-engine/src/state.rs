use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use evento_api::{OperationInput, OperationResult, WorkflowError};
use std::alloc::Global;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct State {
    pub store: Arc<dyn Store>,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn get_operation_results(&self, workflow_id: &Uuid) -> Result<Vec<OperationResult>>;

    /// Fetch operations that whose run_date is less or equal to the current now provided.
    async fn fetch_operations(
        &self,
        current_now: DateTime<Utc>,
    ) -> Result<Vec<OperationExecutionData>>;

    async fn store_execution_result(
        &self,
        workflow_id: &str,
        operation_name: &str,
        result: Result<OperationResult, WorkflowError>,
    ) -> Result<()>;

    /// Update the given Workflow and mark it as completed.
    async fn mark_completed(&self, workflow_id: &Uuid) -> Result<()>;

    /// Queue a single operation to be executed at some point in the future.
    /// If the operation already exists, it will be updated and the retry_count will be incremented
    /// otherwise it will be created with a retry_count of 0.
    async fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()>;

    /// Queue all operations in an atomic transaction where this will only succeed if all
    /// operations are queued successfully or else the operation will rollback.
    /// The queuing logic is the same as the `queue_operation` function.
    async fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()>;
}

pub struct OperationExecutionData {
    pub workflow_id: Uuid,
    pub retry_count: usize,
    input: OperationInput,
}

pub struct InMemoryStore {
    pub operation_results: Mutex<HashMap<Uuid, Vec<OperationResult>>>,
    pub queue: Vec<(OperationExecutionData, DateTime<Utc>)>, // (data, run_date)
}

impl Store for InMemoryStore {
    async fn get_operation_results(&self, workflow_id: &Uuid) -> Result<Vec<OperationResult>> {
        if let Some(results) = self.operation_results.get(workflow_id) {
            Ok(results.clone())
        } else {
            Vec::new()
        }
    }

    async fn fetch_operations(
        &self,
        current_now: DateTime<Utc>,
    ) -> Result<Vec<OperationExecutionData>> {
        self.queue
            .iter()
            .filter(|(data, run_date)| current_now.gt(run_date))
            .map(|(data, _)| data.clone())
            .collect()
    }

    async fn store_execution_result(
        &self,
        workflow_id: &str,
        operation_name: &str,
        result: Result<OperationResult, WorkflowError>,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn mark_completed(&self, workflow_id: &Uuid) -> Result<()> {
        unimplemented!()
    }

    async fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()> {
        unimplemented!()
    }
}
