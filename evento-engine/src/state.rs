use anyhow::Result;
use async_trait::async_trait;
use evento_api::{OperationInput, OperationResult, WorkflowError};
use std::sync::Arc;
use uuid::Uuid;

pub struct State {
    store: Arc<dyn Store>,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn get_operation_results(
        &self,
        workflow_id: &Uuid,
        operation_name: &str,
    ) -> Result<Vec<OperationResult>>;

    async fn fetch_pending_operations(&self) -> Result<Vec<OperationExecutionData>>;

    async fn store_execution_result(
        &self,
        workflow_id: &str,
        operation_name: &str,
        result: Result<OperationResult, WorkflowError>,
    ) -> Result<()>;
}

pub struct OperationExecutionData {
    pub workflow_name: String,
    pub workflow_id: Uuid,
    pub operation_name: String,
    input: OperationInput,
}
