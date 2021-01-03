use crate::registry::{OperationExecutor, WorkflowRegistry};
use crate::state::State;
use crate::{
    CorrelationId, ExternalInputKey, OperationIteration, OperationName, OperationResult, Workflow,
    WorkflowContext, WorkflowData, WorkflowId, WorkflowName, WorkflowStatus,
};
use anyhow::Result;
use std::sync::Arc;
use uuid::Uuid;

pub struct Engine {
    workflow_registry: Arc<dyn WorkflowRegistry>,
    operation_executor: Arc<dyn OperationExecutor>,
    state: State,
}

impl Engine {
    pub fn create_workflow(
        &self,
        correlation_id: CorrelationId,
        workflow_name: WorkflowName,
        context: WorkflowContext,
    ) -> Result<WorkflowName> {
        unimplemented!()
    }

    pub fn run(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
    ) -> Result<WorkflowStatus> {
        unimplemented!()
    }

    pub fn complete_external(&self, correlation_id: ExternalInputKey) -> Result<()> {
        unimplemented!()
    }

    pub fn get_workflow_by_id(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        unimplemented!()
    }

    pub fn get_workflow_by_correlation_id(
        &self,
        workflow_name: WorkflowName,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        unimplemented!()
    }

    pub fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>> {
        unimplemented!()
    }

    pub fn remove_operation_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: OperationIteration,
    ) -> Result<OperationResult> {
        unimplemented!()
    }
}
