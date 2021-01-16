use crate::{
    state::State, CorrelationId, ExternalInputKey, OperationIteration, OperationName,
    OperationResult, WorkflowContext, WorkflowData, WorkflowId, WorkflowName, WorkflowRunner,
};
use crate::{OperationExecutor, WorkflowRegistry};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(i64::MAX, 0), Utc);
}

/// WorkflowFacade is the public interface to the workflow engine.
pub struct WorkflowFacade {
    workflow_registry: Arc<dyn WorkflowRegistry>,
    operation_executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
    state: State,
}

impl WorkflowFacade {
    pub fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<()> {
        self.state
            .store
            .create_workflow(workflow_name, workflow_id, correlation_id, context)
    }

    pub fn complete_external(
        &self,
        external_key: ExternalInputKey,
        external_input_payload: serde_json::Value,
    ) -> Result<()> {
        self.state
            .store
            .complete_external_operation(external_key, external_input_payload)?;
        Ok(())
    }

    pub fn get_workflow_by_id(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        self.state.store.get_workflow(workflow_id)
    }

    pub fn get_workflow_by_correlation_id(
        &self,
        workflow_name: WorkflowName,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        unimplemented!()
    }

    pub fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>> {
        self.state.store.get_operation_results(workflow_id)
    }

    pub fn remove_operation_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: OperationIteration,
    ) -> Result<OperationResult> {
        unimplemented!()
    }

    /// Schedule a workflow to be ran as soon as possible.
    /// This function won't block until execution, actual execution will be done asynchronously.
    pub fn run_async(&self, workflow_id: WorkflowId) -> Result<()> {
        unimplemented!()
    }

    /// Cancel an active workflow.
    /// If the workflow is in any of the completed stages, this will be a no-op.
    ///
    pub fn cancel_workflow(&self, workflow_id: WorkflowId) -> Result<WorkflowData> {
        unimplemented!()
    }
}
