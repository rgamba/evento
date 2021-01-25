use crate::poller::start_polling;
use crate::{
    state::State, CorrelationId, ExternalInputKey, OperationIteration, OperationName,
    OperationResult, WorkflowContext, WorkflowData, WorkflowId, WorkflowName, WorkflowRunner,
    WorkflowStatus,
};
use crate::{OperationExecutor, WorkflowRegistry};
use anyhow::{format_err, Result};
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
    fn new(
        state: State,
        workflow_registry: Arc<dyn WorkflowRegistry>,
        operation_executor: Arc<dyn OperationExecutor>,
        workflow_runner: Arc<dyn WorkflowRunner>,
    ) -> Self {
        start_polling(
            state.clone(),
            operation_executor.clone(),
            workflow_runner.clone(),
        );
        Self {
            workflow_registry,
            workflow_runner,
            state,
            operation_executor,
        }
    }

    /// Creates a new workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_name` - The name of the workflow. This must be unique within the registry.
    /// * `workflow_id` - The workflow ID. Must be universally unique.
    /// * `correlation_id` - A correlation ID or secondary index.
    /// * `context` - The workflow context to inject to the workflow.
    pub fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<()> {
        self.state.store.create_workflow(
            workflow_name.clone(),
            workflow_id,
            correlation_id.clone(),
            context.clone(),
        );
        self.workflow_runner
            .run(WorkflowData {
                id: workflow_id,
                name: workflow_name,
                correlation_id,
                status: WorkflowStatus::Created,
                created_at: Utc::now(),
                context,
            })
            .map_err(|err| format_err!("{:?}", err))?;
        Ok(())
    }

    /// Completes an external wait activity for a workflow.
    ///
    /// # Arguments
    ///
    /// * `external_key` - The external key the uniquely identifies the workflow activity to be completed.
    /// * `external_input_payload` - The data to be used to complete the activity.
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

    /// Returns the workflow data associated to the workflow Id if present, if not
    /// present it returns a [None].
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn get_workflow_by_id(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        self.state.store.get_workflow(workflow_id)
    }

    /// Returns the workflow data associated to the workflow.
    /// Same as [get_workflow_by_id] but gets the workflow based on the correlation ID.
    ///
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn get_workflow_by_correlation_id(
        &self,
        workflow_name: WorkflowName,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        unimplemented!()
    }

    /// Returns the successful operation execution results.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
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
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn run_async(&self, workflow_id: WorkflowId) -> Result<()> {
        unimplemented!()
    }

    /// Cancel an active workflow.
    /// If the workflow is in any of the completed stages, this will be a no-op.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID    
    pub fn cancel_workflow(&self, workflow_id: WorkflowId) -> Result<WorkflowData> {
        unimplemented!()
    }
}
