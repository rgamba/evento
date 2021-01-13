use crate::registry::{OperationExecutor, WorkflowRegistry};
use crate::state::{OperationExecutionData, State};
use crate::{
    CorrelationId, ExternalInputKey, OperationIteration, OperationName, OperationResult, Workflow,
    WorkflowContext, WorkflowData, WorkflowError, WorkflowId, WorkflowName, WorkflowStatus,
};
use anyhow::{format_err, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use std::sync::Arc;
use uuid::Uuid;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(i64::MAX, 0), Utc);
}

pub struct Engine {
    workflow_registry: Arc<dyn WorkflowRegistry>,
    operation_executor: Arc<dyn OperationExecutor>,
    state: State,
}

impl Engine {
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

    /// Create a workflow instance and run it synchronously with the available
    /// execution results.
    pub fn run(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
    ) -> Result<WorkflowStatus, WorkflowError> {
        let workflow_data = self.get_workflow_by_id(workflow_id)?.ok_or(format_err!(
            "Unable to find workflow with id {}",
            workflow_id
        ))?;
        let operation_results = self.get_operation_results(workflow_id)?;
        let workflow = self.workflow_registry.create_workflow(
            workflow_name,
            workflow_id,
            workflow_data.correlation_id,
            workflow_data.context.clone(),
            operation_results,
        )?;
        let result = workflow.run();
        match &result {
            Ok(WorkflowStatus::Completed) => {
                self.state.store.complete_workflow(workflow_id)?;
            }
            Ok(WorkflowStatus::Error(error)) => {
                self.state
                    .store
                    .abort_workflow_with_error(workflow_id, error)?;
            }
            Ok(WorkflowStatus::CompletedWithError(error)) => {
                self.state
                    .store
                    .complete_workflow_with_error(workflow_id, error.error.to_string())?;
            }
            Ok(WorkflowStatus::RunNext(inputs)) => {
                self.state.store.queue_all_operations(
                    inputs
                        .iter()
                        .map(|input| {
                            (
                                OperationExecutionData {
                                    workflow_id,
                                    retry_count: None,
                                    input: input.clone(),
                                },
                                Utc::now(),
                            )
                        })
                        .collect(),
                )?;
            }
            Ok(WorkflowStatus::WaitForExternal((input, timeout))) => {
                self.state.store.queue_operation(
                    OperationExecutionData {
                        workflow_id,
                        retry_count: None,
                        input: input.clone(),
                    },
                    timeout.map_or(*INFINITE_WAIT, |t| t),
                )?;
            }
            Ok(_) => {
                // All others are a no-op
            }
            Err(workflow_error) => {
                // Errors raised from workflow execution should not be expected and are
                // not retriable, hence we'll abort the workflow.
                self.state
                    .store
                    .abort_workflow_with_error(workflow_id, workflow_error)?;
            }
        }
        result
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
