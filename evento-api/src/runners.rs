use std::sync::Arc;

use crate::{
    state::{OperationExecutionData, State},
    WorkflowRegistry, WorkflowRunner, WorkflowStatus,
};
use anyhow::format_err;
use chrono::{DateTime, NaiveDateTime, Utc};
use lazy_static::lazy_static;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(i64::MAX, 0), Utc);
}

/// Workflow runner implementation that tries to run the workflow definition
/// by injecting the available operation executions into the workflow.
/// If any operation or wait has not been completed, it delegates the execution and
/// stops. It relies on another component to actually execute the operation asynchronously
/// and populate the execution results.
struct AsyncWorkflowRunner {
    state: State,
    workflow_registry: Arc<dyn WorkflowRegistry>,
}

impl WorkflowRunner for AsyncWorkflowRunner {
    fn run(
        &self,
        workflow_data: crate::WorkflowData,
    ) -> anyhow::Result<crate::WorkflowStatus, crate::WorkflowError> {
        let workflow_data = self
            .state
            .store
            .get_workflow(workflow_data.id)?
            .ok_or(format_err!(
                "Unable to find workflow with id {}",
                workflow_data.id,
            ))?;
        let operation_results = self.state.store.get_operation_results(workflow_data.id)?;
        let workflow = self.workflow_registry.create_workflow(
            workflow_data.name.clone(),
            workflow_data.id,
            workflow_data.correlation_id.clone(),
            workflow_data.context.clone(),
            operation_results,
        )?;
        let result = workflow.run();
        match &result {
            Ok(WorkflowStatus::Completed) => {
                self.state.store.complete_workflow(workflow_data.id)?;
            }
            Ok(WorkflowStatus::Error(error)) => {
                self.state
                    .store
                    .abort_workflow_with_error(workflow_data.id, error)?;
            }
            Ok(WorkflowStatus::CompletedWithError(error)) => {
                self.state
                    .store
                    .complete_workflow_with_error(workflow_data.id, error.error.to_string())?;
            }
            Ok(WorkflowStatus::RunNext(inputs)) => {
                self.state.store.queue_all_operations(
                    inputs
                        .iter()
                        .map(|input| {
                            (
                                OperationExecutionData {
                                    workflow_id: workflow_data.id,
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
                        workflow_id: workflow_data.id,
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
                    .abort_workflow_with_error(workflow_data.id, workflow_error)?;
            }
        }
        result
    }
}
