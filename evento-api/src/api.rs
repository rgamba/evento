use crate::poller::Poller;
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
use uuid::Uuid;

lazy_static! {
    static ref INFINITE_WAIT: DateTime<Utc> =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(i64::MAX, 0), Utc);
}

/// WorkflowFacade is the public interface to the workflow engine.
#[derive(Clone)]
pub struct WorkflowFacade {
    workflow_registry: Arc<dyn WorkflowRegistry>,
    operation_executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
    state: State,
    poller: Poller,
}

impl WorkflowFacade {
    pub fn new(
        state: State,
        workflow_registry: Arc<dyn WorkflowRegistry>,
        operation_executor: Arc<dyn OperationExecutor>,
        workflow_runner: Arc<dyn WorkflowRunner>,
    ) -> Self {
        let poller = Poller::start_polling(
            state.clone(),
            operation_executor.clone(),
            workflow_runner.clone(),
        );
        Self {
            workflow_registry,
            workflow_runner,
            state,
            operation_executor,
            poller,
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
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<WorkflowData> {
        let workflow_id = Uuid::new_v4();
        let wf_data = self.state.store.create_workflow(
            workflow_name.clone(),
            workflow_id,
            correlation_id.clone(),
            context.clone(),
        )?;
        self.workflow_runner
            .run(WorkflowData {
                id: workflow_id,
                name: workflow_name,
                correlation_id,
                status: WorkflowStatus::Created,
                created_at: Utc::now(),
                context,
            })
            //TODO: in case of error, we'll end up having a zombie workflow, figure out how to fix.
            .map_err(|err| format_err!("{:?}", err))?;
        Ok(wf_data)
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
        let operation_data = self
            .state
            .store
            .find_wait_operation(external_key)?
            .ok_or_else(|| format_err!("Invalid external key provided"))
            .map_err(|err| {
                log::error!(
                    "Operation validation failed. external_key={}, payload={:?}, error={:?}",
                    external_key,
                    external_input_payload,
                    err
                );
                format_err!("{:?}", err)
            })?;
        self.operation_executor
            .validate_external_input(
                operation_data.input.operation_name.clone(),
                external_input_payload.clone(),
            )
            .map_err(|err| {
                log::error!(
                    "Failed to validate external input payload. external_key={}, error={:?}",
                    external_key,
                    err
                );
                format_err!("{:?}", err)
            })?;
        self.state
            .store
            .complete_external_operation(external_key, external_input_payload)
            .map_err(|err| {
                log::error!(
                    "Failed to complete external operation. external_key={}, error={:?}",
                    external_key,
                    err
                );
                format_err!("{:?}", err)
            })?;
        log::info!("Completed external operation with key={}", external_key);
        Ok(())
    }

    /// Returns the workflow data associated to the workflow Id if present, if not
    /// present it returns a [None].
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn get_workflow_by_id(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        self.state.store.get_workflow(workflow_id).map_err(|err| {
            log::error!("Failed to get workflow. error={:?}", err);
            format_err!("{:?}", err)
        })
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
        _workflow_name: WorkflowName,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        self.state
            .store
            .get_workflow_by_correlation_id(correlation_id)
            .map_err(|err| {
                log::error!("Failed to get workflow by correlation id. error={:?}", err);
                format_err!("{:?}", err)
            })
    }

    /// Returns the successful operation execution results.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>> {
        self.state
            .store
            .get_operation_results(workflow_id)
            .map_err(|err| {
                log::error!("Failed to get operation results. error={:?}", err);
                format_err!("{:?}", err)
            })
    }

    pub fn remove_operation_result(
        &self,
        _workflow_id: WorkflowId,
        _operation_name: OperationName,
        _iteration: OperationIteration,
    ) -> Result<OperationResult> {
        unimplemented!()
    }

    /// Schedule a workflow to be ran as soon as possible.
    /// This function won't block until execution, actual execution will be done asynchronously.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn run_async(&self, _workflow_id: WorkflowId) -> Result<()> {
        unimplemented!()
    }

    /// Cancel an active workflow.
    /// If the workflow is in any of the completed stages, this will be a no-op.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID    
    pub fn cancel_workflow(&self, _workflow_id: WorkflowId) -> Result<WorkflowData> {
        unimplemented!()
    }

    pub fn stop(&self) -> Result<()> {
        self.poller.stop_polling()
    }
}
