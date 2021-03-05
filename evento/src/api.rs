use crate::poller::{ExponentialBackoffRetryStrategy, Poller};
use crate::registry::{SimpleOperationExecutorBuilder, SimpleWorkflowRegistryBuilder};
use crate::runners::AsyncWorkflowRunner;
use crate::state::{Store, WorkflowFilter};
use crate::{
    state::State, CorrelationId, ExternalInputKey, Operation, OperationIteration, OperationName,
    OperationResult, WorkflowContext, WorkflowData, WorkflowFactory, WorkflowId, WorkflowName,
    WorkflowRunner, WorkflowStatus,
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

pub struct EventoBuilder {
    executor_builder: SimpleOperationExecutorBuilder,
    registry_builder: SimpleWorkflowRegistryBuilder,
    store: Arc<dyn Store>,
}

impl EventoBuilder {
    pub fn new(store: impl Store + 'static) -> Self {
        Self {
            executor_builder: SimpleOperationExecutorBuilder::default(),
            registry_builder: SimpleWorkflowRegistryBuilder::default(),
            store: Arc::new(store),
        }
    }

    pub fn register_workflow(&mut self, factory: impl WorkflowFactory + 'static) -> &mut Self {
        self.registry_builder.add_factory(factory);
        self
    }

    pub fn register_operation(&mut self, operation: impl Operation + 'static) -> &mut Self {
        self.executor_builder.add(operation);
        self
    }

    pub fn build(&self) -> Evento {
        let registry = self.registry_builder.build();
        let state = State {
            store: self.store.clone(),
        };
        Evento::new(
            state.clone(),
            registry.clone(),
            self.executor_builder.build(),
            Arc::new(AsyncWorkflowRunner::new(state.clone(), registry.clone())),
        )
    }
}

/// WorkflowFacade is the public interface to the workflow engine.
#[derive(Clone)]
pub struct Evento {
    workflow_registry: Arc<dyn WorkflowRegistry>,
    operation_executor: Arc<dyn OperationExecutor>,
    workflow_runner: Arc<dyn WorkflowRunner>,
    state: State,
    poller: Poller,
}

impl Evento {
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
            Arc::new(ExponentialBackoffRetryStrategy::new(10)),
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
                status: WorkflowStatus::active(),
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
                operation_data.input.operation_name,
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

    /// Returns the execution traces for all operation executions, including both successful and
    /// failed executions.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn get_operation_execution_traces(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<OperationResult>> {
        self.state
            .store
            .get_operation_results_with_errors(workflow_id)
            .map_err(|err| {
                log::error!("Failed to get operation results. error={:?}", err);
                format_err!("{:?}", err)
            })
    }

    /// Replay the workflow starting from the given operation.
    ///
    /// This is a non-reversible operation, it will effectively delete ALL operation results
    /// starting from the given operation onwards and will schedule the workflow
    /// to be run as soon as possible.
    pub fn replay(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: OperationIteration,
    ) -> Result<()> {
        self.state
            .store
            .delete_operation_results(workflow_id, operation_name, iteration)
    }

    /// Retry the workflow from the latest successful step.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID
    pub fn retry(&self, workflow_id: WorkflowId) -> Result<()> {
        let workflow = self
            .state
            .store
            .get_workflow(workflow_id)?
            .ok_or_else(|| format_err!("Invalid workflow ID"))?;
        if !workflow.status.is_active() {
            self.state
                .store
                .mark_active(workflow_id, vec![])
                .map_err(|e| {
                    log::error!("Unable to mark the workflow as active: {:?}", e);
                    format_err!("Workflow run returned the following error: {:?}", e)
                })?;
        }
        self.workflow_runner
            .run(workflow)
            .map_err(|e| {
                log::error!("Error while running the workflow: {:?}", e);
                format_err!("Workflow run returned the following error: {:?}", e)
            })
            .map(|_| ())
    }

    /// Cancel an active workflow.
    /// If the workflow is in any of the completed stages, this will be a no-op.
    ///
    /// # Arguments
    ///
    /// - `workflow_id` - The workflow ID    
    pub fn cancel_workflow(&self, workflow_id: WorkflowId, reason: String) -> Result<()> {
        self.state
            .store
            .cancel_workflow(workflow_id, reason)
            .map_err(|err| {
                log::error!("Failed to get operation results. error={:?}", err);
                format_err!("Unable to cancel workflow: {:?}", err)
            })
            .map(|_| ())
    }

    /// Returns a list of workflows matching the filters.
    ///
    /// # Arguments
    ///
    /// - `filters` - The filters to use.
    pub fn get_workflows(&self, filters: WorkflowFilter) -> Result<Vec<WorkflowData>> {
        self.state.store.get_workflows(filters)
    }

    pub fn stop(&self) -> Result<()> {
        self.poller.stop_polling()?;
        self.workflow_runner.stop()
    }
}
