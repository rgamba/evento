use crate::{
    CorrelationId, Operation, OperationExecutor, OperationInput, OperationName, OperationResult,
    Workflow, WorkflowContext, WorkflowError, WorkflowErrorType, WorkflowFactory, WorkflowId,
    WorkflowName, WorkflowRegistry,
};
use anyhow::{format_err, Result};
use serde_json::Value;
use std::sync::Arc;
use std::{collections::HashMap, thread};

/// Simple registry that maps workflows based on simple string name
pub struct SimpleWorkflowRegistry {
    factories: HashMap<String, Arc<dyn WorkflowFactory>>,
}

impl SimpleWorkflowRegistry {
    pub fn new(factories: HashMap<String, Arc<dyn WorkflowFactory>>) -> Self {
        Self { factories }
    }
}

impl WorkflowRegistry for SimpleWorkflowRegistry {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
        execution_results: Vec<OperationResult>,
    ) -> Result<Box<dyn Workflow>> {
        Ok(self
            .factories
            .get(workflow_name.as_str())
            .ok_or_else(|| {
                format_err!(
                    "Workflow with the name '{}' not found in registry",
                    workflow_name
                )
            })?
            .create(workflow_id, correlation_id, context, execution_results))
    }
}

pub struct SimpleWorkflowRegistryBuilder {
    factories: HashMap<String, Arc<dyn WorkflowFactory>>,
}

impl Default for SimpleWorkflowRegistryBuilder {
    fn default() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
}

impl SimpleWorkflowRegistryBuilder {
    pub fn add_factory(&mut self, factory: impl WorkflowFactory + 'static) -> &mut Self {
        self.factories
            .insert(factory.workflow_name().to_string(), Arc::new(factory));
        self
    }

    pub fn build(&self) -> Arc<SimpleWorkflowRegistry> {
        Arc::new(SimpleWorkflowRegistry::new(self.factories.clone()))
    }
}

/// Simple operation executor.
/// This executor works like an operation router with no retries.
pub struct SimpleOperationExecutor {
    pub operation_map: HashMap<String, Arc<dyn Operation>>,
}

impl SimpleOperationExecutor {
    pub fn new(operation_map: HashMap<String, Arc<dyn Operation>>) -> Self {
        Self { operation_map }
    }

    fn execute_internal(
        operation: Arc<dyn Operation>,
        input: OperationInput,
    ) -> Result<Value, WorkflowError> {
        let handle = thread::spawn(move || operation.execute(input));
        match handle.join() {
            Ok(result) => result,
            Err(panic_msg) => {
                log::error!("Operation execution panicked. error={:?}", panic_msg);
                Err(WorkflowError {
                    is_retriable: false,
                    error: format!("{:?}", panic_msg),
                    error_type: WorkflowErrorType::InternalError,
                })
            }
        }
    }
}

impl OperationExecutor for SimpleOperationExecutor {
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
        let operation = self
            .operation_map
            .get(input.operation_name.as_str())
            .ok_or_else(|| {
                format_err!(
                    "Operation '{}' does not exist in registry!",
                    input.operation_name
                )
            })?
            .clone();
        let result = Self::execute_internal(operation.clone(), input.clone());
        Ok(OperationResult::new(
            result,
            input.iteration,
            operation.name().into(),
            input.clone(),
        )?)
    }

    fn validate_external_input(
        &self,
        operation_name: OperationName,
        external_input: serde_json::Value,
    ) -> Result<()> {
        let operation = self
            .operation_map
            .get(operation_name.as_str())
            .unwrap()
            .clone();
        operation.validate_external_input(external_input)
    }
}

pub struct SimpleOperationExecutorBuilder {
    operation_map: HashMap<String, Arc<dyn Operation>>,
}

impl Default for SimpleOperationExecutorBuilder {
    fn default() -> Self {
        Self {
            operation_map: HashMap::new(),
        }
    }
}

impl SimpleOperationExecutorBuilder {
    pub fn add(&mut self, operation: impl Operation + 'static) -> &mut Self {
        self.operation_map
            .insert(operation.name().to_string(), Arc::new(operation));
        self
    }

    pub fn build(&self) -> Arc<SimpleOperationExecutor> {
        Arc::new(SimpleOperationExecutor::new(self.operation_map.clone()))
    }
}
