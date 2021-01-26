use crate::{
    CorrelationId, Operation, OperationExecutor, OperationInput, OperationResult, Workflow,
    WorkflowContext, WorkflowError, WorkflowErrorType, WorkflowFactory, WorkflowId, WorkflowName,
    WorkflowRegistry,
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
            .unwrap()
            .clone();
        let result = Self::execute_internal(operation.clone(), input.clone());
        match result {
            Ok(res) => Ok(OperationResult::new(
                serde_json::to_value(res).unwrap(),
                input.iteration,
                operation.name().into(),
            )
            .unwrap()),
            Err(err) => Err(err),
        }
    }
}

/// Simple operation executor that routes operations and performs
/// immediate retries in case of errors.
pub struct RetryOperationExecutor {
    pub max_retries: usize,
    simple_operation_executor: SimpleOperationExecutor,
}

impl RetryOperationExecutor {
    pub fn new(operation_map: HashMap<String, Arc<dyn Operation>>, max_retries: usize) -> Self {
        Self {
            max_retries,
            simple_operation_executor: SimpleOperationExecutor::new(operation_map),
        }
    }
}

impl OperationExecutor for RetryOperationExecutor {
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
        let mut retries: usize = 0;
        loop {
            match self.simple_operation_executor.execute(input.clone()) {
                Ok(result) => break Ok(result),
                Err(e) => {
                    if e.is_retriable && retries < self.max_retries {
                        retries += 1;
                        continue;
                    } else {
                        break Err(e);
                    }
                }
            }
        }
    }
}
