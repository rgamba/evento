use crate::{
    ExternalOperationInput, Operation, OperationInput, OperationResult, Workflow, WorkflowError,
    WorkflowFactory, WorkflowId, WorkflowName,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Workflow registry is the bag of factories that is solely responsible for recreating
/// a workflow instance given the workflow name and details.
pub trait WorkflowRegistry {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        execution_results: Vec<OperationResult>,
    ) -> Result<Box<dyn Workflow>>;
}

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
            .create(
                workflow_id,
                execution_results.iter().map(|r| r.result.clone()).collect(),
            ))
    }
}

/// OperationExecutor is the operation that routes and executes an operation instruction
/// in the appropriate `Operation` instance for it.
pub trait OperationExecutor {
    fn execute(
        &self,
        input: OperationInput,
        external_input: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError>;
}

pub struct SimpleOperationExecutor {
    pub operation_map: HashMap<String, Box<dyn Operation>>,
}

impl SimpleOperationExecutor {
    pub fn new(operation_map: HashMap<String, Box<dyn Operation>>) -> Self {
        Self { operation_map }
    }
}

impl OperationExecutor for SimpleOperationExecutor {
    fn execute(
        &self,
        input: OperationInput,
        external_input: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError> {
        let operation = self
            .operation_map
            .get(input.operation_name.as_str())
            .unwrap();
        match operation.execute(input.clone(), external_input) {
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

pub struct RetryOperationExecutor {
    pub max_retries: usize,
    simple_operation_executor: SimpleOperationExecutor,
}

impl RetryOperationExecutor {
    pub fn new(operation_map: HashMap<String, Box<dyn Operation>>, max_retries: usize) -> Self {
        Self {
            max_retries,
            simple_operation_executor: SimpleOperationExecutor::new(operation_map),
        }
    }
}

impl OperationExecutor for RetryOperationExecutor {
    fn execute(
        &self,
        input: OperationInput,
        external_input: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError> {
        let mut retries: usize = 0;
        loop {
            match self
                .simple_operation_executor
                .execute(input.clone(), external_input.clone())
            {
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
