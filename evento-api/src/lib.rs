mod operations;
//#[cfg(test)]
//mod tests;

use anyhow::{format_err, Error, Result};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub trait Workflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError>;
}

pub trait WorkflowFactory {
    fn create(
        &self,
        id: Uuid,
        context: serde_json::Value,
        execution_results: Vec<OperationResult>,
    ) -> Box<dyn Workflow>;
}

pub struct WorkflowDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(&mut dyn WorkflowFactoryRegistrar),
}

pub trait WorkflowFactoryRegistrar {
    fn register_factory(&mut self, workflow_name: String, workflow: Box<dyn WorkflowFactory>);
    fn register_operation_factory(&mut self, operation: Arc<dyn Operation>);
}

#[derive(Debug, Clone)]
pub enum WorkflowStatus {
    Completed,
    RunNext(Vec<OperationInput>),
}

#[derive(Debug)]
pub struct WorkflowError {
    pub is_retriable: bool,
    pub error: Error,
}

impl From<anyhow::Error> for WorkflowError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            is_retriable: false,
            error: err,
        }
    }
}

pub trait Operation {
    fn new() -> Self
    where
        Self: Sized;
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError>;
    fn name(&self) -> &str;
    fn validate_input(input: &OperationInput)
    where
        Self: Sized;
}

#[derive(Clone)]
pub struct OperationResult {
    result: serde_json::Value,
    pub iteration: usize,
    pub created_at: DateTime<Utc>,
    pub operation_name: String,
}

impl OperationResult {
    pub fn new<T>(result: T, iteration: usize, operation_name: String) -> Result<Self>
    where
        T: Serialize + Clone,
    {
        Ok(Self {
            result: serde_json::to_value(result.clone())?,
            iteration,
            operation_name,
            created_at: Utc::now(),
        })
    }

    pub fn result<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.result.clone()).map_err(|err| format_err!("{:?}", err))
    }
}

#[derive(Debug, Clone)]
pub struct OperationInput {
    pub operation_name: String,
    pub workflow_name: String,
    pub iteration: usize,
    input: serde_json::Value,
}

impl OperationInput {
    pub fn new<T: Serialize>(
        workflow_name: String,
        operation_name: String,
        iteration: usize,
        value: T,
    ) -> Result<Self> {
        Ok(Self {
            workflow_name,
            operation_name,
            input: serde_json::to_value(value)?,
            iteration,
        })
    }

    pub fn value<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.input.clone()).map_err(|err| format_err!("{:?}", err))
    }
}

pub enum RunResult<T> {
    Return(OperationInput),
    Result(T),
}

#[macro_export]
macro_rules! run {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) =>  {{
        match $crate::_run_internal!($self, $op<$result_type>($arg)) {
            $crate::RunResult::Return(input) =>  return Ok(WorkflowStatus::RunNext(vec![input])),
            $crate::RunResult::Result(r) => r
        }
    }};
}

#[macro_export]
macro_rules! _run_internal {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) => {{
        let operation_name = stringify!($op).to_string();
        let iteration = $self.iteration_counter(&operation_name);
        let workflow_name = $self.name();

        if let Some(result) = $self.find_execution_result(operation_name.clone(), iteration) {
            // We already have a result for this execution. Return it
            $self.increase_iteration_counter(&operation_name);
            ::evento_api::RunResult::Result(result.result::<$result_type>().unwrap())
        } else {
            // Operation has no been executed.
            let input = OperationInput::new(workflow_name, operation_name.clone(), iteration, $arg)
                .unwrap();
            $self.increase_iteration_counter(&operation_name);
            $op::validate_input(&input);
            ::evento_api::RunResult::Return(input)
        }
    }};
}

#[macro_export]
macro_rules! run_all {
    ( $self:ident, $( $op:ident <$result_type:ident> ($arg:expr) ),+ $(,)* ) =>  {{
        let mut results = Vec::new();
        let mut returns = Vec::new();

        $(
            match _run_internal!($self, $op<$result_type>($arg)) {
                ::evento_api::RunResult::Return(input) => {
                    returns.push(input);
                },
                ::evento_api::RunResult::Result(r) => {
                    results.push(r);
                }
            }
        )*

        if !returns.is_empty() {
            return Ok(WorkflowStatus::RunNext(returns));
        }
        results
    }};
}

#[macro_export]
macro_rules! export_workflow {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static workflow_declaration: $crate::WorkflowDeclaration =
            $crate::WorkflowDeclaration {
                rustc_version: $crate::RUSTC_VERSION,
                core_version: $crate::CORE_VERSION,
                register: $register,
            };
    };
}
