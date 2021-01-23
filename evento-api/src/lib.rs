pub mod api;
mod poller;
mod registry;
mod runners;
mod state;

use anyhow::{format_err, Result};
use chrono::{DateTime, Utc};
#[cfg(test)]
use mockall::automock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// Unique identifier for a workflow definition. Must be unique
pub type WorkflowName = String;
/// Internally generated workflow ID. Guaranteed to be unique across all workflows.
pub type WorkflowId = Uuid;
pub type WorkflowContext = serde_json::Value;
pub type ExternalInputKey = Uuid;
/// External identifier for a workflow. Must be unique only within the associated workflow name.
pub type CorrelationId = String;
pub type OperationName = String;
pub type OperationIteration = usize;

#[cfg_attr(test, automock)]
pub trait Workflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError>;
}

#[cfg_attr(test, automock)]
pub trait WorkflowFactory: Send + Sync {
    fn create(
        &self,
        id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
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

#[derive(Clone, Debug)]
pub struct WorkflowData {
    pub id: WorkflowId,
    pub name: String,
    pub correlation_id: CorrelationId,
    pub status: WorkflowStatus,
    pub created_at: DateTime<Utc>,
    pub context: WorkflowContext,
}

/// Represents the status of a Workflow at a given point in time of the execution.
#[derive(Clone, Debug)]
pub enum WorkflowStatus {
    /// Workflow has just been created and has not been executed for the first time yet.
    Created,
    /// Workflow completed successfully happy path.
    Completed,
    /// Workflow completed but exercised an error scenario.
    /// This kind of error is domain related and not an infrastructure error.
    CompletedWithError(WorkflowError),
    /// The task needs to wait for external input in order to proceed.
    /// In case the `Option` datetime is provided, the external input must arrive
    /// before that time, otherwise the task will time out.
    WaitForExternal((OperationInput, Option<DateTime<Utc>>)),
    /// Run the next activities in order to proceed with workflow execution.
    RunNext(Vec<OperationInput>),
    /// Unexpected error happened.
    /// This typically means an infrastructure error raised by an operation not being
    /// able to complete successfully or the number of retries have reached max.
    Error(WorkflowError),
    /// The workflow was manually cancelled.
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct WorkflowError {
    pub is_retriable: bool,
    pub error: String,
}

impl From<anyhow::Error> for WorkflowError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            is_retriable: false,
            error: err.to_string(),
        }
    }
}

impl From<String> for WorkflowError {
    fn from(err: String) -> Self {
        Self {
            is_retriable: false,
            error: err,
        }
    }
}

pub trait Operation: Send + Sync {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError>;

    fn name(&self) -> &str;

    fn validate_input(input: &OperationInput)
    where
        Self: Sized;
}

#[derive(Clone, PartialEq, Debug)]
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

    pub fn new_from_value(
        result: serde_json::Value,
        iteration: usize,
        operation_name: String,
    ) -> Self {
        Self {
            result,
            iteration,
            operation_name,
            created_at: Utc::now(),
        }
    }

    pub fn result<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.result.clone()).map_err(|err| format_err!("{:?}", err))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationInput {
    pub operation_name: String,
    pub workflow_name: String,
    pub iteration: usize,
    /// This is an external id used to correlate the external notification to a particular
    /// external operation. This is an alternative to exposing (workflow_id, operation_name, iteration)
    /// to the external world. Only used for external operations.
    pub external_key: Option<ExternalInputKey>,
    input: serde_json::Value,
    /// This value will only be present for external operation inputs after input has been provided from
    /// the external source.
    external_input: Option<serde_json::Value>,
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
            external_key: None,
            external_input: None,
        })
    }

    pub fn new_external<T: Serialize>(
        workflow_name: String,
        operation_name: String,
        iteration: usize,
        input: T,
        external_key: ExternalInputKey,
    ) -> Result<Self> {
        Ok(Self {
            workflow_name,
            operation_name,
            input: serde_json::to_value(input)?,
            iteration,
            external_key: Some(external_key),
            external_input: None,
        })
    }

    pub fn value<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.input.clone()).map_err(|err| format_err!("{:?}", err))
    }
}

/// Operation executor is the component that will typically maintain a statefull set of
/// `Operation` instances and will delegate execution to the appropriate one.
/// It will also take care of the retry strategy.
#[cfg_attr(test, automock)]
pub trait OperationExecutor: Send + Sync {
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError>;
}

/// Workflow runner is the component that abstracts the workflow execution strategy.
/// It will typically hold a map of workflow factories and will keep a workflow factory registry
/// in order to be able to dynamically create and execute workflows.
pub trait WorkflowRunner: Send + Sync {
    fn run(&self, workflow_data: WorkflowData) -> Result<WorkflowStatus, WorkflowError>;
}

/// Workflow registry is the bag of factories that is solely responsible for recreating
/// a workflow instance given the workflow name and details.
pub trait WorkflowRegistry: Send + Sync {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
        execution_results: Vec<OperationResult>,
    ) -> Result<Box<dyn Workflow>>;
}

/// For usage within the run macros only.
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
macro_rules! wait_for_external {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr), $timeout:expr, $corr_id:expr ) =>  {{
        match $crate::_run_internal!($self, $op<$result_type>($arg)) {
            $crate::RunResult::Return(input) =>  return Ok(WorkflowStatus::WaitForExternal((input, Some($timeout)))),
            $crate::RunResult::Result(r) => r
        }
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

#[macro_export]
macro_rules! operation_ok {
    ($result:expr) => {
        ::anyhow::Result::Ok(serde_json::to_value($result).unwrap())
    };
}

#[macro_export]
macro_rules! parse_input {
    ($input:ident, $type:ident) => {
        $input.value::<$type>().map_err(|err| {
            ::anyhow::format_err!("Unable to cast input value to '{}'", stringify!($type))
        })?;
    };
}

pub mod tests {
    use super::*;
    use crate::Operation;
    use std::collections::HashMap;

    pub struct MockOperation {
        operation_name: String,
        callback:
            Box<dyn Fn(OperationInput) -> Result<serde_json::Value, WorkflowError> + Send + Sync>,
    }

    impl MockOperation {
        pub fn new(
            name: &str,
            callback: impl Fn(OperationInput) -> Result<serde_json::Value, WorkflowError>
                + 'static
                + Send
                + Sync,
        ) -> Self {
            Self {
                operation_name: name.into(),
                callback: Box::new(callback),
            }
        }
    }

    impl Operation for MockOperation {
        fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
            (self.callback)(input)
        }

        fn name(&self) -> &str {
            self.operation_name.as_str()
        }

        fn validate_input(input: &OperationInput)
        where
            Self: Sized,
        {
            unimplemented!()
        }
    }

    /// Workflow runner that allows immediate execution of the workflow and the associated operations
    /// synchronously in the same task.
    pub struct InlineWorkflowRunner {
        operation_executor: Arc<dyn OperationExecutor>,
        workflow_factory_map: HashMap<String, Box<dyn WorkflowFactory>>,
    }

    impl WorkflowRunner for InlineWorkflowRunner {
        fn run(&self, workflow_data: WorkflowData) -> Result<WorkflowStatus, WorkflowError> {
            match self.run_and_return_results(workflow_data) {
                Ok((result, _)) => Ok(result),
                Err(e) => Err(e),
            }
        }
    }

    impl InlineWorkflowRunner {
        pub fn new(
            operation_executor: Arc<dyn OperationExecutor>,
            workflow_factory_map: HashMap<String, Box<dyn WorkflowFactory>>,
        ) -> Self {
            Self {
                operation_executor,
                workflow_factory_map,
            }
        }

        pub fn run_and_return_results(
            &self,
            workflow_data: WorkflowData,
        ) -> Result<(WorkflowStatus, Vec<OperationResult>), WorkflowError> {
            let mut results = Vec::new();
            let factory = self.workflow_factory_map.get(&workflow_data.name).unwrap();
            let res = loop {
                let mut wf = factory.create(
                    workflow_data.id,
                    workflow_data.correlation_id.clone(),
                    workflow_data.context.clone(),
                    results.clone(),
                );
                match wf.run() {
                    Ok(WorkflowStatus::Completed) => break Ok(WorkflowStatus::Completed),
                    Ok(WorkflowStatus::RunNext(next_operations)) => {
                        for op in next_operations {
                            results.push(self.operation_executor.execute(op)?);
                        }
                    }
                    Ok(WorkflowStatus::WaitForExternal((input, timeout))) => {
                        //TODO: figure out how to pass the external input.
                        results.push(self.operation_executor.execute(input)?);
                    }
                    Ok(other) => break Ok(other),
                    Err(e) => break Err(e),
                }
            };
            match res {
                Ok(s) => Ok((s, results)),
                Err(e) => Err(e),
            }
        }
    }

    /// Operator executor that executes the operations immediately on the same thread and with
    /// an immediate retry strategy.
    pub struct InlineOperationExecutor {
        pub operation_map: HashMap<String, Box<dyn Operation>>,
        pub max_retries: usize,
    }

    impl OperationExecutor for InlineOperationExecutor {
        fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
            let operation = self
                .operation_map
                .get(input.operation_name.as_str())
                .unwrap();
            let mut retries: usize = 0;
            let result = loop {
                match operation.execute(input.clone()) {
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
            };
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

    /// Utility function to easily run workflows.
    pub fn run_to_completion(
        factory: Box<dyn WorkflowFactory>,
        context: ::serde_json::Value,
        operation_map: HashMap<String, Box<dyn Operation>>,
    ) -> Result<(WorkflowStatus, Vec<OperationResult>), WorkflowError> {
        let executor = InlineOperationExecutor {
            operation_map,
            max_retries: 10,
        };
        let mut factories = HashMap::new();
        factories.insert("test".to_string(), factory);
        let runner = InlineWorkflowRunner::new(Arc::new(executor), factories);
        runner.run_and_return_results(WorkflowData {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            created_at: Utc::now(),
            context: context,
            correlation_id: "test".to_string(),
            status: WorkflowStatus::Created,
        })
    }
}
