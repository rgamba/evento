//! Framework for declarative workflows.
pub mod api;
pub mod poller;
pub mod registry;
pub mod runners;
pub mod state;

use anyhow::{format_err, Result};
use chrono::{DateTime, Utc};
#[cfg(test)]
use mockall::automock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// Unique identifier for a workflow definition. Must be unique
pub type WorkflowName = String;
/// Internally generated workflow ID. Guaranteed to be unique across all workflows.
pub type WorkflowId = Uuid;
/// Workflow context is the initial data that gets injected to the workflow.
pub type WorkflowContext = serde_json::Value;
/// It will typically be used to identify a wait activity in order to mark it as completed.
pub type ExternalInputKey = Uuid;
/// External identifier for a workflow. Must be unique only within the associated workflow name.
pub type CorrelationId = String;
pub type OperationName = String;
pub type OperationIteration = usize;

#[cfg_attr(test, automock)]
pub trait Workflow {
    /// Returns the new `WorkflowStatus` after the run operation or a `WorkflowError`
    /// in case of error.
    ///
    /// This is the main function that will contain the implementation of the workflow.
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

/// Represents a workflow operation to be executed in isolation.
pub trait Operation: Send + Sync {
    /// Returns a result of `Value` containing the execution output in case of success
    /// or a `WorkflowError` in case something went wrong.
    ///
    /// Implementation must be idempotent. It is possible that a workflow operation be executed
    /// more than once. The implementation must guarantee that multiple execution won't leave the
    /// system in an inconsistent state.
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError>;

    /// Returns the name of the operation.
    ///
    /// This will typically be the same as the struct name, but can be different.
    /// Must be unique in the registry.
    fn name(&self) -> &str;

    /// Returns an error in case the input is not valid for this operation.
    ///
    /// In case this operation returns an error, it typically means this is a programming error,
    /// or data corruption. It's OK to panic.
    fn validate_input(input: &OperationInput) -> Result<()>
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

/// This is the data that will be persisted in order to execute the operation at some
/// point in the future.
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
    /// Executes the given operation and returns an [OperationResult] in case of success or
    /// [WorkflowError] in case any expected or unexpected error happened.
    ///
    /// This function should not panic.
    ///
    /// # Arguments
    ///
    /// * `input` - The operation input as passed in by the workflow.
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError>;
}

/// Workflow runner is the component that abstracts the workflow execution strategy.
/// It will typically hold a map of workflow factories and will keep a workflow factory registry
/// in order to be able to dynamically create and execute workflows.
#[cfg_attr(test, automock)]
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

/// Returns the activity result in case the activity has already been executed or
/// returns the with a `RunNext` so that the given operation can be scheduled for execution.
///
/// It is important to note that this is only syntactic sugar and this will not execute the
/// operation immediately, it will rather try to get the pre-recorded result or schedule it for
/// execution if it is not available.
///
/// # Arguments
///
/// 1. Always pass in the `self` reference as first argument.
/// 2. Operation call in the format: `OperationName<ReturnType>(input_argument)`, where the
/// the `OperationName` is the name as defined in the operation registry, which can be the same
/// as the struct name but could also be different.
///
/// # Examples
///
/// ```
/// # use evento_api::{WorkflowError, WorkflowStatus};
/// # struct WfTest;
/// # impl WfTest {
///     fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
/// #       let my_name = "Ricardo".to_string();
///         // This reads: run `GreetOperation` operation with `my_name` as input parameter
///         // and I expect to get a `String` as result.
///         let result = run!(self, GreetOperation<String>(my_name));
///     }
/// # }
/// ```
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
            $op::validate_input(&input).unwrap();
            ::evento_api::RunResult::Return(input)
        }
    }};
}

/// Works exactly the same as [run] but when a fanout of activities needs to be executed
/// and returns a `Vec` of results once **all** the activities have been executed.
///
/// The results in the result `Vec` will come in the exact same order as they appear on
/// the declaration.
/// All activity return types must have the same return type.
///
/// # Examples
///
/// ```
/// # use evento_api::{WorkflowError, WorkflowStatus};
/// # struct WfTest;
/// # impl WfTest {
///     fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
/// #       let my_name = "Ricardo".to_string();
/// #       let other_name = "Jose".to_string();
///         let results = run!(self, GreetOperation<String>(my_name), GreetOperation<String>(other_name));
///     }
/// # }
/// ```
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

/// Waits for the completion of an external activity or returns the result value if the
/// activity has been completed.
///
/// This is similar to a regular [`run`] activity in that it has an operation associated with it
/// and it can receive any type of input and return an output.
/// The difference with regular operations is that the wait operation will first wait for the
/// external input to be present before it can begin to execute the operation.
///
/// # Arguments
///
/// 1. Always `self`
/// 2. Operation call in the format `OperationName<ReturnType>(input_argument)`
/// 3. External key identifier, must be of the type [`ExternalInputKey`]
/// 4. [std::time::Duration] as a timeout relative to the current time. If the operation has not been
/// completed by this time, then the workflow will be aborted.
///
/// # Examples
///
/// ```
/// # use evento_api::{WorkflowError, WorkflowStatus};
/// # use uuid::Uuid;
/// # struct WfTest;
/// # impl WfTest {
///     fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
///         // This is the key that will be used to identify and complete the wait activity.
///         let external_key = Uuid::new_v4();
///         let approval_signature = wait_for_external!(self, Approval<String>(), external_key, Duration::from_hours(1));
///     }
/// # }
/// ```
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

        fn validate_input(input: &OperationInput) -> Result<()>
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
