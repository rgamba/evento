//! Framework for declarative workflows.

#[macro_use]
extern crate diesel;
pub mod admin;
pub mod api;
pub mod db;
pub mod poller;
pub mod registry;
pub mod runners;
pub mod state;

use actix_web::{HttpResponse, ResponseError};
use anyhow::{bail, format_err, Result};
use chrono::{DateTime, Utc};
#[cfg(test)]
use mockall::automock;
use serde::de::DeserializeOwned;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Mutex;
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

    fn workflow_name(&self) -> &str;
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct WorkflowData {
    pub id: WorkflowId,
    pub name: String,
    pub correlation_id: CorrelationId,
    pub status: WorkflowStatus,
    pub created_at: DateTime<Utc>,
    pub context: WorkflowContext,
}

/// Represents the status of a Workflow at a given point in time of the execution.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "data")]
pub enum WorkflowStatus {
    /// Workflow is active.
    /// The vector of [NextInput] represent the operations that should be run next.
    Active(Vec<NextInput>),
    /// Workflow completed successfully happy path.
    Completed,
    /// Workflow completed but exercised an error scenario.
    /// This kind of error is domain related and not an infrastructure error.
    CompletedWithError(WorkflowError),
    /// Unexpected error happened.
    /// This typically means an infrastructure error raised by an operation not being
    /// able to complete successfully or the number of retries have reached max.
    Error(WorkflowError),
    /// The workflow was manually cancelled.
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NextInput {
    pub input: OperationInput,
    pub wait_params: Option<WaitParams>,
}

impl NextInput {
    pub fn new(input: OperationInput, wait_params: Option<WaitParams>) -> Self {
        Self { input, wait_params }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WaitParams {
    /// The time until the wait is going to block before timing out the operation.
    /// External input must come in before this date.
    pub timeout: Option<DateTime<Utc>>,
    /// The unique key used to identify this wait operation externally. The caller will use this
    /// in order to signal external input.
    pub external_input_key: ExternalInputKey,
}

impl WaitParams {
    pub fn new(external_input_key: ExternalInputKey, timeout: DateTime<Utc>) -> Self {
        Self {
            external_input_key,
            timeout: Some(timeout),
        }
    }

    pub fn without_timeout(external_input_key: ExternalInputKey) -> Self {
        Self {
            external_input_key,
            timeout: None,
        }
    }
}

impl WorkflowStatus {
    pub fn active() -> Self {
        Self::Active(vec![])
    }

    pub fn is_active(&self) -> bool {
        match *self {
            Self::Completed | Self::CompletedWithError(_) | Self::Cancelled | Self::Error(_) => {
                false
            }
            _ => true,
        }
    }
}

impl WorkflowStatus {
    pub fn get_terminal_status() -> Vec<String> {
        vec![
            "Completed".to_string(),
            "CompletedWithError".to_string(),
            "Cancelled".to_string(),
            "Error".to_string(),
        ]
    }

    pub fn to_string_without_data(&self) -> String {
        let s = match *self {
            Self::Active(_) => "Active",
            Self::Completed => "Completed",
            Self::CompletedWithError(_) => "CompletedWithError",
            Self::Error(_) => "Error",
            Self::Cancelled => "Cancelled",
        };
        String::from(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowError {
    pub is_retriable: bool,
    pub error: String,
    pub error_type: WorkflowErrorType,
}

impl ResponseError for WorkflowError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().json::<WorkflowError>(self.clone())
    }
}

impl Display for WorkflowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowErrorType {
    DomainError,
    InternalError,
    OperationExecutionError,
}

impl WorkflowError {
    pub fn internal_error(error: String) -> Self {
        Self {
            is_retriable: true,
            error_type: WorkflowErrorType::InternalError,
            error,
        }
    }

    pub fn retriable_domain_error(error: String) -> Self {
        Self {
            is_retriable: true,
            error_type: WorkflowErrorType::DomainError,
            error,
        }
    }

    pub fn non_retriable_domain_error(error: String) -> Self {
        Self {
            is_retriable: false,
            error_type: WorkflowErrorType::DomainError,
            error,
        }
    }

    pub fn is_internal_error(&self) -> bool {
        matches!(self.error_type, WorkflowErrorType::InternalError)
    }

    pub fn is_domain_error(&self) -> bool {
        matches!(self.error_type, WorkflowErrorType::DomainError)
    }
}

impl From<anyhow::Error> for WorkflowError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            is_retriable: false,
            error: err.to_string(),
            error_type: WorkflowErrorType::InternalError,
        }
    }
}

impl From<String> for WorkflowError {
    fn from(err: String) -> Self {
        Self {
            is_retriable: false,
            error: err,
            error_type: WorkflowErrorType::InternalError,
        }
    }
}

/// Represents a workflow operation to be executed in isolation.
#[cfg_attr(test, automock)]
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
    /// or data corruption.
    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized;

    fn validate_external_input(&self, _input: serde_json::Value) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct OperationResult {
    pub result: serde_json::Value,
    pub iteration: usize,
    pub created_at: DateTime<Utc>,
    pub operation_name: String,
    /// The input that produced the result.
    pub operation_input: OperationInput,
}

impl OperationResult {
    pub fn new<T>(
        result: T,
        iteration: usize,
        operation_name: String,
        operation_input: OperationInput,
    ) -> Result<Self>
    where
        T: Serialize + Clone,
    {
        Ok(Self {
            result: serde_json::to_value(result)?,
            iteration,
            operation_name,
            created_at: Utc::now(),
            operation_input,
        })
    }

    pub fn result<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.result.clone())
            .map_err(|err| format_err!("Unable to convert result: {:?}", err))
    }
}

/// This is the data that will be persisted in order to execute the operation at some
/// point in the future.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationInput {
    pub operation_name: String,
    pub workflow_name: String,
    pub iteration: usize,
    /// This is an external id used to correlate the external notification to a particular
    /// external operation. This is an alternative to exposing (workflow_id, operation_name, iteration)
    /// to the external world. Only used for external operations.
    pub external_key: Option<ExternalInputKey>,
    pub input: serde_json::Value,
    /// This value will only be present for external operation inputs after input has been provided from
    /// the external source.
    pub external_input: Option<serde_json::Value>,
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

    pub fn external_value<T: DeserializeOwned>(&self) -> Result<T> {
        if let Some(value) = &self.external_input {
            serde_json::from_value(value.clone()).map_err(|err| format_err!("{:?}", err))
        } else {
            bail!("No external input found")
        }
    }
}

/// Operation executor is the component that will typically maintain a stateful set of
/// [`Operation`] instances and will delegate execution to the appropriate one.
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

    fn validate_external_input(
        &self,
        _operation_name: OperationName,
        _external_input: serde_json::Value,
    ) -> Result<()> {
        Ok(())
    }
}

/// Workflow runner is the component that abstracts the workflow execution strategy.
/// It will typically hold a map of workflow factories and will keep a workflow factory registry
/// in order to be able to dynamically create and execute workflows.
#[cfg_attr(test, automock)]
pub trait WorkflowRunner: Send + Sync {
    /// Returns the new [WorkflowStatus] after the workflow has been ran.
    ///
    /// The implementation can decide whether the execution is done synchronously or not.
    /// In case it is done synchronously, the caller must handle retry in case of infra error.
    /// In case the implementation is asynchronous (meaning the request is sent to a queue to
    /// be processed eventually), then result should be ignored but there must be a guarantee that
    /// the workflow will be run at eventually and retries should be taken care of by the impl.
    ///
    /// # Arguments
    ///
    /// * `workflow_data` - The data that uniquely identify the workflow to be ran.
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
/// ```ignore
/// // With return type
/// let result: String = run!(self, GreetOperation<String>(my_name));
/// // When no return is needed:
/// run!(self, GreetOperation(my_name));
/// // When no arguments are needed:
/// run!(self, GreetOperation());
/// ```
#[macro_export]
macro_rules! run {
    ( $self:ident, $op:ident $(< $result_type:ty >)? ($( $arg:expr )?) ) =>  {{
        match $crate::_run_internal!($self, $op $(< $result_type >)? ($( $arg )?)) {
            $crate::RunResult::Return(input) =>  return Ok($crate::WorkflowStatus::Active(vec![$crate::NextInput::new(input, None)])),
            $crate::RunResult::Result(r) => r
        }
    }};
}

#[macro_export]
macro_rules! _run_internal {
    ( $self:ident, $op:ident $(< $result_type:ty >)? ($( $arg:expr )?) ) => {{
        let operation_name = stringify!($op).to_string();
        let iteration = $self.__state.iteration_counter(&operation_name);
        let workflow_name = $self.name();

        if let Some(result) = $self
            .__state
            .find_execution_result(operation_name.clone(), iteration)
        {
            $self.__state.increase_iteration_counter(&operation_name);
            $crate::RunResult::Result(result.result.clone()) // See comment below.
            $(
                ; // Kind of a hack: if there is no return type, the previous line will return, otherwise
                // this semi-colon will ignore the line above and continue with the line below as the last stmt.

                // We already have a result for this execution.
                $crate::RunResult::Result(
                    result
                        .result::<$result_type>()
                        .map_err(|e| WorkflowError::internal_error(format!("{:?}", e)))?,
                )
            )?
        } else {
            // Operation has no been executed.
            let arg = {
                ::serde_json::Value::Null
                $(
                    ; // Ignore the null value above and set the real arg value if args is present.
                    $arg
                )?
            };
            let input =
                $crate::OperationInput::new(workflow_name, operation_name.clone(), iteration, arg)
                    .unwrap();
            $self.__state.increase_iteration_counter(&operation_name);
            if let Err(e) = $op::validate_input(&input) {
                return Err($crate::WorkflowError::internal_error(format!(
                    "Invalid input format for operation {}: {}",
                    operation_name, e
                )));
            }
            $crate::RunResult::Return(input)
        }
    }};
}

/// Works exactly the same as [`run`] but when a fanout of activities needs to be executed
/// and returns a [`Vec`] of results once **all** the activities have been executed.
///
/// The results in the result [`Vec`] will come in the exact same order as they appear on
/// the declaration.
/// All activity return types must have the same return type.
///
/// # Examples
///
/// ```ignore
/// let results: Vec<String> = run_all!(self, GreetOperation<String>(my_name), GreetOperation<String>(other_name));
/// ```
///
/// Wait and regular operations can be ran together:
///
/// ```ignore
/// run_all!(self,
///     GreetOperation(my_name),
///     WaitOperation(input) with wait WaitParams::new()
/// )
/// ```
#[macro_export]
macro_rules! run_all {
    ( $self:ident, $( $op:ident $(<$result_type:ty>)? ($( $arg:expr )?) $(with wait $wait:expr)? ),+ $(,)* ) =>  {
        // The code `{None::<$crate::WaitParams> $(; Some($wait) )?}` is kind of a hack in order to
        // pass in a None value in case there is not `with wait` argument or the actual wait as optional if provided.
        run_all!($self, $( $op $(<$result_type>)? ($( $arg )?) @wait {None::<$crate::WaitParams> $(; Some($wait) )?} ),+)
    };
    ( $self:ident, $( $op:ident $(<$result_type:ty>)? ($( $arg:expr )?) @wait $wait_opt:expr ),+ $(,)* ) =>  {{
        let mut results = Vec::new();
        let mut returns = Vec::new();

        $(
            match $crate::_run_internal!($self, $op ($( $arg )?)) {
                $crate::RunResult::Return(input) => {
                    returns.push($crate::NextInput::new(input, $wait_opt));
                },
                $crate::RunResult::Result(r) => {
                    results.push(r);
                }
            }
        )*

        if !returns.is_empty() {
            return Ok($crate::WorkflowStatus::Active(returns));
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
/// ```ignore
/// // This is the key that will be used to identify and complete the wait activity.
/// let external_key = Uuid::new_v4();
/// let approval_signature = wait_for_external!(self, Approval<String>(true), Utc::now(), external_key);
/// ```
#[macro_export]
macro_rules! wait {
    ( $self:ident, $op:ident $(< $result_type:ty >)? ($( $arg:expr )?), $wait_params:expr ) =>  {{

        match $crate::_run_internal!($self, $op $(< $result_type >)? ($( $arg )?)) {
            $crate::RunResult::Return(input) =>  {
                let wait_input = $crate::NextInput::new(input, Some($wait_params));
                return Ok($crate::WorkflowStatus::Active(vec![wait_input]));
            },
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
    ($input:ident, $type:ty) => {
        $input.value::<$type>().map_err(|err| {
            ::anyhow::format_err!("Unable to cast input value to '{}'", stringify!($type))
        })?;
    };
}

pub struct WorkflowInnerState {
    pub id: WorkflowId,
    pub correlation_id: CorrelationId,
    pub operation_results: Vec<OperationResult>,
    pub iteration_counter_map: Mutex<HashMap<String, usize>>,
}

impl WorkflowInnerState {
    pub fn new(
        id: WorkflowId,
        correlation_id: CorrelationId,
        operation_results: Vec<OperationResult>,
    ) -> Self {
        Self {
            id,
            correlation_id,
            operation_results,
            iteration_counter_map: Mutex::new(HashMap::new()),
        }
    }

    pub fn increase_iteration_counter(&self, operation_name: &str) {
        let mut guard = self.iteration_counter_map.lock().unwrap();
        let count = {
            if let Some(c) = guard.get(operation_name) {
                *c
            } else {
                0
            }
        };
        guard.insert(operation_name.to_string(), count + 1);
    }

    pub fn iteration_counter(&self, operation_name: &str) -> usize {
        let guard = self.iteration_counter_map.lock().unwrap();
        guard.get(operation_name).map_or(0, |v| *v)
    }

    pub fn find_execution_result(
        &self,
        operation_name: String,
        iteration: usize,
    ) -> Option<OperationResult> {
        self.operation_results
            .clone()
            .into_iter()
            .find(|r| r.operation_name == operation_name && r.iteration == iteration)
    }
}
