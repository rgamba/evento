mod operations;

use anyhow::{format_err, Error, Result};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub trait Workflow {
    fn run(&mut self) -> Result<WorkflowStatus, WorkflowError>;
}

pub trait WorkflowMetadata {
    fn id(&self) -> uuid::Uuid;

    fn name(&self) -> String;

    fn execution_results(&self) -> Vec<OperationResult>;

    fn iteration_counter_map(&mut self) -> &mut HashMap<String, AtomicUsize>;

    fn context<T: DeserializeOwned>(&self) -> Result<T>;

    fn find_execution_result(
        &self,
        operation_name: String,
        iteration: usize,
    ) -> Option<OperationResult> {
        self.execution_results()
            .clone()
            .into_iter()
            .find(|r| r.operation_name == operation_name && r.iteration == iteration)
    }

    fn iteration_counter(&mut self, operation_name: &String) -> usize {
        self.iteration_counter_map()
            .get(operation_name.as_str())
            .map_or(0, |v| v.load(Ordering::SeqCst))
    }

    fn increase_iteration_counter(&mut self, operation_name: &String) {
        if let Some(count) = self.iteration_counter_map().get(operation_name.as_str()) {
            count.fetch_add(1, Ordering::SeqCst);
        } else {
            self.iteration_counter_map()
                .insert(operation_name.clone(), AtomicUsize::new(1));
        }
    }
}

pub trait WorkflowFactory {
    fn create(&self, id: Uuid, execution_results: Vec<OperationResult>) -> Box<dyn Workflow>;
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
    fn from(err: Error) -> Self {
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

enum RunResult<T> {
    Return(OperationInput),
    Result(T),
}

#[macro_export]
macro_rules! run {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) =>  {{
        match _run_internal!($self, $op<$result_type>($arg)) {
            RunResult::Return(input) =>  return Ok(WorkflowStatus::RunNext(vec![input])),
            RunResult::Result(r) => r
        }
    }};
}

macro_rules! _run_internal {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) => {{
        let operation_name = stringify!($op).to_string();
        let iteration = $self.iteration_counter(&operation_name);
        let workflow_name = $self.name();

        if let Some(result) = $self.find_execution_result(operation_name.clone(), iteration) {
            // We already have a result for this execution. Return it
            $self.increase_iteration_counter(&operation_name);
            RunResult::Result(result.result::<$result_type>().unwrap())
        } else {
            // Operation has no been executed.
            let input = OperationInput::new(workflow_name, operation_name.clone(), iteration, $arg)
                .unwrap();
            $self.increase_iteration_counter(&operation_name);
            $crate::operations::$op::validate_input(&input);
            RunResult::Return(input)
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
                RunResult::Return(input) => {
                    returns.push(input);
                },
                RunResult::Result(r) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::SumOperationInput;
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct TestWorkflow {
        results: Vec<OperationResult>,
        iteration_counter_map: HashMap<String, AtomicUsize>,
        __context: serde_json::Value,
    }

    impl Workflow for TestWorkflow {
        fn run(&mut self) -> Result<WorkflowStatus, WorkflowError> {
            let context = self.context::<String>()?;
            assert_eq!("Ricardo".to_string(), context);

            let result = run!(self, SumOperation<u64>(SumOperationInput{a: 1, b: 2}));
            assert_eq!(3, result);
            let other_result = run!(self, SumOperation<u64>(SumOperationInput{a: 2, b: 2}));
            assert_eq!(4, other_result);

            run_all!(self,
                SumOperation<u64>(SumOperationInput{a: 10, b: 20}),
                SumOperation<u64>(SumOperationInput{a: 3, b: 4}),
            );

            Ok(WorkflowStatus::Completed)
        }
    }

    impl WorkflowMetadata for TestWorkflow {
        fn execution_results(&self) -> Vec<OperationResult> {
            self.results.clone()
        }
        fn id(&self) -> Uuid {
            Uuid::new_v4()
        }
        fn name(&self) -> String {
            String::from("TestWorkflow")
        }

        fn iteration_counter_map(&mut self) -> &mut HashMap<String, AtomicUsize, RandomState> {
            &mut self.iteration_counter_map
        }

        fn context<T: DeserializeOwned>(&self) -> Result<T> {
            serde_json::from_value(self.__context.clone())
                .map_err(|e| format_err!("Unable to deserialize workflow context: {:?}", e))
        }
    }

    #[test]
    fn it_works() {
        let mut wf = TestWorkflow {
            results: vec![
                OperationResult::new(3, 0, String::from("SumOperation")).unwrap(),
                OperationResult::new(4, 1, String::from("SumOperation")).unwrap(),
                OperationResult::new(7, 3, String::from("SumOperation")).unwrap(),
            ],
            iteration_counter_map: HashMap::new(),
            __context: "Ricardo".to_string().into(),
        };
        let result = wf.run();
        if let Ok(WorkflowStatus::RunNext(inputs)) = result {
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs.get(0).unwrap().iteration, 2);
        }
    }
}
