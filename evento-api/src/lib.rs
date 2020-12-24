mod operations;

use anyhow::{Error, Result, format_err};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub trait Workflow {
    fn run(&mut self) -> Result<WorkflowStatus, WorkflowError>;
    fn id(&self) -> uuid::Uuid;
    fn execution_results(&self) -> Vec<OperationResult>;
    fn find_execution_result(&self, operation_name: String, iteration: usize) -> Option<OperationResult> {
        self.execution_results().clone()
            .into_iter()
            .find(|r| r.operation_name == operation_name && r.iteration == iteration)
    }
}

#[derive(Debug, Clone)]
pub enum WorkflowStatus {
    Completed,
    RunNext(Vec<(String, OperationInput)>)
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
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError>;
    fn name(&self) -> &str;
    fn validate_input(input: &OperationInput);
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
    where T: Serialize + Clone {
        Ok(Self {
            result: serde_json::to_value(result.clone())?,
            iteration,
            operation_name,
            created_at: Utc::now(),
        })
    }

    pub fn result<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.result.clone())
            .map_err(|err| format_err!("{:?}", err))
    }
}

#[derive(Debug, Clone)]
pub struct OperationInput {
    input: serde_json::Value,
    pub iteration: usize,
}

impl OperationInput {
    pub fn new<T: Serialize>(value: T) -> Result<Self> {
        Self::new_with_iteration(value, 0)
    }

    pub fn new_with_iteration<T: Serialize>(value: T, iteration: usize) -> Result<Self> {
        Ok(Self {
            input: serde_json::to_value(value)?,
            iteration
        })
    }

    pub fn value<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.input.clone())
            .map_err(|err| format_err!("{:?}", err))
    }
}

enum RunResult<T> {
    Return((String, OperationInput)),
    Result(T)
}

macro_rules! run {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) =>  {{
        match _run_internal!($self, $op<$result_type>($arg)) {
            RunResult::Return((operation_name, input)) =>  return Ok(WorkflowStatus::RunNext(vec![
                (operation_name, input)
            ])),
            RunResult::Result(r) => r
        }
    }};
}

macro_rules! _run_internal {
    ( $self:ident, $op:ident <$result_type:ident> ($arg:expr) ) =>  {{
        let operation_name = stringify!($op).to_string();
        let iteration = $self.iteration_counter(&operation_name);

        if let Some(result) = $self.find_execution_result(operation_name.clone(), iteration) {
            // We already have a result for this execution. Return it
            $self.increase_iteration_counter(&operation_name);
            RunResult::Result(result.result::<$result_type>().unwrap())
        } else {
            // Operation has no been executed.
            let input = OperationInput::new_with_iteration($arg, iteration).unwrap();
            $crate::operations::$op::validate_input(&input);
            RunResult::Return((operation_name, input))
        }
    }};
}

macro_rules! run_all {
    ( $self:ident, $( $op:ident <$result_type:ident> ($arg:expr) ),+ $(,)* ) =>  {{
        let mut results = Vec::new();
        let mut returns = Vec::new();

        $(
            match _run_internal!($self, $op<$result_type>($arg)) {
                RunResult::Return((operation_name, input)) => {
                    returns.push((operation_name, input));
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

#[cfg(test)]
mod tests { 
    use super::*;
    use crate::operations::SumOperationInput;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::collections::HashMap;
    use uuid::Uuid;

    struct TestWorkflow {
        results: Vec<OperationResult>,
        //TODO: automatically add this field with macros
        iteration_counter_map: HashMap<String, AtomicUsize>,
    }

    impl Workflow for TestWorkflow {
        fn run(&mut self) -> Result<WorkflowStatus, WorkflowError> {
            let result = run!(self, SumOperation<u64>(SumOperationInput{a: 1, b: 2}));
            assert_eq!(3, result);
            let other_result = run!(self, SumOperation<u64>(SumOperationInput{a: 2, b: 2}));
            assert_eq!(4, other_result);

            run_all!(self,
                SumOperation<u64>(SumOperationInput{a: 1, b: 2}),
                SumOperation<u64>(SumOperationInput{a: 3, b: 4}),
            );

            Ok(WorkflowStatus::Completed)
        }

        fn execution_results(&self) -> Vec<OperationResult> {
            self.results.clone()
        }

        fn id(&self) -> Uuid {
            Uuid::new_v4()
        }
    }

    // TODO:automatically add these methods with derive macro
    impl TestWorkflow {
        fn iteration_counter(&self, operation_name: &String) -> usize {
            self.iteration_counter_map.get(operation_name.as_str()).map_or(0, |v| v.load(Ordering::SeqCst))
        }

        fn increase_iteration_counter(&mut self, operation_name: &String) {
            if let Some(count)  = self.iteration_counter_map.get(operation_name.as_str()) {
                count.fetch_add(1, Ordering::SeqCst);
            } else {
                self.iteration_counter_map.insert(operation_name.clone(), AtomicUsize::new(1));
            }
        }
    }

    #[test]
    fn it_works() {
        let mut wf = TestWorkflow{
            results: vec![
                OperationResult::new(3, 0,String::from("SumOperation")).unwrap(),
                OperationResult::new(4, 1,String::from("SumOperation")).unwrap(),
            ],
            iteration_counter_map: HashMap::new(),
        };
        let result = wf.run();
        panic!("{:?}", result);
    }
}
