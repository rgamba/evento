use crate::operations::SumOperationInput;
use crate::{
    run, run_all, OperationResult, Workflow, WorkflowError, WorkflowMetadata, WorkflowStatus,
};
use anyhow::Result;
use serde::de::DeserializeOwned;
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
