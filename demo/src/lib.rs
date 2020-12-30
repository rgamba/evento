use anyhow::Result;
use evento_api::{
    run, Operation, OperationInput, OperationResult, Workflow, WorkflowError, WorkflowStatus,
};
use evento_derive::workflow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[workflow(DemoWorkflowContext)]
pub struct DemoWorkflow;
impl Workflow for DemoWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        let context: DemoWorkflowContext = self.context();
        let sum_result = run!(self, SumOperation<u64>(SumOperationInput{
            a: context.sum_a,
            b: context.sum_b,
        }));
        Ok(WorkflowStatus::Completed)
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DemoWorkflowContext {
    pub sum_a: u64,
    pub sum_b: u64,
}

pub struct SumOperation {}
impl Operation for SumOperation {
    fn new() -> Self {
        Self {}
    }

    fn name(&self) -> &str {
        "SumOperation"
    }

    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
        let values = input.value::<SumOperationInput>().unwrap();
        let result = values.a + values.b;
        Ok(OperationResult::new(result, 0, self.name().to_string())?)
    }

    fn validate_input(input: &OperationInput) {
        input.value::<SumOperationInput>().unwrap();
    }
}

#[derive(Deserialize, Serialize)]
pub struct SumOperationInput {
    pub a: u64,
    pub b: u64,
}

#[test]
fn it_works() {
    let wf = DemoWorkflow::new(
        Uuid::nil(),
        DemoWorkflowContext { sum_a: 1, sum_b: 2 },
        vec![],
    );
    let result = wf.run();
    match result {
        Err(e) => panic!(format!("{:?}", e)),
        Ok(WorkflowStatus::RunNext(next)) => {
            panic!(format!("{:?}", next))
        }
        _ => {}
    }
}
