use crate::{Operation, OperationInput, OperationResult, WorkflowError};
use serde::{Serialize, Deserialize};

pub struct SumOperation {}
impl Operation for SumOperation {
    fn execute(&self, input: OperationInput) -> Result<OperationResult, WorkflowError> {
        let values = input.value::<SumOperationInput>().unwrap();
        let result = values.a + values.b;
        Ok(OperationResult::new(result, 0, self.name().to_string())?)
    }

    fn name(&self) -> &str {
        "SumOperation"
    }

    fn validate_input(input: &OperationInput) {
        input.value::<SumOperationInput>().unwrap();
    }
}

#[derive(Deserialize, Serialize)]
pub struct SumOperationInput {
    pub a: u64,
    pub b: u64
}