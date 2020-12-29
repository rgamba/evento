use evento_api::{OperationResult, Workflow, WorkflowError, WorkflowStatus};
use evento_derive::workflow;
use uuid::Uuid;

#[workflow]
pub struct DemoWorkflow;
impl Workflow for DemoWorkflow {
    fn run(&mut self) -> Result<WorkflowStatus, WorkflowError> {
        Ok(WorkflowStatus::Completed)
    }
}
