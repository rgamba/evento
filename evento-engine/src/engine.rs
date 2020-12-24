use crate::registry::ExternalWorkflows;
use crate::state::{OperationExecutionData, State};
use anyhow::Result;
use evento_api::{Operation, Workflow};
use std::collections::HashMap;
use std::sync::Arc;

pub struct WorkflowEngine {
    state: Arc<State>,
    registry: ExternalWorkflows,
}

impl WorkflowEngine {
    pub async fn execute(&self, data: OperationExecutionData) -> Result<()> {
        let workflow = self.get_workflow(&data.workflow_name)?;

        Ok(())
    }

    fn get_workflow(&self, workflow_name: &str) -> Result<Arc<dyn Workflow>> {
        unimplemented!()
    }
}
