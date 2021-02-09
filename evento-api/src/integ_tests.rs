use crate as evento_api;
use crate::api::WorkflowFacade;
use crate::registry::{SimpleOperationExecutor, SimpleWorkflowRegistry};
use crate::runners::tests::wait_for_workflow_to_complete;
use crate::runners::AsyncWorkflowRunner;
use crate::state::{InMemoryStore, State};
use crate::{
    run, wait_for_external, Operation, OperationInput, Workflow, WorkflowError, WorkflowFactory,
    WorkflowStatus,
};
use anyhow::{format_err, Result};
use chrono::Utc;
use evento_derive::workflow;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn integration_tests() {
    env_logger::try_init().unwrap();

    let state = State {
        store: Arc::new(InMemoryStore::new()),
    };
    let mut factories: HashMap<String, Arc<dyn WorkflowFactory>> = HashMap::new();
    factories.insert(
        "SimpleWorkflow".to_string(),
        Arc::new(SimpleWorkflowFactory {}),
    );
    factories.insert("WaitWorkflow".to_string(), Arc::new(WaitWorkflowFactory {}));
    let registry = Arc::new(SimpleWorkflowRegistry::new(factories));

    let operation_a = A {};

    let mut operation_map: HashMap<String, Arc<dyn Operation>> = HashMap::new();
    operation_map.insert("A".to_string(), Arc::new(operation_a));
    let executor = Arc::new(SimpleOperationExecutor::new(operation_map));
    let runner = Arc::new(AsyncWorkflowRunner::new(state.clone(), registry.clone()));
    let facade = WorkflowFacade::new(
        state.clone(),
        registry.clone(),
        executor.clone(),
        runner.clone(),
    );

    let wf_id = facade
        .create_workflow(
            "WaitWorkflow".to_string(),
            "test".to_string(),
            serde_json::to_value("test".to_string()).unwrap(),
        )
        .unwrap()
        .id;
    facade.get_workflow_by_id(wf_id).unwrap().unwrap();
    thread::sleep(Duration::from_secs(3));
    facade
        .complete_external(Uuid::nil(), serde_json::Value::Bool(true))
        .unwrap();

    wait_for_workflow_to_complete(wf_id, state.clone(), Duration::from_secs(5)).unwrap();
    runner.stop().unwrap();
    facade.stop().unwrap();
}

#[workflow]
struct SimpleWorkflow {
    #[allow(dead_code)]
    context: String,
}

impl Workflow for SimpleWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        run!(self, A<bool>(true));
        run!(self, A<bool>(true));
        run!(self, A<bool>(true));
        Ok(WorkflowStatus::Completed)
    }
}

#[workflow]
struct WaitWorkflow {
    #[allow(dead_code)]
    context: String,
}

impl Workflow for WaitWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        run!(self, A<bool>(true));
        let timeout = Utc::now()
            .checked_add_signed(chrono::Duration::hours(1))
            .unwrap();
        wait_for_external!(self, A<bool>(true), timeout, Uuid::nil());
        run!(self, A<bool>(true));
        Ok(WorkflowStatus::Completed)
    }
}

struct A;
impl Operation for A {
    fn execute(&self, _input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        Ok(serde_json::Value::Bool(true))
    }

    fn name(&self) -> &str {
        "A"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        serde_json::from_value::<bool>(input.input.clone())
            .map(|_| ())
            .map_err(|e| format_err!("{:?}", e))
    }
}
