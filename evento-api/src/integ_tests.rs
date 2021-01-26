use crate::api::WorkflowFacade;
use crate::registry::{SimpleOperationExecutor, SimpleWorkflowRegistry};
use crate::runners::AsyncWorkflowRunner;
use crate::state::{InMemoryStore, State};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[test]
fn integration_tests() {
    env_logger::init();
    let state = State {
        store: Arc::new(InMemoryStore::new()),
    };
    let factories = HashMap::new();
    let registry = Arc::new(SimpleWorkflowRegistry::new(factories));
    let operation_map = HashMap::new();
    let executor = Arc::new(SimpleOperationExecutor::new(operation_map));
    let runner = Arc::new(AsyncWorkflowRunner::new(state.clone(), registry.clone()));
    let facade = WorkflowFacade::new(state.clone(), registry.clone(), executor.clone(), runner);

    let wf_id = Uuid::new_v4();
    facade
        .create_workflow(
            "test_workflow".to_string(),
            wf_id,
            "test".to_string(),
            serde_json::Value::Null,
        )
        .unwrap();
}
