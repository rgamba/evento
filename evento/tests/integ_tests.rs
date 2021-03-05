use anyhow::{format_err, Result};
use chrono::Utc;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Error, PoolError};
use diesel::{Connection, PgConnection};
use dotenv::dotenv;
use evento::api::{Evento, EventoBuilder};
use evento::db::sql_store::{DbPool, PgPool, SqlStore};
use evento::registry::{
    SimpleOperationExecutor, SimpleOperationExecutorBuilder, SimpleWorkflowRegistry,
    SimpleWorkflowRegistryBuilder,
};
use evento::runners::AsyncWorkflowRunner;
use evento::state::State;
use evento::{
    operation_ok, parse_input, run, run_all, wait, Operation, OperationInput, WaitParams, Workflow,
    WorkflowData, WorkflowError, WorkflowFactory, WorkflowId, WorkflowStatus,
};
use evento_derive::workflow;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

// -------------------------------------------------------------------------------------------------
// Workflow definitions
// -------------------------------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
struct User {
    name: String,
    age: u32,
}

lazy_static! {
    static ref USERS: Arc<Mutex<Vec<User>>> = Arc::new(Mutex::new(Vec::new()));
    static ref SHOULD_FAIL: AtomicBool = AtomicBool::new(false);
    static ref WAIT_KEY_1: Uuid = Uuid::new_v4();
    static ref WAIT_KEY_2: Uuid = Uuid::new_v4();
}

#[workflow]
struct SimpleWorkflow {
    #[allow(dead_code)]
    context: String,
}

impl Workflow for SimpleWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        run_all!(self, Echo(1), Echo<i64>(2));
        run!(self, Echo(3));
        run!(self, Echo<i64>(4));
        Ok(WorkflowStatus::Completed)
    }
}

#[workflow]
struct WaitWorkflow {
    #[allow(dead_code)]
    context: u32,
}

impl Workflow for WaitWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        let users = run!(self, GetUsers<Vec<User>>(GetUsersFilter{name: None, min_age: Some(self.context)}));
        assert_eq!(users.len(), 2);
        let result = run!(self, Echo<i64>(10));
        assert_eq!(result, 10);
        // When no return type is specified, it should return a Value
        let result = run!(self, Echo(15));
        assert_eq!(result, serde_json::Value::Number(15.into()));
        // Test fanout of wait operations
        let timeout = Utc::now()
            .checked_add_signed(chrono::Duration::seconds(20))
            .unwrap();
        let wait_results = run_all!(self,
            Echo(20) with wait WaitParams::new(*WAIT_KEY_1, timeout),
            Approve() with wait WaitParams::new(*WAIT_KEY_2, timeout)
        );
        assert_eq!(wait_results.get(0).unwrap(), 20);
        let approved = wait_results.get(1).unwrap().as_bool().unwrap();
        if !approved {
            return Ok(WorkflowStatus::CompletedWithError(
                WorkflowError::non_retriable_domain_error("Not approved".to_string()),
            ));
        }

        Ok(WorkflowStatus::Completed)
    }
}

// -------------------------------------------------------------------------------------------------
// Operation definitions
// -------------------------------------------------------------------------------------------------

struct GetUsers;
impl Operation for GetUsers {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        let filter = parse_input!(input, GetUsersFilter);
        let guard = USERS.lock().unwrap();
        let copy = guard
            .iter()
            .filter(|u| {
                if let Some(age) = filter.min_age {
                    u.age >= age
                } else {
                    true
                }
            })
            .cloned()
            .collect::<Vec<User>>();
        operation_ok!(copy)
    }

    fn name(&self) -> &str {
        "GetUsers"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        serde_json::from_value::<GetUsersFilter>(input.input.clone())
            .map(|_| ())
            .map_err(|e| format_err!("{:?}", e))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct GetUsersFilter {
    pub name: Option<String>,
    pub min_age: Option<u32>,
}

struct Approve;
impl Operation for Approve {
    fn execute(&self, input: OperationInput) -> Result<Value, WorkflowError> {
        let ext_input = input.external_value::<bool>()?;
        operation_ok!(ext_input)
    }

    fn name(&self) -> &str {
        "Approve"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn validate_external_input(&self, input: Value) -> Result<()> {
        serde_json::from_value::<bool>(input)
            .map(|_| ())
            .map_err(|e| format_err!("{:?}", e))
    }
}

struct Echo;
impl Operation for Echo {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        if SHOULD_FAIL.load(Ordering::SeqCst) {
            SHOULD_FAIL.store(false, Ordering::SeqCst);
            return Err(WorkflowError::internal_error(
                "Unable to do operation now".to_string(),
            ));
        } else {
            SHOULD_FAIL.store(true, Ordering::SeqCst);
        }
        let inp = parse_input!(input, i64);
        operation_ok!(inp)
    }

    fn name(&self) -> &str {
        "Echo"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        serde_json::from_value::<i64>(input.input.clone())
            .map(|_| ())
            .map_err(|e| format_err!("{:?}", e))
    }
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

#[derive(Debug)]
struct TestTransaction;

impl CustomizeConnection<PgConnection, Error> for TestTransaction {
    fn on_acquire(&self, conn: &mut PgConnection) -> ::std::result::Result<(), Error> {
        conn.begin_test_transaction().unwrap();
        Ok(())
    }
}

pub fn new_test_db_pool(database_url: &str) -> Result<DbPool, PoolError> {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    PgPool::builder()
        .min_idle(Some(1))
        .max_size(1)
        .connection_customizer(Box::new(TestTransaction))
        .build(manager)
        .map(Arc::new)
}

#[test]
fn integration_tests() {
    {
        let mut guard = USERS.lock().unwrap();
        guard.push(User {
            name: "John".to_string(),
            age: 20,
        });
        guard.push(User {
            name: "Mary".to_string(),
            age: 30,
        });
        guard.push(User {
            name: "Joe".to_string(),
            age: 40,
        });
    }

    dotenv().ok();
    env_logger::try_init().unwrap();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set!");
    let store = SqlStore::new_with_pool(new_test_db_pool(database_url.as_str()).unwrap());
    let facade = EventoBuilder::new(store)
        // Workflows
        .register_workflow(SimpleWorkflowFactory::default())
        .register_workflow(WaitWorkflowFactory::default())
        // Operations
        .register_operation(Echo {})
        .register_operation(GetUsers {})
        .register_operation(Approve {})
        .build();

    let wf_id = facade
        .create_workflow(
            "WaitWorkflow".to_string(),
            "test".to_string(),
            serde_json::to_value(25).unwrap(),
        )
        .unwrap()
        .id;
    facade.get_workflow_by_id(wf_id).unwrap().unwrap();
    thread::sleep(Duration::from_secs(3));
    facade
        .complete_external(*WAIT_KEY_2, serde_json::Value::Bool(true))
        .unwrap();
    facade
        .complete_external(*WAIT_KEY_1, serde_json::Value::Bool(true))
        .unwrap();

    wait_for_workflow_to_complete(wf_id, &facade, Duration::from_secs(5));
    facade.stop().unwrap();
}

pub fn wait_for_workflow_to_complete(workflow_id: WorkflowId, facade: &Evento, timeout: Duration) {
    let time_timeout = Utc::now()
        .checked_add_signed(chrono::Duration::from_std(timeout).unwrap())
        .unwrap();
    loop {
        assert!(
            Utc::now().lt(&time_timeout),
            "Workflow failed to reach completed status"
        );
        let wf = facade.get_workflow_by_id(workflow_id).unwrap().unwrap();
        if let WorkflowStatus::Completed = wf.status {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
}
