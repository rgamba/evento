use actix_web::rt::blocking::run;
use anyhow::Result;
use chrono::Utc;
use evento_api::{
    parse_input, run, wait_for_external, Operation, OperationInput, Workflow, WorkflowError,
    WorkflowStatus,
};
use evento_derive::workflow;
use log;
use reqwest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[workflow]
pub struct TestWorkflow {
    context: TestContext,
}

impl Workflow for TestWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        let users = run!(self, FetchUsers<Vec<User>>(self.context.keyword.clone()));
        log::debug!("GOT USERS: {:?}", users);
        if users.is_empty() {
            return Err(WorkflowError::non_retriable_domain_error(
                "No users found".to_string(),
            ));
        }
        let timeout = Utc::now()
            .checked_add_signed(chrono::Duration::seconds(30))
            .unwrap();
        let external_key = Uuid::new_v4();
        log::info!("External key for wait: {}", external_key);
        let filtered =
            wait_for_external!(self, WaitAndFilterUsers<Vec<String>>(users), timeout, external_key);
        run!(self, StoreResult<bool>(filtered));
        Ok((WorkflowStatus::Completed))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestContext {
    pub keyword: String,
    pub age_filter: u64,
}

pub struct FetchUsers;
impl Operation for FetchUsers {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        let keyword = parse_input!(input, String);
        let body: serde_json::Value =
            reqwest::blocking::get("http://dummy.restapiexample.com/api/v1/employees")
                .map_err(|e| WorkflowError::retriable_domain_error(format!("{:?}", e)))?
                .json()
                .map_err(|e| WorkflowError::retriable_domain_error(format!("{:?}", e)))?;
        let results: Vec<User> = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .collect();
        let v = serde_json::to_value(results).unwrap();
        Ok(v)
    }

    fn name(&self) -> &str {
        "FetchUsers"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        input.value::<String>().map(|_| ())
    }
}

#[test]
fn test_fetch() {
    let fetch = FetchUsers {};
    let result = fetch
        .execute(
            OperationInput::new(
                "".to_string(),
                "".to_string(),
                1,
                serde_json::Value::String("test".to_string()),
            )
            .unwrap(),
        )
        .unwrap();
}

pub struct WaitAndFilterUsers;
impl Operation for WaitAndFilterUsers {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        //TODO: workflow is reaching this line when
        if input.external_input.is_none() {
            return Err(WorkflowError::non_retriable_domain_error(
                "Expected external input not present!".to_string(),
            ));
        }
        Ok(input.external_input.clone().unwrap())
    }

    fn name(&self) -> &str {
        "WaitAndFilterUsers"
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }
}

struct StoreResult;
impl Operation for StoreResult {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        unimplemented!()
    }

    fn name(&self) -> &str {
        unimplemented!()
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct User {
    id: u64,
    employee_name: String,
    employee_salary: u64,
    employee_age: u64,
    profile_image: String,
}
