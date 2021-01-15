use crate::{
    CorrelationId, ExternalInputKey, OperationInput, OperationName, OperationResult,
    WorkflowContext, WorkflowData, WorkflowError, WorkflowId, WorkflowName, WorkflowStatus,
};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct State {
    pub store: Arc<dyn Store>,
}

pub trait Store {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<()>;

    fn get_workflow(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>>;

    fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>>;

    /// Fetch operations that whose run_date is less or equal to the current now provided.
    fn fetch_operations(&self, current_now: DateTime<Utc>) -> Result<Vec<OperationExecutionData>>;

    fn complete_external_operation(
        &self,
        external_key: ExternalInputKey,
        external_input_payload: serde_json::Value,
    ) -> Result<OperationExecutionData>;

    fn store_execution_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        result: Result<OperationResult, WorkflowError>,
    ) -> Result<()>;

    /// Update the given Workflow and mark it as completed.
    fn complete_workflow(&self, workflow_id: WorkflowId) -> Result<()>;

    fn complete_workflow_with_error(&self, workflow_id: WorkflowId, error: String) -> Result<()>;

    fn cancel_workflow(&self, workflow_id: WorkflowId, reason: String) -> Result<()>;

    fn abort_workflow_with_error(
        &self,
        workflow_id: WorkflowId,
        error: &WorkflowError,
    ) -> Result<()>;

    /// Queue a single operation to be executed at some point in the future.
    /// If the operation already exists, it will be updated and the retry_count will be incremented
    /// otherwise it will be created with a retry_count of 0.
    fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()>;

    /// Queue all operations in an atomic transaction where this will only succeed if all
    /// operations are queued successfully or else the operation will rollback.
    /// The queuing logic is the same as the `queue_operation` function.
    fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct OperationExecutionData {
    pub workflow_id: WorkflowId,
    pub retry_count: Option<usize>,
    pub input: OperationInput,
}

pub struct InMemoryStore {
    pub operation_results: Mutex<HashMap<WorkflowId, Vec<(OperationName, OperationResult)>>>,
    pub queue: Mutex<Vec<(OperationExecutionData, DateTime<Utc>)>>, // (data, run_date)
    pub workflows: Mutex<Vec<WorkflowData>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            operation_results: Mutex::new(HashMap::new()),
            queue: Mutex::new(Vec::new()),
            workflows: Mutex::new(Vec::new()),
        }
    }
}

impl Store for InMemoryStore {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<()> {
        let mut guard = self.workflows.lock().unwrap();
        guard.push(WorkflowData {
            id: workflow_id,
            name: workflow_name,
            correlation_id,
            status: WorkflowStatus::Created,
            created_at: Utc::now(),
            context,
        });
        Ok(())
    }

    fn get_workflow(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        let mut guard = self.workflows.lock().unwrap();
        Ok(guard
            .iter()
            .find(|w| w.id == workflow_id)
            .map(|w| w.clone()))
    }

    fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>> {
        let guard = self.operation_results.lock().unwrap();
        if let Some(results) = guard.get(&workflow_id) {
            Ok(results
                .clone()
                .into_iter()
                .map(|(_, result)| result)
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn fetch_operations(&self, current_now: DateTime<Utc>) -> Result<Vec<OperationExecutionData>> {
        let guard = self.queue.lock().unwrap();
        Ok(guard
            .iter()
            .filter(|(data, run_date)| current_now.gt(run_date))
            .map(|(data, _)| data.clone())
            .collect())
    }

    fn complete_external_operation(
        &self,
        external_key: ExternalInputKey,
        external_input_payload: Value,
    ) -> Result<OperationExecutionData> {
        let guard = self.queue.lock().unwrap();
        match guard
            .iter()
            .find(|(data, _)| match data.input.correlation_id {
                Some(id) => external_key == id,
                None => false,
            })
            .map(|(data, _)| data.clone())
        {
            None => bail!("Invalid external key provided"),
            Some(data) => Ok(data),
        }
    }

    fn store_execution_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        result: Result<OperationResult, WorkflowError>,
    ) -> Result<()> {
        let mut guard = self.operation_results.lock().unwrap();
        if !guard.contains_key(&workflow_id) {
            guard.insert(workflow_id, vec![]);
        }
        let mut list = guard.get_mut(&workflow_id).unwrap();
        match result {
            Ok(res) => list.push((operation_name.clone(), res)),
            Err(_) => (),
        };
        Ok(())
    }

    fn complete_workflow(&self, workflow_id: WorkflowId) -> Result<()> {
        unimplemented!()
    }

    fn complete_workflow_with_error(&self, workflow_id: WorkflowId, error: String) -> Result<()> {
        unimplemented!()
    }

    fn cancel_workflow(&self, workflow_id: WorkflowId, reason: String) -> Result<()> {
        unimplemented!()
    }

    fn abort_workflow_with_error(
        &self,
        workflow_id: WorkflowId,
        error: &WorkflowError,
    ) -> Result<()> {
        unimplemented!()
    }

    fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()> {
        let mut guard = self.queue.lock().unwrap();
        guard.push((execution_data, run_date));
        Ok(())
    }

    fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inmemory_store() {
        let wf_name = "test".to_string();
        let wf_id = Uuid::new_v4();
        let correlation_id = "correlationid".to_string();
        let context = serde_json::Value::String("test".to_string());
        let operation_name = "test_operation".to_string();
        let store = InMemoryStore::new();
        // Create workflow
        store
            .create_workflow(
                wf_name.clone(),
                wf_id,
                correlation_id.clone(),
                context.clone(),
            )
            .unwrap();
        // Get workflow
        match store.get_workflow(wf_id).unwrap() {
            None => panic!("Unable to get workflow!"),
            Some(wf) => {
                assert_eq!(wf.id, wf_id);
                assert_eq!(wf.name, wf_name);
                assert_eq!(wf.context, context);
                assert_eq!(wf.correlation_id, correlation_id);
            }
        }
        // Store execution result
        let result_content = "test_result".to_string();
        let result_content_2 = "test_result2".to_string();
        let operation_result =
            OperationResult::new(result_content.clone(), 0, operation_name.clone()).unwrap();
        let operation_result_2 =
            OperationResult::new(result_content_2.clone(), 0, operation_name.clone()).unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), Ok(operation_result.clone()))
            .unwrap();
        store
            .store_execution_result(
                wf_id,
                operation_name.clone(),
                Ok(operation_result_2.clone()),
            )
            .unwrap();
        // Fetch all execution results
        let results = store.get_operation_results(wf_id).unwrap();
        assert_eq!(2, results.len());
        // Queue operation
        let execution_data = OperationExecutionData {
            workflow_id: wf_id,
            retry_count: None,
            input: OperationInput::new(
                wf_name.clone(),
                operation_name.clone(),
                0,
                result_content.clone(),
            )
            .unwrap(),
        };
        store
            .queue_operation(execution_data.clone(), Utc::now())
            .unwrap();
    }
}
