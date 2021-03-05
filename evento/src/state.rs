use crate::{
    CorrelationId, ExternalInputKey, NextInput, OperationInput, OperationName, OperationResult,
    WorkflowContext, WorkflowData, WorkflowError, WorkflowId, WorkflowName, WorkflowStatus,
};
use anyhow::{bail, format_err, Result};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkflowFilter {
    pub name: Option<String>,
    pub created_from: Option<DateTime<Utc>>,
    pub created_to: Option<DateTime<Utc>>,
    pub status: Option<WorkflowStatus>,
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Clone)]
pub struct State {
    pub store: Arc<dyn Store>,
}

lazy_static! {
    pub static ref SAFE_RETRY_DURATION: chrono::Duration = chrono::Duration::seconds(60);
}

pub trait Store: Send + Sync {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<WorkflowData>;

    fn get_workflow(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>>;

    fn get_workflow_by_correlation_id(
        &self,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>>;

    fn find_wait_operation(
        &self,
        external_key: ExternalInputKey,
    ) -> Result<Option<OperationExecutionData>>;

    fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>>;

    fn get_operation_results_with_errors(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<OperationResult>>;

    /// Fetch operations that whose run_date is less or equal to the current now provided.
    ///
    /// A lease for the resulting operations will start. The caller must dequeue the operation
    /// before the lease ends, otherwise, the operation(s) will be re-delivered to the caller on
    /// subsequent calls to this function.
    fn fetch_operations(&self, current_now: DateTime<Utc>) -> Result<Vec<OperationExecutionData>>;

    fn count_queued_elements(&self) -> Result<u64>;

    /// Returns the execution data for the external operation.
    ///
    /// The implementation should add the external input to the operation execution data and given
    /// that the external input is available at this time, the operation should be scheduled to be
    /// executed ASAP.
    ///
    /// # Arguments
    ///
    /// * `external_key` - The external key used to uniquely identify the wait operation.
    /// * `external_input_payload` - The payload that will be used to complete the operation.
    fn complete_external_operation(
        &self,
        external_key: ExternalInputKey,
        external_input_payload: serde_json::Value,
    ) -> Result<OperationExecutionData>;

    /// Persist an operation execution result.
    ///
    /// Only a single successful execution is allowed for any workflow_id, operation_name, iteration
    /// combination. Attempt to record multiple successful execution will result in error.
    ///
    /// Multiple errored executions are allowed and all will be persisted and not replaced.
    fn store_execution_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        result: OperationResult,
    ) -> Result<()>;

    /// Update the given Workflow and mark it as completed.
    /// If the workflow's status is not active, this will be a no-op.
    fn complete_workflow(&self, workflow_id: WorkflowId) -> Result<()>;

    /// Complete the workflow with a domain-specific error.
    /// If the workflow's status is not active, this will be a no-op.
    fn complete_workflow_with_error(&self, workflow_id: WorkflowId, error: String) -> Result<()>;

    /// Manually cancel the workflow.
    /// If the workflow's status is not active, this will be a no-op.
    fn cancel_workflow(&self, workflow_id: WorkflowId, reason: String) -> Result<()>;

    /// Manually mark the workflow as active.
    /// If the workflow's status is not active, this will be a no-op.
    fn mark_active(&self, workflow_id: WorkflowId, next_input: Vec<NextInput>) -> Result<()>;

    /// Abort the workflow.
    /// If the workflow's status is not active, this will be a no-op.
    fn abort_workflow_with_error(
        &self,
        workflow_id: WorkflowId,
        error: WorkflowError,
    ) -> Result<()>;

    /// Queue a single operation to be executed at some point in the future.
    /// If the operation already exists, it will be updated and the retry_count will be incremented
    /// otherwise it will be created with a retry_count of 0.
    fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()>;

    /// Dequeue and remove an operation from the queue.
    /// This is typically performed once the operation has been executed so it doesn't get delivered
    /// again.
    fn dequeue_operation(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: usize,
    ) -> Result<OperationExecutionData>;

    /// Queue all operations in an atomic transaction where this will only succeed if all
    /// operations are queued successfully or else the operation will rollback.
    /// The queuing logic is the same as the `queue_operation` function.
    fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()>;

    fn get_workflows(&self, filters: WorkflowFilter) -> Result<Vec<WorkflowData>>;

    /// Removes the operation result provided **and all subsequent operations**
    ///
    /// This operation will effectively recreate history, so its important that the implementation
    /// also dequeues any pending operation.
    fn delete_operation_results(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: usize,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct OperationExecutionData {
    pub workflow_id: WorkflowId,
    pub correlation_id: CorrelationId,
    pub retry_count: Option<usize>,
    pub input: OperationInput,
}

type InMemoryQueue = Mutex<Vec<(OperationExecutionData, DateTime<Utc>, &'static str)>>;
type OperationResults = Mutex<HashMap<WorkflowId, Vec<(OperationName, OperationResult)>>>;

pub struct InMemoryStore {
    pub operation_results: OperationResults,
    pub queue: InMemoryQueue, // (data, run_date)
    pub workflows: Mutex<Vec<WorkflowData>>,
}

impl InMemoryStore {
    const QUEUED: &'static str = "Q";
    const RETRY: &'static str = "R";
}

impl Default for InMemoryStore {
    fn default() -> Self {
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
    ) -> Result<WorkflowData> {
        let mut guard = self.workflows.lock().unwrap();
        let data = WorkflowData {
            id: workflow_id,
            name: workflow_name,
            correlation_id,
            status: WorkflowStatus::active(),
            created_at: Utc::now(),
            context,
        };
        guard.push(data.clone());
        Ok(data)
    }

    fn get_workflow(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        let guard = self.workflows.lock().unwrap();
        Ok(guard.iter().find(|w| w.id == workflow_id).cloned())
    }

    fn get_workflow_by_correlation_id(
        &self,
        correlation_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        let guard = self.workflows.lock().unwrap();
        Ok(guard
            .iter()
            .find(|w| w.correlation_id == correlation_id)
            .cloned())
    }

    fn find_wait_operation(
        &self,
        external_key: ExternalInputKey,
    ) -> Result<Option<OperationExecutionData>> {
        let guard = self.queue.lock().unwrap();
        let data = guard
            .iter()
            .find(|(data, _, _)| {
                data.input.external_key.is_some()
                    && data.input.external_key.unwrap() == external_key
            })
            .map(|(data, _, _)| data.clone());
        Ok(data)
    }

    fn get_operation_results(&self, workflow_id: WorkflowId) -> Result<Vec<OperationResult>> {
        let guard = self.operation_results.lock().unwrap();
        if let Some(results) = guard.get(&workflow_id) {
            Ok(results
                .clone()
                .into_iter()
                .filter(|(_, result)| !result.is_error())
                .map(|(_, result)| result)
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn get_operation_results_with_errors(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<OperationResult>> {
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
        let mut guard = self.queue.lock().unwrap();
        Ok(guard
            .iter_mut()
            .filter(|(_, run_date, state)| {
                (*state == Self::QUEUED || *state == Self::RETRY)
                    && (&current_now == run_date || current_now.gt(run_date))
            })
            .map(|(data, next_run_date, state)| {
                let copy = data.clone();
                *state = Self::RETRY;
                // Give the caller some time to execute and requeue/dequeue the task, otherwise
                // it needs to be delivered to guarantee at least once execution.
                *next_run_date = next_run_date
                    .checked_add_signed(*SAFE_RETRY_DURATION)
                    .unwrap();
                copy
            })
            .collect())
    }

    fn count_queued_elements(&self) -> Result<u64> {
        let guard = self.queue.lock().unwrap();
        let count = guard
            .iter()
            .filter(|(_, _, state)| *state == Self::QUEUED)
            .count();
        Ok(count as u64)
    }

    fn complete_external_operation(
        &self,
        external_key: ExternalInputKey,
        external_input_payload: serde_json::Value,
    ) -> Result<OperationExecutionData> {
        let mut guard = self.queue.lock().unwrap();
        let data = guard
            .iter_mut()
            .find(|(data, _, _)| match data.input.external_key {
                Some(id) => external_key == id,
                None => false,
            } && data.input.external_input.is_none())
            .map(|(ref mut data, run_date, state)| {
                *state = Self::QUEUED;
                data.input.external_input = Some(external_input_payload.clone());
                *run_date = Utc::now(); // Queue this for immediate execution
                data.clone()
            })
            .ok_or_else(|| format_err!(
                "Unable to find operation with external key provided"
            ))?;
        Ok(data)
    }

    fn store_execution_result(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        result: OperationResult,
    ) -> Result<()> {
        let mut guard = self.operation_results.lock().unwrap();
        if !guard.contains_key(&workflow_id) {
            guard.insert(workflow_id, vec![]);
        }
        let list = guard.get_mut(&workflow_id).unwrap();
        list.push((operation_name, result));
        Ok(())
    }

    fn complete_workflow(&self, workflow_id: WorkflowId) -> Result<()> {
        let mut guard = self.workflows.lock().unwrap();
        match guard
            .iter_mut()
            .find(|wf| wf.id == workflow_id && wf.status.is_active())
        {
            Some(wf) => {
                wf.status = WorkflowStatus::Completed;
                Ok(())
            }
            None => bail!("Unable to find workflow with id: {}", workflow_id),
        }
    }

    fn mark_active(&self, workflow_id: WorkflowId, next_inputs: Vec<NextInput>) -> Result<()> {
        let mut guard = self.workflows.lock().unwrap();
        match guard.iter_mut().find(|wf| wf.id == workflow_id) {
            Some(wf) => {
                wf.status = WorkflowStatus::Active(next_inputs);
                Ok(())
            }
            None => bail!("Unable to find workflow with id: {}", workflow_id),
        }
    }

    fn complete_workflow_with_error(&self, workflow_id: WorkflowId, error: String) -> Result<()> {
        let mut guard = self.workflows.lock().unwrap();
        match guard
            .iter_mut()
            .find(|wf| wf.id == workflow_id && wf.status.is_active())
        {
            Some(wf) => {
                wf.status = WorkflowStatus::CompletedWithError(error.into());
                Ok(())
            }
            None => bail!("Unable to find workflow with id: {}", workflow_id),
        }
    }

    fn cancel_workflow(&self, _workflow_id: WorkflowId, _reason: String) -> Result<()> {
        unimplemented!()
    }

    fn abort_workflow_with_error(
        &self,
        workflow_id: WorkflowId,
        error: WorkflowError,
    ) -> Result<()> {
        let mut guard = self.workflows.lock().unwrap();
        match guard
            .iter_mut()
            .find(|wf| wf.id == workflow_id && wf.status.is_active())
        {
            Some(wf) => {
                wf.status = WorkflowStatus::Error(error);
                Ok(())
            }
            None => bail!("Unable to find workflow with id: {}", workflow_id),
        }
    }

    fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()> {
        //TODO:this needs to check if the element is present and update it if it is.
        {
            self.dequeue_operation(
                execution_data.workflow_id,
                execution_data.input.operation_name.clone(),
                execution_data.input.iteration,
            )
            .ok();
        }
        let mut guard = self.queue.lock().unwrap();
        guard.push((execution_data, run_date, Self::QUEUED));
        Ok(())
    }

    fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()> {
        for (operation, run_at) in operations.into_iter() {
            self.queue_operation(operation, run_at)?;
        }
        Ok(())
    }

    fn dequeue_operation(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: usize,
    ) -> Result<OperationExecutionData> {
        let mut guard = self.queue.lock().unwrap();
        let index = guard
            .iter_mut()
            .position(|(data, _, _)| {
                data.workflow_id == workflow_id
                    && data.input.operation_name == operation_name
                    && data.input.iteration == iteration
            })
            .ok_or_else(|| format_err!("Unable to find operation"))?;
        let (data, _, _) = guard.remove(index);
        log::info!("Dequeued element. queue size={}", guard.len());
        Ok(data)
    }

    fn get_workflows(&self, _filters: WorkflowFilter) -> Result<Vec<WorkflowData>> {
        let guard = self.workflows.lock().unwrap();
        Ok(guard.iter().cloned().collect())
    }

    fn delete_operation_results(
        &self,
        workflow_id: WorkflowId,
        operation_name: OperationName,
        iteration: usize,
    ) -> Result<()> {
        let (_, deleted_result) = {
            let mut guard = self.operation_results.lock().unwrap();
            let results = guard.get(&workflow_id).unwrap();
            let mut new_results = vec![];
            let mut element = None;
            for (operation_name_, result) in results {
                if operation_name_ == &operation_name {
                    if result.iteration == iteration {
                        element = Some((operation_name, result.clone()));
                        break;
                    }
                }
                new_results.push((operation_name.clone(), result.clone()))
            }
            guard.insert(workflow_id, new_results);
            element
        }
        .ok_or_else(|| format_err!("Unable to find operation"))?;
        // Mark the wf as active
        {
            let mut guard = self.workflows.lock().unwrap();
            match guard.iter_mut().find(|wf| wf.id == workflow_id) {
                Some(wf) => {
                    wf.status = WorkflowStatus::active();
                }
                None => bail!("Unable to find workflow with id: {}", workflow_id),
            };
        }
        // Now enqueue
        self.queue_operation(
            OperationExecutionData {
                workflow_id,
                correlation_id: "".to_string(),
                retry_count: None,
                input: deleted_result.operation_input,
            },
            Utc::now(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use chrono::Duration;
    use uuid::Uuid;

    use super::*;

    pub fn create_test_state() -> State {
        State {
            store: Arc::new(InMemoryStore::default()),
        }
    }

    #[test]
    fn test_delete_operation_results() {
        let wf_id = Uuid::new_v4();
        let result_content = "test_result".to_string();
        let operation_name = "test_operation".to_string();
        let test_input = OperationInput::new(
            "test".to_string(),
            "test".to_string(),
            0,
            serde_json::Value::Null,
        )
        .unwrap();
        let operation_result_1 = OperationResult::new(
            Ok(result_content.clone()),
            0,
            operation_name.clone(),
            test_input.clone(),
        )
        .unwrap();
        let operation_result_2 = OperationResult::new(
            Ok(result_content.clone()),
            1,
            operation_name.clone(),
            test_input.clone(),
        )
        .unwrap();
        let operation_result_3 = OperationResult::new(
            Ok(result_content.clone()),
            2,
            operation_name.clone(),
            test_input.clone(),
        )
        .unwrap();
        let store = InMemoryStore::default();
        store
            .create_workflow(
                "test".to_string(),
                wf_id,
                "test".to_string(),
                serde_json::Value::Null,
            )
            .unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), operation_result_1)
            .unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), operation_result_2)
            .unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), operation_result_3)
            .unwrap();
        store
            .delete_operation_results(wf_id, operation_name, 1)
            .unwrap();
        let result = store.get_operation_results(wf_id).unwrap();
        assert_eq!(1, result.len());
        assert_eq!(0, result.get(0).unwrap().iteration);
    }

    #[test]
    fn test_inmemory_store() {
        let wf_name = "test".to_string();
        let wf_id = Uuid::new_v4();
        let correlation_id = "correlationid".to_string();
        let context = serde_json::Value::String("test".to_string());
        let operation_name = "test_operation".to_string();
        let store = InMemoryStore::default();
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
        let wf = store.get_workflow(wf_id).unwrap().unwrap();
        assert_eq!(wf.id, wf_id);
        assert_eq!(wf.name, wf_name);
        assert_eq!(wf.context, context);
        assert_eq!(wf.correlation_id, correlation_id);
        assert!(matches!(wf.status, WorkflowStatus::Active(_)));
        // Mark as completed
        store.complete_workflow(wf_id).unwrap();
        let wf = store.get_workflow(wf_id).unwrap().unwrap();
        assert!(matches!(wf.status, WorkflowStatus::Completed));
        // Store execution result
        let result_content = "test_result".to_string();
        let input = OperationInput::new(
            wf_name.clone(),
            operation_name.clone(),
            0,
            result_content.clone(),
        )
        .unwrap();
        let result_content_2 = "test_result2".to_string();
        let operation_result = OperationResult::new(
            Ok(result_content.clone()),
            0,
            operation_name.clone(),
            input.clone(),
        )
        .unwrap();
        let operation_result_2 = OperationResult::new(
            Ok(result_content_2.clone()),
            0,
            operation_name.clone(),
            input.clone(),
        )
        .unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), operation_result)
            .unwrap();
        store
            .store_execution_result(wf_id, operation_name.clone(), operation_result_2)
            .unwrap();
        // Fetch all execution results
        let results = store.get_operation_results(wf_id).unwrap();
        assert_eq!(2, results.len());
        // Queue operation
        let execution_data = OperationExecutionData {
            workflow_id: wf_id,
            correlation_id: String::new(),
            retry_count: None,
            input,
        };
        // Try fetch an element with a run_date greater than current time
        let now = Utc::now();
        store.queue_operation(execution_data, now).unwrap();
        let results = store
            .fetch_operations(now.checked_sub_signed(Duration::seconds(5)).unwrap())
            .unwrap();
        assert!(results.is_empty());
        // Try fetching an element that is meant to be fetched
        let results = store
            .fetch_operations(now.checked_add_signed(Duration::seconds(5)).unwrap())
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(0, results.get(0).unwrap().retry_count.unwrap_or_default());
        // Try fetching again without dequeuing the element
        let results = store
            .fetch_operations(now.checked_add_signed(*SAFE_RETRY_DURATION).unwrap())
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(0, results.get(0).unwrap().retry_count.unwrap_or_default());
        // Dequeue the element and check that it is actually removed from the queue.
        store
            .dequeue_operation(wf_id, operation_name.clone(), 0)
            .unwrap();
        let results = store
            .fetch_operations(now.checked_add_signed(Duration::seconds(5)).unwrap())
            .unwrap();
        assert_eq!(0, results.len());
        // Queue wait operation
        // Queue operation
        let external_key = Uuid::new_v4();
        let execution_data = OperationExecutionData {
            workflow_id: wf_id,
            correlation_id: String::new(),
            retry_count: None,
            input: OperationInput::new_external(
                wf_name,
                operation_name,
                0,
                result_content,
                external_key,
            )
            .unwrap(),
        };
        store
            .queue_operation(
                execution_data,
                now.checked_add_signed(Duration::seconds(10)).unwrap(),
            )
            .unwrap();
        let ext_payload_input = serde_json::to_value(result_content_2).unwrap();
        let operation = store
            .complete_external_operation(external_key, ext_payload_input.clone())
            .unwrap();
        assert!(matches!(
            operation.input.external_input,
            Some(x) if x == ext_payload_input
        ));
        store.find_wait_operation(external_key).unwrap().unwrap();
    }
}
