use crate::db::models::{ExecutionResultDTO, OperationQueueDTO, WorkflowDTO};
use crate::state::{OperationExecutionData, Store, SAFE_RETRY_DURATION};
use crate::{
    CorrelationId, ExternalInputKey, OperationInput, OperationName, OperationResult,
    WorkflowContext, WorkflowData, WorkflowError, WorkflowId, WorkflowName, WorkflowStatus,
};
use anyhow::{format_err, Result};
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use serde::export::Formatter;
use serde_json::Value;
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::sync::Arc;
use uuid::Uuid;

pub type PgPool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;
pub type DbPool = Arc<PgPool>;

pub enum QueueState {
    Queued,
    Dequeued,
    Retry,
    Aborted,
}

impl Display for QueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let printable = match *self {
            Self::Queued => "Q",
            Self::Dequeued => "D",
            Self::Retry => "R",
            Self::Aborted => "A",
        };
        write!(f, "{}", printable)
    }
}

pub struct SqlStore {
    pub database_url: String,
    db_pool: DbPool,
}

impl SqlStore {
    const FETCH_BATCH_SIZE: i64 = 10;

    pub fn new(database_url: String) -> Result<Self> {
        let manager = ConnectionManager::<PgConnection>::new(database_url.clone());
        Ok(Self {
            database_url,
            db_pool: PgPool::builder()
                .build(manager)
                .map(Arc::new)
                .map_err(|err| format_err!("{:?}", err))?,
        })
    }

    pub fn new_with_pool(db_pool: DbPool) -> Self {
        Self {
            database_url: String::new(),
            db_pool,
        }
    }
}

impl Store for SqlStore {
    fn create_workflow(
        &self,
        workflow_name: WorkflowName,
        workflow_id: WorkflowId,
        correlation_id: CorrelationId,
        context: WorkflowContext,
    ) -> Result<WorkflowData> {
        use crate::db::schema::workflows;
        let status = serde_json::to_value(WorkflowStatus::Created).unwrap();
        let data = WorkflowDTO {
            id: workflow_id,
            name: workflow_name,
            correlation_id,
            status: serde_json::to_string(&status)?,
            created_at: Utc::now(),
            context,
        };
        diesel::insert_into(workflows::table)
            .values(&data)
            .get_result::<WorkflowDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to insert workflow: {:?}", err))?;
        Ok(data.try_into()?)
    }

    fn get_workflow(&self, workflow_id: WorkflowId) -> Result<Option<WorkflowData>> {
        use crate::db::schema::workflows::dsl::*;
        let result = workflows
            .find(workflow_id)
            .first::<WorkflowDTO>(&self.db_pool.get()?);
        match result {
            Ok(r) => Ok(Some(WorkflowDTO::try_into(r)?)),
            Err(diesel::result::Error::NotFound) => Ok(None),
            Err(e) => Err(format_err!("{:?}", e)),
        }
    }

    fn get_workflow_by_correlation_id(
        &self,
        corr_id: CorrelationId,
    ) -> Result<Option<WorkflowData>> {
        use crate::db::schema::workflows::dsl::*;
        let result = workflows
            .filter(correlation_id.eq(corr_id))
            .first::<WorkflowDTO>(&self.db_pool.get()?);
        match result {
            Ok(r) => Ok(Some(WorkflowDTO::try_into(r)?)),
            Err(diesel::result::Error::NotFound) => Ok(None),
            Err(e) => Err(format_err!("{:?}", e)),
        }
    }

    fn find_wait_operation(
        &self,
        external_key_: ExternalInputKey,
    ) -> Result<Option<OperationExecutionData>> {
        use crate::db::schema::operations_queue::dsl::*;
        let con = self.db_pool.get()?;
        match operations_queue
            .filter(external_key.eq(external_key_))
            .first::<OperationQueueDTO>(&con)
            .optional()?
        {
            Some(dto) => Ok(Some(dto.try_into()?)),
            None => Ok(None),
        }
    }

    fn get_operation_results(&self, workflow_id_: WorkflowId) -> Result<Vec<OperationResult>> {
        use crate::db::schema::execution_results::dsl::*;
        let results = execution_results
            .filter(workflow_id.eq(workflow_id_))
            .filter(is_error.eq(false))
            .order(created_at.asc())
            .load::<ExecutionResultDTO>(&self.db_pool.get()?)?
            .into_iter()
            .map(|dto| dto.to_operation_result()?)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format_err!("{:?}", e))?;
        Ok(results)
    }

    fn get_operation_results_with_errors(
        &self,
        workflow_id_: WorkflowId,
    ) -> Result<Vec<Result<OperationResult, WorkflowError>>> {
        use crate::db::schema::execution_results::dsl::*;
        let results = execution_results
            .filter(workflow_id.eq(workflow_id_))
            .order(created_at.asc())
            .load::<ExecutionResultDTO>(&self.db_pool.get()?)?
            .into_iter()
            .map(|dto| dto.to_operation_result())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format_err!("{:?}", e))?;
        Ok(results)
    }

    fn fetch_operations(&self, current_now: DateTime<Utc>) -> Result<Vec<OperationExecutionData>> {
        use crate::db::schema::operations_queue::dsl::*;
        let con = self.db_pool.get()?;
        let ids = con.transaction::<Vec<OperationQueueDTO>, diesel::result::Error, _>(|| {
            // Get and block the next queue elements
            let ids = operations_queue
                .select(id)
                .for_update()
                .skip_locked()
                .filter(state.eq_any(vec![
                    QueueState::Queued.to_string(),
                    QueueState::Retry.to_string(),
                ]))
                .filter(next_run_date.le(current_now))
                .limit(Self::FETCH_BATCH_SIZE)
                .load::<Uuid>(&con)?;
            let safe_retry_date = current_now
                .checked_add_signed(*SAFE_RETRY_DURATION)
                .unwrap();
            // Block the elements until `safe_retry_date` and return them.
            let dtos = diesel::update(operations_queue.filter(id.eq_any(ids.clone())))
                .set((
                    state.eq(QueueState::Retry.to_string()),
                    next_run_date.eq(safe_retry_date),
                ))
                .get_results::<OperationQueueDTO>(&con)?;
            Ok(dtos)
        })?;
        ids.into_iter()
            .map(|dto| dto.try_into())
            .collect::<Result<Vec<_>>>()
    }

    fn count_queued_elements(&self) -> Result<u64> {
        unimplemented!()
    }

    fn complete_external_operation(
        &self,
        external_key_: ExternalInputKey,
        external_input_payload_: Value,
    ) -> Result<OperationExecutionData> {
        use crate::db::schema::operations_queue::dsl::*;
        let con = self.db_pool.get()?;
        let dto = con.transaction::<OperationQueueDTO, diesel::result::Error, _>(|| {
            let input_ = operations_queue
                .select(input)
                .for_update()
                .filter(external_key.eq(external_key_))
                .first::<serde_json::Value>(&con)?;
            let mut new_input: OperationInput = serde_json::from_value(input_)
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
            new_input.external_input = Some(external_input_payload_);
            let new_input_value = serde_json::to_value(new_input)
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
            diesel::update(operations_queue.filter(external_key.eq(external_key_)))
                .set((input.eq(new_input_value),))
                .get_result::<OperationQueueDTO>(&con)
        })?;
        Ok(dto.try_into()?)
    }

    fn store_execution_result(
        &self,
        workflow_id_: WorkflowId,
        operation_name_: OperationName,
        result_: Result<OperationResult, WorkflowError>,
    ) -> Result<()> {
        use crate::db::schema::execution_results::dsl::*;
        let dto = ExecutionResultDTO::try_from((workflow_id_, operation_name_, result_))?;
        diesel::insert_into(execution_results)
            .values(&dto)
            .get_result::<ExecutionResultDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to store execution result: {:?}", err))?;
        Ok(())
    }

    fn complete_workflow(&self, workflow_id: WorkflowId) -> Result<()> {
        use crate::db::schema::workflows::dsl::*;
        let new_status = serde_json::to_value(WorkflowStatus::Completed).unwrap();
        diesel::update(workflows.find(workflow_id))
            .set(status.eq(serde_json::to_string(&new_status).unwrap()))
            .get_result::<WorkflowDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to dequeue operation: {}", err))?;
        Ok(())
    }

    fn complete_workflow_with_error(&self, workflow_id: WorkflowId, error: String) -> Result<()> {
        use crate::db::schema::workflows::dsl::*;
        let new_status =
            serde_json::to_value(WorkflowStatus::CompletedWithError(error.into())).unwrap();
        diesel::update(workflows.find(workflow_id))
            .set(status.eq(serde_json::to_string(&new_status).unwrap()))
            .get_result::<WorkflowDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to dequeue operation: {}", err))?;
        Ok(())
    }

    fn cancel_workflow(&self, workflow_id: WorkflowId, _reason: String) -> Result<()> {
        use crate::db::schema::workflows::dsl::*;
        let new_status = serde_json::to_value(WorkflowStatus::Cancelled).unwrap();
        diesel::update(workflows.find(workflow_id))
            .set(status.eq(serde_json::to_string(&new_status).unwrap()))
            .get_result::<WorkflowDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to dequeue operation: {}", err))?;
        Ok(())
    }

    fn abort_workflow_with_error(
        &self,
        workflow_id: WorkflowId,
        error: WorkflowError,
    ) -> Result<()> {
        use crate::db::schema::workflows::dsl::*;
        let new_status = serde_json::to_value(WorkflowStatus::Error(error.into())).unwrap();
        diesel::update(workflows.find(workflow_id))
            .set(status.eq(serde_json::to_string(&new_status).unwrap()))
            .get_result::<WorkflowDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to dequeue operation: {}", err))?;
        Ok(())
    }

    fn queue_operation(
        &self,
        execution_data: OperationExecutionData,
        run_date: DateTime<Utc>,
    ) -> Result<()> {
        use crate::db::schema::operations_queue::dsl::*;
        let mut dto: OperationQueueDTO = execution_data.try_into()?;
        dto.state = QueueState::Queued.to_string();
        dto.next_run_date = run_date;
        diesel::insert_into(operations_queue)
            .values(&dto)
            .on_conflict((workflow_id, operation_name, iteration))
            .do_update()
            .set(&dto)
            .get_result::<OperationQueueDTO>(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to queue operation: {}", err))?;
        Ok(())
    }

    fn dequeue_operation(
        &self,
        workflow_id_: WorkflowId,
        operation_name_: OperationName,
        iteration_: usize,
    ) -> Result<OperationExecutionData> {
        use crate::db::schema::operations_queue::dsl::*;
        let data = diesel::update(
            operations_queue
                .filter(workflow_id.eq(workflow_id_))
                .filter(operation_name.eq(operation_name_))
                .filter(iteration.eq(iteration_ as i32)),
        )
        .set(state.eq(QueueState::Dequeued.to_string()))
        .get_result::<OperationQueueDTO>(&self.db_pool.get()?)
        .map_err(|err| format_err!("Unable to dequeue operation: {}", err))?;
        Ok(data.try_into()?)
    }

    fn queue_all_operations(
        &self,
        operations: Vec<(OperationExecutionData, DateTime<Utc>)>,
    ) -> Result<()> {
        use crate::db::schema::operations_queue::dsl::*;
        let dtos = operations
            .into_iter()
            .map(|(data, run_date)| {
                let mut dto: OperationQueueDTO = data.try_into()?;
                dto.state = QueueState::Queued.to_string();
                dto.next_run_date = run_date;
                Ok(dto)
            })
            .collect::<Result<Vec<_>>>()?;
        diesel::insert_into(operations_queue)
            .values(&dtos)
            .execute(&self.db_pool.get()?)
            .map_err(|err| format_err!("Unable to queue operations: {}", err))?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db::sql_store::SqlStore;
    use crate::state::SAFE_RETRY_DURATION;
    use crate::OperationInput;
    use chrono::Duration;
    use diesel::r2d2::{CustomizeConnection, Error, PoolError};
    use serde_json::json;
    use uuid::Uuid;

    fn create_store() -> SqlStore {
        SqlStore::new_with_pool(new_test_db_pool("postgresql://gamba@127.0.0.1/evento").unwrap())
    }

    #[test]
    fn test_sql_store() {
        let wf_name = "test".to_string();
        let wf_id = Uuid::new_v4();
        let correlation_id = "correlationid".to_string();
        let context = json!({"name": "ricardo"});
        let operation_name = "test_operation".to_string();
        let store = create_store();
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
        assert!(matches!(wf.status, WorkflowStatus::Created));
        // Mark as completed
        store.complete_workflow(wf_id).unwrap();
        let wf = store.get_workflow(wf_id).unwrap().unwrap();
        assert!(matches!(wf.status, WorkflowStatus::Completed));
        // Store execution result
        let result_content = "test_result".to_string();
        let result_content_2 = "test_result2".to_string();
        let operation_result =
            OperationResult::new(result_content.clone(), 0, operation_name.clone()).unwrap();
        let operation_result_2 =
            OperationResult::new(result_content_2.clone(), 1, operation_name.clone()).unwrap();
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
            correlation_id: String::new(),
            retry_count: None,
            input: OperationInput::new(
                wf_name.clone(),
                operation_name.clone(),
                0,
                result_content.clone(),
            )
            .unwrap(),
        };
        // Try fetch an element with a run_date greater than current time
        let now = Utc::now();
        store
            .queue_operation(execution_data.clone(), now.clone())
            .unwrap();
        let results = store
            .fetch_operations(now.checked_sub_signed(Duration::seconds(5)).unwrap())
            .unwrap();
        assert!(results.is_empty());
        // Try fetching an element that is meant to be fetched
        let results = store.fetch_operations(now).unwrap();
        assert_eq!(1, results.len());
        assert_eq!(0, results.get(0).unwrap().retry_count.unwrap_or_default());
        // Try fetching again without dequeuing
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
                wf_name.clone(),
                operation_name.clone(),
                0,
                result_content.clone(),
                external_key.clone(),
            )
            .unwrap(),
        };
        store
            .queue_operation(
                execution_data,
                now.checked_add_signed(Duration::seconds(10)).unwrap(),
            )
            .unwrap();
        let ext_payload_input = serde_json::to_value(result_content_2.clone()).unwrap();
        let operation = store
            .complete_external_operation(external_key, ext_payload_input.clone())
            .unwrap();
        assert!(matches!(
            operation.input.external_input,
            Some(x) if x == ext_payload_input
        ));
        store
            .find_wait_operation(external_key.clone())
            .unwrap()
            .unwrap();
    }

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
}
