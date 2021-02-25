use crate::db::schema::*;
use crate::db::sql_store::QueueState;
use crate::state::OperationExecutionData;
use crate::{
    OperationName, OperationResult, WorkflowData, WorkflowError, WorkflowId, WorkflowStatus,
};
use anyhow::{bail, ensure, format_err, Context};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

#[derive(
    Clone,
    Queryable,
    QueryableByName,
    Identifiable,
    Debug,
    AsChangeset,
    Insertable,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[primary_key("id")]
#[table_name = "workflows"]
pub struct WorkflowDTO {
    pub id: Uuid,
    pub correlation_id: String,
    pub name: String,
    pub status: String,
    pub context: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub status_data: Option<serde_json::Value>,
}

impl TryInto<WorkflowData> for WorkflowDTO {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<WorkflowData, Self::Error> {
        let status = match self.status.as_str() {
            "Created" => WorkflowStatus::Created,
            "Completed" => WorkflowStatus::Completed,
            "CompletedWithError" | "WaitForExternal" | "RunNext" | "Error" => {
                serde_json::from_value::<WorkflowStatus>(
                    self.status_data
                        .ok_or_else(|| format_err!("status_data is required to be not null"))?,
                )?
            }
            "Cancelled" => WorkflowStatus::Cancelled,
            _ => bail!("Invalid workflow status provided: {:?}", self.status),
        };
        ensure!(
            self.status == status.to_string_without_data(),
            "Status and status data are inconsistent!"
        );
        Ok(WorkflowData {
            id: self.id,
            name: self.name.clone(),
            correlation_id: self.correlation_id,
            status,
            created_at: self.created_at,
            context: self.context,
        })
    }
}

#[derive(
    Clone,
    Queryable,
    QueryableByName,
    Identifiable,
    Debug,
    AsChangeset,
    Insertable,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[primary_key("id")]
#[table_name = "operations_queue"]
pub struct OperationQueueDTO {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub correlation_id: String,
    pub retry_count: i32,
    pub input: serde_json::Value,
    pub state: String,
    pub next_run_date: DateTime<Utc>,
    pub external_key: Option<Uuid>,
    pub operation_name: String,
    pub iteration: i32,
}

impl TryInto<OperationExecutionData> for OperationQueueDTO {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<OperationExecutionData, Self::Error> {
        Ok(OperationExecutionData {
            workflow_id: self.workflow_id,
            correlation_id: self.correlation_id.clone(),
            retry_count: Some(self.retry_count as usize),
            input: serde_json::from_value(self.input)
                .context("Unable to deserialize WorkflowInput")?,
        })
    }
}

impl TryFrom<OperationExecutionData> for OperationQueueDTO {
    type Error = anyhow::Error;

    fn try_from(value: OperationExecutionData) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Uuid::new_v4(),
            workflow_id: value.workflow_id,
            correlation_id: value.correlation_id,
            retry_count: value.retry_count.unwrap_or(0) as i32,
            input: serde_json::to_value(value.input.clone())?,
            state: QueueState::Queued.to_string(),
            next_run_date: Utc::now(),
            external_key: value.input.external_key,
            operation_name: value.input.operation_name.clone(),
            iteration: value.input.iteration as i32,
        })
    }
}

#[derive(
    Clone,
    Queryable,
    QueryableByName,
    Identifiable,
    Debug,
    AsChangeset,
    Insertable,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[primary_key("id")]
#[table_name = "execution_results"]
pub struct ExecutionResultDTO {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub is_error: bool,
    pub operation_name: String,
    pub iteration: i32,
    pub result: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub error: Option<serde_json::Value>,
    pub operation_input: serde_json::Value,
}

impl ExecutionResultDTO {
    pub fn to_operation_result(
        &self,
    ) -> anyhow::Result<anyhow::Result<OperationResult, WorkflowError>> {
        if self.is_error {
            if self.error.is_none() {
                bail!(
                    "Operation marked as errored is expected to have error body: {:?}",
                    self
                )
            }
            let error: WorkflowError = serde_json::from_value(self.error.clone().unwrap())?;
            Ok(Err(error))
        } else {
            Ok(Ok(OperationResult {
                result: self.result.clone(),
                iteration: self.iteration as usize,
                created_at: self.created_at,
                operation_name: self.operation_name.clone(),
                operation_input: serde_json::from_value(self.operation_input.clone())?,
            }))
        }
    }
}

impl
    TryFrom<(
        WorkflowId,
        OperationName,
        Result<OperationResult, WorkflowError>,
    )> for ExecutionResultDTO
{
    type Error = anyhow::Error;

    fn try_from(
        (workflow_id, operation_name, value): (
            WorkflowId,
            OperationName,
            Result<OperationResult, WorkflowError>,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Uuid::new_v4(),
            workflow_id,
            is_error: value.is_err(),
            operation_name,
            iteration: if let Ok(r) = value.clone() {
                r.iteration as i32
            } else {
                -1
            },
            result: if let Ok(r) = value.clone() {
                r.result
            } else {
                serde_json::Value::Null
            },
            created_at: Utc::now(),
            error: if let Err(err) = value.clone() {
                Some(serde_json::to_value(err).unwrap())
            } else {
                None
            },
            operation_input: if let Ok(r) = value {
                serde_json::to_value(r.operation_input)?
            } else {
                serde_json::Value::Null
            },
        })
    }
}
