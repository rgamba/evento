use anyhow::{format_err, Result};
use chrono::{DateTime, Utc};
use evento_api::{
    run, wait_for_external, ExternalOperationInput, Operation, OperationInput, OperationResult,
    Workflow, WorkflowError, WorkflowStatus,
};
use evento_derive::workflow;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Clone)]
pub struct ProposalApprovalWorkflowContext {
    pub operation_type: OperationType,
    pub body: serde_json::Value,
}

#[workflow]
pub struct ProposalApprovalWorkflow {
    context: ProposalApprovalWorkflowContext,
}
impl Workflow for ProposalApprovalWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        let proposal: Proposal = run!(self, GenerateProposal<Proposal>(ProposalInput{
            operation_type: self.context.operation_type.clone(),
            body: self.context.body.clone(),
        }));
        run!(self, NotifyApprovers<bool>(proposal.clone()));
        let mut approval_count = 0;
        while approval_count < self.required_approvals(&proposal) {
            approval_count = match wait_for_external!(self, ProcessApproval<ProcessApprovalResult>(proposal.clone()), self.proposal_expiration(&proposal))
            {
                ProcessApprovalResult::Ok(new_count) => new_count,
                ProcessApprovalResult::Declined(reason) => {
                    return Ok(WorkflowStatus::CompletedWithError(
                        format!("Proposal declined: {}", reason).into(),
                    ));
                }
            };
        }
        /*match proposal.operation_type {
            OperationType::Mint => {
                wait_for_external!(self, WaitForFiatReceived<_>(proposal.clone()));
            }
            OperationType::Burn => {
                wait_for_external!(self, WaitForReserveOutToDD<_>(proposal.clone()));
            }
        };
        run!(self, GenerateTxn<_>(proposal.clone()));*/
        Ok(WorkflowStatus::Completed)
    }
}

impl ProposalApprovalWorkflow {
    fn required_approvals(&self, proposal: &Proposal) -> usize {
        return 3;
    }

    fn proposal_expiration(&self, proposal: &Proposal) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum OperationType {
    Mint,
    Burn,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Proposal {
    pub operation_type: OperationType,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProposalInput {
    pub operation_type: OperationType,
    pub body: serde_json::Value,
}

pub struct GenerateProposal;
impl Operation for GenerateProposal {
    fn new() -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn execute(
        &self,
        input: OperationInput,
        external_input: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError> {
        let new_input = input.value::<ProposalInput>().unwrap();
        Ok(OperationResult::new(
            serde_json::to_value(Proposal {
                operation_type: new_input.operation_type,
            })
            .unwrap(),
            input.iteration,
            self.name().into(),
        )
        .unwrap())
    }

    fn name(&self) -> &str {
        "GenerateProposal"
    }

    fn validate_input(input: &OperationInput)
    where
        Self: Sized,
    {
        input.value::<ProposalInput>().unwrap();
    }
}

pub struct NotifyApprovers {}
impl Operation for NotifyApprovers {
    fn new() -> Self {
        Self {}
    }

    fn name(&self) -> &str {
        "NotifyApprovers"
    }

    fn execute(
        &self,
        input: OperationInput,
        _: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError> {
        Ok(OperationResult::new(
            serde_json::to_value(true).unwrap(),
            input.iteration,
            self.name().into(),
        )
        .unwrap())
    }

    fn validate_input(input: &OperationInput) {
        input.value::<Proposal>().unwrap();
    }
}

pub struct ProcessApproval;
impl Operation for ProcessApproval {
    fn new() -> Self
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn execute(
        &self,
        input: OperationInput,
        external_input: Option<ExternalOperationInput>,
    ) -> Result<OperationResult, WorkflowError> {
        Ok(OperationResult::new(
            serde_json::to_value(ProcessApprovalResult::Ok(input.iteration + 1)).unwrap(),
            input.iteration,
            self.name().into(),
        )
        .unwrap())
    }

    fn name(&self) -> &str {
        "ProcessApproval"
    }

    fn validate_input(input: &OperationInput)
    where
        Self: Sized,
    {
        input.value::<Proposal>().unwrap();
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub enum ProcessApprovalResult {
    Ok(usize),
    Declined(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use evento_api::tests::run_to_completion;

    #[test]
    fn test_run() {
        let mut operation_map: HashMap<String, Box<dyn Operation>> = HashMap::new();
        operation_map.insert("GenerateProposal".into(), Box::new(GenerateProposal {}));
        operation_map.insert("NotifyApprovers".into(), Box::new(NotifyApprovers {}));
        operation_map.insert("ProcessApproval".into(), Box::new(ProcessApproval {}));
        let context = ProposalApprovalWorkflowContext {
            operation_type: OperationType::Mint,
            body: serde_json::Value::default(),
        };
        match run_to_completion(
            Box::new(ProposalApprovalWorkflowFactory {}),
            serde_json::to_value(context).unwrap(),
            operation_map,
        ) {
            Ok((status, results)) => {
                println!("OK");
            }
            Err(err) => {
                panic!("Failed to complete")
            }
        }
    }
}
