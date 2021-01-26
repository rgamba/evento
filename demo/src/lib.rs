use anyhow::Result;
use chrono::{DateTime, Utc};
use evento_api::{
    operation_ok, parse_input, run, wait_for_external, Operation, OperationInput, OperationResult,
    Workflow, WorkflowError, WorkflowStatus,
};
use evento_derive::workflow;
use serde::{Deserialize, Serialize};

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
            approval_count = match wait_for_external!(self, ProcessApproval<ProcessApprovalResult>(proposal.clone()), self.proposal_expiration(&proposal), proposal.id)
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
    fn required_approvals(&self, _proposal: &Proposal) -> usize {
        return 3;
    }

    fn proposal_expiration(&self, _proposal: &Proposal) -> DateTime<Utc> {
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
    fn name(&self) -> &str {
        "GenerateProposal"
    }

    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        let new_input = parse_input!(input, ProposalInput);
        operation_ok!(Proposal {
            operation_type: new_input.operation_type,
        })
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        input.value::<ProposalInput>().map(|_| ())
    }
}

pub struct NotifyApprovers {}
impl Operation for NotifyApprovers {
    fn name(&self) -> &str {
        "NotifyApprovers"
    }

    fn execute(&self, _input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        operation_ok!(true)
    }

    fn validate_input(input: &OperationInput) -> Result<()> {
        input.value::<Proposal>().map(|_| ())
    }
}

pub struct ProcessApproval;
impl Operation for ProcessApproval {
    fn name(&self) -> &str {
        "ProcessApproval"
    }

    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        operation_ok!(ProcessApprovalResult::Ok(input.iteration + 1))
    }

    fn validate_input(input: &OperationInput) -> Result<()>
    where
        Self: Sized,
    {
        input.value::<Proposal>().map(|_| ())
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub enum ProcessApprovalResult {
    Ok(usize),
    Declined(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use evento_api::tests::{run_to_completion, MockOperation};

    #[test]
    fn test_run() {
        let gen_proposal = MockOperation::new("GenerateProposal", |_| {
            operation_ok!(Proposal {
                operation_type: OperationType::Burn,
            })
        });

        let mut operation_map: HashMap<String, Box<dyn Operation>> = HashMap::new();
        operation_map.insert("GenerateProposal".into(), Box::new(gen_proposal));
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
                panic!("Failed to complete: {:?}", err)
            }
        }
    }
}
