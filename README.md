# evento

evento is a workflow engine that allows creating complex workflows in a declarative fashion.
The main principle is to let the use express the domain logic in the cleanest way possible without
having to worry about low level stuff like error handling, retries and orchestration.

## Defining workflows

In evento, a workflow logic is defined in a single function `run` which takes a workflow context as
input, calls one or more `Operation`s in order to complete the workflow, and completes.

Workflow definitions must be deterministic. All mutations and side effects must be carried out by 
the `Operation`s.

### Example

```rust
#[workflow]
struct DiscountWorkflow {
    context: DiscountDetails,
}

impl Workflow for DiscountWorkflow {
    fn run(&self) -> Result<WorkflowStatus, WorkflowError> {
        if self.context.discount_amount > 50 {
            let supervisor_approval = wait!(self, GetApproval<bool>("supervisor@email.com"));
            if !supervisor_approval {
                return self.complete_with_error("Approval got rejected");
            }
        }
        let discount_id = run!(self, CreateDiscount<Uuid>(context));
        run!(self, SendNotification(context.user_email));
        Ok(WorkflowStatus::Completed)
    }
}
```

## Defining operations

Operations are stateless components that perform idempotent actions that typically mutate state or produce side effects.
Operation executions are expected to fail and are automatically retried by the workflow engine.

### Example

```rust
struct CreateDiscount;
impl Operation for CreateDiscount {
    fn execute(&self, input: OperationInput) -> Result<serde_json::Value, WorkflowError> {
        let discount = parse_input!(input, DiscountDetails);
        // Talk to the DB or any external system here...
        let id = self.create_id_from_external(&discount);
        operation_ok!(id);
    }
}
```

## Putting it all together

//TODO

## Admin UI

//TODO