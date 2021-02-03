table! {
    execution_results (id) {
        id -> Uuid,
        workflow_id -> Uuid,
        is_error -> Bool,
        operation_name -> Varchar,
        iteration -> Int4,
        result -> Json,
        created_at -> Timestamptz,
    }
}

table! {
    operations_queue (id) {
        id -> Uuid,
        workflow_id -> Uuid,
        correlation_id -> Uuid,
        retry_count -> Nullable<Int4>,
        external_key -> Nullable<Varchar>,
        input -> Nullable<Json>,
        state -> Bpchar,
        next_run_date -> Timestamptz,
    }
}

table! {
    workflows (id) {
        id -> Uuid,
        correlation_id -> Uuid,
        name -> Varchar,
        status -> Varchar,
        context -> Nullable<Json>,
        created_at -> Timestamptz,
    }
}

allow_tables_to_appear_in_same_query!(
    execution_results,
    operations_queue,
    workflows,
);
