table! {
    execution_results (id) {
        id -> Uuid,
        workflow_id -> Uuid,
        is_error -> Bool,
        operation_name -> Varchar,
        iteration -> Int4,
        result -> Json,
        created_at -> Timestamptz,
        error -> Nullable<Json>,
    }
}

table! {
    operations_queue (id) {
        id -> Uuid,
        workflow_id -> Uuid,
        correlation_id -> Varchar,
        retry_count -> Int4,
        input -> Json,
        state -> Bpchar,
        next_run_date -> Timestamptz,
        external_key -> Nullable<Uuid>,
        operation_name -> Text,
        iteration -> Int4,
    }
}

table! {
    workflows (id) {
        id -> Uuid,
        correlation_id -> Varchar,
        name -> Varchar,
        status -> Varchar,
        context -> Json,
        created_at -> Timestamptz,
    }
}

allow_tables_to_appear_in_same_query!(
    execution_results,
    operations_queue,
    workflows,
);
