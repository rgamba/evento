CREATE TABLE execution_results (
    id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    is_error boolean NOT NULL DEFAULT false,
    operation_name character varying NOT NULL,
    iteration integer NOT NULL DEFAULT 0,
    result json NOT NULL,
    created_at timestamp with time zone NOT NULL
);

-- Indices -------------------------------------------------------

CREATE INDEX execution_results_workflow_id ON execution_results(workflow_id);
CREATE INDEX execution_results_operation_name ON execution_results(workflow_id ,operation_name);

CREATE TABLE operations_queue (
    id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    correlation_id uuid NOT NULL,
    retry_count integer DEFAULT 0,
    external_key character varying,
    input json,
    state character(1) NOT NULL,
    next_run_date timestamp with time zone NOT NULL
);

-- Indices -------------------------------------------------------

CREATE INDEX operations_queue_workflow_id ON operations_queue(workflow_id);
CREATE INDEX operations_queue_correlation_id ON operations_queue(correlation_id);
CREATE UNIQUE INDEX operations_queue_external_key ON operations_queue(external_key);
CREATE INDEX operations_queue_next_run_data ON operations_queue(next_run_date);
CREATE INDEX operations_queue_state ON operations_queue(state);

CREATE TABLE workflows (
    id uuid PRIMARY KEY,
    correlation_id uuid NOT NULL,
    name character varying NOT NULL,
    status character varying(50) NOT NULL,
    context json,
    created_at timestamp with time zone NOT NULL
);

-- Indices -------------------------------------------------------

CREATE UNIQUE INDEX workflows_correlation_id ON workflows(name, correlation_id);