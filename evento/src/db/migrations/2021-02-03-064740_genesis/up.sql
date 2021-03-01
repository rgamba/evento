CREATE TABLE execution_results (
    id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    is_error boolean NOT NULL DEFAULT false,
    operation_name character varying NOT NULL,
    iteration integer NOT NULL DEFAULT 0,
    result json NOT NULL,
    created_at timestamp with time zone NOT NULL,
    error json,
    operation_input json NOT NULL
);

-- Indices -------------------------------------------------------

CREATE INDEX execution_results_workflow_id ON execution_results(workflow_id);
CREATE INDEX execution_results_operation_name ON execution_results(workflow_id,operation_name);
CREATE UNIQUE INDEX execution_results_success_unique ON execution_results(workflow_id,operation_name,iteration,is_error) WHERE NOT is_error = true;

CREATE TABLE operations_queue (
    id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    correlation_id character varying NOT NULL,
    retry_count integer NOT NULL DEFAULT 0,
    input json NOT NULL,
    state character(1) NOT NULL,
    next_run_date timestamp with time zone NOT NULL,
    external_key uuid,
    operation_name text NOT NULL,
    iteration integer NOT NULL DEFAULT 0
);

-- Indices -------------------------------------------------------

CREATE INDEX operations_queue_workflow_id ON operations_queue(workflow_id);
CREATE INDEX operations_queue_next_run_data ON operations_queue(next_run_date);
CREATE INDEX operations_queue_state ON operations_queue(state);
CREATE INDEX operations_queue_correlation_id ON operations_queue(correlation_id);
CREATE UNIQUE INDEX operations_queue_external_key ON operations_queue(external_key, state) WHERE NOT state = 'D';
CREATE UNIQUE INDEX operations_queue_wf_id_name_iter ON operations_queue(workflow_id,operation_name,iteration);

CREATE TABLE workflows (
    id uuid PRIMARY KEY,
    correlation_id character varying NOT NULL,
    name character varying NOT NULL,
    status character varying(200) NOT NULL,
    context json NOT NULL,
    created_at timestamp with time zone NOT NULL
);

-- Indices -------------------------------------------------------

CREATE UNIQUE INDEX workflows_correlation_id ON workflows(name,correlation_id);
