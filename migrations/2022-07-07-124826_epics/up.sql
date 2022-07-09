CREATE TABLE epics (
    id CHAR(36) PRIMARY KEY,
    column_id CHAR(36) NOT NULL,
    assignee_id CHAR(36),
    reporter_id CHAR(36) NOT NULL,
    name VARCHAR(50) NOT NULL,
    description TEXT,
    start_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    due_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '1' day
);