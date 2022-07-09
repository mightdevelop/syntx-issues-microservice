CREATE TABLE dependencies (
    id CHAR(36) PRIMARY KEY,
    blocking_epic_id CHAR(36) NOT NULL,
    blocked_epic_id CHAR(36) NOT NULL
);