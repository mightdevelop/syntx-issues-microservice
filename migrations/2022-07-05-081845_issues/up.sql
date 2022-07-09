CREATE TABLE issues (
    id CHAR(36) PRIMARY KEY,
    column_id CHAR(36) NOT NULL,
    epic_id CHAR(36) NOT NULL,
    title VARCHAR(50) NOT NULL,
    description TEXT NOT NULL
);