CREATE TABLE issues (
    id CHAR(36) PRIMARY KEY,
    column_id CHAR(36) NOT NULL,
    title VARCHAR(50) NOT NULL,
    body TEXT NOT NULL
);