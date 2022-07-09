table! {
    boards (id) {
        id -> Bpchar,
        project_id -> Bpchar,
    }
}

table! {
    columns (id) {
        id -> Bpchar,
        board_id -> Bpchar,
        name -> Varchar,
    }
}

table! {
    dependencies (id) {
        id -> Bpchar,
        blocking_epic_id -> Bpchar,
        blocked_epic_id -> Bpchar,
    }
}

table! {
    epics (id) {
        id -> Bpchar,
        column_id -> Bpchar,
        assignee_id -> Nullable<Bpchar>,
        reporter_id -> Bpchar,
        name -> Varchar,
        description -> Nullable<Text>,
        start_date -> Timestamptz,
        due_date -> Timestamptz,
    }
}

table! {
    issues (id) {
        id -> Bpchar,
        column_id -> Bpchar,
        epic_id -> Bpchar,
        title -> Varchar,
        description -> Text,
    }
}

allow_tables_to_appear_in_same_query!(
    boards,
    columns,
    dependencies,
    epics,
    issues,
);
