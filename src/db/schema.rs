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
    issues (id) {
        id -> Bpchar,
        column_id -> Bpchar,
        title -> Varchar,
        body -> Text,
    }
}

allow_tables_to_appear_in_same_query!(
    boards,
    columns,
    issues,
);
