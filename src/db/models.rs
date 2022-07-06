use super::schema::{boards, columns, issues};

#[derive(Queryable)]
pub struct Board {
    pub id: String,
    pub project_id: String,
}

#[derive(Insertable)]
#[table_name="boards"]
pub struct NewBoard<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
}

#[derive(Queryable)]
pub struct Column {
    pub id: String,
    pub board_id: String,
    pub name: String,
}

#[derive(Insertable)]
#[table_name="columns"]
pub struct NewColumn<'a> {
    pub id: &'a str,
    pub board_id: &'a str,
    pub name: &'a str,
}

#[derive(Queryable)]
pub struct Issue {
    pub id: String,
    pub column_id: String,
    pub title: String,
    pub body: String,
}

#[derive(Insertable)]
#[table_name="issues"]
pub struct NewIssue<'a> {
    pub id: &'a str,
    pub column_id: &'a str,
    pub title: &'a str,
    pub body: &'a str,
}