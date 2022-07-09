use std::io::Error;

use crate::db;
use db::schema::issues;

use diesel::{
    RunQueryDsl,
    r2d2::ConnectionManager,
    PgConnection,
    ExpressionMethods,
    insert_into,
    update,
    delete
};
use r2d2::PooledConnection;

#[derive(Queryable)]
pub struct Issue {
    pub id: String,
    pub column_id: String,
    pub epic_id: String,
    pub title: String,
    pub description: String,
}

#[derive(Insertable)]
#[table_name="issues"]
pub struct NewIssue<'a> {
    pub id: &'a str,
    pub column_id: &'a str,
    pub epic_id: &'a str,
    pub title: &'a str,
    pub description: &'a str,
}

#[derive(AsChangeset)]
#[table_name="issues"]
pub struct IssueChangeSet {
    pub column_id: Option<String>,
    pub epic_id: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[tonic::async_trait]
pub trait CreateIssue {
    async fn create<'a>(
        new_issue: NewIssue<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error>;
}

#[tonic::async_trait]
impl CreateIssue for Issue {
    async fn create<'a>(
        new_issue: NewIssue<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error> {
        let result: Vec<Issue> = insert_into(issues::dsl::issues)
            .values(new_issue)
            .get_results(&*db_connection)
            .expect("Create issue error");

        let issue: &Issue = result
            .first()
            .unwrap();

        Ok(Issue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait UpdateIssue {
    async fn update<'a>(
        issue_id: &'a str,
        change_set: IssueChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error>;
}

#[tonic::async_trait]
impl UpdateIssue for Issue {
    async fn update<'a>(
        issue_id: &'a str,
        change_set: IssueChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error> {
        let result: Vec<Issue> = update(issues::dsl::issues)
            .filter(issues::dsl::id.eq(issue_id))
            .set(change_set)
            .get_results(&*db_connection)
            .expect("Update issue error");

        let issue: &Issue = result
            .first()
            .unwrap();

        Ok(Issue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait DeleteIssue {
    async fn delete<'a>(
        issue_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error>;
}

#[tonic::async_trait]
impl DeleteIssue for Issue {
    async fn delete<'a>(
        issue_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Issue, Error> {
        let result: Vec<Issue> = delete(issues::dsl::issues)
            .filter(issues::dsl::id.eq(issue_id))
            .get_results(&*db_connection)
            .expect("Update issue error");

        let issue: &Issue = result
            .first()
            .unwrap();

        Ok(Issue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        })
    }
}