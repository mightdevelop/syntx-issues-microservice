use diesel::result::Error;

use crate::db;
use db::schema::epics;


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

use chrono::NaiveDateTime;

#[derive(Queryable, PartialEq)]
pub struct Epic {
    pub id: String,
    pub column_id: String,
    pub assignee_id: Option<String>,
    pub name: String,
    pub reporter_id: String,
    pub description: Option<String>,
    pub start_date: NaiveDateTime,
    pub due_date: NaiveDateTime,
}

#[derive(Insertable)]
#[table_name="epics"]
pub struct NewEpic<'a> {
    pub id: &'a str,
    pub column_id: &'a str,
    pub assignee_id: Option<&'a str>,
    pub reporter_id: &'a str,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub start_date: Option<NaiveDateTime>,
    pub due_date: Option<NaiveDateTime>,
}

#[derive(AsChangeset)]
#[table_name="epics"]
pub struct EpicChangeSet {
    pub column_id: Option<String>,
    pub assignee_id: Option<String>,
    pub name: Option<String>,
    pub reporter_id: Option<String>,
    pub description: Option<String>,
    pub start_date: Option<NaiveDateTime>,
    pub due_date: Option<NaiveDateTime>,
}

#[tonic::async_trait]
pub trait CreateEpic {
    async fn create<'a>(
        new_epic: NewEpic<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error>;
}

#[tonic::async_trait]
impl CreateEpic for Epic {
    async fn create<'a>(
        new_epic: NewEpic<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error> {
        let result: Vec<Epic> = match insert_into(epics::dsl::epics)
            .values(new_epic)
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let epic: &Epic = result
            .first()
            .unwrap();

        Ok(Epic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            name: epic.name.clone(),
            reporter_id: epic.reporter_id.clone(),
            start_date: epic.start_date.clone(),
            due_date: epic.due_date.clone(),
            description: epic.description.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait UpdateEpic {
    async fn update<'a>(
        epic_id: &'a str,
        change_set: EpicChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error>;
}

#[tonic::async_trait]
impl UpdateEpic for Epic {
    async fn update<'a>(
        epic_id: &'a str,
        change_set: EpicChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error> {
        let result: Vec<Epic> = match update(epics::dsl::epics)
            .filter(epics::dsl::id.eq(epic_id))
            .set(change_set)
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let epic: &Epic = match result.first() {
            Some(ep) => ep,
            None => return Err(Error::NotFound),
        };

        Ok(Epic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            name: epic.name.clone(),
            reporter_id: epic.reporter_id.clone(),
            start_date: epic.start_date.clone(),
            due_date: epic.due_date.clone(),
            description: epic.description.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait DeleteEpic {
    async fn delete<'a>(
        epic_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error>;
}

#[tonic::async_trait]
impl DeleteEpic for Epic {
    async fn delete<'a>(
        epic_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Epic, Error> {
        let result: Vec<Epic> = match delete(epics::dsl::epics)
            .filter(epics::dsl::id.eq(epic_id))
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let epic: &Epic = match result.first() {
            Some(ep) => ep,
            None => return Err(Error::NotFound),
        };

        Ok(Epic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            name: epic.name.clone(),
            reporter_id: epic.reporter_id.clone(),
            start_date: epic.start_date.clone(),
            due_date: epic.due_date.clone(),
            description: epic.description.clone(),
        })
    }
}