use std::io::Error;

use crate::db;
use db::schema::columns;

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

#[derive(AsChangeset)]
#[table_name="columns"]
pub struct ColumnChangeSet {
    pub name: Option<String>,
}

#[tonic::async_trait]
pub trait CreateColumn {
    async fn create<'a>(
        new_column: NewColumn<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error>;
}

#[tonic::async_trait]
impl CreateColumn for Column {
    async fn create<'a>(
        new_column: NewColumn<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error> {
        let result: Vec<Column> = insert_into(columns::dsl::columns)
            .values(new_column)
            .get_results(&*db_connection)
            .expect("Create column error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Column {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait UpdateColumn {
    async fn update<'a>(
        column_id: &'a str,
        change_set: ColumnChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error>;
}

#[tonic::async_trait]
impl UpdateColumn for Column {
    async fn update<'a>(
        column_id: &'a str,
        change_set: ColumnChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error> {
        let result: Vec<Column> = update(columns::dsl::columns)
            .filter(columns::dsl::id.eq(column_id))
            .set(change_set)
            .get_results(&*db_connection)
            .expect("Update column error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Column {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait DeleteColumn {
    async fn delete<'a>(
        column_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error>;
}

#[tonic::async_trait]
impl DeleteColumn for Column {
    async fn delete<'a>(
        column_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Column, Error> {
        let result: Vec<Column> = delete(columns::dsl::columns)
            .filter(columns::dsl::id.eq(column_id))
            .get_results(&*db_connection)
            .expect("Update column error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Column {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        })
    }
}