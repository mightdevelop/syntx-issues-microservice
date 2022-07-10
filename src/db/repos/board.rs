use diesel::result::Error;

use crate::db;
use db::schema::boards;

use diesel::{
    RunQueryDsl,
    r2d2::ConnectionManager,
    PgConnection,
    ExpressionMethods,
    insert_into,
    delete
};
use r2d2::PooledConnection;

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

#[tonic::async_trait]
pub trait CreateBoard {
    async fn create<'a>(
        new_board: NewBoard<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Board, Error>;
}

#[tonic::async_trait]
impl CreateBoard for Board {
    async fn create<'a>(
        new_board: NewBoard<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Board, Error> {
        let result: Vec<Board> = match insert_into(boards::dsl::boards)
            .values(new_board)
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Board {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait DeleteBoard {
    async fn delete<'a>(
        board_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Board, Error>;
}

#[tonic::async_trait]
impl DeleteBoard for Board {
    async fn delete<'a>(
        board_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Board, Error> {
        let result: Vec<Board> = match delete(boards::dsl::boards)
            .filter(boards::dsl::id.eq(board_id))
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let board: &Board = match result.first() {
            Some(brd) => brd,
            None => return Err(Error::NotFound),
        };

        Ok(Board {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        })
    }
}