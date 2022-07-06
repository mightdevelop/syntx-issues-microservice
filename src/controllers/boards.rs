use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, insert_into, delete};
use tonic::{Request, Response, Status};
use proto::{
    boards::{
        Board as ProtoBoard,
        BoardId,
        ProjectId,
        boards_service_server::BoardsService
    },
};

use crate::{
    db::{
        models::Board,
        models::NewBoard,
        schema::boards::dsl::*, 
        connection::PgPool,
    },
};

pub struct BoardsController {
    pub pool: PgPool
}

#[tonic::async_trait]
impl BoardsService for BoardsController {
    async fn get_board_by_id(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Board> = boards
            .filter(id.eq(&request.get_ref().board_id))
            .limit(1)
            .load::<Board>(&*db_connection)
            .expect("Get board by id error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: String::from(&board.id),
            project_id: String::from(&board.project_id)
        }))
    }

    async fn get_board_by_project_id(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Board> = boards
            .filter(project_id.eq(&request.get_ref().project_id))
            .limit(1)
            .load::<Board>(&*db_connection)
            .expect("Get board by project id error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: String::from(&board.id),
            project_id: String::from(&board.project_id)
        }))
    }

    async fn create_board(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().unwrap();
        let new_board = NewBoard {
            id: &uuid::Uuid::new_v4().to_string(),
            project_id: &request.get_ref().project_id,
        };
        let result: Vec<Board> = insert_into(boards)
            .values(new_board)
            .get_results(&*db_connection)
            .expect("Create board error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: String::from(&board.id),
            project_id: String::from(&board.project_id)
        }))
    }

    async fn delete_board(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Board> = delete(boards)
            .filter(id.eq(&request.get_ref().board_id))
            .get_results(&*db_connection)
            .expect("Delete board by id error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: String::from(&board.id),
            project_id: String::from(&board.project_id)
        }))
    }
}