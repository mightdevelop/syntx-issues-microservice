use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods};
use tonic::{Request, Response, Status, Code};
use proto::{
    issues::{
        Board as ProtoBoard,
        BoardId,
        ProjectId,
        boards_service_server::BoardsService
    },
};

use crate::{
    db::{
        repos::board::{Board, NewBoard, DeleteBoard, CreateBoard},
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
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Board> = boards
            .filter(id.eq(&request.get_ref().board_id))
            .limit(1)
            .load::<Board>(&*db_connection)
            .expect("Get board by id error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        }))
    }

    async fn get_board_by_project_id(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Board> = boards
            .filter(project_id.eq(&request.get_ref().project_id))
            .limit(1)
            .load::<Board>(&*db_connection)
            .expect("Get board by project id error");

        let board: &Board = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoBoard {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        }))
    }

    async fn create_board(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let new_board = NewBoard {
            id: &uuid::Uuid::new_v4().to_string(),
            project_id: &request.get_ref().project_id,
        };

        let board: Board = match Board::create(new_board, db_connection).await {
            Ok(brd) => brd,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoBoard {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        }))
    }

    async fn delete_board(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let board: Board;
        
        match Board::delete(&data.board_id, db_connection).await {
            Ok(brd) => board = brd,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoBoard {
            id: board.id.clone(),
            project_id: board.project_id.clone(),
        }))
    }
}