use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, QueryResult, result::Error::NotFound};
use tonic::{Request, Response, Status, Code, transport::Channel};
use proto::{
    issues::{
        Board as ProtoBoard,
        BoardId,
        ProjectId,
        boards_service_server::BoardsService
    }, 
    eventbus::{
        self,
        boards_events_service_client::BoardsEventsServiceClient,
        BoardEvent,
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
    pub pool: PgPool,
    pub eventbus_service_client: BoardsEventsServiceClient<Channel>
}

#[tonic::async_trait]
impl BoardsService for BoardsController {
    async fn get_board_by_id(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let result: QueryResult<Vec<Board>> = boards
            .filter(id.eq(data.board_id.clone()))
            .limit(1)
            .load::<Board>(&*db_connection);

        match result {
            Ok(vec) => {
                if let Some(brd) = vec.first() {
                    let board = eventbus::Board {
                        id: Some(brd.id.clone()),
                        project_id: Some(brd.project_id.clone())
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_board_by_id_event(req).await;
                    });
                    Ok(Response::new(ProtoBoard {
                        id: brd.id.clone(),
                        project_id: brd.project_id.clone(),
                    }))
                } else {
                    let board = eventbus::Board {
                        id: Some(data.board_id.clone()),
                        project_id: None
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Board not found")
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn( async move {
                        service.get_board_by_id_event(req).await;
                    });
                    Err(Status::not_found("Board not found"))
                }
            }
            Err(err) => {
                let board = eventbus::Board {
                    id: Some(data.board_id.clone()),
                    project_id: None
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(BoardEvent {
                    board: Some(board),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_board_by_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    async fn get_board_by_project_id(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let result: QueryResult<Vec<Board>> = boards
            .filter(project_id.eq(&request.get_ref().project_id))
            .limit(1)
            .load::<Board>(&*db_connection);

        match result {
            Ok(vec) => {
                if let Some(brd) = vec.first() {
                    let board = eventbus::Board {
                        id: Some(brd.id.clone()),
                        project_id: Some(brd.project_id.clone())
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_board_by_project_id_event(req).await;
                    });
                    Ok(Response::new(ProtoBoard {
                        id: brd.id.clone(),
                        project_id: brd.project_id.clone(),
                    }))
                } else {
                    let board = eventbus::Board {
                        id: None,
                        project_id: Some(data.project_id.clone())
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Board not found")
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_board_by_project_id_event(req).await;
                    });
                    Err(Status::not_found("Board not found"))
                }
            }
            Err(err) => {
                let board = eventbus::Board {
                    id: None,
                    project_id: Some(data.project_id.clone())
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(BoardEvent {
                    board: Some(board),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_board_by_project_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    async fn create_board(
        &self,
        request: Request<ProjectId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        let new_board = NewBoard {
            id: &uuid::Uuid::new_v4().to_string(),
            project_id: &request.get_ref().project_id,
        };

        match Board::create(new_board, db_connection).await {
            Ok(brd) => {
                let board = eventbus::Board {
                    id: Some(brd.id.clone()),
                    project_id: Some(brd.project_id.clone())
                };
                let req = Request::new(BoardEvent {
                    board: Some(board),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_board_event(req).await;
                });
                Ok(Response::new(ProtoBoard {
                    id: brd.id.clone(),
                    project_id: brd.project_id.clone(),
                }))
            }
            Err(err) => {
                let board = eventbus::Board {
                    id: None,
                    project_id: Some(data.project_id.clone())
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(BoardEvent {
                    board: Some(board),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_board_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    async fn delete_board(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<ProtoBoard>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        
        match Board::delete(&data.board_id, db_connection).await {
            Ok(brd) => {
                let board = eventbus::Board {
                    id: Some(brd.id.clone()),
                    project_id: Some(brd.project_id.clone())
                };
                let req = Request::new(BoardEvent {
                    board: Some(board),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.delete_board_event(req).await;
                });
                Ok(Response::new(ProtoBoard {
                    id: brd.id.clone(),
                    project_id: brd.project_id.clone(),
                }))
            }
            Err(err) => {
                if err == NotFound {
                    let board = eventbus::Board {
                        id: Some(data.board_id.clone()),
                        project_id: None
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_board_event(req).await;
                    });
                    Err(Status::not_found("Board not found"))
                } else {
                    let board = eventbus::Board {
                        id: Some(data.board_id.clone()),
                        project_id: None
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(BoardEvent {
                        board: Some(board),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_board_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            }
        }
    }
}