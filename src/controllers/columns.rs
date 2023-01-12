use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, QueryResult, result::Error::NotFound};
use tonic::{Request, Response, Status, Code, transport::Channel};
use futures::Stream;
use proto::{
    issues::{
        self,
        columns_service_server::ColumnsService, 
        Column as ProtoColumn, 
        ColumnId,
        BoardIdAndColumnName,
        ColumnIdAndName,
    },
    eventbus::{
        self,
        columns_events_service_client::ColumnsEventsServiceClient, 
        ColumnEvent, 
        SearchColumnsEvent,
    },
};

use crate::{
    db::{
        repos::column::{NewColumn, Column, CreateColumn, UpdateColumn, ColumnChangeSet, DeleteColumn},
        schema::columns::dsl::*, 
        connection::PgPool,
    },
};
pub struct ColumnsController {
    pub pool: PgPool,
    pub eventbus_service_client: ColumnsEventsServiceClient<Channel>
}

#[tonic::async_trait]
impl ColumnsService for ColumnsController {
    async fn get_column_by_id(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let result: QueryResult<Vec<Column>> = columns
            .filter(id.eq(&request.get_ref().column_id))
            .limit(1)
            .load::<Column>(&*db_connection);

        match result {
            Ok(vec) => {
                if let Some(clmn) = vec.first() {
                    let column = eventbus::Column {
                        id: Some(clmn.id.clone()),
                        board_id: Some(clmn.board_id.clone()),
                        name: Some(clmn.name.clone()),
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_column_by_id_event(req).await;
                    });
                    Ok(Response::new(ProtoColumn {
                        id: clmn.id.clone(),
                        board_id: clmn.board_id.clone(),
                        name: clmn.name.clone(),
                    }))
                } else {
                    let column = eventbus::Column {
                        id: Some(data.column_id.clone()),
                        board_id: None,
                        name: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Column not found")
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_column_by_id_event(req).await;
                    });
                    Err(Status::not_found("Column not found"))
                }
            }
            Err(err) => {
                let column = eventbus::Column {
                    id: Some(data.column_id.clone()),
                    board_id: None,
                    name: None,
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(ColumnEvent {
                    column: Some(column),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_column_by_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    type searchColumnsStream = Pin<Box<dyn Stream<Item = Result<ProtoColumn, Status>> + Send>>;

    async fn search_columns(
        &self,
        request: Request<issues::SearchColumnsParams>,
    ) -> Result<Response<Self::searchColumnsStream>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        
        let mut query = columns.into_boxed();

        let columns_ids = match data.columns_ids.is_empty() {
            false => Some(&data.columns_ids),
            true => None,
        };

        if let Some(clmns_ids) = columns_ids {
            query = query.filter(id.eq_any(clmns_ids));
        }

        if let Some(brd_id) = &data.board_id {
            query = query.filter(board_id.eq(brd_id));
        }

        let result: QueryResult<Vec<Column>> = query
            .load::<Column>(&*db_connection);

        match result {
            Ok(vec) => {
                let clmns = vec
                    .iter()
                    .map(|column| eventbus::Column {
                        id: Some(column.id.clone()),
                        board_id: Some(column.board_id.clone()),
                        name: Some(column.name.clone()),
                    })
                    .collect::<Vec<eventbus::Column>>();
                let search_params = eventbus::SearchColumnsParams {
                    board_id: data.board_id.clone(),
                    columns_ids: data.columns_ids.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };

                let req = Request::new(SearchColumnsEvent {
                    columns: clmns,
                    error: None,
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();
                let proto_columns: Vec<ProtoColumn> = vec.iter().map(|column| ProtoColumn {
                    id: column.id.clone(),
                    board_id: column.board_id.clone(),
                    name: column.name.clone(),
                }).collect();
        
                let mut stream = tokio_stream::iter(proto_columns);
                let (sender, receiver) = mpsc::channel(1);
        
                tokio::spawn(async move {
                    while let Some(column) = stream.next().await {
                        match sender.send(Result::<ProtoColumn, Status>::Ok(column)).await {
                            Ok(_) => {},
                            Err(_err) => break
                        };
                    };
                    service.search_columns_event(req).await;
                });
                let output_stream = ReceiverStream::new(receiver);
        
                Ok(Response::new(
                    Box::pin(output_stream) as Self::searchColumnsStream
                ))
            }
            Err(err) => {
                let clmns = data.columns_ids
                    .iter()
                    .map(|column_id| eventbus::Column {
                        id: Some(column_id.to_owned()),
                        board_id: None,
                        name: None,
                    })
                    .collect::<Vec<eventbus::Column>>();
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(SearchColumnsEvent {
                    columns: clmns,
                    error: Some(error),
                    search_params: Some(eventbus::SearchColumnsParams {
                        board_id: data.board_id.clone(),
                        columns_ids: data.columns_ids.clone(),
                        limit: data.limit.clone(),
                        offset: data.offset.clone(),
                    })
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.search_columns_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    async fn create_column(
        &self,
        request: Request<BoardIdAndColumnName>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let new_column = NewColumn {
            id: &uuid::Uuid::new_v4().to_string(),
            board_id: &data.board_id,
            name: &data.column_name
        };

        match Column::create(new_column, db_connection).await {
            Ok(col) => {
                let column = eventbus::Column {
                    id: Some(col.id.clone()),
                    board_id: Some(col.board_id.clone()),
                    name: Some(col.name.clone()),
                };
                let req = Request::new(ColumnEvent {
                    column: Some(column),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_column_event(req).await;
                });

                Ok(Response::new(ProtoColumn {
                    id: col.id.clone(),
                    board_id: col.board_id.clone(),
                    name: col.name.clone(),
                }))
            },
            Err(err) => {
                let column = eventbus::Column {
                    id: None,
                    board_id: Some(data.board_id.clone()),
                    name: Some(data.column_name.clone()),
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(ColumnEvent {
                    column: Some(column),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_column_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            },
        }
    }

    async fn update_column(
        &self,
        request: Request<ColumnIdAndName>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let change_set = ColumnChangeSet {
            name: Some(data.column_name.clone()),
        };
        
        match Column::update(&data.column_id, change_set, db_connection).await {
            Ok(col) => {
                let column = eventbus::Column {
                    id: Some(col.id.clone()),
                    board_id: Some(col.board_id.clone()),
                    name: Some(col.name.clone()),
                };
                let req = Request::new(ColumnEvent {
                    column: Some(column),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.update_column_event(req).await;
                });

                Ok(Response::new(ProtoColumn {
                    id: col.id.clone(),
                    board_id: col.board_id.clone(),
                    name: col.name.clone(),
                }))
            },
            Err(err) => {
                if err == NotFound {
                    let column = eventbus::Column {
                        id: Some(data.column_id.clone()),
                        board_id: None,
                        name: Some(data.column_name.clone()),
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_column_event(req).await;
                    });
                    Err(Status::not_found("Column not found"))
                } else {
                    let column = eventbus::Column {
                        id: Some(data.column_id.clone()),
                        board_id: None,
                        name: Some(data.column_name.clone()),
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_column_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            },
        }
    }

    async fn delete_column(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        match Column::delete(&data.column_id, db_connection).await {
            Ok(clmn) => {
                let column = eventbus::Column {
                    id: Some(clmn.id.clone()),
                    board_id: Some(clmn.board_id.clone()),
                    name: Some(clmn.name.clone()),
                };
                let req = Request::new(ColumnEvent {
                    column: Some(column),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.delete_column_event(req).await;
                });
                Ok(Response::new(ProtoColumn {
                    id: clmn.id.clone(),
                    board_id: clmn.board_id.clone(),
                    name: clmn.name.clone(),
                }))
            }
            Err(err) => {
                if err == NotFound {
                    let column = eventbus::Column {
                        id: Some(data.column_id.clone()),
                        board_id: None,
                        name: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_column_event(req).await;
                    });
                    Err(Status::not_found("Column not found"))
                } else {
                    let column = eventbus::Column {
                        id: Some(data.column_id.clone()),
                        board_id: None,
                        name: None,
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(ColumnEvent {
                        column: Some(column),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_column_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            }
        }
    }
}
