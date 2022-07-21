use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods};
use tonic::{Request, Response, Status, Code};
use futures::Stream;
use proto::{
    issues::{
        columns_service_server::ColumnsService, 
        Column as ProtoColumn, 
        ColumnId,
        BoardIdAndColumnName,
        ColumnIdAndName,
        SearchColumnsParams,
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
    pub pool: PgPool
}

#[tonic::async_trait]
impl ColumnsService for ColumnsController {
    async fn get_column_by_id(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Column> = columns
            .filter(id.eq(&request.get_ref().column_id))
            .limit(1)
            .load::<Column>(&*db_connection)
            .expect("Get column by id error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoColumn {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        }))
    }

    type searchColumnsStream = Pin<Box<dyn Stream<Item = Result<ProtoColumn, Status>> + Send>>;

    async fn search_columns(
        &self,
        request: Request<SearchColumnsParams>,
    ) -> Result<Response<Self::searchColumnsStream>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        
        let mut query = columns.into_boxed();

        let columns_ids = match data.columns_ids.is_empty() {
            false => Some(&data.columns_ids),
            true => None,
        };

        if let Some(col_ids) = columns_ids {
            query = query.filter(id.eq_any(col_ids));
        }

        if let Some(brd_id) = &data.board_id {
            query = query.filter(board_id.eq(brd_id));
        }

        let result: Vec<Column> = query
            .load::<Column>(&*db_connection)
            .expect("Get dependency by blocking epic id error");
            
        let proto_dependencies: Vec<ProtoColumn> = result.iter().map(|dependency| ProtoColumn {
            id: dependency.id.clone(),
            board_id: dependency.board_id.clone(),
            name: dependency.name.clone(),
        }).collect();

        let mut stream = tokio_stream::iter(proto_dependencies);
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            while let Some(dependency) = stream.next().await {
                match sender.send(Result::<ProtoColumn, Status>::Ok(dependency)).await {
                    Ok(_) => {},
                    Err(_err) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::searchColumnsStream
        ))
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

        let column: Column = match Column::create(new_column, db_connection).await {
            Ok(col) => col,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoColumn {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        }))
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

        let column: Column;
        
        match Column::update(&data.column_id, change_set, db_connection).await {
            Ok(col) => column = col,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoColumn {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        }))
    }

    async fn delete_column(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let column: Column;
        
        match Column::delete(&data.column_id, db_connection).await {
            Ok(col) => column = col,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoColumn {
            id: column.id.clone(),
            board_id: column.board_id.clone(),
            name: column.name.clone(),
        }))
    }
}
