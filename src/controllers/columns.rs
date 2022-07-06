use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, insert_into, delete, update};
use tonic::{Request, Response, Status};
use futures::Stream;
use proto::{
    boards::{
        columns_service_server::ColumnsService, 
        Column as ProtoColumn, 
        ColumnId,
        BoardId,
        BoardIdAndColumnName,
        ColumnIdAndName,
    },
};

use crate::{
    db::{
        models::{NewColumn, Column},
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
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Column> = columns
            .filter(id.eq(&request.get_ref().column_id))
            .limit(1)
            .load::<Column>(&*db_connection)
            .expect("Get column by id error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoColumn {
            id: String::from(&column.id),
            board_id: String::from(&column.board_id),
            name: String::from(&column.name),
        }))
    }
    
    type getColumnsByBoardIdStream = Pin<Box<dyn Stream<Item = Result<ProtoColumn, Status>> + Send>>;

    async fn get_columns_by_board_id(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<Self::getColumnsByBoardIdStream>, Status> {
        let db_connection = self.pool.get().unwrap();

        let result: Vec<Column> = columns
            .filter(board_id.eq(&request.get_ref().board_id))
            .load::<Column>(&*db_connection)
            .expect("Get column by board id error");
            
        let columns_vec: Vec<ProtoColumn> = result.iter().map(|col| ProtoColumn {
            id: String::from(&col.id),
            board_id: String::from(&col.board_id),
            name: String::from(&col.name),
        }).collect();

        let mut stream = tokio_stream::iter(columns_vec);
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            while let Some(column) = stream.next().await {
                match sender.send(Result::<ProtoColumn, Status>::Ok(column)).await {
                    Ok(_) => {},
                    Err(_item) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::getColumnsByBoardIdStream
        ))
    }

    async fn create_column(
        &self,
        request: Request<BoardIdAndColumnName>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().unwrap();
        let new_column = NewColumn {
            id: &uuid::Uuid::new_v4().to_string(),
            board_id: &data.board_id,
            name: &data.column_name
        };
        let result: Vec<Column> = insert_into(columns)
            .values(new_column)
            .get_results(&*db_connection)
            .expect("Create column error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoColumn {
            id: String::from(&column.id),
            board_id: String::from(&column.board_id),
            name: String::from(&column.name),
        }))
    }

    async fn update_column(
        &self,
        request: Request<ColumnIdAndName>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Column> = update(columns)
            .filter(id.eq(&request.get_ref().column_id))
            .set(name.eq(&data.column_name))
            .get_results(&*db_connection)
            .expect("Update column error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoColumn {
            id: String::from(&column.id),
            board_id: String::from(&column.board_id),
            name: String::from(&column.name),
        }))
    }

    async fn delete_column(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<ProtoColumn>, Status> {
        let db_connection = self.pool.get().unwrap();
        let result: Vec<Column> = delete(columns)
            .filter(id.eq(&request.get_ref().column_id))
            .get_results(&*db_connection)
            .expect("Delete column by id error");

        let column: &Column = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoColumn {
            id: String::from(&column.id),
            board_id: String::from(&column.board_id),
            name: String::from(&column.name),
        }))
    }
}
