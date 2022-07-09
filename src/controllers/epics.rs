use std::{pin::Pin, str::FromStr};
use chrono::NaiveDateTime;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{
    RunQueryDsl,
    QueryDsl,
    ExpressionMethods,
};
use tonic::{Request, Response, Status, Code};
use futures::Stream;
use proto::issues::{
    epics_service_server::EpicsService, 
    Epic as ProtoEpic, 
    EpicId,
    ColumnId,
    CreateEpicRequest, 
    UpdateEpicRequest
};

use crate::{
    db::{
        repos::{
            epic::{NewEpic, Epic, EpicChangeSet, CreateEpic, UpdateEpic, DeleteEpic},
            column::Column
        },
        schema::{epics::dsl::*, columns::dsl::columns}, 
        connection::PgPool,
    },
};

pub struct EpicsController {
    pub pool: PgPool
}

#[tonic::async_trait]
impl EpicsService for EpicsController {
    async fn get_epic_by_id(
        &self,
        request: Request<EpicId>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Epic> = epics
            .filter(id.eq(&request.get_ref().epic_id))
            .limit(1)
            .load::<Epic>(&*db_connection)
            .expect("Get epic by id error");

        let epic: &Epic = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: Some(epic.assignee_id.clone().unwrap()),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: Some(epic.description.clone().unwrap()),
            start_date: epic.start_date.timestamp().to_string().clone(),
            due_date: epic.due_date.timestamp().to_string().clone(),
        }))
    }

    type getEpicsByColumnIdStream = Pin<Box<dyn Stream<Item = Result<ProtoEpic, Status>> + Send>>;

    async fn get_epics_by_column_id(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<Self::getEpicsByColumnIdStream>, Status> {
        let db_connection = self.pool.get().expect("Db error");

        let result: Vec<Epic> = epics
            .filter(column_id.eq(&request.get_ref().column_id))
            .load::<Epic>(&*db_connection)
            .expect("Get epic by column id error");
            
        let proto_epics: Vec<ProtoEpic> = result.iter().map(|epic| ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: Some(epic.assignee_id.clone().unwrap()),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: Some(epic.description.clone().unwrap()),
            start_date: epic.start_date.timestamp().to_string().clone(),
            due_date: epic.due_date.timestamp().to_string().clone(),
        }).collect();

        let mut stream = tokio_stream::iter(proto_epics);
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            while let Some(epic) = stream.next().await {
                match sender.send(Result::<ProtoEpic, Status>::Ok(epic)).await {
                    Ok(_) => {},
                    Err(_err) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::getEpicsByColumnIdStream
        ))
    }

    async fn create_epic(
        &self,
        request: Request<CreateEpicRequest>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let col_id = match data.column_id.clone() {
            Some(col_id) => col_id,
            None => {
                let result: Vec<Column> = columns
                    .limit(1)
                    .load::<Column>(&*db_connection)
                    .expect("Get epic by id error");

                let column = result
                    .first()
                    .unwrap();

                column.id.clone()
            },
        };

        let new_epic = NewEpic {
            id: &uuid::Uuid::new_v4().to_string(),
            column_id: &col_id,
            assignee_id: Some(data.assignee_id.as_ref().unwrap()),
            reporter_id: &data.reporter_id,
            name: &data.name,
            description: Some(data.description.as_ref().unwrap()),
            start_date: Some(NaiveDateTime::from_str(data.start_date.as_ref().unwrap()).unwrap()),
            due_date: Some(NaiveDateTime::from_str(data.due_date.as_ref().unwrap()).unwrap()),
        };
        
        let epic: Epic = match Epic::create(new_epic, db_connection).await {
            Ok(ep) => ep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };


        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: Some(epic.assignee_id.clone().unwrap()),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: Some(epic.description.clone().unwrap()),
            start_date: epic.start_date.timestamp().to_string().clone(),
            due_date: epic.due_date.timestamp().to_string().clone(),
        }))
    }

    async fn update_epic(
        &self,
        request: Request<UpdateEpicRequest>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let change_set = EpicChangeSet {
            column_id: data.to_owned().column_id,
            assignee_id: data.to_owned().assignee_id,
            name: data.to_owned().name,
            reporter_id: data.to_owned().reporter_id,
            description: data.to_owned().description,
            start_date: Some(NaiveDateTime::from_str(data.start_date.as_ref().unwrap()).unwrap()),
            due_date: Some(NaiveDateTime::from_str(data.due_date.as_ref().unwrap()).unwrap()),
        };
        
        let epic: Epic;
        
        match Epic::update(&data.epic_id, change_set, db_connection).await {
            Ok(ep) => epic = ep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: Some(epic.assignee_id.clone().unwrap()),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: Some(epic.description.clone().unwrap()),
            start_date: epic.start_date.timestamp().to_string().clone(),
            due_date: epic.due_date.timestamp().to_string().clone(),
        }))
    }

    async fn delete_epic(
        &self,
        request: Request<EpicId>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let epic: Epic;
    
        match Epic::delete(&data.epic_id, db_connection).await {
            Ok(ep) => epic = ep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: Some(epic.assignee_id.clone().unwrap()),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: Some(epic.description.clone().unwrap()),
            start_date: epic.start_date.timestamp().to_string().clone(),
            due_date: epic.due_date.timestamp().to_string().clone(),
        }))
    }
}
