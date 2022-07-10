use std::{pin::Pin, time::SystemTime};
use chrono::{NaiveDateTime, DateTime, Utc};
use prost_types::Timestamp;
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
    EpicsSearchParams,
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
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Epic> = epics
            .filter(id.eq(&data.epic_id))
            .limit(1)
            .load::<Epic>(&*db_connection)
            .expect("Get epic by id error");

        let epic: &Epic = result
            .first()
            .unwrap();

        let start = Option::from(Timestamp {
            seconds: epic.start_date.timestamp(),
            nanos: epic.start_date.timestamp_subsec_nanos().try_into().unwrap(),
        });
        let due = Option::from(Timestamp {
            seconds: epic.due_date.timestamp(),
            nanos: epic.due_date.timestamp_subsec_nanos().try_into().unwrap(),
        });
        
        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: epic.description.clone(),
            start_date: start,
            due_date: due,
        }))
    }

    type searchEpicsStream = Pin<Box<dyn Stream<Item = Result<ProtoEpic, Status>> + Send>>;

    async fn search_epics(
        &self,
        request: Request<EpicsSearchParams>,
    ) -> Result<Response<Self::searchEpicsStream>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let mut query = epics.into_boxed();

        let epics_ids = match data.epics_ids.is_empty() {
            false => Some(&data.epics_ids),
            true => None,
        };

        if let Some(ep_ids) = epics_ids {
            query = query.filter(id.eq_any(ep_ids));
        }

        if let Some(col_id) = &data.column_id {
            query = query.filter(column_id.eq(col_id));
        }
        
        if let Some(start) = Option::from({
            if let Some(seconds) = data.min_start_date.as_ref().map(|x| x.seconds) {
                if let Some(nanos) = data.min_start_date.as_ref().map(|x| x.nanos) {
                    Option::from(
                        NaiveDateTime::from_timestamp(seconds, nanos.try_into().unwrap())
                    )
                } else {None}
            } else {None}
        }) as Option<NaiveDateTime> {
            query = query.filter(start_date.ge(start));
        }
        
        if let Some(due) = Option::from({
            if let Some(seconds) = data.max_due_date.as_ref().map(|x| x.seconds) {
                if let Some(nanos) = data.max_due_date.as_ref().map(|x| x.nanos) {
                    Option::from(
                        NaiveDateTime::from_timestamp(seconds, nanos.try_into().unwrap())
                    )
                } else {None}
            } else {None}
        }) as Option<NaiveDateTime> {
            query = query.filter(start_date.le(due));
        }

        let result: Vec<Epic> = query
            .load::<Epic>(&*db_connection)
            .expect("Search epics error");
            
        let proto_epics: Vec<ProtoEpic> = result.iter().map(|epic| ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: epic.description.clone(),
            start_date: Option::from(Timestamp::from(SystemTime::from(
                DateTime::<Utc>::from_utc(epic.start_date.clone(), Utc)
            ))),
            due_date: Option::from(Timestamp::from(SystemTime::from(
                DateTime::<Utc>::from_utc(epic.due_date.clone(), Utc)
            ))),
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
            Box::pin(output_stream) as Self::searchEpicsStream
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
                    .expect("Create epic error");

                let column = result
                    .first()
                    .unwrap();

                column.id.clone()
            },
        };

        let start = NaiveDateTime::from_timestamp(
            data.start_date.as_ref().unwrap().seconds,
            0,
        );

        let due = NaiveDateTime::from_timestamp(
            data.due_date.as_ref().unwrap().seconds,
            0,
        );

        let new_epic = NewEpic {
            id: &uuid::Uuid::new_v4().to_string(),
            column_id: &col_id,
            assignee_id: data.assignee_id.as_ref().map(|x| &**x),
            reporter_id: &data.reporter_id,
            name: &data.name,
            description: data.description.as_ref().map(|x| &**x),
            start_date: Some(start),
            due_date: Some(due),
        };
        
        let epic: Epic = match Epic::create(new_epic, db_connection).await {
            Ok(ep) => ep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        let start = Option::from(Timestamp {
            seconds: epic.start_date.timestamp(),
            nanos: epic.start_date.timestamp_subsec_nanos().try_into().unwrap(),
        });
        let due = Option::from(Timestamp {
            seconds: epic.due_date.timestamp(),
            nanos: epic.due_date.timestamp_subsec_nanos().try_into().unwrap(),
        });

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: epic.description.clone(),
            start_date: start,
            due_date: due,
        }))
    }

    async fn update_epic(
        &self,
        request: Request<UpdateEpicRequest>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let start = NaiveDateTime::from_timestamp(
            data.start_date.as_ref().unwrap().seconds,
            0,
        );

        let due = NaiveDateTime::from_timestamp(
            data.due_date.as_ref().unwrap().seconds,
            0,
        );

        let change_set = EpicChangeSet {
            column_id: data.to_owned().column_id,
            assignee_id: data.to_owned().assignee_id,
            name: data.to_owned().name,
            reporter_id: data.to_owned().reporter_id,
            description: data.to_owned().description,
            start_date: Option::from(start),
            due_date: Option::from(due),
        };
        
        let epic: Epic;
        
        match Epic::update(&data.epic_id, change_set, db_connection).await {
            Ok(ep) => epic = ep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        let start = Option::from(Timestamp {
            seconds: epic.start_date.timestamp(),
            nanos: epic.start_date.timestamp_subsec_nanos().try_into().unwrap(),
        });
        let due = Option::from(Timestamp {
            seconds: epic.due_date.timestamp(),
            nanos: epic.due_date.timestamp_subsec_nanos().try_into().unwrap(),
        });

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: epic.description.clone(),
            start_date: start,
            due_date: due,
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

        let start = Option::from(Timestamp {
            seconds: epic.start_date.timestamp(),
            nanos: epic.start_date.timestamp_subsec_nanos().try_into().unwrap(),
        });
        let due = Option::from(Timestamp {
            seconds: epic.due_date.timestamp(),
            nanos: epic.due_date.timestamp_subsec_nanos().try_into().unwrap(),
        });

        Ok(Response::new(ProtoEpic {
            id: epic.id.clone(),
            column_id: epic.column_id.clone(),
            assignee_id: epic.assignee_id.clone(),
            reporter_id: epic.reporter_id.clone(),
            name: epic.name.clone(),
            description: epic.description.clone(),
            start_date: start,
            due_date: due,
        }))
    }
}
