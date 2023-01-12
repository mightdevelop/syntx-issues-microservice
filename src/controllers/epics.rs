use std::{pin::Pin, time::SystemTime};
use chrono::{NaiveDateTime, DateTime, Utc};
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{
    RunQueryDsl,
    QueryDsl,
    ExpressionMethods, QueryResult, result::Error::NotFound,
};
use tonic::{Request, Response, Status, Code, transport::Channel};
use futures::Stream;
use proto::{
    issues::{
        epics_service_server::EpicsService, 
        Epic as ProtoEpic, 
        EpicId,
        SearchEpicsParams,
        CreateEpicRequest, 
        UpdateEpicRequest
    }, 
    eventbus::{
        self,
        epics_events_service_client::EpicsEventsServiceClient, EpicEvent, SearchEpicsEvent,
    }
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
    pub pool: PgPool,
    pub eventbus_service_client: EpicsEventsServiceClient<Channel>
}

#[tonic::async_trait]
impl EpicsService for EpicsController {
    async fn get_epic_by_id(
        &self,
        request: Request<EpicId>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        let result: QueryResult<Vec<Epic>> = epics
            .filter(id.eq(&data.epic_id))
            .limit(1)
            .load::<Epic>(&*db_connection);


        match result {
            Ok(vec) => {
                if let Some(ep) = vec.first() {
                    let epic = eventbus::Epic {
                        id: Some(ep.id.clone()),
                        column_id: Some(ep.column_id.clone()),
                        assignee_id: ep.assignee_id.clone(),
                        reporter_id: Some(ep.reporter_id.clone()),
                        name: Some(ep.name.clone()),
                        description: ep.description.clone(),
                        start_date: Some(ep.start_date.clone().to_string()),
                        due_date: Some(ep.due_date.clone().to_string()),
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_epic_by_id_event(req).await;
                    });
                    let start_timestamp = Option::from(Timestamp {
                        seconds: ep.start_date.timestamp(),
                        nanos: ep.start_date.timestamp_subsec_nanos().try_into().unwrap(),
                    });
                    let due_timestamp = Option::from(Timestamp {
                        seconds: ep.due_date.timestamp(),
                        nanos: ep.due_date.timestamp_subsec_nanos().try_into().unwrap(),
                    });
                    Ok(Response::new(ProtoEpic {
                        id: ep.id.clone(),
                        column_id: ep.column_id.clone(),
                        assignee_id: ep.assignee_id.clone(),
                        reporter_id: ep.reporter_id.clone(),
                        name: ep.name.clone(),
                        description: ep.description.clone(),
                        start_date: start_timestamp,
                        due_date: due_timestamp,
                    }))
                } else {
                    let epic = eventbus::Epic {
                        id: Some(data.epic_id.clone()),
                        column_id: None,
                        assignee_id: None,
                        reporter_id: None,
                        name: None,
                        description: None,
                        start_date: None,
                        due_date: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Epic not found")
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_epic_by_id_event(req).await;
                    });
                    Err(Status::not_found("Epic not found"))
                }
            }
            Err(err) => {
                let epic = eventbus::Epic {
                    id: Some(data.epic_id.clone()),
                    column_id: None,
                    assignee_id: None,
                    reporter_id: None,
                    name: None,
                    description: None,
                    start_date: None,
                    due_date: None,
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(EpicEvent {
                    epic: Some(epic),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_epic_by_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    type searchEpicsStream = Pin<Box<dyn Stream<Item = Result<ProtoEpic, Status>> + Send>>;

    async fn search_epics(
        &self,
        request: Request<SearchEpicsParams>,
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

        if let Some(limit) = data.limit.clone() {
            query = query.limit(limit.try_into().unwrap());
        }

        if let Some(offset) = data.offset.clone() {
            query = query.offset(offset.try_into().unwrap());
        }

        let result: QueryResult<Vec<Epic>> = query
            .load::<Epic>(&*db_connection);

        match result {
            Ok(vec) => {
                let eps = vec
                    .iter()
                    .map(|epic| eventbus::Epic {
                        id: Some(epic.id.clone()),
                        column_id: Some(epic.column_id.clone()),
                        assignee_id: epic.assignee_id.clone(),
                        reporter_id: Some(epic.reporter_id.clone()),
                        name: Some(epic.name.clone()),
                        description: epic.description.clone(),
                        start_date: Some(epic.start_date.clone().to_string()),
                        due_date: Some(epic.due_date.clone().to_string()),
                    })
                    .collect::<Vec<eventbus::Epic>>();
                let search_params = eventbus::SearchEpicsParams {
                    epics_ids: data.epics_ids.clone(),
                    column_id: data.column_id.clone(),
                    min_start_date: data.min_start_date.clone(),
                    max_due_date: data.max_due_date.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };

                let req = Request::new(SearchEpicsEvent {
                    epics: eps,
                    error: None,
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();

                let proto_epics: Vec<ProtoEpic> = vec.iter().map(|epic| ProtoEpic {
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
                    service.search_epics_event(req).await;
                });
        
                let output_stream = ReceiverStream::new(receiver);
        
                Ok(Response::new(
                    Box::pin(output_stream) as Self::searchEpicsStream
                ))
            }
            Err(err) => {
                let eps = data.epics_ids
                    .iter()
                    .map(|epic_id| eventbus::Epic {
                        id: Some(epic_id.clone()),
                        column_id: None,
                        assignee_id: None,
                        reporter_id: None,
                        name: None,
                        description: None,
                        start_date: None,
                        due_date: None,
                    })
                    .collect::<Vec<eventbus::Epic>>();
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let search_params = eventbus::SearchEpicsParams {
                    epics_ids: data.epics_ids.clone(),
                    column_id: data.column_id.clone(),
                    min_start_date: data.min_start_date.clone(),
                    max_due_date: data.max_due_date.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };

                let req = Request::new(SearchEpicsEvent {
                    epics: eps,
                    error: Some(error),
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.search_epics_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
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

        match Epic::create(new_epic, db_connection).await {
            Ok(ep) => {
                let epic = eventbus::Epic {
                    id: Some(ep.id.clone()),
                    column_id: Some(ep.column_id.clone()),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: Some(ep.reporter_id.clone()),
                    name: Some(ep.name.clone()),
                    description: ep.description.clone(),
                    start_date: Some(ep.start_date.clone().to_string()),
                    due_date: Some(ep.due_date.clone().to_string()),
                };
                let req = Request::new(EpicEvent {
                    epic: Some(epic),
                    error: None
                });
                
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_epic_event(req).await;
                });

                let start_timestamp = Option::from(Timestamp {
                    seconds: start.timestamp(),
                    nanos: start.timestamp_subsec_nanos().try_into().unwrap(),
                });
                let due_timestamp = Option::from(Timestamp {
                    seconds: due.timestamp(),
                    nanos: due.timestamp_subsec_nanos().try_into().unwrap(),
                });

                Ok(Response::new(ProtoEpic {
                    id: ep.id.clone(),
                    column_id: ep.column_id.clone(),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: ep.reporter_id.clone(),
                    name: ep.name.clone(),
                    description: ep.description.clone(),
                    start_date: start_timestamp,
                    due_date: due_timestamp,
                }))
            },
            Err(err) => {
                let epic = eventbus::Epic {
                    id: None,
                    column_id: data.column_id.clone(),
                    assignee_id: data.assignee_id.clone(),
                    reporter_id: Some(data.reporter_id.clone()),
                    name: Some(data.name.clone()),
                    description: data.description.clone(),
                    start_date: Some(start.to_string()),
                    due_date: Some(due.to_string()),
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(EpicEvent {
                    epic: Some(epic),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_epic_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            },
        }
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
        
        match Epic::update(&data.epic_id, change_set, db_connection).await {
            Ok(ep) => {
                let epic = eventbus::Epic {
                    id: Some(ep.id.clone()),
                    column_id: Some(ep.column_id.clone()),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: Some(ep.reporter_id.clone()),
                    name: Some(ep.name.clone()),
                    description: ep.description.clone(),
                    start_date: Some(ep.start_date.clone().to_string()),
                    due_date: Some(ep.due_date.clone().to_string()),
                };
                let req = Request::new(EpicEvent {
                    epic: Some(epic),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.update_epic_event(req).await;
                });

                let start_timestamp = Option::from(Timestamp {
                    seconds: start.timestamp(),
                    nanos: start.timestamp_subsec_nanos().try_into().unwrap(),
                });
                let due_timestamp = Option::from(Timestamp {
                    seconds: due.timestamp(),
                    nanos: due.timestamp_subsec_nanos().try_into().unwrap(),
                });
        
                Ok(Response::new(ProtoEpic {
                    id: ep.id.clone(),
                    column_id: ep.column_id.clone(),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: ep.reporter_id.clone(),
                    name: ep.name.clone(),
                    description: ep.description.clone(),
                    start_date: start_timestamp,
                    due_date: due_timestamp,
                }))
            },
            Err(err) => {
                if err == NotFound {
                    let epic = eventbus::Epic {
                        id: Some(data.epic_id.clone()),
                        column_id: data.column_id.clone(),
                        assignee_id: data.assignee_id.clone(),
                        reporter_id: data.reporter_id.clone(),
                        name: data.name.clone(),
                        description: data.description.clone(),
                        start_date: Some(start.clone().to_string()),
                        due_date: Some(due.clone().to_string()),
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_epic_event(req).await;
                    });
                    Err(Status::not_found("Epic not found"))
                } else {
                    let epic = eventbus::Epic {
                        id: Some(data.epic_id.clone()),
                        column_id: data.column_id.clone(),
                        assignee_id: data.assignee_id.clone(),
                        reporter_id: data.reporter_id.clone(),
                        name: data.name.clone(),
                        description: data.description.clone(),
                        start_date: Some(start.clone().to_string()),
                        due_date: Some(due.clone().to_string()),
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_epic_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            },
        }
    }

    async fn delete_epic(
        &self,
        request: Request<EpicId>,
    ) -> Result<Response<ProtoEpic>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        match Epic::delete(&data.epic_id, db_connection).await {
            Ok(ep) => {
                let epic = eventbus::Epic {
                    id: Some(ep.id.clone()),
                    column_id: Some(ep.column_id.clone()),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: Some(ep.reporter_id.clone()),
                    name: Some(ep.name.clone()),
                    description: ep.description.clone(),
                    start_date: Some(ep.start_date.clone().to_string()),
                    due_date: Some(ep.due_date.clone().to_string()),
                };
                let req = Request::new(EpicEvent {
                    epic: Some(epic),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.delete_epic_event(req).await;
                });

                let start_timestamp = Option::from(Timestamp {
                    seconds: ep.start_date.timestamp(),
                    nanos: ep.start_date.timestamp_subsec_nanos().try_into().unwrap(),
                });
                let due_timestamp = Option::from(Timestamp {
                    seconds: ep.due_date.timestamp(),
                    nanos: ep.due_date.timestamp_subsec_nanos().try_into().unwrap(),
                });

                Ok(Response::new(ProtoEpic {
                    id: ep.id.clone(),
                    column_id: ep.column_id.clone(),
                    assignee_id: ep.assignee_id.clone(),
                    reporter_id: ep.reporter_id.clone(),
                    name: ep.name.clone(),
                    description: ep.description.clone(),
                    start_date: start_timestamp,
                    due_date: due_timestamp,
                }))
            }
            Err(err) => {
                if err == NotFound {
                    let epic = eventbus::Epic {
                        id: Some(data.epic_id.clone()),
                        column_id: None,
                        assignee_id: None,
                        reporter_id: None,
                        name: None,
                        description: None,
                        start_date: None,
                        due_date: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_epic_event(req).await;
                    });
                    Err(Status::not_found("Epic not found"))
                } else {
                    let epic = eventbus::Epic {
                        id: Some(data.epic_id.clone()),
                        column_id: None,
                        assignee_id: None,
                        reporter_id: None,
                        name: None,
                        description: None,
                        start_date: None,
                        due_date: None,
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(EpicEvent {
                        epic: Some(epic),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_epic_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            }
        }
    }
}
