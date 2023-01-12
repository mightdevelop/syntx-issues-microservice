use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, QueryResult, result::Error::NotFound};
use tonic::{Request, Response, Status, Code, transport::Channel};
use futures::Stream;
use proto::{
    issues::{
        issues_service_server::IssuesService,
        Issue as ProtoIssue,
        IssueId,
        CreateIssueRequest,
        UpdateIssueRequest,
        SearchIssuesParams,
    }, 
    eventbus::{
        self,
        issues_events_service_client::IssuesEventsServiceClient, IssueEvent, SearchIssuesEvent,
    },
};

use crate::{
    db::{
        repos::issue::{NewIssue, Issue, CreateIssue, UpdateIssue, IssueChangeSet, DeleteIssue},
        schema::issues::dsl::*,
        connection::PgPool
    },
};

pub struct IssuesController {
    pub pool: PgPool,
    pub eventbus_service_client: IssuesEventsServiceClient<Channel>
}

#[tonic::async_trait]
impl IssuesService for IssuesController {
    async fn get_issue_by_id(
        &self,
        request: Request<IssueId>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        let result: QueryResult<Vec<Issue>> = issues
            .filter(id.eq(&request.get_ref().issue_id))
            .limit(1)
            .load::<Issue>(&*db_connection);

        match result {
            Ok(vec) => {
                if let Some(iss) = vec.first() {
                    let issue = eventbus::Issue {
                        id: Some(iss.id.clone()),
                        column_id: Some(iss.column_id.clone()),
                        epic_id: Some(iss.epic_id.clone()),
                        title: Some(iss.title.clone()),
                        description: Some(iss.description.clone()),
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_issue_by_id_event(req).await;
                    });

                    Ok(Response::new(ProtoIssue {
                        id: iss.id.clone(),
                        column_id: iss.column_id.clone(),
                        epic_id: iss.epic_id.clone(),
                        title: iss.title.clone(),
                        description: iss.description.clone(),
                    }))
                } else {
                    let issue = eventbus::Issue {
                        id: Some(data.issue_id.clone()),
                        column_id: None,
                        epic_id: None,
                        title: None,
                        description: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Issue not found")
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_issue_by_id_event(req).await;
                    });
                    Err(Status::not_found("Issue not found"))
                }
            }
            Err(err) => {
                let issue = eventbus::Issue {
                    id: Some(data.issue_id.clone()),
                    column_id: None,
                    epic_id: None,
                    title: None,
                    description: None,
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(IssueEvent {
                    issue: Some(issue),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_issue_by_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    type searchIssuesStream = Pin<Box<dyn Stream<Item = Result<ProtoIssue, Status>> + Send>>;

    async fn search_issues(
        &self,
        request: Request<SearchIssuesParams>,
    ) -> Result<Response<Self::searchIssuesStream>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let mut query = issues.into_boxed();

        let issues_ids = match data.issues_ids.is_empty() {
            false => Some(&data.issues_ids),
            true => None,
        };

        if let Some(is_ids) = issues_ids {
            query = query.filter(id.eq_any(is_ids));
        }

        if let Some(col_id) = &data.column_id {
            query = query.filter(column_id.eq(col_id));
        }

        if let Some(col_id) = &data.epic_id {
            query = query.filter(column_id.eq(col_id));
        }

        if let Some(limit) = data.limit.clone() {
            query = query.limit(limit.try_into().unwrap());
        }

        if let Some(offset) = data.offset.clone() {
            query = query.offset(offset.try_into().unwrap());
        }

        let result: QueryResult<Vec<Issue>> = query
            .load::<Issue>(&*db_connection);
            
        match result {
            Ok(vec) => {
                let iss = vec
                    .iter()
                    .map(|issue| eventbus::Issue {
                        id: Some(issue.id.clone()),
                        column_id: Some(issue.column_id.clone()),
                        epic_id: Some(issue.epic_id.clone()),
                        title: Some(issue.title.clone()),
                        description: Some(issue.description.clone()),
                    })
                    .collect::<Vec<eventbus::Issue>>();
                let search_params = eventbus::SearchIssuesParams {
                    issues_ids: data.issues_ids.clone(),
                    column_id: data.column_id.clone(),
                    epic_id: data.epic_id.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };
        
                let req = Request::new(SearchIssuesEvent {
                    issues: iss,
                    error: None,
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();
        
                let proto_issues: Vec<ProtoIssue> = vec.iter().map(|issue| ProtoIssue {
                    id: issue.id.clone(),
                    column_id: issue.column_id.clone(),
                    epic_id: issue.epic_id.clone(),
                    title: issue.title.clone(),
                    description: issue.description.clone(),
                }).collect();
        
                let mut stream = tokio_stream::iter(proto_issues);
                let (sender, receiver) = mpsc::channel(1);
        
                tokio::spawn(async move {
                    while let Some(issue) = stream.next().await {
                        match sender.send(Result::<ProtoIssue, Status>::Ok(issue)).await {
                            Ok(_) => {},
                            Err(_err) => break
                        }
                    }
                    service.search_issues_event(req).await;
                });
        
                let output_stream = ReceiverStream::new(receiver);
        
                Ok(Response::new(
                    Box::pin(output_stream) as Self::searchIssuesStream
                ))
            }
            Err(err) => {
                let iss = data.issues_ids
                    .iter()
                    .map(|issue_id| eventbus::Issue {
                        id: Some(issue_id.clone()),
                        column_id: None,
                        epic_id: None,
                        title: None,
                        description: None,
                    })
                    .collect::<Vec<eventbus::Issue>>();
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let search_params = eventbus::SearchIssuesParams {
                    issues_ids: data.issues_ids.clone(),
                    column_id: data.column_id.clone(),
                    epic_id: data.epic_id.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };
        
                let req = Request::new(SearchIssuesEvent {
                    issues: iss,
                    error: Some(error),
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.search_issues_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    async fn create_issue(
        &self,
        request: Request<CreateIssueRequest>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let new_issue = NewIssue {
            id: &uuid::Uuid::new_v4().to_string(),
            column_id: &data.column_id,
            epic_id: &data.epic_id,
            title: &data.title,
            description: &data.description,
        };

        match Issue::create(new_issue, db_connection).await {
            Ok(iss) => {
                let issue = eventbus::Issue {
                    id: Some(iss.id.clone()),
                    column_id: Some(iss.column_id.clone()),
                    epic_id: Some(iss.epic_id.clone()),
                    title: Some(iss.title.clone()),
                    description: Some(iss.description.clone()),
                };
                let req = Request::new(IssueEvent {
                    issue: Some(issue),
                    error: None
                });
                
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_issue_event(req).await;
                });

                Ok(Response::new(ProtoIssue {
                    id: iss.id.clone(),
                    column_id: iss.column_id.clone(),
                    epic_id: iss.epic_id.clone(),
                    title: iss.title.clone(),
                    description: iss.description.clone(),
                }))
            },
            Err(err) => {
                let issue = eventbus::Issue {
                    id: None,
                    column_id: Some(data.column_id.clone()),
                    epic_id: Some(data.epic_id.clone()),
                    title: Some(data.title.clone()),
                    description: Some(data.description.clone()),
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(IssueEvent {
                    issue: Some(issue),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_issue_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            },
        }
    }

    async fn update_issue(
        &self,
        request: Request<UpdateIssueRequest>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let change_set = IssueChangeSet {
            column_id: data.column_id.clone(),
            epic_id: data.epic_id.clone(),
            title: data.title.clone(),
            description: data.description.clone(),
        };
        
        match Issue::update(&data.issue_id, change_set, db_connection).await {
            Ok(iss) => {
                let issue = eventbus::Issue {
                    id: Some(iss.id.clone()),
                    column_id: Some(iss.column_id.clone()),
                    epic_id: Some(iss.epic_id.clone()),
                    title: Some(iss.title.clone()),
                    description: Some(iss.description.clone()),
                };
                let req = Request::new(IssueEvent {
                    issue: Some(issue),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.update_issue_event(req).await;
                });
        
                Ok(Response::new(ProtoIssue {
                    id: iss.id.clone(),
                    column_id: iss.column_id.clone(),
                    epic_id: iss.epic_id.clone(),
                    title: iss.title.clone(),
                    description: iss.description.clone(),
                }))
            },
            Err(err) => {
                if err == NotFound {
                    let issue = eventbus::Issue {
                        id: Some(data.issue_id.clone()),
                        column_id: data.column_id.clone(),
                        epic_id: data.epic_id.clone(),
                        title: data.title.clone(),
                        description: data.description.clone(),
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_issue_event(req).await;
                    });
                    Err(Status::not_found("Issue not found"))
                } else {
                    let issue = eventbus::Issue {
                        id: Some(data.issue_id.clone()),
                        column_id: data.column_id.clone(),
                        epic_id: data.epic_id.clone(),
                        title: data.title.clone(),
                        description: data.description.clone(),
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.update_issue_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            },
        }
    }

    async fn delete_issue(
        &self,
        request: Request<IssueId>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        match Issue::delete(&data.issue_id, db_connection).await {
            Ok(iss) => {
                let issue = eventbus::Issue {
                    id: Some(iss.id.clone()),
                    column_id: Some(iss.column_id.clone()),
                    epic_id: Some(iss.epic_id.clone()),
                    title: Some(iss.title.clone()),
                    description: Some(iss.description.clone()),
                };
                let req = Request::new(IssueEvent {
                    issue: Some(issue),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.delete_issue_event(req).await;
                });
        
                Ok(Response::new(ProtoIssue {
                    id: iss.id.clone(),
                    column_id: iss.column_id.clone(),
                    epic_id: iss.epic_id.clone(),
                    title: iss.title.clone(),
                    description: iss.description.clone(),
                }))
            }
            Err(err) => {
                if err == NotFound {
                    let issue = eventbus::Issue {
                        id: Some(data.issue_id.clone()),
                        column_id: None,
                        epic_id: None,
                        title: None,
                        description: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_issue_event(req).await;
                    });
                    Err(Status::not_found("Issue not found"))
                } else {
                    let issue = eventbus::Issue {
                        id: Some(data.issue_id.clone()),
                        column_id: None,
                        epic_id: None,
                        title: None,
                        description: None,
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(IssueEvent {
                        issue: Some(issue),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_issue_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            }
        }
    }
}
