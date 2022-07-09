use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods};
use tonic::{Request, Response, Status, Code};
use futures::Stream;
use proto::{
    issues::{
        issues_service_server::IssuesService,
        Issue as ProtoIssue,
        IssueId,
        ColumnId,
        CreateIssueRequest,
        UpdateIssueRequest,
        EpicId,
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
    pub pool: PgPool
}

#[tonic::async_trait]
impl IssuesService for IssuesController {
    async fn get_issue_by_id(
        &self,
        request: Request<IssueId>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Issue> = issues
            .filter(id.eq(&request.get_ref().issue_id))
            .limit(1)
            .load::<Issue>(&*db_connection)
            .expect("Get issue by id error");

        let issue: &Issue = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoIssue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        }))
    }

    type getIssuesByColumnIdStream = Pin<Box<dyn Stream<Item = Result<ProtoIssue, Status>> + Send>>;

    async fn get_issues_by_column_id(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<Self::getIssuesByColumnIdStream>, Status> {
        let db_connection = self.pool.get().expect("Db error");

        let result: Vec<Issue> = issues
            .filter(column_id.eq(&request.get_ref().column_id))
            .load::<Issue>(&*db_connection)
            .expect("Get issue by column id error");
            
        let proto_issues: Vec<ProtoIssue> = result.iter().map(|issue| ProtoIssue {
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
                    Err(_item) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::getIssuesByColumnIdStream
        ))
    }

    type getIssuesByEpicIdStream = Pin<Box<dyn Stream<Item = Result<ProtoIssue, Status>> + Send>>;

    async fn get_issues_by_epic_id(
        &self,
        request: Request<EpicId>,
    ) -> Result<Response<Self::getIssuesByEpicIdStream>, Status> {
        let db_connection = self.pool.get().expect("Db error");

        let result: Vec<Issue> = issues
            .filter(epic_id.eq(&request.get_ref().epic_id))
            .load::<Issue>(&*db_connection)
            .expect("Get issue by column id error");
            
        let proto_issues: Vec<ProtoIssue> = result.iter().map(|issue| ProtoIssue {
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
                    Err(_item) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::getIssuesByColumnIdStream
        ))
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

        let issue: Issue = match Issue::create(new_issue, db_connection).await {
            Ok(iss) => iss,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoIssue {
            id: issue.id,
            column_id: issue.column_id,
            epic_id: issue.epic_id,
            title: issue.title,
            description: issue.description,
        }))
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

        let issue: Issue;
        
        match Issue::update(&data.issue_id, change_set, db_connection).await {
            Ok(iss) => issue = iss,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoIssue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        }))
    }

    async fn delete_issue(
        &self,
        request: Request<IssueId>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let issue: Issue;
        
        match Issue::delete(&data.issue_id, db_connection).await {
            Ok(iss) => issue = iss,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoIssue {
            id: issue.id.clone(),
            column_id: issue.column_id.clone(),
            epic_id: issue.epic_id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
        }))
    }
}
