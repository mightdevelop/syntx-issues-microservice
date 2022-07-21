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
        CreateIssueRequest,
        UpdateIssueRequest,
        SearchIssuesParams,
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

        let result: Vec<Issue> = query
            .load::<Issue>(&*db_connection)
            .expect("Search epics error");
            
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
            while let Some(epic) = stream.next().await {
                match sender.send(Result::<ProtoIssue, Status>::Ok(epic)).await {
                    Ok(_) => {},
                    Err(_err) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::searchIssuesStream
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
