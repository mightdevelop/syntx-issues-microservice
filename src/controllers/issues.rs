use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use diesel::{RunQueryDsl, QueryDsl, ExpressionMethods, insert_into, delete, update};
use tonic::{Request, Response, Status};
use futures::Stream;
use proto::{
    boards::{
        issues_service_server::IssuesService, 
        Issue as ProtoIssue, 
        IssueId,
        BoardId,
        ColumnId, CreateIssueRequest, UpdateIssueRequest,
    },
};

use crate::{
    db::{
        models::{
            NewIssue,
            Issue,
            Column
        },
        schema::{
            issues,
            columns
        },
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
        let db_connection = self.pool.get().unwrap();
        let issues: Vec<Issue> = issues::dsl::issues
            .filter(issues::dsl::id.eq(&request.get_ref().issue_id))
            .limit(1)
            .load::<Issue>(&*db_connection)
            .expect("Get issue by id error");

        let issue: &Issue = issues
            .first()
            .unwrap();

        Ok(Response::new(ProtoIssue {
            id: String::from(&issue.id),
            column_id: String::from(&issue.column_id),
            title: String::from(&issue.title),
            body: String::from(&issue.body),
        }))
    }
    
    type getIssuesByBoardIdStream = Pin<Box<dyn Stream<Item = Result<ProtoIssue, Status>> + Send>>;

    async fn get_issues_by_board_id(
        &self,
        request: Request<BoardId>,
    ) -> Result<Response<Self::getIssuesByBoardIdStream>, Status> {
        let db_connection = self.pool.get().unwrap();
        
        let columns: Vec<Column> = columns::dsl::columns
            .filter(columns::dsl::board_id.eq(&request.get_ref().board_id))
            .load::<Column>(&*db_connection)
            .expect("Get column by board id error");

        let columns_ids: Vec<&String> = columns.iter().map(|column| &column.id).collect();

        let issues: Vec<Issue> = issues::dsl::issues
            .filter(issues::dsl::column_id.eq_any(columns_ids))
            .load::<Issue>(&*db_connection)
            .expect("Get issue by column id error");
            
        let proto_issues: Vec<ProtoIssue> = issues.iter().map(|iss| ProtoIssue {
            id: String::from(&iss.id),
            column_id: String::from(&iss.column_id),
            title: String::from(&iss.title),
            body: String::from(&iss.body),
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
            Box::pin(output_stream) as Self::getIssuesByBoardIdStream
        ))
    }

    type getIssuesByColumnIdStream = Pin<Box<dyn Stream<Item = Result<ProtoIssue, Status>> + Send>>;

    async fn get_issues_by_column_id(
        &self,
        request: Request<ColumnId>,
    ) -> Result<Response<Self::getIssuesByBoardIdStream>, Status> {
        let db_connection = self.pool.get().unwrap();

        let issues: Vec<Issue> = issues::dsl::issues
            .filter(issues::dsl::column_id.eq(&request.get_ref().column_id))
            .load::<Issue>(&*db_connection)
            .expect("Get issue by column id error");
            
        let issues_vec: Vec<ProtoIssue> = issues.iter().map(|iss| ProtoIssue {
            id: String::from(&iss.id),
            column_id: String::from(&iss.column_id),
            title: String::from(&iss.title),
            body: String::from(&iss.body),
        }).collect();

        let mut stream = tokio_stream::iter(issues_vec);
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
            Box::pin(output_stream) as Self::getIssuesByBoardIdStream
        ))
    }

    async fn create_issue(
        &self,
        request: Request<CreateIssueRequest>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().unwrap();
        let new_issue = NewIssue {
            id: &uuid::Uuid::new_v4().to_string(),
            column_id: &data.column_id,
            title: &data.title,
            body: &data.body,
        };
        let issues: Vec<Issue> = insert_into(issues::dsl::issues)
            .values(new_issue)
            .get_results(&*db_connection)
            .expect("Create issue error");

        let issue: &Issue = issues
            .first()
            .unwrap();

        Ok(Response::new(ProtoIssue {
            id: String::from(&issue.id),
            column_id: String::from(&issue.column_id),
            title: String::from(&issue.title),
            body: String::from(&issue.body),
        }))
    }

    async fn update_issue(
        &self,
        request: Request<UpdateIssueRequest>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().unwrap();

        let issues: Vec<Issue> = update(issues::dsl::issues)
            .filter(issues::dsl::id.eq(&data.issue_id))
            .set((
                issues::dsl::title.eq(&data.title.to_owned().unwrap()),
                issues::dsl::body.eq(&data.body.to_owned().unwrap())
            ))
            .get_results(&*db_connection)
            .expect("Update issue error");

        let issue: &Issue = issues
            .first()
            .unwrap();

        Ok(Response::new(ProtoIssue {
            id: String::from(&issue.id),
            column_id: String::from(&issue.column_id),
            title: String::from(&issue.title),
            body: String::from(&issue.body),
        }))
    }

    async fn delete_issue_by_id(
        &self,
        request: Request<IssueId>,
    ) -> Result<Response<ProtoIssue>, Status> {
        let db_connection = self.pool.get().unwrap();
        let issues: Vec<Issue> = delete(issues::dsl::issues)
            .filter(issues::dsl::id.eq(&request.get_ref().issue_id))
            .get_results(&*db_connection)
            .expect("Delete issue by id error");

        let issue: &Issue = issues
            .first()
            .unwrap();

        Ok(Response::new(ProtoIssue {
            id: String::from(&issue.id),
            column_id: String::from(&issue.column_id),
            title: String::from(&issue.title),
            body: String::from(&issue.body),
        }))
    }
}
