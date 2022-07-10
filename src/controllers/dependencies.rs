use std::pin::Pin;
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
    dependencies_service_server::DependenciesService, 
    Dependency as ProtoDependency, 
    DependencyId,
    CreateDependencyRequest, DependenciesSearchParams,
};

use crate::{
    db::{
        repos::dependency::{NewDependency, Dependency, CreateDependency, DeleteDependency},
        schema::dependencies::dsl::*, 
        connection::PgPool,
    },
};

pub struct DependenciesController {
    pub pool: PgPool
}

#[tonic::async_trait]
impl DependenciesService for DependenciesController {
    async fn get_dependency_by_id(
        &self,
        request: Request<DependencyId>,
    ) -> Result<Response<ProtoDependency>, Status> {
        let db_connection = self.pool.get().expect("Db error");
        let result: Vec<Dependency> = dependencies
            .filter(id.eq(&request.get_ref().dependency_id))
            .limit(1)
            .load::<Dependency>(&*db_connection)
            .expect("Get dependency by id error");

        let dependency: &Dependency = result
            .first()
            .unwrap();

        Ok(Response::new(ProtoDependency {
            id: dependency.id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
        }))
    }

    type searchDependenciesStream = Pin<Box<dyn Stream<Item = Result<ProtoDependency, Status>> + Send>>;

    async fn search_dependencies(
        &self,
        request: Request<DependenciesSearchParams>,
    ) -> Result<Response<Self::searchDependenciesStream>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        
        let mut query = dependencies.into_boxed();

        let dependencies_ids = match data.dependencies_ids.is_empty() {
            false => Some(&data.dependencies_ids),
            true => None,
        };

        if let Some(dep_ids) = dependencies_ids {
            query = query.filter(id.eq_any(dep_ids));
        }

        if let Some(blocking_ep_id) = &data.blocking_epic_id {
            query = query.filter(blocking_epic_id.eq(blocking_ep_id));
        }

        if let Some(blocked_ep_id) = &data.blocked_epic_id {
            query = query.filter(blocked_epic_id.eq(blocked_ep_id));
        }

        let result: Vec<Dependency> = query
            .load::<Dependency>(&*db_connection)
            .expect("Get dependency by blocking epic id error");
            
        let proto_dependencies: Vec<ProtoDependency> = result.iter().map(|dependency| ProtoDependency {
            id: dependency.id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
        }).collect();

        let mut stream = tokio_stream::iter(proto_dependencies);
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            while let Some(dependency) = stream.next().await {
                match sender.send(Result::<ProtoDependency, Status>::Ok(dependency)).await {
                    Ok(_) => {},
                    Err(_err) => break
                }
            }
        });

        let output_stream = ReceiverStream::new(receiver);

        Ok(Response::new(
            Box::pin(output_stream) as Self::searchDependenciesStream
        ))
    }

    async fn create_dependency(
        &self,
        request: Request<CreateDependencyRequest>,
    ) -> Result<Response<ProtoDependency>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let new_dependency = NewDependency {
            id: &uuid::Uuid::new_v4().to_string(),
            blocking_epic_id: &data.blocking_epic_id,
            blocked_epic_id: &data.blocked_epic_id,
        };

        let dependency: Dependency = match Dependency::create(new_dependency, db_connection).await {
            Ok(dep) => dep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoDependency {
            id: dependency.id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
        }))
    }

    async fn delete_dependency(
        &self,
        request: Request<DependencyId>,
    ) -> Result<Response<ProtoDependency>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");
        
        let dependency: Dependency;
        
        match Dependency::delete(&data.dependency_id, db_connection).await {
            Ok(dep) => dependency = dep,
            Err(err) => return Err(Status::new(Code::Unavailable, err.to_string())),
        };

        Ok(Response::new(ProtoDependency {
            id: dependency.id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
        }))
    }
}
