use std::pin::Pin;
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
        dependencies_service_server::DependenciesService, 
        Dependency as ProtoDependency, 
        DependencyId,
        CreateDependencyRequest,
        SearchDependenciesParams,
    }, 
    eventbus::{dependencies_events_service_client::DependenciesEventsServiceClient, DependencyEvent, self, SearchDependenciesEvent}
};

use crate::{
    db::{
        repos::dependency::{NewDependency, Dependency, CreateDependency, DeleteDependency},
        schema::dependencies::dsl::*, 
        connection::PgPool,
    },
};

pub struct DependenciesController {
    pub pool: PgPool,
    pub eventbus_service_client: DependenciesEventsServiceClient<Channel>
}

#[tonic::async_trait]
impl DependenciesService for DependenciesController {
    async fn get_dependency_by_id(
        &self,
        request: Request<DependencyId>,
    ) -> Result<Response<ProtoDependency>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        let result: QueryResult<Vec<Dependency>> = dependencies
            .filter(id.eq(&request.get_ref().dependency_id))
            .limit(1)
            .load::<Dependency>(&*db_connection);

        match result {
            Ok(vec) => {
                if let Some(dep) = vec.first() {
                    let dependency = eventbus::Dependency {
                        id: Some(dep.id.clone()),
                        blocked_epic_id: Some(dep.blocked_epic_id.clone()),
                        blocking_epic_id: Some(dep.blocking_epic_id.clone()),
                    };
                    let req = Request::new(DependencyEvent {
                        dependency: Some(dependency),
                        error: None
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_dependency_by_id_event(req).await;
                    });
                    Ok(Response::new(ProtoDependency {
                        id: dep.id.clone(),
                        blocking_epic_id: dep.blocking_epic_id.clone(),
                        blocked_epic_id: dep.blocked_epic_id.clone(),
                    }))
                } else {
                    let dependency = eventbus::Dependency {
                        id: Some(data.dependency_id.clone()),
                        blocked_epic_id: None,
                        blocking_epic_id: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: String::from("Dependency not found")
                    };
                    let req = Request::new(DependencyEvent {
                        dependency: Some(dependency),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.get_dependency_by_id_event(req).await;
                    });
                    Err(Status::not_found("Dependency not found"))
                }
            }
            Err(err) => {
                let dependency = eventbus::Dependency {
                    id: Some(data.dependency_id.clone()),
                    blocked_epic_id: None,
                    blocking_epic_id: None,
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(DependencyEvent {
                    dependency: Some(dependency),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.get_dependency_by_id_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
    }

    type searchDependenciesStream = Pin<Box<dyn Stream<Item = Result<ProtoDependency, Status>> + Send>>;

    async fn search_dependencies(
        &self,
        request: Request<SearchDependenciesParams>,
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

        let result: QueryResult<Vec<Dependency>> = query
            .load::<Dependency>(&*db_connection);

        match result {
            Ok(vec) => {
                let deps = vec
                    .iter()
                    .map(|dependency| eventbus::Dependency {
                        id: Some(dependency.id.clone()),
                        blocked_epic_id: Some(dependency.blocked_epic_id.clone()),
                        blocking_epic_id: Some(dependency.blocking_epic_id.clone()),
                    })
                    .collect::<Vec<eventbus::Dependency>>();
                let search_params = eventbus::SearchDependenciesParams {
                    dependencies_ids: data.dependencies_ids.clone(),
                    blocked_epic_id: data.blocked_epic_id.clone(),
                    blocking_epic_id: data.blocking_epic_id.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };

                let req = Request::new(SearchDependenciesEvent {
                    dependencies: deps,
                    error: None,
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();

                let proto_dependencies: Vec<ProtoDependency> = vec
                    .iter()
                    .map(|dependency| ProtoDependency {
                        id: dependency.id.clone(),
                        blocked_epic_id: dependency.blocked_epic_id.clone(),
                        blocking_epic_id: dependency.blocking_epic_id.clone(),
                    })
                    .collect();
        
                let mut stream = tokio_stream::iter(proto_dependencies);
                let (sender, receiver) = mpsc::channel(1);
        
                tokio::spawn(async move {
                    while let Some(dependency) = stream.next().await {
                        match sender.send(Result::<ProtoDependency, Status>::Ok(dependency)).await {
                            Ok(_) => {},
                            Err(_err) => break
                        }
                    }
                    service.search_dependencies_event(req).await;
                });
        
                let output_stream = ReceiverStream::new(receiver);
        
                Ok(Response::new(
                    Box::pin(output_stream) as Self::searchDependenciesStream
                ))
            }
            Err(err) => {
                let deps = data.dependencies_ids
                    .iter()
                    .map(|dependency_id| eventbus::Dependency {
                        id: Some(dependency_id.clone()),
                        blocked_epic_id: None,
                        blocking_epic_id: None,
                    })
                    .collect::<Vec<eventbus::Dependency>>();
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let search_params = eventbus::SearchDependenciesParams {
                    dependencies_ids: data.dependencies_ids.clone(),
                    blocked_epic_id: data.blocked_epic_id.clone(),
                    blocking_epic_id: data.blocking_epic_id.clone(),
                    limit: data.limit.clone(),
                    offset: data.offset.clone(),
                };

                let req = Request::new(SearchDependenciesEvent {
                    dependencies: deps,
                    error: Some(error),
                    search_params: Some(search_params)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.search_dependencies_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            }
        }
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

        match Dependency::create(new_dependency, db_connection).await {
            Ok(dep) => {
                let dependency = eventbus::Dependency {
                    id: Some(dep.id.clone()),
                    blocking_epic_id: Some(dep.blocking_epic_id.clone()),
                    blocked_epic_id: Some(dep.blocked_epic_id.clone()),
                };
                let req = Request::new(DependencyEvent {
                    dependency: Some(dependency),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_dependency_event(req).await;
                });

                Ok(Response::new(ProtoDependency {
                    id: dep.id.clone(),
                    blocking_epic_id: dep.blocking_epic_id.clone(),
                    blocked_epic_id: dep.blocked_epic_id.clone(),
                }))
            },
            Err(err) => {
                let dependency = eventbus::Dependency {
                    id: None,
                    blocking_epic_id: Some(data.blocking_epic_id.clone()),
                    blocked_epic_id: Some(data.blocked_epic_id.clone()),
                };
                let error = eventbus::Error {
                    code: Code::Unavailable.into(),
                    message: err.to_string()
                };
                let req = Request::new(DependencyEvent {
                    dependency: Some(dependency),
                    error: Some(error)
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.create_dependency_event(req).await;
                });
                Err(Status::unavailable("Database is unavailable"))
            },
        }
    }

    async fn delete_dependency(
        &self,
        request: Request<DependencyId>,
    ) -> Result<Response<ProtoDependency>, Status> {
        let data = request.get_ref();
        let db_connection = self.pool.get().expect("Db error");

        match Dependency::delete(&data.dependency_id, db_connection).await {
            Ok(dep) => {
                let dependency = eventbus::Dependency {
                    id: Some(dep.id.clone()),
                    blocked_epic_id: Some(dep.blocked_epic_id.clone()),
                    blocking_epic_id: Some(dep.blocking_epic_id.clone()),
                };
                let req = Request::new(DependencyEvent {
                    dependency: Some(dependency),
                    error: None
                });
                let mut service = self.eventbus_service_client.clone();
                tokio::spawn(async move {
                    service.delete_dependency_event(req).await;
                });
                Ok(Response::new(ProtoDependency {
                    id: dep.id.clone(),
                    blocking_epic_id: dep.blocking_epic_id.clone(),
                    blocked_epic_id: dep.blocked_epic_id.clone(),
                }))
            }
            Err(err) => {
                if err == NotFound {
                    let dependency = eventbus::Dependency {
                        id: Some(data.dependency_id.clone()),
                        blocked_epic_id: None,
                        blocking_epic_id: None,
                    };
                    let error = eventbus::Error {
                        code: Code::NotFound.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(DependencyEvent {
                        dependency: Some(dependency),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_dependency_event(req).await;
                    });
                    Err(Status::not_found("Dependency not found"))
                } else {
                    let dependency = eventbus::Dependency {
                        id: Some(data.dependency_id.clone()),
                        blocked_epic_id: None,
                        blocking_epic_id: None,
                    };
                    let error = eventbus::Error {
                        code: Code::Unavailable.into(),
                        message: err.to_string()
                    };
                    let req = Request::new(DependencyEvent {
                        dependency: Some(dependency),
                        error: Some(error)
                    });
                    let mut service = self.eventbus_service_client.clone();
                    tokio::spawn(async move {
                        service.delete_dependency_event(req).await;
                    });
                    Err(Status::unavailable("Database is unavailable"))
                }
            }
        }
    }
}
