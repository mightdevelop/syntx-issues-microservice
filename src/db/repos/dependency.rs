use diesel::result::Error;

use crate::db;
use db::schema::dependencies;

use diesel::{
    RunQueryDsl,
    r2d2::ConnectionManager,
    PgConnection,
    ExpressionMethods,
    insert_into,
    update,
    delete
};
use r2d2::PooledConnection;

#[derive(Queryable)]
pub struct Dependency {
    pub id: String,
    pub blocking_epic_id: String,
    pub blocked_epic_id: String,
}

#[derive(Insertable)]
#[table_name="dependencies"]
pub struct NewDependency<'a> {
    pub id: &'a str,
    pub blocking_epic_id: &'a str,
    pub blocked_epic_id: &'a str,
}

#[derive(AsChangeset)]
#[table_name="dependencies"]
pub struct DependencyChangeSet {
    pub blocking_epic_id: Option<String>,
    pub blocked_epic_id: Option<String>,
}

#[tonic::async_trait]
pub trait CreateDependency {
    async fn create<'a>(
        new_dependency: NewDependency<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error>;
}

#[tonic::async_trait]
impl CreateDependency for Dependency {
    async fn create<'a>(
        new_dependency: NewDependency<'a>,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error> {
        let result: Vec<Dependency> = match insert_into(dependencies::dsl::dependencies)
            .values(new_dependency)
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let dependency: &Dependency = result
            .first()
            .unwrap();

        Ok(Dependency {
            id: dependency.id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait UpdateDependency {
    async fn update<'a>(
        dependency_id: &'a str,
        change_set: DependencyChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error>;
}

#[tonic::async_trait]
impl UpdateDependency for Dependency {
    async fn update<'a>(
        dependency_id: &'a str,
        change_set: DependencyChangeSet,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error> {
        let result: Vec<Dependency> = match update(dependencies::dsl::dependencies)
            .filter(dependencies::dsl::id.eq(dependency_id))
            .set(change_set)
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let dependency: &Dependency = match result.first() {
            Some(dep) => dep,
            None => return Err(Error::NotFound),
        };

        Ok(Dependency {
            id: dependency.id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
        })
    }
}

#[tonic::async_trait]
pub trait DeleteDependency {
    async fn delete<'a>(
        dependency_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error>;
}

#[tonic::async_trait]
impl DeleteDependency for Dependency {
    async fn delete<'a>(
        dependency_id: &'a str,
        db_connection: PooledConnection<ConnectionManager<PgConnection>>
    ) -> Result<Dependency, Error> {
        let result: Vec<Dependency> = match delete(dependencies::dsl::dependencies)
            .filter(dependencies::dsl::id.eq(dependency_id))
            .get_results(&*db_connection) {
                Ok(res) => res,
                Err(err) => return Err(err),
            };

        let dependency: &Dependency = match result.first() {
            Some(dep) => dep,
            None => return Err(Error::NotFound),
        };

        Ok(Dependency {
            id: dependency.id.clone(),
            blocked_epic_id: dependency.blocked_epic_id.clone(),
            blocking_epic_id: dependency.blocking_epic_id.clone(),
        })
    }
}