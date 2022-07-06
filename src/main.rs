#[macro_use]
extern crate diesel;

mod controllers;
mod db;


use tonic::transport::Server;
use controllers::{
    boards::BoardsController,
    columns::ColumnsController,
    issues::IssuesController,
};
use proto::boards::{
    boards_service_server::BoardsServiceServer,
    columns_service_server::ColumnsServiceServer,
    issues_service_server::IssuesServiceServer,
};
use dotenv::dotenv;
use std::env;

use crate::db::connection::establish_connection;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let app_url = env::var("APP_URL")?.parse()?;

    let pool = establish_connection();

    let boards_controller = BoardsController {pool: pool.clone()};
    let columns_controller = ColumnsController {pool: pool.clone()};
    let issues_controller = IssuesController {pool: pool.clone()};

    let boards_service_server = BoardsServiceServer::new(boards_controller);
    let columns_service_server = ColumnsServiceServer::new(columns_controller);
    let issues_service_server = IssuesServiceServer::new(issues_controller);

    println!("Boards service listening on {}", app_url);
    Server::builder()
        .add_service(boards_service_server)
        .add_service(columns_service_server)
        .add_service(issues_service_server)
        .serve(app_url)
        .await?;

    Ok(())
}