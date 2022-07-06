use diesel::{r2d2::{ConnectionManager, PoolError}, PgConnection};
use dotenv::dotenv;
use r2d2::Pool;
use std::env;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

fn init_pool(database_url: &str) -> Result<PgPool, PoolError> {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder().build(manager)
}

pub fn establish_connection() -> PgPool {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL env variable must be set");
    init_pool(&database_url).expect("Failed to create pool")
}