use super::{users, User};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
//#[cfg(not(feature = "sqlite"))]
//use diesel_async::SaveChangesDsl;

#[tokio::test]
#[cfg(feature = "bb8")]
async fn save_changes_bb8() {
    use diesel_async::pooled_connection::bb8::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let config = AsyncDieselConnectionManager::<super::TestConnection>::new(db_url);
    let pool = Pool::builder()
        .max_size(1)
        .build(config)
        .await
        .expect("build bb8 pool");

    let mut conn = pool.get().await.expect("checkout bb8 connection");

    super::setup(&mut conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .expect("insert user");

    let u = users::table
        .first::<User>(&mut conn)
        .await
        .expect("load first user");
    assert_eq!(u.name, "John");
    drop(conn);
}

#[tokio::test]
#[cfg(feature = "deadpool")]
async fn save_changes_deadpool() {
    use diesel_async::pooled_connection::deadpool::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let config = AsyncDieselConnectionManager::<super::TestConnection>::new(db_url);
    let pool = Pool::builder(config)
        .max_size(1)
        .build()
        .expect("build deadpool pool");

    let mut conn = pool.get().await.expect("checkout deadpool connection");

    super::setup(&mut conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .expect("insert user");

    let u = users::table
        .first::<User>(&mut conn)
        .await
        .expect("load first user");
    assert_eq!(u.name, "John");
    drop(conn);
}

#[tokio::test]
#[cfg(feature = "mobc")]
async fn save_changes_mobc() {
    use diesel_async::pooled_connection::mobc::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = Pool::new(AsyncDieselConnectionManager::<super::TestConnection>::new(
        db_url,
    ));

    let mut conn = pool.get().await.expect("checkout mobc connection");

    super::setup(&mut conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .expect("insert user");

    let u = users::table
        .first::<User>(&mut conn)
        .await
        .expect("load first user");
    assert_eq!(u.name, "John");
    drop(conn);
}

#[tokio::test]
#[cfg(feature = "r2d2")]
// Clippy can't see that `conn` is moved into the trailing `spawn_blocking(move || drop(conn))`,
// so it thinks the connection is held until the end of the fn. The drop already happens
// as early as it can without panicking on a tokio worker thread.
#[allow(clippy::significant_drop_tightening)]
async fn save_changes_r2d2() {
    use diesel::r2d2::{ConnectionManager, Pool};
    use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;

    type AsyncWrapper = AsyncConnectionWrapper<super::TestConnection>;

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let config: ConnectionManager<AsyncWrapper> = ConnectionManager::new(&db_url);
    let pool = Pool::builder().build(config).expect("build r2d2 pool");

    let mut conn =
        tokio::task::spawn_blocking(move || pool.get().expect("checkout r2d2 connection"))
            .await
            .expect("spawn_blocking checkout");

    super::setup(&mut conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .expect("insert user");

    let u = users::table
        .first::<User>(&mut conn)
        .await
        .expect("load first user");
    assert_eq!(u.name, "John");

    // The wrapper owns an internal tokio runtime; dropping it on a tokio
    // worker thread would panic, so move the drop to a blocking thread.
    tokio::task::spawn_blocking(move || drop(conn))
        .await
        .expect("spawn_blocking drop");
}
