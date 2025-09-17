use super::backend::TursoBackend;
use super::AsyncTursoConnection;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::QueryResult;
use diesel_async::*;
use scoped_futures::ScopedFutureExt;
use std::fmt::Debug;

#[cfg(any(
    feature = "bb8",
    feature = "deadpool",
    feature = "mobc",
    feature = "r2d2"
))]
mod pooling;
mod type_check;

async fn transaction_test<C: AsyncConnection<Backend = TestBackend>>(
    conn: &mut C,
) -> QueryResult<()> {
    let res = conn
        .transaction::<i32, diesel::result::Error, _>(|conn| {
            async move {
                let users: Vec<User> = users::table.load(conn).await?;
                assert_eq!(&users[0].name, "John Doe");
                assert_eq!(&users[1].name, "Jane Doe");

                let user: Option<User> = users::table.find(42).first(conn).await.optional()?;
                assert_eq!(user, None::<User>);

                let res = conn
                    .transaction::<_, diesel::result::Error, _>(|conn| {
                        async move {
                            diesel::insert_into(users::table)
                                .values(users::name.eq("Dave"))
                                .execute(conn)
                                .await?;
                            let count = users::table.count().get_result::<i64>(conn).await?;
                            assert_eq!(count, 3);
                            Ok(())
                        }
                        .scope_boxed()
                    })
                    .await;
                assert!(res.is_ok());
                let count = users::table.count().get_result::<i64>(conn).await?;
                assert_eq!(count, 3);

                let res = diesel::insert_into(users::table)
                    .values(users::name.eq("Eve"))
                    .execute(conn)
                    .await?;

                assert_eq!(res, 1, "Insert in transaction returned wrong result");
                let count = users::table.count().get_result::<i64>(conn).await?;
                assert_eq!(count, 4);

                Err(diesel::result::Error::RollbackTransaction)
            }
            .scope_boxed()
        })
        .await;
    assert_eq!(
        res,
        Err(diesel::result::Error::RollbackTransaction),
        "Failed to rollback transaction"
    );

    let count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(count, 2, "user got committed, but transaction rolled back");

    Ok(())
}

diesel::table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(
    diesel::Queryable,
    diesel::Selectable,
    Debug,
    PartialEq,
    diesel::AsChangeset,
    diesel::Identifiable,
)]
struct User {
    id: i32,
    name: String,
}

type TestConnection = AsyncTursoConnection;
type TestBackend = TursoBackend;

#[tokio::test]
async fn test_basic_insert_and_load() -> QueryResult<()> {
    let conn = &mut connection().await;
    // Insertion split into 2 since Sqlite batch insert isn't supported for diesel_async yet
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("John Doe"))
        .execute(conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("Jane Doe"))
        .execute(conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let users = users::table.load::<User>(conn).await?;
    assert_eq!(&users[0].name, "John Doe", "User name [0] does not match");
    assert_eq!(&users[1].name, "Jane Doe", "User name [1] does not match");

    transaction_test(conn).await?;

    Ok(())
}

async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn connection() -> TestConnection {
    let mut conn = connection_without_transaction().await;
    setup(&mut conn).await;
    conn.begin_test_transaction().await.unwrap();
    conn
}

async fn connection_without_transaction() -> TestConnection {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    TestConnection::establish(&db_url).await.unwrap()
}
