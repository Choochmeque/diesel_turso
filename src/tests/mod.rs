use super::backend::TursoBackend;
use super::AsyncTursoConnection;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{JoinOnDsl, QueryResult, TextExpressionMethods};
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
mod turso_unit_tests;

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

diesel::table! {
    posts {
        id -> Integer,
        title -> Text,
        body -> Text,
        published -> Bool,
        user_id -> Integer,
        created_at -> Timestamp,
    }
}

diesel::table! {
    comments {
        id -> Integer,
        post_id -> Integer,
        user_id -> Integer,
        content -> Text,
        rating -> Nullable<Integer>,
    }
}

diesel::table! {
    categories {
        id -> Integer,
        name -> Text,
        description -> Nullable<Text>,
    }
}

diesel::table! {
    post_categories (post_id, category_id) {
        post_id -> Integer,
        category_id -> Integer,
    }
}

diesel::joinable!(posts -> users (user_id));
diesel::joinable!(comments -> posts (post_id));
diesel::joinable!(comments -> users (user_id));
diesel::joinable!(post_categories -> posts (post_id));
diesel::joinable!(post_categories -> categories (category_id));

diesel::allow_tables_to_appear_in_same_query!(users, posts, comments, categories, post_categories,);

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

#[derive(diesel::Insertable)]
#[diesel(table_name = users)]
struct NewUser {
    name: String,
}

#[derive(
    diesel::Queryable,
    diesel::Selectable,
    Debug,
    PartialEq,
    diesel::AsChangeset,
    diesel::Identifiable,
)]
#[diesel(table_name = posts)]
struct Post {
    id: i32,
    title: String,
    body: String,
    published: bool,
    user_id: i32,
    created_at: chrono::NaiveDateTime,
}

#[derive(diesel::Insertable)]
#[diesel(table_name = posts)]
struct NewPost<'a> {
    title: &'a str,
    body: &'a str,
    published: bool,
    user_id: i32,
    created_at: chrono::NaiveDateTime,
}

#[derive(
    diesel::Queryable,
    diesel::Selectable,
    Debug,
    PartialEq,
    diesel::AsChangeset,
    diesel::Identifiable,
)]
#[diesel(table_name = comments)]
struct Comment {
    id: i32,
    post_id: i32,
    user_id: i32,
    content: String,
    rating: Option<i32>,
}

#[derive(diesel::Insertable)]
#[diesel(table_name = comments)]
struct NewComment<'a> {
    post_id: i32,
    user_id: i32,
    content: &'a str,
    rating: Option<i32>,
}

#[derive(
    diesel::Queryable,
    diesel::Selectable,
    Debug,
    PartialEq,
    diesel::AsChangeset,
    diesel::Identifiable,
)]
#[diesel(table_name = categories)]
struct Category {
    id: i32,
    name: String,
    description: Option<String>,
}

type TestConnection = AsyncTursoConnection;
type TestBackend = TursoBackend;

#[tokio::test]
async fn test_basic_insert_and_load() -> QueryResult<()> {
    let conn = &mut connection().await;

    // let res = diesel::sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
    //     .execute(conn)
    //     .await;
    // assert!(res.is_ok(), "Failed to set journal mode");

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

    diesel::sql_query(
        "CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                published BOOLEAN NOT NULL DEFAULT 0,
                user_id INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TABLE comments (
                id INTEGER PRIMARY KEY,
                post_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                rating INTEGER,
                FOREIGN KEY (post_id) REFERENCES posts(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT
            )",
    )
    .execute(connection)
    .await
    .unwrap();

    diesel::sql_query(
        "CREATE TABLE post_categories (
                post_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                PRIMARY KEY (post_id, category_id),
                FOREIGN KEY (post_id) REFERENCES posts(id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

pub async fn connection() -> TestConnection {
    let mut conn = connection_without_transaction().await;
    setup(&mut conn).await;
    conn.begin_test_transaction().await.unwrap();
    conn
}

async fn connection_without_transaction() -> TestConnection {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    TestConnection::establish(&db_url).await.unwrap()
}

#[tokio::test]
async fn test_crud_operations() -> QueryResult<()> {
    let conn = &mut connection().await;

    for name in &["Alice", "Bob", "Charlie"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let user_count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(user_count, 3);

    let alice = users::table
        .filter(users::name.eq("Alice"))
        .first::<User>(conn)
        .await?;
    assert_eq!(alice.name, "Alice");

    let now = chrono::Utc::now().naive_utc();
    let new_post = NewPost {
        title: "My First Post",
        body: "This is the content",
        published: true,
        user_id: alice.id,
        created_at: now,
    };

    diesel::insert_into(posts::table)
        .values(&new_post)
        .execute(conn)
        .await?;

    let post = posts::table
        .filter(posts::title.eq("My First Post"))
        .first::<Post>(conn)
        .await?;

    assert_eq!(post.title, "My First Post");
    assert_eq!(post.body, "This is the content");
    assert_eq!(post.published, true);
    assert_eq!(post.user_id, alice.id);

    Ok(())
}

#[tokio::test]
async fn test_filtering_and_where_clauses() -> QueryResult<()> {
    let conn = &mut connection().await;

    for i in 1..=10 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("User{}", i)))
            .execute(conn)
            .await?;
    }

    let now = chrono::Utc::now().naive_utc();
    for i in 1..=5 {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: &format!("Post {}", i),
                body: &format!("Content {}", i),
                published: i % 2 == 0,
                user_id: i,
                created_at: now,
            })
            .execute(conn)
            .await?;
    }

    let published_posts = posts::table
        .filter(posts::published.eq(true))
        .load::<Post>(conn)
        .await?;
    assert_eq!(published_posts.len(), 2);

    let users_with_posts = users::table
        .filter(users::id.le(5))
        .load::<User>(conn)
        .await?;
    assert_eq!(users_with_posts.len(), 5);

    let specific_users = users::table
        .filter(users::name.like("User%"))
        .filter(users::id.between(3, 7))
        .load::<User>(conn)
        .await?;
    assert_eq!(specific_users.len(), 5);

    let posts_by_user = posts::table
        .filter(posts::user_id.eq_any(vec![1, 3, 5]))
        .filter(posts::published.eq(false))
        .load::<Post>(conn)
        .await?;
    assert_eq!(posts_by_user.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_update_operations() -> QueryResult<()> {
    let conn = &mut connection().await;

    for name in &["UpdateMe", "KeepMe"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let updated_count = diesel::update(users::table)
        .filter(users::name.eq("UpdateMe"))
        .set(users::name.eq("Updated"))
        .execute(conn)
        .await?;
    assert_eq!(updated_count, 1);

    let updated_user = users::table
        .filter(users::name.eq("Updated"))
        .first::<User>(conn)
        .await?;
    assert_eq!(updated_user.name, "Updated");

    let unchanged_user = users::table
        .filter(users::name.eq("KeepMe"))
        .first::<User>(conn)
        .await?;
    assert_eq!(unchanged_user.name, "KeepMe");

    let now = chrono::Utc::now().naive_utc();
    for (title, body) in &[("Draft 1", "Content 1"), ("Draft 2", "Content 2")] {
        diesel::insert_into(posts::table)
            .values(NewPost {
                title,
                body,
                published: false,
                user_id: updated_user.id,
                created_at: now.clone(),
            })
            .execute(conn)
            .await?;
    }

    let published_count = diesel::update(posts::table)
        .filter(posts::published.eq(false))
        .set(posts::published.eq(true))
        .execute(conn)
        .await?;
    assert_eq!(published_count, 2);

    let all_published = posts::table
        .filter(posts::published.eq(false))
        .load::<Post>(conn)
        .await?;
    assert_eq!(all_published.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_delete_operations() -> QueryResult<()> {
    let conn = &mut connection().await;

    for i in 1..=5 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("DeleteUser{}", i)))
            .execute(conn)
            .await?;
    }

    let initial_count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(initial_count, 5);

    let deleted = diesel::delete(users::table)
        .filter(users::name.like("DeleteUser%"))
        .filter(users::id.gt(2))
        .execute(conn)
        .await?;
    assert_eq!(deleted, 3);

    let remaining_count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(remaining_count, 2);

    let user = users::table.first::<User>(conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for i in 1..=3 {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: &format!("Delete Post {}", i),
                body: "Will be deleted",
                published: true,
                user_id: user.id,
                created_at: now.clone(),
            })
            .execute(conn)
            .await?;
    }

    diesel::delete(posts::table)
        .filter(posts::title.like("Delete Post%"))
        .execute(conn)
        .await?;

    let posts_count = posts::table.count().get_result::<i64>(conn).await?;
    assert_eq!(posts_count, 0);

    Ok(())
}

#[tokio::test]
async fn test_ordering_and_limiting() -> QueryResult<()> {
    let conn = &mut connection().await;

    let names = vec!["Zara", "Alice", "Bob", "Charlie", "David"];
    for name in &names {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let ordered_asc = users::table
        .order(users::name.asc())
        .load::<User>(conn)
        .await?;
    assert_eq!(ordered_asc[0].name, "Alice");
    assert_eq!(ordered_asc[4].name, "Zara");

    let ordered_desc = users::table
        .order(users::name.desc())
        .load::<User>(conn)
        .await?;
    assert_eq!(ordered_desc[0].name, "Zara");
    assert_eq!(ordered_desc[4].name, "Alice");

    let top_3 = users::table
        .order(users::name.asc())
        .limit(3)
        .load::<User>(conn)
        .await?;
    assert_eq!(top_3.len(), 3);
    assert_eq!(top_3[0].name, "Alice");
    assert_eq!(top_3[2].name, "Charlie");

    let middle_2 = users::table
        .order(users::name.asc())
        .limit(2)
        .offset(2)
        .load::<User>(conn)
        .await?;
    assert_eq!(middle_2.len(), 2);
    assert_eq!(middle_2[0].name, "Charlie");
    assert_eq!(middle_2[1].name, "David");

    Ok(())
}

#[tokio::test]
async fn test_aggregate_functions() -> QueryResult<()> {
    let conn = &mut connection().await;

    for i in 1..=10 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("User{:02}", i)))
            .execute(conn)
            .await?;
    }

    let now = chrono::Utc::now().naive_utc();
    for i in 1..=5 {
        for j in 1..=i {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post {}-{}", i, j),
                    body: &format!("Content for post {}-{}", i, j),
                    published: true,
                    user_id: i,
                    created_at: now.clone(),
                })
                .execute(conn)
                .await?;
        }
    }

    let total_users = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(total_users, 10);

    let total_posts = posts::table.count().get_result::<i64>(conn).await?;
    assert_eq!(total_posts, 15);

    let max_user_id = users::table
        .select(diesel::dsl::max(users::id))
        .first::<Option<i32>>(conn)
        .await?;
    assert_eq!(max_user_id, Some(10));

    let min_user_id = users::table
        .select(diesel::dsl::min(users::id))
        .first::<Option<i32>>(conn)
        .await?;
    assert_eq!(min_user_id, Some(1));

    let comments = [
        NewComment {
            post_id: 1,
            user_id: 1,
            content: "Great!",
            rating: Some(5),
        },
        NewComment {
            post_id: 1,
            user_id: 2,
            content: "Good",
            rating: Some(4),
        },
        NewComment {
            post_id: 1,
            user_id: 3,
            content: "OK",
            rating: Some(3),
        },
    ];
    for comment in &comments {
        diesel::insert_into(comments::table)
            .values(comment)
            .execute(conn)
            .await?;
    }

    let sum_rating = comments::table
        .select(diesel::dsl::sum(comments::rating))
        .first::<Option<i64>>(conn)
        .await?;
    assert_eq!(sum_rating, Some(12));

    Ok(())
}

#[tokio::test]
async fn test_join_operations() -> QueryResult<()> {
    let conn = &mut connection().await;

    for name in &["Author1", "Author2", "Author3"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let users_list = users::table.load::<User>(conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for (i, user) in users_list.iter().enumerate() {
        for j in 0..=i {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post by {}", user.name),
                    body: &format!("Content {}", j),
                    published: true,
                    user_id: user.id,
                    created_at: now.clone(),
                })
                .execute(conn)
                .await?;
        }
    }

    let posts_with_users = posts::table
        .inner_join(users::table)
        .select((posts::title, users::name))
        .load::<(String, String)>(conn)
        .await?;

    assert_eq!(posts_with_users.len(), 6);

    let author1_posts = posts::table
        .inner_join(users::table)
        .filter(users::name.eq("Author1"))
        .select(posts::id)
        .load::<i32>(conn)
        .await?;
    assert_eq!(author1_posts.len(), 1);

    let author3_posts = posts::table
        .inner_join(users::table)
        .filter(users::name.eq("Author3"))
        .select(posts::title)
        .load::<String>(conn)
        .await?;
    assert_eq!(author3_posts.len(), 3);

    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("Tech"),
            categories::description.eq(Some("Technology posts")),
        ))
        .execute(conn)
        .await?;
    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("Life"),
            categories::description.eq(None::<&str>),
        ))
        .execute(conn)
        .await?;

    let categories_list = categories::table.load::<Category>(conn).await?;
    let posts_list = posts::table.limit(3).load::<Post>(conn).await?;

    for post in &posts_list {
        diesel::insert_into(post_categories::table)
            .values((
                post_categories::post_id.eq(post.id),
                post_categories::category_id.eq(categories_list[0].id),
            ))
            .execute(conn)
            .await?;
    }

    let tech_posts = posts::table
        .inner_join(post_categories::table.on(posts::id.eq(post_categories::post_id)))
        .inner_join(categories::table.on(post_categories::category_id.eq(categories::id)))
        .filter(categories::name.eq("Tech"))
        .select(posts::title)
        .load::<String>(conn)
        .await?;

    assert_eq!(tech_posts.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_batch_operations() -> QueryResult<()> {
    let conn = &mut connection().await;

    // Create a vector of users for batch insert
    let new_users: Vec<NewUser> = (1..=100)
        .map(|i| NewUser {
            name: format!("BatchUser{:03}", i),
        })
        .collect();

    // Batch insert all users at once
    diesel::insert_into(users::table)
        .values(&new_users)
        .execute(conn)
        .await?;

    let count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(count, 100);

    // Batch update: update all users with id <= 50
    let batch_update = diesel::update(users::table)
        .filter(users::id.le(50))
        .set(users::name.eq("BatchUpdated"))
        .execute(conn)
        .await?;
    assert_eq!(batch_update, 50);

    let updated_count = users::table
        .filter(users::name.eq("BatchUpdated"))
        .count()
        .get_result::<i64>(conn)
        .await?;
    assert_eq!(updated_count, 50);

    // Batch delete: delete all users with id > 75
    let batch_delete = diesel::delete(users::table)
        .filter(users::id.gt(75))
        .execute(conn)
        .await?;
    assert_eq!(batch_delete, 25);

    let remaining = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(remaining, 75);

    Ok(())
}

#[tokio::test]
async fn test_nullable_fields() -> QueryResult<()> {
    let conn = &mut connection().await;

    for (name, desc) in &[
        ("WithDesc", Some("Has description")),
        ("NoDesc", None),
        ("EmptyDesc", Some("")),
    ] {
        diesel::insert_into(categories::table)
            .values((categories::name.eq(name), categories::description.eq(*desc)))
            .execute(conn)
            .await?;
    }

    let all_categories = categories::table.load::<Category>(conn).await?;
    assert_eq!(all_categories.len(), 3);

    let with_desc = categories::table
        .filter(categories::description.is_not_null())
        .load::<Category>(conn)
        .await?;
    assert_eq!(with_desc.len(), 2);

    let without_desc = categories::table
        .filter(categories::description.is_null())
        .load::<Category>(conn)
        .await?;
    assert_eq!(without_desc.len(), 1);
    assert_eq!(without_desc[0].name, "NoDesc");

    for name in &["CommentUser1", "CommentUser2"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let users_list = users::table.load::<User>(conn).await?;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "Test Post",
            body: "Content",
            published: true,
            user_id: users_list[0].id,
            created_at: now.clone(),
        })
        .execute(conn)
        .await?;

    let post = posts::table.first::<Post>(conn).await?;

    let comments = [
        NewComment {
            post_id: post.id,
            user_id: users_list[0].id,
            content: "Rated",
            rating: Some(5),
        },
        NewComment {
            post_id: post.id,
            user_id: users_list[1].id,
            content: "Unrated",
            rating: None,
        },
    ];
    for comment in &comments {
        diesel::insert_into(comments::table)
            .values(comment)
            .execute(conn)
            .await?;
    }

    let rated_comments = comments::table
        .filter(comments::rating.is_not_null())
        .load::<Comment>(conn)
        .await?;
    assert_eq!(rated_comments.len(), 1);
    assert_eq!(rated_comments[0].rating, Some(5));

    let unrated_comments = comments::table
        .filter(comments::rating.is_null())
        .load::<Comment>(conn)
        .await?;
    assert_eq!(unrated_comments.len(), 1);
    assert_eq!(unrated_comments[0].content, "Unrated");

    Ok(())
}

#[tokio::test]
async fn test_distinct_and_grouping() -> QueryResult<()> {
    let conn = &mut connection().await;

    for name in &["Alice", "Bob", "Alice", "Charlie", "Bob"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(conn)
            .await?;
    }

    let all_names = users::table
        .select(users::name)
        .load::<String>(conn)
        .await?;
    assert_eq!(all_names.len(), 5);

    let distinct_names = users::table
        .select(users::name)
        .distinct()
        .order(users::name.asc())
        .load::<String>(conn)
        .await?;
    assert_eq!(distinct_names.len(), 3);
    assert_eq!(distinct_names, vec!["Alice", "Bob", "Charlie"]);

    let users_list = users::table.load::<User>(conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for user in &users_list {
        for i in 1..=user.id % 3 + 1 {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post {}", i),
                    body: "Content",
                    published: true,
                    user_id: user.id,
                    created_at: now.clone(),
                })
                .execute(conn)
                .await?;
        }
    }

    let post_counts = posts::table
        .group_by(posts::user_id)
        .select((posts::user_id, diesel::dsl::count(posts::id)))
        .load::<(i32, i64)>(conn)
        .await?;

    assert!(!post_counts.is_empty());

    let users_with_multiple_posts = posts::table
        .group_by(posts::user_id)
        .select(posts::user_id)
        .having(diesel::dsl::count(posts::id).gt(1))
        .load::<i32>(conn)
        .await?;

    assert!(!users_with_multiple_posts.is_empty());

    Ok(())
}
