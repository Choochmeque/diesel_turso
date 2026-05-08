use super::backend::TursoBackend;
use super::AsyncTursoConnection;
use diesel::connection::CacheSize;
use diesel::expression_methods::AggregateExpressionMethods;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{
    BelongingToDsl, BoolExpressionMethods, GroupedBy, JoinOnDsl, NullableExpressionMethods,
    QueryResult, SelectableHelper, TextExpressionMethods,
};
use diesel_async::*;
use std::fmt::Debug;

#[cfg(any(
    feature = "bb8",
    feature = "deadpool",
    feature = "mobc",
    feature = "r2d2"
))]
mod pooling;
mod turso_unit_tests;
mod type_check;

async fn transaction_test<C: AsyncConnection<Backend = TestBackend>>(
    conn: &mut C,
) -> QueryResult<()> {
    let res = conn
        .transaction::<i32, diesel::result::Error, _>(async |conn| {
            let users: Vec<User> = users::table.load(conn).await?;
            assert_eq!(&users[0].name, "John Doe");
            assert_eq!(&users[1].name, "Jane Doe");

            let user: Option<User> = users::table.find(42).first(conn).await.optional()?;
            assert_eq!(user, None::<User>);

            let res = conn
                .transaction::<_, diesel::result::Error, _>(async |conn| {
                    diesel::insert_into(users::table)
                        .values(users::name.eq("Dave"))
                        .execute(conn)
                        .await?;
                    let count = users::table.count().get_result::<i64>(conn).await?;
                    assert_eq!(count, 3);
                    Ok(())
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

diesel::alias!(posts as posts_p1: PostsP1, posts as posts_p2: PostsP2);

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
    diesel::Associations,
)]
#[diesel(table_name = posts, belongs_to(User))]
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
    let mut conn = connection().await;

    let res = diesel::sql_query("PRAGMA journal_mode = WAL;")
        .execute(&mut conn)
        .await;
    assert!(res.is_ok(), "Failed to set journal mode");

    // Insertion split into 2 since Sqlite batch insert isn't supported for diesel_async yet
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("John Doe"))
        .execute(&mut conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("Jane Doe"))
        .execute(&mut conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let users = users::table.load::<User>(&mut conn).await?;
    assert_eq!(&users[0].name, "John Doe", "User name [0] does not match");
    assert_eq!(&users[1].name, "Jane Doe", "User name [1] does not match");

    transaction_test(&mut conn).await?;

    drop(conn);
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
    .expect("create users table");

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
    .expect("create posts table");

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
    .expect("create comments table");

    diesel::sql_query(
        "CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT
            )",
    )
    .execute(connection)
    .await
    .expect("create categories table");

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
    .expect("create post_categories table");
}

pub async fn connection() -> TestConnection {
    let mut conn = connection_without_transaction().await;
    setup(&mut conn).await;
    conn.begin_test_transaction()
        .await
        .expect("begin test transaction");
    conn
}

async fn connection_without_transaction() -> TestConnection {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    TestConnection::establish(&db_url)
        .await
        .expect("establish turso connection")
}

#[tokio::test]
async fn test_crud_operations() -> QueryResult<()> {
    let mut conn = connection().await;

    for name in &["Alice", "Bob", "Charlie"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let user_count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(user_count, 3);

    let alice = users::table
        .filter(users::name.eq("Alice"))
        .first::<User>(&mut conn)
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
        .execute(&mut conn)
        .await?;

    let post = posts::table
        .filter(posts::title.eq("My First Post"))
        .first::<Post>(&mut conn)
        .await?;

    assert_eq!(post.title, "My First Post");
    assert_eq!(post.body, "This is the content");
    assert!(post.published);
    assert_eq!(post.user_id, alice.id);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_filtering_and_where_clauses() -> QueryResult<()> {
    let mut conn = connection().await;

    for i in 1..=10 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("User{i}")))
            .execute(&mut conn)
            .await?;
    }

    let now = chrono::Utc::now().naive_utc();
    for i in 1..=5 {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: &format!("Post {i}"),
                body: &format!("Content {i}"),
                published: i % 2 == 0,
                user_id: i,
                created_at: now,
            })
            .execute(&mut conn)
            .await?;
    }

    let published_posts = posts::table
        .filter(posts::published.eq(true))
        .load::<Post>(&mut conn)
        .await?;
    assert_eq!(published_posts.len(), 2);

    let users_with_posts = users::table
        .filter(users::id.le(5))
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(users_with_posts.len(), 5);

    let specific_users = users::table
        .filter(users::name.like("User%"))
        .filter(users::id.between(3, 7))
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(specific_users.len(), 5);

    let posts_by_user = posts::table
        .filter(posts::user_id.eq_any(vec![1, 3, 5]))
        .filter(posts::published.eq(false))
        .load::<Post>(&mut conn)
        .await?;
    assert_eq!(posts_by_user.len(), 3);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_update_operations() -> QueryResult<()> {
    let mut conn = connection().await;

    for name in &["UpdateMe", "KeepMe"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let updated_count = diesel::update(users::table)
        .filter(users::name.eq("UpdateMe"))
        .set(users::name.eq("Updated"))
        .execute(&mut conn)
        .await?;
    assert_eq!(updated_count, 1);

    let updated_user = users::table
        .filter(users::name.eq("Updated"))
        .first::<User>(&mut conn)
        .await?;
    assert_eq!(updated_user.name, "Updated");

    let unchanged_user = users::table
        .filter(users::name.eq("KeepMe"))
        .first::<User>(&mut conn)
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
                created_at: now,
            })
            .execute(&mut conn)
            .await?;
    }

    let published_count = diesel::update(posts::table)
        .filter(posts::published.eq(false))
        .set(posts::published.eq(true))
        .execute(&mut conn)
        .await?;
    assert_eq!(published_count, 2);

    let all_published = posts::table
        .filter(posts::published.eq(false))
        .load::<Post>(&mut conn)
        .await?;
    assert_eq!(all_published.len(), 0);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_delete_operations() -> QueryResult<()> {
    let mut conn = connection().await;

    for i in 1..=5 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("DeleteUser{i}")))
            .execute(&mut conn)
            .await?;
    }

    let initial_count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(initial_count, 5);

    let deleted = diesel::delete(users::table)
        .filter(users::name.like("DeleteUser%"))
        .filter(users::id.gt(2))
        .execute(&mut conn)
        .await?;
    assert_eq!(deleted, 3);

    let remaining_count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(remaining_count, 2);

    let user = users::table.first::<User>(&mut conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for i in 1..=3 {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: &format!("Delete Post {i}"),
                body: "Will be deleted",
                published: true,
                user_id: user.id,
                created_at: now,
            })
            .execute(&mut conn)
            .await?;
    }

    diesel::delete(posts::table)
        .filter(posts::title.like("Delete Post%"))
        .execute(&mut conn)
        .await?;

    let posts_count = posts::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(posts_count, 0);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_ordering_and_limiting() -> QueryResult<()> {
    let mut conn = connection().await;

    let names = vec!["Zara", "Alice", "Bob", "Charlie", "David"];
    for name in &names {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let ordered_asc = users::table
        .order(users::name.asc())
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(ordered_asc[0].name, "Alice");
    assert_eq!(ordered_asc[4].name, "Zara");

    let ordered_desc = users::table
        .order(users::name.desc())
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(ordered_desc[0].name, "Zara");
    assert_eq!(ordered_desc[4].name, "Alice");

    let top_3 = users::table
        .order(users::name.asc())
        .limit(3)
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(top_3.len(), 3);
    assert_eq!(top_3[0].name, "Alice");
    assert_eq!(top_3[2].name, "Charlie");

    let middle_2 = users::table
        .order(users::name.asc())
        .limit(2)
        .offset(2)
        .load::<User>(&mut conn)
        .await?;
    assert_eq!(middle_2.len(), 2);
    assert_eq!(middle_2[0].name, "Charlie");
    assert_eq!(middle_2[1].name, "David");

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_aggregate_functions() -> QueryResult<()> {
    let mut conn = connection().await;

    for i in 1..=10 {
        diesel::insert_into(users::table)
            .values(users::name.eq(format!("User{i:02}")))
            .execute(&mut conn)
            .await?;
    }

    let now = chrono::Utc::now().naive_utc();
    for i in 1..=5 {
        for j in 1..=i {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post {i}-{j}"),
                    body: &format!("Content for post {i}-{j}"),
                    published: true,
                    user_id: i,
                    created_at: now,
                })
                .execute(&mut conn)
                .await?;
        }
    }

    let total_users = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(total_users, 10);

    let total_posts = posts::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(total_posts, 15);

    let max_user_id = users::table
        .select(diesel::dsl::max(users::id))
        .first::<Option<i32>>(&mut conn)
        .await?;
    assert_eq!(max_user_id, Some(10));

    let min_user_id = users::table
        .select(diesel::dsl::min(users::id))
        .first::<Option<i32>>(&mut conn)
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
            .execute(&mut conn)
            .await?;
    }

    let sum_rating = comments::table
        .select(diesel::dsl::sum(comments::rating))
        .first::<Option<i64>>(&mut conn)
        .await?;
    assert_eq!(sum_rating, Some(12));

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_join_operations() -> QueryResult<()> {
    let mut conn = connection().await;

    for name in &["Author1", "Author2", "Author3"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let users_list = users::table.load::<User>(&mut conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for (i, user) in users_list.iter().enumerate() {
        for j in 0..=i {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post by {}", user.name),
                    body: &format!("Content {j}"),
                    published: true,
                    user_id: user.id,
                    created_at: now,
                })
                .execute(&mut conn)
                .await?;
        }
    }

    let posts_with_users = posts::table
        .inner_join(users::table)
        .select((posts::title, users::name))
        .load::<(String, String)>(&mut conn)
        .await?;

    assert_eq!(posts_with_users.len(), 6);

    let author1_posts = posts::table
        .inner_join(users::table)
        .filter(users::name.eq("Author1"))
        .select(posts::id)
        .load::<i32>(&mut conn)
        .await?;
    assert_eq!(author1_posts.len(), 1);

    let author3_posts = posts::table
        .inner_join(users::table)
        .filter(users::name.eq("Author3"))
        .select(posts::title)
        .load::<String>(&mut conn)
        .await?;
    assert_eq!(author3_posts.len(), 3);

    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("Tech"),
            categories::description.eq(Some("Technology posts")),
        ))
        .execute(&mut conn)
        .await?;
    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("Life"),
            categories::description.eq(None::<&str>),
        ))
        .execute(&mut conn)
        .await?;

    let categories_list = categories::table.load::<Category>(&mut conn).await?;
    let posts_list = posts::table.limit(3).load::<Post>(&mut conn).await?;

    for post in &posts_list {
        diesel::insert_into(post_categories::table)
            .values((
                post_categories::post_id.eq(post.id),
                post_categories::category_id.eq(categories_list[0].id),
            ))
            .execute(&mut conn)
            .await?;
    }

    let tech_posts = posts::table
        .inner_join(post_categories::table.on(posts::id.eq(post_categories::post_id)))
        .inner_join(categories::table.on(post_categories::category_id.eq(categories::id)))
        .filter(categories::name.eq("Tech"))
        .select(posts::title)
        .load::<String>(&mut conn)
        .await?;

    assert_eq!(tech_posts.len(), 3);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_batch_operations() -> QueryResult<()> {
    let mut conn = connection().await;

    // Create a vector of users for batch insert
    let new_users: Vec<NewUser> = (1..=100)
        .map(|i| NewUser {
            name: format!("BatchUser{i:03}"),
        })
        .collect();

    // Batch insert all users at once
    diesel::insert_into(users::table)
        .values(&new_users)
        .execute(&mut conn)
        .await?;

    let count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(count, 100);

    // Batch update: update all users with id <= 50
    let batch_update = diesel::update(users::table)
        .filter(users::id.le(50))
        .set(users::name.eq("BatchUpdated"))
        .execute(&mut conn)
        .await?;
    assert_eq!(batch_update, 50);

    let updated_count = users::table
        .filter(users::name.eq("BatchUpdated"))
        .count()
        .get_result::<i64>(&mut conn)
        .await?;
    assert_eq!(updated_count, 50);

    // Batch delete: delete all users with id > 75
    let batch_delete = diesel::delete(users::table)
        .filter(users::id.gt(75))
        .execute(&mut conn)
        .await?;
    assert_eq!(batch_delete, 25);

    let remaining = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(remaining, 75);

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_nullable_fields() -> QueryResult<()> {
    let mut conn = connection().await;

    for (name, desc) in &[
        ("WithDesc", Some("Has description")),
        ("NoDesc", None),
        ("EmptyDesc", Some("")),
    ] {
        diesel::insert_into(categories::table)
            .values((categories::name.eq(name), categories::description.eq(*desc)))
            .execute(&mut conn)
            .await?;
    }

    let all_categories = categories::table.load::<Category>(&mut conn).await?;
    assert_eq!(all_categories.len(), 3);

    let with_desc = categories::table
        .filter(categories::description.is_not_null())
        .load::<Category>(&mut conn)
        .await?;
    assert_eq!(with_desc.len(), 2);

    let without_desc = categories::table
        .filter(categories::description.is_null())
        .load::<Category>(&mut conn)
        .await?;
    assert_eq!(without_desc.len(), 1);
    assert_eq!(without_desc[0].name, "NoDesc");

    for name in &["CommentUser1", "CommentUser2"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let users_list = users::table.load::<User>(&mut conn).await?;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "Test Post",
            body: "Content",
            published: true,
            user_id: users_list[0].id,
            created_at: now,
        })
        .execute(&mut conn)
        .await?;

    let post = posts::table.first::<Post>(&mut conn).await?;

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
            .execute(&mut conn)
            .await?;
    }

    let rated_comments = comments::table
        .filter(comments::rating.is_not_null())
        .load::<Comment>(&mut conn)
        .await?;
    assert_eq!(rated_comments.len(), 1);
    assert_eq!(rated_comments[0].rating, Some(5));

    let unrated_comments = comments::table
        .filter(comments::rating.is_null())
        .load::<Comment>(&mut conn)
        .await?;
    assert_eq!(unrated_comments.len(), 1);
    assert_eq!(unrated_comments[0].content, "Unrated");

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_distinct_and_grouping() -> QueryResult<()> {
    let mut conn = connection().await;

    for name in &["Alice", "Bob", "Alice", "Charlie", "Bob"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }

    let all_names = users::table
        .select(users::name)
        .load::<String>(&mut conn)
        .await?;
    assert_eq!(all_names.len(), 5);

    let distinct_names = users::table
        .select(users::name)
        .distinct()
        .order(users::name.asc())
        .load::<String>(&mut conn)
        .await?;
    assert_eq!(distinct_names.len(), 3);
    assert_eq!(distinct_names, vec!["Alice", "Bob", "Charlie"]);

    let users_list = users::table.load::<User>(&mut conn).await?;
    let now = chrono::Utc::now().naive_utc();

    for user in &users_list {
        for i in 1..=user.id % 3 + 1 {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("Post {i}"),
                    body: "Content",
                    published: true,
                    user_id: user.id,
                    created_at: now,
                })
                .execute(&mut conn)
                .await?;
        }
    }

    let post_counts = posts::table
        .group_by(posts::user_id)
        .select((posts::user_id, diesel::dsl::count(posts::id)))
        .load::<(i32, i64)>(&mut conn)
        .await?;

    assert!(!post_counts.is_empty());

    let users_with_multiple_posts = posts::table
        .group_by(posts::user_id)
        .select(posts::user_id)
        .having(diesel::dsl::count(posts::id).gt(1))
        .load::<i32>(&mut conn)
        .await?;

    assert!(!users_with_multiple_posts.is_empty());

    drop(conn);
    Ok(())
}

// Successful commit path: closure returns Ok, all writes are observable after.
// `transaction_test` only exercises the rollback case, so the commit case
// previously went untested at the diesel-async layer.
#[tokio::test]
async fn test_transaction_commit() -> QueryResult<()> {
    let mut conn = connection().await;

    let result: Result<(), diesel::result::Error> = conn
        .transaction(async |conn| {
            diesel::insert_into(users::table)
                .values(users::name.eq("Alice"))
                .execute(conn)
                .await?;
            diesel::insert_into(users::table)
                .values(users::name.eq("Bob"))
                .execute(conn)
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_ok(), "commit path should succeed: {result:?}");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(names, vec!["Alice", "Bob"]);

    drop(conn);
    Ok(())
}

// Outer transaction commits while a nested transaction is explicitly
// rolled back. The outer's own writes must persist; the inner's writes
// must not be observable after.
#[tokio::test]
async fn test_transaction_nested_inner_rollback() -> QueryResult<()> {
    let mut conn = connection().await;

    let outer: Result<(), diesel::result::Error> = conn
        .transaction(async |conn| {
            diesel::insert_into(users::table)
                .values(users::name.eq("Outer"))
                .execute(conn)
                .await?;

            let inner: Result<(), diesel::result::Error> = conn
                .transaction(async |conn| {
                    diesel::insert_into(users::table)
                        .values(users::name.eq("Inner"))
                        .execute(conn)
                        .await?;
                    Err(diesel::result::Error::RollbackTransaction)
                })
                .await;
            assert_eq!(inner, Err(diesel::result::Error::RollbackTransaction));

            // Inner's row must already be gone within the outer scope.
            let count_in_outer = users::table.count().get_result::<i64>(conn).await?;
            assert_eq!(
                count_in_outer, 1,
                "after inner rollback only outer's row remains"
            );

            Ok(())
        })
        .await;
    assert!(outer.is_ok(), "outer should commit: {outer:?}");

    let names: Vec<String> = users::table.select(users::name).load(&mut conn).await?;
    assert_eq!(names, vec!["Outer"]);

    drop(conn);
    Ok(())
}

// A constraint violation inside a transaction must propagate as an Err
// from `.transaction(...)` and the writes that succeeded before the
// violation must be rolled back.
#[tokio::test]
async fn test_transaction_constraint_violation_rolls_back() -> QueryResult<()> {
    let mut conn = connection().await;

    let result: Result<(), diesel::result::Error> = conn
        .transaction(async |conn| {
            diesel::insert_into(users::table)
                .values(users::name.eq("Will be rolled back"))
                .execute(conn)
                .await?;
            // `users.name` is declared NOT NULL; this insert violates the
            // constraint and short-circuits the closure with the DB error.
            diesel::sql_query("INSERT INTO users (name) VALUES (NULL)")
                .execute(conn)
                .await?;
            Ok(())
        })
        .await;
    assert!(
        result.is_err(),
        "transaction with constraint violation must error, got {result:?}"
    );

    let count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(
        count, 0,
        "the pre-violation insert must have been rolled back"
    );

    drop(conn);
    Ok(())
}

// LEFT JOIN with `.nullable()` projection — every user is returned even when
// no matching post exists, with `Option<String>` for the post title.
// Exercises the LEFT JOIN code path through the parenthesized-FROM flatten
// because diesel often wraps the JOIN in parens during SQL generation.
#[tokio::test]
async fn test_left_join_with_nullable_projection() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(users::table)
        .values(users::name.eq("WithPosts"))
        .execute(&mut conn)
        .await?;
    diesel::insert_into(users::table)
        .values(users::name.eq("NoPosts"))
        .execute(&mut conn)
        .await?;
    let users_list = users::table
        .order(users::id.asc())
        .load::<User>(&mut conn)
        .await?;

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "Hello",
            body: "World",
            published: true,
            user_id: users_list[0].id,
            created_at: now,
        })
        .execute(&mut conn)
        .await?;

    let rows: Vec<(String, Option<String>)> = users::table
        .left_join(posts::table)
        .select((users::name, posts::title.nullable()))
        .order(users::id.asc())
        .load(&mut conn)
        .await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        ("WithPosts".to_string(), Some("Hello".to_string()))
    );
    assert_eq!(rows[1], ("NoPosts".to_string(), None));

    drop(conn);
    Ok(())
}

// Aggregate over a join: GROUP BY user with COUNT of joined posts. INNER JOIN
// drops users with zero posts, so only users that actually have posts appear.
#[tokio::test]
async fn test_aggregate_over_join() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    for name in &["A", "B", "C"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(name))
            .execute(&mut conn)
            .await?;
    }
    let users_list = users::table
        .order(users::id.asc())
        .load::<User>(&mut conn)
        .await?;

    // A: 1 post, B: 3 posts, C: 0 posts.
    let post_counts = [(0usize, 1), (1, 3)];
    for (user_idx, count) in post_counts {
        for i in 0..count {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("post-{i}"),
                    body: "x",
                    published: true,
                    user_id: users_list[user_idx].id,
                    created_at: now,
                })
                .execute(&mut conn)
                .await?;
        }
    }

    let pairs: Vec<(String, i64)> = users::table
        .inner_join(posts::table)
        .group_by(users::id)
        .select((users::name, diesel::dsl::count(posts::id)))
        .order(users::id.asc())
        .load(&mut conn)
        .await?;

    assert_eq!(pairs.len(), 2, "C has no posts and should not appear");
    assert_eq!(pairs[0], ("A".to_string(), 1));
    assert_eq!(pairs[1], ("B".to_string(), 3));

    drop(conn);
    Ok(())
}

// Self-join: pairs of posts by the same author with `p1.id < p2.id`.
// 3 posts by one user → 3 unique unordered pairs.
#[tokio::test]
async fn test_self_join() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(users::table)
        .values(users::name.eq("Author"))
        .execute(&mut conn)
        .await?;
    let user = users::table.first::<User>(&mut conn).await?;

    for i in 1..=3 {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: &format!("Post{i}"),
                body: "x",
                published: true,
                user_id: user.id,
                created_at: now,
            })
            .execute(&mut conn)
            .await?;
    }

    let pairs: Vec<(i32, i32)> = posts_p1
        .inner_join(
            posts_p2.on(posts_p1
                .field(posts::user_id)
                .eq(posts_p2.field(posts::user_id))
                .and(posts_p1.field(posts::id).lt(posts_p2.field(posts::id)))),
        )
        .select((posts_p1.field(posts::id), posts_p2.field(posts::id)))
        .order((
            posts_p1.field(posts::id).asc(),
            posts_p2.field(posts::id).asc(),
        ))
        .load(&mut conn)
        .await?;

    assert_eq!(pairs.len(), 3, "expected C(3,2) = 3 unordered pairs");
    let posts_in_pairs: Vec<i32> = pairs.iter().flat_map(|(a, b)| [*a, *b]).collect();
    assert!(posts_in_pairs.iter().all(|&id| (1..=3).contains(&id)));

    drop(conn);
    Ok(())
}

// Multi-condition ON: the join constraint combines an FK match with a
// row-level predicate (rating > 3) using `.and(...)`. Comments with low
// or NULL ratings must not appear in the result.
#[tokio::test]
async fn test_join_with_multi_condition_on() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(users::table)
        .values(users::name.eq("Reviewer"))
        .execute(&mut conn)
        .await?;
    let user = users::table.first::<User>(&mut conn).await?;

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "P",
            body: "x",
            published: true,
            user_id: user.id,
            created_at: now,
        })
        .execute(&mut conn)
        .await?;
    let post = posts::table.first::<Post>(&mut conn).await?;

    let comments_data = [("Excellent", Some(5)), ("Mid", Some(3)), ("Unrated", None)];
    for (content, rating) in comments_data {
        diesel::insert_into(comments::table)
            .values(&NewComment {
                post_id: post.id,
                user_id: user.id,
                content,
                rating,
            })
            .execute(&mut conn)
            .await?;
    }

    let high_rated: Vec<(String, String)> = comments::table
        .inner_join(users::table.on(comments::user_id.eq(users::id).and(comments::rating.gt(3))))
        .select((comments::content, users::name))
        .load(&mut conn)
        .await?;

    assert_eq!(high_rated.len(), 1, "only rating > 3 should match");
    assert_eq!(
        high_rated[0],
        ("Excellent".to_string(), "Reviewer".to_string())
    );

    drop(conn);
    Ok(())
}

// AsChangeset + Identifiable: update a row by passing an updated entity to
// `diesel::update(&entity).set(&entity)`. This is the idiomatic ORM update
// pattern that the structs already derive support for.
#[tokio::test]
async fn test_update_with_aschangeset() -> QueryResult<()> {
    let mut conn = connection().await;

    diesel::insert_into(users::table)
        .values(users::name.eq("Original"))
        .execute(&mut conn)
        .await?;

    let mut user: User = users::table.first(&mut conn).await?;
    user.name = "Updated".into();

    let changed = diesel::update(&user).set(&user).execute(&mut conn).await?;
    assert_eq!(changed, 1);

    let reloaded: User = users::table.first(&mut conn).await?;
    assert_eq!(reloaded.name, "Updated");

    drop(conn);
    Ok(())
}

// Selectable: load via `User::as_select()` instead of relying on positional
// `Queryable`. Recommended for evolving schemas.
#[tokio::test]
async fn test_selectable() -> QueryResult<()> {
    let mut conn = connection().await;

    diesel::insert_into(users::table)
        .values(users::name.eq("Sel"))
        .execute(&mut conn)
        .await?;

    let users_list: Vec<User> = users::table
        .select(User::as_select())
        .load(&mut conn)
        .await?;

    assert_eq!(users_list.len(), 1);
    assert_eq!(users_list[0].name, "Sel");

    drop(conn);
    Ok(())
}

// `.into_boxed()`: compose a query at runtime, applying filters conditionally.
#[tokio::test]
async fn test_boxed_query() -> QueryResult<()> {
    let mut conn = connection().await;

    for n in &["Alice", "Bob", "Charlie"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }

    let needle: Option<&str> = Some("Alice");
    let mut query = users::table.into_boxed();
    if let Some(name) = needle {
        query = query.filter(users::name.eq(name));
    }

    let names: Vec<String> = query
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(names, vec!["Alice"]);

    // Same query type with no filter should return all rows.
    let all: Vec<String> = users::table
        .into_boxed()
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(all, vec!["Alice", "Bob", "Charlie"]);

    drop(conn);
    Ok(())
}

// `.first().optional()`: explicit Some/None for "row may or may not exist".
#[tokio::test]
async fn test_first_optional() -> QueryResult<()> {
    let mut conn = connection().await;

    let absent: Option<User> = users::table.find(99).first(&mut conn).await.optional()?;
    assert!(absent.is_none());

    diesel::insert_into(users::table)
        .values(users::name.eq("Eve"))
        .execute(&mut conn)
        .await?;

    let present: Option<User> = users::table.first(&mut conn).await.optional()?;
    let present = present.expect("a user was just inserted");
    assert_eq!(present.name, "Eve");

    drop(conn);
    Ok(())
}

// EXISTS subquery: filter users by whether a matching post exists.
#[tokio::test]
async fn test_exists_subquery() -> QueryResult<()> {
    use diesel::dsl::exists;
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    for n in &["Author", "Reader"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }
    let users_list = users::table
        .order(users::id.asc())
        .load::<User>(&mut conn)
        .await?;

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "P",
            body: "x",
            published: true,
            user_id: users_list[0].id,
            created_at: now,
        })
        .execute(&mut conn)
        .await?;

    let with_posts: Vec<String> = users::table
        .filter(exists(posts::table.filter(posts::user_id.eq(users::id))))
        .select(users::name)
        .load(&mut conn)
        .await?;
    assert_eq!(with_posts, vec!["Author"]);

    drop(conn);
    Ok(())
}

// IN-subquery: `users.id IN (SELECT posts.user_id FROM posts)`.
#[tokio::test]
async fn test_in_subquery() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    for n in &["A", "B", "C"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }
    let users_list = users::table
        .order(users::id.asc())
        .load::<User>(&mut conn)
        .await?;

    // Only A and C have posts; B does not.
    for &i in &[0usize, 2] {
        diesel::insert_into(posts::table)
            .values(&NewPost {
                title: "P",
                body: "x",
                published: true,
                user_id: users_list[i].id,
                created_at: now,
            })
            .execute(&mut conn)
            .await?;
    }

    let names: Vec<String> = users::table
        .filter(users::id.eq_any(posts::table.select(posts::user_id)))
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(names, vec!["A", "C"]);

    drop(conn);
    Ok(())
}

// LIKE pattern matching via `TextExpressionMethods::like`.
#[tokio::test]
async fn test_like_pattern_matching() -> QueryResult<()> {
    let mut conn = connection().await;

    for n in &["Alice", "Alex", "Bob"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }

    let starts_with_a: Vec<String> = users::table
        .filter(users::name.like("A%"))
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(starts_with_a, vec!["Alex", "Alice"]);

    let three_letters: Vec<String> = users::table
        .filter(users::name.like("___"))
        .select(users::name)
        .load(&mut conn)
        .await?;
    assert_eq!(three_letters, vec!["Bob"]);

    drop(conn);
    Ok(())
}

#[derive(diesel::QueryableByName, Debug, PartialEq)]
struct WindowRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    rn: i64,
}

// Window functions via `sql_query` + `QueryableByName`. Diesel's typed window
// DSL is more involved; this is a smoke test that turso evaluates the OVER
// clause and that diesel deserializes the result.
#[tokio::test]
async fn test_window_function_row_number() -> QueryResult<()> {
    let mut conn = connection().await;

    for n in &["X", "Y", "Z"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }

    let rows: Vec<WindowRow> = diesel::sql_query(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users ORDER BY id",
    )
    .load(&mut conn)
    .await?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].rn, 1);
    assert_eq!(rows[1].rn, 2);
    assert_eq!(rows[2].rn, 3);

    drop(conn);
    Ok(())
}

// ON CONFLICT DO UPDATE (UPSERT): re-inserting a row with the same PK
// updates the existing row's columns instead of erroring on the conflict.
// Also covers `on_conflict_do_nothing` as the no-op variant.
#[tokio::test]
async fn test_on_conflict_upsert() -> QueryResult<()> {
    let mut conn = connection().await;

    diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("Original")))
        .execute(&mut conn)
        .await?;

    // do_update: re-insert with same id, name should change to "Updated"
    diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("ignored")))
        .on_conflict(users::id)
        .do_update()
        .set(users::name.eq("Updated"))
        .execute(&mut conn)
        .await?;

    let user: User = users::table.first(&mut conn).await?;
    assert_eq!(user.name, "Updated");
    let count = users::table.count().get_result::<i64>(&mut conn).await?;
    assert_eq!(count, 1, "no new row should be inserted");

    // do_nothing: same conflict, no change
    diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("AlsoIgnored")))
        .on_conflict_do_nothing()
        .execute(&mut conn)
        .await?;

    let user_after: User = users::table.first(&mut conn).await?;
    assert_eq!(
        user_after.name, "Updated",
        "do_nothing must not change name"
    );

    drop(conn);
    Ok(())
}

// Associations: load all users, then load posts belonging to those users,
// then group posts by user via `grouped_by` to materialize the 1-to-many
// relationship into `Vec<Vec<Post>>`.
#[tokio::test]
async fn test_associations_grouped_by() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    for n in &["U1", "U2"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }
    let users_list: Vec<User> = users::table.order(users::id.asc()).load(&mut conn).await?;

    // U1: 2 posts, U2: 1 post.
    for (idx, count) in [(0usize, 2), (1, 1)] {
        for j in 0..count {
            diesel::insert_into(posts::table)
                .values(&NewPost {
                    title: &format!("p-{j}"),
                    body: "x",
                    published: true,
                    user_id: users_list[idx].id,
                    created_at: now,
                })
                .execute(&mut conn)
                .await?;
        }
    }

    let posts_for_users: Vec<Post> = Post::belonging_to(&users_list).load(&mut conn).await?;
    let posts_grouped: Vec<Vec<Post>> = posts_for_users.grouped_by(&users_list);

    assert_eq!(posts_grouped.len(), 2);
    assert_eq!(posts_grouped[0].len(), 2);
    assert_eq!(posts_grouped[1].len(), 1);

    drop(conn);
    Ok(())
}

// IS NULL / IS NOT NULL: filter by nullability of a Nullable<Text> column.
#[tokio::test]
async fn test_is_null_is_not_null() -> QueryResult<()> {
    let mut conn = connection().await;

    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("HasDesc"),
            categories::description.eq(Some("Some text")),
        ))
        .execute(&mut conn)
        .await?;
    diesel::insert_into(categories::table)
        .values((
            categories::name.eq("NoDesc"),
            categories::description.eq(None::<&str>),
        ))
        .execute(&mut conn)
        .await?;

    let with_desc: Vec<String> = categories::table
        .filter(categories::description.is_not_null())
        .select(categories::name)
        .load(&mut conn)
        .await?;
    assert_eq!(with_desc, vec!["HasDesc"]);

    let without_desc: Vec<String> = categories::table
        .filter(categories::description.is_null())
        .select(categories::name)
        .load(&mut conn)
        .await?;
    assert_eq!(without_desc, vec!["NoDesc"]);

    drop(conn);
    Ok(())
}

#[derive(diesel::QueryableByName, Debug, PartialEq)]
struct AvgRow {
    #[diesel(sql_type = diesel::sql_types::Double)]
    avg_rating: f64,
}

// AVG (via `sql_query` since diesel's typed `avg` requires the
// `numeric`/`bigdecimal` feature) and COUNT DISTINCT (via diesel's typed
// `count_distinct`).
#[tokio::test]
async fn test_avg_and_count_distinct() -> QueryResult<()> {
    let mut conn = connection().await;
    let now = chrono::Utc::now().naive_utc();

    diesel::insert_into(users::table)
        .values(users::name.eq("U"))
        .execute(&mut conn)
        .await?;
    let user: User = users::table.first(&mut conn).await?;

    diesel::insert_into(posts::table)
        .values(&NewPost {
            title: "P",
            body: "x",
            published: true,
            user_id: user.id,
            created_at: now,
        })
        .execute(&mut conn)
        .await?;
    let post: Post = posts::table.first(&mut conn).await?;

    // ratings: 2, 4, 4, 6 → avg = 4.0, distinct = 3 (2, 4, 6)
    for r in &[2, 4, 4, 6] {
        diesel::insert_into(comments::table)
            .values(&NewComment {
                post_id: post.id,
                user_id: user.id,
                content: "c",
                rating: Some(*r),
            })
            .execute(&mut conn)
            .await?;
    }

    let avg_rows: Vec<AvgRow> = diesel::sql_query("SELECT AVG(rating) AS avg_rating FROM comments")
        .load(&mut conn)
        .await?;
    assert_eq!(avg_rows.len(), 1);
    assert!(
        (avg_rows[0].avg_rating - 4.0).abs() < f64::EPSILON,
        "expected avg 4.0, got {}",
        avg_rows[0].avg_rating
    );

    let distinct_count: i64 = comments::table
        .select(diesel::dsl::count(comments::rating).aggregate_distinct())
        .first(&mut conn)
        .await?;
    assert_eq!(distinct_count, 3);

    drop(conn);
    Ok(())
}

// INSERT INTO ... SELECT ... via `sql_query`. Diesel's typed DSL for
// insert-from-select is awkward (requires column-shape adapters); raw SQL
// is the natural shape and confirms turso supports the pattern.
#[tokio::test]
async fn test_insert_from_select() -> QueryResult<()> {
    let mut conn = connection().await;

    for n in &["A", "B", "C"] {
        diesel::insert_into(users::table)
            .values(users::name.eq(n))
            .execute(&mut conn)
            .await?;
    }

    diesel::sql_query("INSERT INTO categories (name) SELECT name FROM users")
        .execute(&mut conn)
        .await?;

    let cat_names: Vec<String> = categories::table
        .select(categories::name)
        .order(categories::name.asc())
        .load(&mut conn)
        .await?;
    assert_eq!(cat_names, vec!["A", "B", "C"]);

    drop(conn);
    Ok(())
}

// `set_prepared_statement_cache_size` must be a real configuration knob, not
// a runtime panic. Verify both modes round-trip the same query results.
//
// We can't observe the cache contents directly through the public diesel
// API, so this test asserts behavioral parity: identical query semantics
// whether the cache is enabled or disabled, including a repeated call that
// would hit the cache when enabled and re-prepare from scratch when disabled.
#[tokio::test]
async fn test_prepared_statement_cache_size_configurable() -> QueryResult<()> {
    let mut conn = connection().await;

    diesel::insert_into(users::table)
        .values(users::name.eq("CacheUser"))
        .execute(&mut conn)
        .await?;

    // Default (Unbounded): repeated identical queries should both succeed.
    let n1: i64 = users::table.count().get_result(&mut conn).await?;
    let n2: i64 = users::table.count().get_result(&mut conn).await?;
    assert_eq!((n1, n2), (1, 1));

    // Disable: cache cleared, subsequent queries re-prepare and still work.
    conn.set_prepared_statement_cache_size(CacheSize::Disabled);
    let n3: i64 = users::table.count().get_result(&mut conn).await?;
    let n4: i64 = users::table.count().get_result(&mut conn).await?;
    assert_eq!((n3, n4), (1, 1));

    // Re-enable: cache fills again, queries still work.
    conn.set_prepared_statement_cache_size(CacheSize::Unbounded);
    let n5: i64 = users::table.count().get_result(&mut conn).await?;
    assert_eq!(n5, 1);

    drop(conn);
    Ok(())
}
