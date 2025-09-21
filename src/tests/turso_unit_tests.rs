use turso::{Builder, Connection, Value};

async fn setup(connection: &Connection) {
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            published BOOLEAN NOT NULL DEFAULT 0,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            rating INTEGER,
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        )",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS post_categories (
            post_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            PRIMARY KEY (post_id, category_id),
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();
}

async fn connection() -> Connection {
    let conn = connection_without_transaction().await;
    setup(&conn).await;
    conn
}

async fn connection_without_transaction() -> Connection {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let db = Builder::new_local(&db_url).build().await.unwrap();
    db.connect().unwrap()
}

#[tokio::test]
async fn test_basic_insert_and_select() -> Result<(), turso::Error> {
    let conn = connection().await;

    let insert1 = conn
        .execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text("John Doe".to_string())],
        )
        .await?;
    assert_eq!(insert1, 1);

    let insert2 = conn
        .execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text("Jane Doe".to_string())],
        )
        .await?;
    assert_eq!(insert2, 1);

    let mut stmt = conn.prepare("SELECT name FROM users ORDER BY id").await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    let mut names = Vec::new();
    while let Some(row) = rows.next().await? {
        let name_value = row.get_value(0)?;
        if let Value::Text(name) = name_value {
            names.push(name);
        }
    }
    assert_eq!(names, vec!["John Doe", "Jane Doe"]);

    Ok(())
}

#[tokio::test]
async fn test_crud_operations() -> Result<(), turso::Error> {
    let conn = connection().await;

    let insert_result = conn
        .execute(
            "INSERT INTO users (name) VALUES (?), (?), (?)",
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("Charlie".to_string()),
            ],
        )
        .await?;
    assert_eq!(insert_result, 3);

    let post_insert = conn
        .execute(
            "INSERT INTO posts (title, body, published, user_id, created_at) 
                       VALUES (?, ?, ?, ?, datetime('now'))",
            vec![
                Value::Text("My First Post".to_string()),
                Value::Text("This is the content".to_string()),
                Value::Integer(1),
                Value::Integer(1),
            ],
        )
        .await?;
    assert_eq!(post_insert, 1);

    let mut stmt = conn
        .prepare("SELECT count(*) FROM posts WHERE title = ?")
        .await?;
    let mut rows = stmt
        .query(vec![Value::Text("My First Post".to_string())])
        .await?;
    if let Some(row) = rows.next().await? {
        let count_value = row.get_value(0)?;
        if let Value::Integer(count) = count_value {
            assert_eq!(count, 1);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_filtering_and_where_clauses() -> Result<(), turso::Error> {
    let conn = connection().await;

    for i in 1..=10 {
        conn.execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text(format!("User{}", i))],
        )
        .await?;
    }

    for i in 1..=5 {
        let published = if i % 2 == 0 { 1 } else { 0 };
        conn.execute(
            "INSERT INTO posts (title, body, published, user_id, created_at) 
             VALUES (?, ?, ?, ?, datetime('now'))",
            vec![
                Value::Text(format!("Post {}", i)),
                Value::Text(format!("Content {}", i)),
                Value::Integer(published),
                Value::Integer(i),
            ],
        )
        .await?;
    }

    let mut stmt = conn
        .prepare("SELECT count(*) FROM posts WHERE published = ?")
        .await?;
    let mut rows = stmt.query(vec![Value::Integer(1)]).await?;
    if let Some(row) = rows.next().await? {
        let count_value = row.get_value(0)?;
        if let Value::Integer(count) = count_value {
            assert_eq!(count, 2);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_update_operations() -> Result<(), turso::Error> {
    let conn = connection().await;

    conn.execute(
        "INSERT INTO users (name) VALUES (?), (?)",
        vec![
            Value::Text("UpdateMe".to_string()),
            Value::Text("KeepMe".to_string()),
        ],
    )
    .await?;

    let updated = conn
        .execute(
            "UPDATE users SET name = ? WHERE name = ?",
            vec![
                Value::Text("Updated".to_string()),
                Value::Text("UpdateMe".to_string()),
            ],
        )
        .await?;
    assert_eq!(updated, 1);

    let mut stmt = conn
        .prepare("SELECT name FROM users WHERE name = ?")
        .await?;
    let mut rows = stmt.query(vec![Value::Text("Updated".to_string())]).await?;
    if let Some(row) = rows.next().await? {
        let name_value = row.get_value(0)?;
        if let Value::Text(name) = name_value {
            assert_eq!(name, "Updated");
        }
    }

    conn.execute(
        "INSERT INTO posts (title, body, published, user_id, created_at) 
                       VALUES (?, ?, ?, ?, datetime('now')), (?, ?, ?, ?, datetime('now'))",
        vec![
            Value::Text("Draft 1".to_string()),
            Value::Text("Content 1".to_string()),
            Value::Integer(0),
            Value::Integer(1),
            Value::Text("Draft 2".to_string()),
            Value::Text("Content 2".to_string()),
            Value::Integer(0),
            Value::Integer(1),
        ],
    )
    .await?;

    let published = conn
        .execute(
            "UPDATE posts SET published = ? WHERE published = ?",
            vec![Value::Integer(1), Value::Integer(0)],
        )
        .await?;
    assert_eq!(published, 2);

    Ok(())
}

#[tokio::test]
async fn test_delete_operations() -> Result<(), turso::Error> {
    let conn = connection().await;

    for i in 1..=5 {
        conn.execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text(format!("DeleteUser{}", i))],
        )
        .await?;
    }

    let mut stmt = conn.prepare("SELECT count(*) FROM users").await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    if let Some(row) = rows.next().await? {
        let count_value = row.get_value(0)?;
        if let Value::Integer(count) = count_value {
            assert_eq!(count, 5);
        }
    }

    let deleted = conn
        .execute(
            "DELETE FROM users WHERE name LIKE ? AND id > ?",
            vec![Value::Text("DeleteUser%".to_string()), Value::Integer(2)],
        )
        .await?;
    assert_eq!(deleted, 3);

    conn.execute("INSERT INTO posts (title, body, published, user_id, created_at) 
                       VALUES (?, ?, ?, ?, datetime('now')), (?, ?, ?, ?, datetime('now')), (?, ?, ?, ?, datetime('now'))", 
        vec![
            Value::Text("Delete Post 1".to_string()),
            Value::Text("Will be deleted".to_string()),
            Value::Integer(1),
            Value::Integer(1),
            Value::Text("Delete Post 2".to_string()),
            Value::Text("Will be deleted".to_string()),
            Value::Integer(1),
            Value::Integer(1),
            Value::Text("Delete Post 3".to_string()),
            Value::Text("Will be deleted".to_string()),
            Value::Integer(1),
            Value::Integer(1)
        ])
        .await?;

    conn.execute(
        "DELETE FROM posts WHERE title LIKE ?",
        vec![Value::Text("Delete Post%".to_string())],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_ordering_and_limiting() -> Result<(), turso::Error> {
    let conn = connection().await;

    let insert_result = conn
        .execute(
            "INSERT INTO users (name) VALUES (?), (?), (?), (?), (?)",
            vec![
                Value::Text("Zara".to_string()),
                Value::Text("Alice".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("Charlie".to_string()),
                Value::Text("David".to_string()),
            ],
        )
        .await?;
    assert_eq!(insert_result, 5);

    let mut stmt = conn
        .prepare("SELECT name FROM users ORDER BY name ASC LIMIT 3")
        .await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    let mut names = Vec::new();
    while let Some(row) = rows.next().await? {
        let name_value = row.get_value(0)?;
        if let Value::Text(name) = name_value {
            names.push(name);
        }
    }
    assert_eq!(names.len(), 3);
    assert_eq!(names[0], "Alice");

    Ok(())
}

#[tokio::test]
async fn test_aggregate_functions() -> Result<(), turso::Error> {
    let conn = connection().await;

    for i in 1..=10 {
        conn.execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text(format!("User{:02}", i))],
        )
        .await?;
    }

    for i in 1..=5 {
        for j in 1..=i {
            conn.execute(
                "INSERT INTO posts (title, body, published, user_id, created_at) 
                 VALUES (?, ?, ?, ?, datetime('now'))",
                vec![
                    Value::Text(format!("Post {}-{}", i, j)),
                    Value::Text(format!("Content for post {}-{}", i, j)),
                    Value::Integer(1),
                    Value::Integer(i),
                ],
            )
            .await?;
        }
    }

    let mut stmt = conn.prepare("SELECT count(*) FROM users").await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    if let Some(row) = rows.next().await? {
        let count_value = row.get_value(0)?;
        if let Value::Integer(count) = count_value {
            assert_eq!(count, 10);
        }
    }

    conn.execute(
        "INSERT INTO comments (post_id, user_id, content, rating) 
                       VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Text("Great!".to_string()),
            Value::Integer(5),
            Value::Integer(1),
            Value::Integer(2),
            Value::Text("Good".to_string()),
            Value::Integer(4),
            Value::Integer(1),
            Value::Integer(3),
            Value::Text("OK".to_string()),
            Value::Integer(3),
        ],
    )
    .await?;

    let comment_insert = conn
        .execute(
            "INSERT INTO comments (post_id, user_id, content, rating) 
                       VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("Great!".to_string()),
                Value::Integer(5),
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("Good".to_string()),
                Value::Integer(4),
                Value::Integer(1),
                Value::Integer(3),
                Value::Text("OK".to_string()),
                Value::Integer(3),
            ],
        )
        .await?;
    assert_eq!(comment_insert, 3);

    Ok(())
}

#[tokio::test]
async fn test_join_operations() -> Result<(), turso::Error> {
    let conn = connection().await;

    conn.execute(
        "INSERT INTO users (name) VALUES (?), (?), (?)",
        vec![
            Value::Text("Author1".to_string()),
            Value::Text("Author2".to_string()),
            Value::Text("Author3".to_string()),
        ],
    )
    .await?;

    for i in 1..=3 {
        for j in 0..i {
            conn.execute(
                "INSERT INTO posts (title, body, published, user_id, created_at) 
                 VALUES (?, ?, ?, ?, datetime('now'))",
                vec![
                    Value::Text(format!("Post by Author{}", i)),
                    Value::Text(format!("Content {}", j)),
                    Value::Integer(1),
                    Value::Integer(i),
                ],
            )
            .await?;
        }
    }

    let mut stmt = conn
        .prepare(
            "SELECT posts.title, users.name FROM posts 
                               INNER JOIN users ON posts.user_id = users.id",
        )
        .await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    let mut count = 0;
    while (rows.next().await?).is_some() {
        count += 1;
    }
    assert_eq!(count, 6);

    conn.execute(
        "INSERT INTO categories (name, description) VALUES (?, ?), (?, ?)",
        vec![
            Value::Text("Tech".to_string()),
            Value::Text("Technology posts".to_string()),
            Value::Text("Life".to_string()),
            Value::Null,
        ],
    )
    .await?;

    conn.execute(
        "INSERT INTO post_categories (post_id, category_id) VALUES (?, ?), (?, ?), (?, ?)",
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(1),
            Value::Integer(3),
            Value::Integer(1),
        ],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_nullable_fields() -> Result<(), turso::Error> {
    let conn = connection().await;

    conn.execute(
        "INSERT INTO categories (name, description) VALUES 
                       (?, ?), (?, ?), (?, ?)",
        vec![
            Value::Text("WithDesc".to_string()),
            Value::Text("Has description".to_string()),
            Value::Text("NoDesc".to_string()),
            Value::Null,
            Value::Text("EmptyDesc".to_string()),
            Value::Text("".to_string()),
        ],
    )
    .await?;

    let category_insert = conn
        .execute(
            "INSERT INTO categories (name, description) VALUES 
                       (?, ?), (?, ?), (?, ?)",
            vec![
                Value::Text("WithDesc".to_string()),
                Value::Text("Has description".to_string()),
                Value::Text("NoDesc".to_string()),
                Value::Null,
                Value::Text("EmptyDesc".to_string()),
                Value::Text("".to_string()),
            ],
        )
        .await?;
    assert_eq!(category_insert, 3);

    conn.execute(
        "INSERT INTO users (name) VALUES (?), (?)",
        vec![
            Value::Text("CommentUser1".to_string()),
            Value::Text("CommentUser2".to_string()),
        ],
    )
    .await?;

    conn.execute(
        "INSERT INTO posts (title, body, published, user_id, created_at) 
                       VALUES (?, ?, ?, ?, datetime('now'))",
        vec![
            Value::Text("Test Post".to_string()),
            Value::Text("Content".to_string()),
            Value::Integer(1),
            Value::Integer(1),
        ],
    )
    .await?;

    conn.execute(
        "INSERT INTO comments (post_id, user_id, content, rating) 
                       VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Text("Rated".to_string()),
            Value::Integer(5),
            Value::Integer(1),
            Value::Integer(2),
            Value::Text("Unrated".to_string()),
            Value::Null,
        ],
    )
    .await?;

    let comment_insert2 = conn
        .execute(
            "INSERT INTO comments (post_id, user_id, content, rating) 
                       VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("Rated".to_string()),
                Value::Integer(5),
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("Unrated".to_string()),
                Value::Null,
            ],
        )
        .await?;
    assert_eq!(comment_insert2, 2);

    Ok(())
}

#[tokio::test]
async fn test_distinct_and_grouping() -> Result<(), turso::Error> {
    let conn = connection().await;

    conn.execute(
        "INSERT INTO users (name) VALUES (?), (?), (?), (?), (?)",
        vec![
            Value::Text("Alice".to_string()),
            Value::Text("Bob".to_string()),
            Value::Text("Alice".to_string()),
            Value::Text("Charlie".to_string()),
            Value::Text("Bob".to_string()),
        ],
    )
    .await?;

    let user_insert = conn
        .execute(
            "INSERT INTO users (name) VALUES (?), (?), (?), (?), (?)",
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("Alice".to_string()),
                Value::Text("Charlie".to_string()),
                Value::Text("Bob".to_string()),
            ],
        )
        .await?;
    assert_eq!(user_insert, 5);

    for i in 1..=5 {
        let post_count = i % 3 + 1;
        for j in 1..=post_count {
            conn.execute(
                "INSERT INTO posts (title, body, published, user_id, created_at) 
                 VALUES (?, ?, ?, ?, datetime('now'))",
                vec![
                    Value::Text(format!("Post {}", j)),
                    Value::Text("Content".to_string()),
                    Value::Integer(1),
                    Value::Integer(i),
                ],
            )
            .await?;
        }
    }

    let mut stmt = conn
        .prepare("SELECT DISTINCT name FROM users ORDER BY name")
        .await?;
    let mut rows = stmt.query(Vec::<Value>::new()).await?;
    let mut distinct_names = Vec::new();
    while let Some(row) = rows.next().await? {
        let name_value = row.get_value(0)?;
        if let Value::Text(name) = name_value {
            distinct_names.push(name);
        }
    }
    assert!(distinct_names.contains(&"Alice".to_string()));
    assert!(distinct_names.contains(&"Bob".to_string()));
    assert!(distinct_names.contains(&"Charlie".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_transactions() -> Result<(), turso::Error> {
    let conn = connection().await;

    let initial_insert = conn
        .execute(
            "INSERT INTO users (name) VALUES (?), (?)",
            vec![
                Value::Text("John Doe".to_string()),
                Value::Text("Jane Doe".to_string()),
            ],
        )
        .await?;
    assert_eq!(initial_insert, 2);

    let dave_insert = conn
        .execute(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text("Dave".to_string())],
        )
        .await?;
    assert_eq!(dave_insert, 1);

    Ok(())
}
