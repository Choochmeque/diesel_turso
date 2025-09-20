use turso::{Builder, Value};

/// Simplified pure Turso test with correct API
#[tokio::test]
async fn test_simple_turso_operations() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Create table
    let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)";
    conn.execute(sql, ()).await.unwrap();

    // Insert with text value
    let sql = "INSERT INTO test (value) VALUES (?)";
    conn.execute(sql, (Value::Text("test".to_string()),))
        .await
        .unwrap();

    // Try update with text value - should work
    let sql = "UPDATE test SET value = ? WHERE id = ?";
    let result = conn
        .execute(sql, (Value::Text("updated".to_string()), Value::Integer(1)))
        .await;

    match result {
        Ok(changes) => {
            println!("Text update succeeded! Changes: {}", changes);
        }
        Err(e) => {
            println!("Text update failed: {}", e);
            if e.to_string().contains("MustBeInt") {
                println!("*** FOUND MustBeInt ERROR with text update ***");
                panic!("MustBeInt error: {}", e);
            }
        }
    }

    // Try update with integer as text in a WHERE clause that expects integer
    let sql = "UPDATE test SET value = ? WHERE id = ?";
    let result = conn
        .execute(
            sql,
            (
                Value::Text("updated2".to_string()),
                Value::Text("1".to_string()),
            ),
        )
        .await;

    match result {
        Ok(changes) => {
            println!("Text-as-ID update succeeded! Changes: {}", changes);
        }
        Err(e) => {
            println!("Text-as-ID update failed: {}", e);
            if e.to_string().contains("MustBeInt") {
                println!("*** FOUND MustBeInt ERROR with text-as-ID ***");
                panic!("Reproduced MustBeInt: {}", e);
            }
        }
    }

    println!("Simple Turso test completed!");
}

/// Pure Turso test to replicate the exact diesel test scenario
#[tokio::test]
async fn test_pure_turso_update_operations() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Create tables similar to our diesel test
    let create_users_sql = "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
    ";

    let create_posts_sql = "
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            published BOOLEAN NOT NULL DEFAULT 0,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    ";

    // Create tables
    conn.execute(create_users_sql, ()).await.unwrap();
    conn.execute(create_posts_sql, ()).await.unwrap();

    // Insert test users (this is what works in our diesel test)
    for name in &["UpdateMe", "KeepMe"] {
        conn.execute(
            "INSERT INTO users (name) VALUES (?)",
            (Value::Text(name.to_string()),),
        )
        .await
        .unwrap();
    }

    // Try the update operation that fails in diesel
    println!("Executing: UPDATE users SET name = ? WHERE name = ?");
    println!(
        "Binds: [{:?}, {:?}]",
        Value::Text("Updated".to_string()),
        Value::Text("UpdateMe".to_string())
    );

    let result = conn
        .execute(
            "UPDATE users SET name = ? WHERE name = ?",
            (
                Value::Text("Updated".to_string()),
                Value::Text("UpdateMe".to_string()),
            ),
        )
        .await;

    match result {
        Ok(changes) => {
            println!("Update succeeded! Changes: {}", changes);
        }
        Err(e) => {
            println!("Update failed with error: {}", e);
            if e.to_string().contains("MustBeInt") {
                println!("*** REPRODUCED MustBeInt ERROR in exact diesel scenario! ***");
                panic!("Reproduced MustBeInt: {}", e);
            } else {
                panic!("Pure Turso update failed: {}", e);
            }
        }
    }

    // Get the updated user ID for posts
    let mut rows = conn
        .query(
            "SELECT id FROM users WHERE name = ?",
            (Value::Text("Updated".to_string()),),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let user_id = row.get::<i64>(0).unwrap();

    // Now the critical part: insert posts and do boolean update (this is where diesel fails)
    let now = "2024-01-01 12:00:00";
    for (title, body) in &[("Draft 1", "Content 1"), ("Draft 2", "Content 2")] {
        conn.execute("INSERT INTO posts (title, body, published, user_id, created_at) VALUES (?, ?, ?, ?, ?)", (
            Value::Text(title.to_string()),
            Value::Text(body.to_string()),
            Value::Integer(0), // false as integer
            Value::Integer(user_id),
            Value::Text(now.to_string()),
        )).await.unwrap();
    }

    // The critical boolean update that fails in diesel
    println!(
        "Executing the critical boolean update: UPDATE posts SET published = ? WHERE published = ?"
    );
    println!("Binds: [{:?}, {:?}]", Value::Integer(1), Value::Integer(0));

    let result = conn
        .execute(
            "UPDATE posts SET published = ? WHERE published = ?",
            (
                Value::Integer(1), // true
                Value::Integer(0), // false
            ),
        )
        .await;

    match result {
        Ok(changes) => {
            println!("Boolean update succeeded! Changes: {}", changes);
        }
        Err(e) => {
            println!("Boolean update failed: {}", e);
            if e.to_string().contains("MustBeInt") {
                println!("*** REPRODUCED MustBeInt ERROR in boolean update! ***");
                panic!("Reproduced MustBeInt in boolean update: {}", e);
            } else {
                panic!("Boolean update failed: {}", e);
            }
        }
    }

    // Verify the updates worked
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM posts WHERE published = ?",
            (Value::Integer(1),),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let count = row.get::<i64>(0).unwrap();
    assert_eq!(count, 2);

    println!("Pure Turso test passed - ALL operations including boolean updates work fine!");
}

/// Test boolean operations specifically
#[tokio::test]
async fn test_pure_turso_boolean_operations() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Create posts table
    let create_posts_sql = "
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            published BOOLEAN NOT NULL DEFAULT 0
        )
    ";

    conn.execute(create_posts_sql, ()).await.unwrap();

    // Insert test posts
    for (title, published) in &[("Draft 1", false), ("Draft 2", false)] {
        conn.execute(
            "INSERT INTO posts (title, published) VALUES (?, ?)",
            (
                Value::Text(title.to_string()),
                Value::Integer(if *published { 1 } else { 0 }),
            ),
        )
        .await
        .unwrap();
    }

    // Try the boolean update operation that might cause MustBeInt
    println!("Executing: UPDATE posts SET published = ? WHERE published = ?");
    println!("Binds: [{:?}, {:?}]", Value::Integer(1), Value::Integer(0));

    let result = conn
        .execute(
            "UPDATE posts SET published = ? WHERE published = ?",
            (
                Value::Integer(1), // true
                Value::Integer(0), // false
            ),
        )
        .await;

    match result {
        Ok(changes) => {
            println!("Boolean update succeeded! Changes: {}", changes);
        }
        Err(e) => {
            println!("Boolean update failed with error: {}", e);
            if e.to_string().contains("MustBeInt") {
                println!("*** FOUND MustBeInt ERROR ***");
                panic!("Pure Turso boolean update failed: {}", e);
            }
        }
    }

    println!("Pure Turso boolean test passed!");
}

/// Test with various value types to see which one triggers MustBeInt
#[tokio::test]
async fn test_pure_turso_value_types() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Create a test table with various column types
    let create_sql = "
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            int_col INTEGER,
            real_col REAL,
            bool_col BOOLEAN
        )
    ";

    conn.execute(create_sql, ()).await.unwrap();

    // Test different value types
    let test_values = vec![
        ("Text", Value::Text("test".to_string())),
        ("Integer", Value::Integer(42)),
        ("Real", Value::Real(3.14)),
        ("Boolean_as_Integer", Value::Integer(1)),
        ("Boolean_as_Real", Value::Real(1.0)),
        ("Null", Value::Null),
    ];

    for (test_name, value) in test_values {
        println!("\nTesting value type: {} -> {:?}", test_name, value);

        // Insert the value
        let result = conn.execute("INSERT INTO test_table (text_col, int_col, real_col, bool_col) VALUES (?, ?, ?, ?)", (
            value.clone(),
            value.clone(),
            value.clone(),
            value.clone(),
        )).await;

        match result {
            Ok(_) => println!("  Insert succeeded"),
            Err(e) => {
                println!("  Insert failed: {}", e);
                if e.to_string().contains("MustBeInt") {
                    println!(
                        "  *** FOUND MustBeInt ERROR on INSERT with value type: {} ***",
                        test_name
                    );
                }
            }
        }

        // Try to update with the same value type
        let result = conn
            .execute(
                "UPDATE test_table SET text_col = ? WHERE id = (SELECT MAX(id) FROM test_table)",
                (value.clone(),),
            )
            .await;

        match result {
            Ok(_) => println!("  Update succeeded"),
            Err(e) => {
                println!("  Update failed: {}", e);
                if e.to_string().contains("MustBeInt") {
                    println!(
                        "  *** FOUND MustBeInt ERROR on UPDATE with value type: {} ***",
                        test_name
                    );
                }
            }
        }
    }

    println!("Value type testing completed!");
}

/// Test the exact working pattern vs failing pattern side by side
#[tokio::test]
async fn test_limit_clause_issue() -> Result<(), turso::Error> {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Setup
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        (),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO users (name) VALUES (?)",
        (Value::Text("Updated".to_string()),),
    )
    .await
    .unwrap();

    let sql = "SELECT `users`.`id`, `users`.`name` FROM `users` WHERE (`users`.`name` = ?) LIMIT ?";
    let params = vec![Value::Text("Updated".to_string()), Value::Integer(1)];

    // Pattern 1: The working pattern from earlier tests (no row iteration)
    println!("Testing working pattern (no row iteration)...");
    {
        let mut stmt = conn.prepare(sql).await.unwrap();
        let result = stmt.query(params.clone()).await;
        match result {
            Ok(_) => println!("  Working pattern - SUCCESS"),
            Err(e) => println!("  Working pattern - FAILED: {}", e),
        }
    }

    // Pattern 2: The failing pattern (with row iteration)
    println!("Testing failing pattern (with row iteration)...");
    {
        let mut prepared = conn.prepare(sql).await.unwrap();
        let mut rows = prepared.query(params.clone()).await.unwrap();

        println!("  Query executed, now trying rows.next()...");
        let result = rows.next().await;
        match result {
            Ok(Some(_)) => println!("  Failing pattern - SUCCESS (got row)"),
            Ok(None) => println!("  Failing pattern - SUCCESS (no rows)"),
            Err(e) => {
                println!("  Failing pattern - FAILED: {}", e);
                if e.to_string().contains("MustBeInt") {
                    println!("  *** CONFIRMED: rows.next() causes MustBeInt error! ***");
                }
            }
        }
    }

    println!("Pattern comparison completed!");
    Ok(())
}
