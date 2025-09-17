# diesel-turso

A [Diesel](https://diesel.rs/) backend and connection implementation for [Turso Database](https://github.com/tursodatabase/turso), an in-process SQL database written in Rust, compatible with SQLite..

> ⚠️ **Early Development**  
> This project is experimental and **not suitable for production use**.  
> APIs may change, features may be missing, and stability is not guaranteed.

## Overview

`diesel-turso` lets you use Diesel ORM with Turso databases, combining Diesel’s type-safe query builder with Turso’s distributed SQLite platform.  
It provides async support through [`diesel-async`](https://github.com/weiznich/diesel_async) and a custom backend for Turso.

## Features

- ✅ Async/await support via `diesel-async`  
- ✅ Connection pooling (bb8, deadpool, mobc, r2d2)  
- ✅ Optional `chrono` support for date/time types  
- ✅ Type-safe query building with Diesel  
- 🌍 Edge-ready for Turso’s distributed SQLite  

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
diesel-turso = { git = "https://github.com/Choochmeque/diesel_turso" }
```

### Feature Flags

- `chrono` (default): Enable `chrono` date/time types  
- `bb8`: bb8 connection pool  
- `deadpool`: deadpool connection pool  
- `mobc`: mobc connection pool  
- `r2d2`: r2d2 connection pool  

## Quick Start

```rust
use diesel::prelude::*;
use diesel_turso::AsyncTursoConnection;
use diesel_async::RunQueryDsl;

#[derive(Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = AsyncTursoConnection::new(":memory:").await?;
    
    let results = users::table
        .select(User::as_select())
        .load(&mut conn)
        .await?;
    
    for user in results {
        println!("User: {} - {}", user.id, user.name);
    }
    
    Ok(())
}
```

## Connection Pooling Example

```rust
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_turso::AsyncTursoConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = AsyncDieselConnectionManager::<AsyncTursoConnection>::new(":memory:");
    let pool = Pool::builder().build(manager).await?;
    
    let mut conn = pool.get().await?;
    // Use the connection...
    
    Ok(())
}
```

## Current Limitations

- Not all Diesel features are supported  
- Performance optimizations in progress  
- Edge cases may not be fully handled  
- Documentation is incomplete  

## Contributing

Contributions are welcome!  
Areas for contribution include:

- Bug reports & fixes  
- Feature implementations  
- Documentation improvements  
- Performance optimizations  
- Expanding test coverage  

Open issues or submit PRs on [GitHub](https://github.com/Choochmeque/diesel_turso).

## License

Licensed under **MIT**. See [LICENSE](LICENSE) for details.

## Acknowledgments

- [Diesel](https://diesel.rs/) – ORM foundation  
- [Turso Database](https://github.com/tursodatabase/turso) – An in-process SQL database written in Rust, compatible with SQLite. 
- [diesel-async](https://github.com/weiznich/diesel_async) – Async Diesel support  

## Roadmap

- [ ] Feature parity with Diesel’s SQLite backend  
- [ ] Comprehensive test suite  
- [ ] Performance benchmarks  
- [ ] Production-ready stability  
- [ ] Full documentation & examples  
- [ ] Turso-specific features (replicas, etc.)  

---
