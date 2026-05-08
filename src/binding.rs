use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use turso::{Builder, Connection, Database, Statement, Value};

#[derive(Debug, Clone)]
pub struct TursoDatabase {
    pub db: Database,
}

#[derive(Debug, Clone)]
pub struct TursoConnection {
    pub conn: Arc<Connection>,
    /// Per-connection prepared-statement cache keyed by SQL text.
    /// Default is enabled (matches diesel's `CacheSize::Unbounded`).
    cache: Arc<Mutex<StatementCache>>,
}

struct StatementCache {
    enabled: bool,
    entries: HashMap<String, Statement>,
}

impl std::fmt::Debug for StatementCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementCache")
            .field("enabled", &self.enabled)
            .field("len", &self.entries.len())
            .finish()
    }
}

impl Default for StatementCache {
    fn default() -> Self {
        Self {
            enabled: true,
            entries: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TursoPreparedStatement {
    pub sql: String,
    pub binds: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct TursoResult {
    pub column_names: Arc<[String]>,
    pub rows: Vec<Vec<Value>>,
    pub error: Option<String>,
    pub changes: usize,
}

impl TursoDatabase {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let db = Builder::new_local(path).build().await?;
        Ok(Self { db })
    }

    #[allow(clippy::unused_async)]
    pub async fn connect(&self) -> Result<TursoConnection, turso::Error> {
        let conn = Arc::new(self.db.connect()?);
        Ok(TursoConnection {
            conn,
            cache: Arc::new(Mutex::new(StatementCache::default())),
        })
    }
}

impl TursoConnection {
    #[allow(clippy::unused_self)]
    pub fn prepare(&self, query: &str) -> TursoPreparedStatement {
        TursoPreparedStatement {
            sql: query.to_string(),
            binds: Vec::new(),
        }
    }

    /// Enable or disable the prepared-statement cache. Disabling clears any
    /// already-cached entries.
    pub fn set_cache_enabled(&self, enabled: bool) {
        let mut cache = self.cache.lock().expect("statement cache mutex poisoned");
        cache.enabled = enabled;
        if !enabled {
            cache.entries.clear();
        }
    }

    /// Look up a cached prepared statement for `sql` or prepare and cache one.
    /// Returns an owned `Statement` clone (cheap — `Statement` is internally
    /// `Arc<Mutex<…>>`).
    async fn prepare_cached(&self, sql: &str) -> Result<Statement, turso::Error> {
        // Bind to a local so the MutexGuard is released before the `await`
        // below — never hold a sync `MutexGuard` across `.await`.
        let cached = self
            .cache
            .lock()
            .expect("statement cache mutex poisoned")
            .lookup(sql);
        if let Some(stmt) = cached {
            return Ok(stmt);
        }
        let stmt = self.conn.prepare(sql).await?;
        self.cache
            .lock()
            .expect("statement cache mutex poisoned")
            .insert(sql, &stmt);
        Ok(stmt)
    }

    pub async fn execute(
        &self,
        stmt: &TursoPreparedStatement,
    ) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.prepare_cached(&stmt.sql).await?;
        let params: Vec<Value> = stmt.binds.clone();
        let result = prepared.execute(params).await;

        // Workaround: some statements (e.g. PRAGMA) return rows but were
        // dispatched here. Re-route through `query()` which keeps the same
        // cached prepared statement.
        let rows_affected = match result {
            Ok(res) => res,
            Err(turso::Error::Misuse(msg)) if msg.contains("unexpected row") => {
                return self.query(stmt).await;
            }
            Err(e) => return Err(e),
        };

        Ok(TursoResult {
            column_names: Arc::from([]),
            rows: Vec::new(),
            error: None,
            changes: usize::try_from(rows_affected).map_err(|_| {
                turso::Error::ConversionFailure(format!(
                    "rows_affected ({rows_affected}) exceeds usize::MAX"
                ))
            })?,
        })
    }

    pub async fn execute_batch(&self, stmt: &TursoPreparedStatement) -> Result<(), turso::Error> {
        // Batch SQL is multi-statement and not cacheable as a single
        // prepared statement.
        self.conn.execute_batch(&stmt.sql).await?;
        Ok(())
    }

    pub async fn query(&self, stmt: &TursoPreparedStatement) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.prepare_cached(&stmt.sql).await?;
        let params: Vec<Value> = stmt.binds.clone();
        let mut rows_iter = prepared.query(params).await?;
        let column_names: Arc<[String]> = prepared
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let column_count = column_names.len();

        let mut rows = Vec::new();
        while let Some(row) = rows_iter.next().await? {
            let row_data: Vec<Value> = (0..column_count)
                .map(|idx| row.get_value(idx))
                .collect::<Result<_, _>>()?;
            rows.push(row_data);
        }

        Ok(TursoResult {
            column_names,
            rows,
            error: None,
            changes: 0,
        })
    }
}

impl StatementCache {
    fn lookup(&self, sql: &str) -> Option<Statement> {
        if !self.enabled {
            return None;
        }
        self.entries.get(sql).cloned()
    }

    fn insert(&mut self, sql: &str, stmt: &Statement) {
        if self.enabled {
            self.entries.insert(sql.to_string(), stmt.clone());
        }
    }
}

impl TursoPreparedStatement {
    pub fn bind(&mut self, values: Vec<Value>) -> &mut Self {
        self.binds = values;
        self
    }
}
