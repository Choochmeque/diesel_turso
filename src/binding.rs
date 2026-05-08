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
    /// Mirrors diesel's `QueryFragment::is_safe_to_cache_prepared` signal.
    /// When `false`, the per-connection statement cache is bypassed and a
    /// fresh `turso::Statement` is prepared for this call. Defaults to
    /// `true`; the lib-level callers (`load`/`execute_returning_count`)
    /// flip it off when diesel reports the query is unsafe to cache.
    pub cacheable: bool,
}

#[derive(Debug, Clone)]
pub struct TursoResult {
    pub column_names: Arc<[String]>,
    pub rows: Vec<Vec<Value>>,
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
            cacheable: true,
        }
    }

    /// Number of currently cached prepared statements. Exposed for tests
    /// that need to assert the per-connection cache grew (or didn't) after
    /// a query — there's no other way to observe cache state from outside
    /// the binding module.
    #[cfg(test)]
    pub(crate) fn cache_len(&self) -> usize {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len()
    }

    /// Enable or disable the prepared-statement cache. Disabling clears any
    /// already-cached entries.
    pub fn set_cache_enabled(&self, enabled: bool) {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.enabled = enabled;
        if !enabled {
            cache.entries.clear();
        }
    }

    /// Look up a cached prepared statement for `sql` or prepare and cache one.
    /// Returns an owned `Statement` clone (cheap — `Statement` is internally
    /// `Arc<Mutex<…>>`). When `cacheable` is `false` (mirroring diesel's
    /// `QueryFragment::is_safe_to_cache_prepared`), the cache is bypassed
    /// in both directions: no lookup, no insert.
    async fn prepare_cached(&self, sql: &str, cacheable: bool) -> Result<Statement, turso::Error> {
        if !cacheable {
            return self.conn.prepare(sql).await;
        }
        // Bind to a local so the MutexGuard is released before the `await`
        // below — never hold a sync `MutexGuard` across `.await`.
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lookup(sql);
        if let Some(stmt) = cached {
            return Ok(stmt);
        }
        let stmt = self.conn.prepare(sql).await?;
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(sql, &stmt);
        Ok(stmt)
    }

    pub async fn execute(
        &self,
        stmt: &TursoPreparedStatement,
    ) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.prepare_cached(&stmt.sql, stmt.cacheable).await?;

        // If the prepared statement produces result columns (SELECT, PRAGMA
        // with output, INSERT/UPDATE/DELETE … RETURNING, …) we can't call
        // `Statement::execute` on it — turso surfaces the first stepped row
        // as a `Misuse("unexpected row …")` error. Detect via column metadata
        // at prepare time and route through `query()` instead. This is
        // structural (no error-string matching) and stable across SDK
        // versions.
        if !prepared.columns().is_empty() {
            return self.query(stmt).await;
        }

        let params: Vec<Value> = stmt.binds.clone();
        let rows_affected = prepared.execute(params).await?;

        // Single-statement DDL routed through `execute()` (e.g. via
        // `diesel::sql_query("CREATE TABLE …").execute(&mut conn)`) can
        // invalidate the schema-bound metadata of *other* cached prepared
        // statements. Detect DDL by inspecting the leading keyword and flush
        // the cache so subsequent queries re-prepare against the current
        // schema. Multi-statement batches go through `execute_batch`, which
        // flushes unconditionally.
        if is_ddl(&stmt.sql) {
            self.cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .clear();
        }

        Ok(TursoResult {
            column_names: Arc::from([]),
            rows: Vec::new(),
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
        let result = self.conn.execute_batch(&stmt.sql).await;

        // Clear the cache regardless of success: SQLite-style batch
        // execution can partially apply earlier DDL (CREATE/ALTER/DROP)
        // before a later statement errors, which can leave cached
        // prepared statements bound to stale schema metadata. The cache
        // stays *enabled*; we only flush its entries.
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .clear();

        result
    }

    pub async fn query(&self, stmt: &TursoPreparedStatement) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.prepare_cached(&stmt.sql, stmt.cacheable).await?;
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

        // Capture rows-affected after stepping. `Statement::n_change()` is
        // 0 for plain SELECTs (so this is correct for `load()`), and for
        // INSERT/UPDATE/DELETE … RETURNING — which `execute()` re-routes
        // here — it reports the actual mutation count rather than 0.
        let changes = prepared.n_change();

        Ok(TursoResult {
            column_names,
            rows,
            changes: usize::try_from(changes).map_err(|_| {
                turso::Error::ConversionFailure(format!(
                    "rows_affected ({changes}) exceeds usize::MAX"
                ))
            })?,
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
    /// Mark this statement as not safe for prepared-statement caching,
    /// mirroring diesel's `QueryFragment::is_safe_to_cache_prepared`.
    pub const fn set_cacheable(&mut self, cacheable: bool) -> &mut Self {
        self.cacheable = cacheable;
        self
    }

    pub fn bind(&mut self, values: Vec<Value>) -> &mut Self {
        self.binds = values;
        self
    }
}

/// Returns `true` if `sql` begins with a `SQLite` DDL keyword. DDL changes
/// the schema and can invalidate the metadata of cached prepared
/// statements. `turso`'s public API does not expose a parsed statement
/// kind or a `readonly` flag, so we fall back to first-keyword inspection.
fn is_ddl(sql: &str) -> bool {
    sql.split_whitespace().next().is_some_and(|w| {
        matches!(
            w.to_ascii_uppercase().as_str(),
            "CREATE" | "DROP" | "ALTER" | "REINDEX" | "ATTACH" | "DETACH" | "VACUUM"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::is_ddl;

    #[test]
    fn detects_ddl() {
        assert!(is_ddl("CREATE TABLE t (id INTEGER)"));
        assert!(is_ddl("create table t (id integer)"));
        assert!(is_ddl("  DROP TABLE t"));
        assert!(is_ddl("ALTER TABLE t ADD COLUMN c INTEGER"));
        assert!(is_ddl("VACUUM"));
    }

    #[test]
    fn rejects_non_ddl() {
        assert!(!is_ddl("SELECT 1"));
        assert!(!is_ddl("INSERT INTO t VALUES (1)"));
        assert!(!is_ddl("UPDATE t SET c = 1"));
        assert!(!is_ddl("DELETE FROM t"));
        assert!(!is_ddl(""));
        assert!(!is_ddl("   "));
    }
}
