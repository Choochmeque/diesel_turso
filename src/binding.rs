use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso::{Builder, Connection, Database, Statement, Value};

#[derive(Debug, Clone)]
pub struct TursoDatabase {
    pub db: Database,
}

#[derive(Debug, Clone)]
pub struct TursoConnection {
    pub conn: Arc<Connection>,
    /// Whether prepared statements should route through turso's
    /// per-connection cache (`Connection::prepare_cached`) or always
    /// re-prepare (`Connection::prepare`). Mirrors diesel's `CacheSize`
    /// — `Unbounded` (the default) is `true`, `Disabled` is `false`.
    cache_enabled: Arc<AtomicBool>,
    /// Test-only routing counters. There's no public API on turso's
    /// `Connection` to inspect the cache state from outside, so tests
    /// instead assert that we *chose* the cached vs. uncached path the
    /// expected number of times.
    #[cfg(test)]
    counters: Arc<TestCounters>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TestCounters {
    cached_calls: std::sync::atomic::AtomicU64,
    uncached_calls: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub struct TursoPreparedStatement {
    pub sql: String,
    pub binds: Vec<Value>,
    /// Mirrors diesel's `QueryFragment::is_safe_to_cache_prepared` signal.
    /// When `false`, this call bypasses turso's prepared-statement cache
    /// regardless of the per-connection cache toggle.
    pub cacheable: bool,
}

#[derive(Debug, Clone)]
pub struct TursoResult {
    pub changes: usize,
}

impl TursoDatabase {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let db = Builder::new_local(path).build().await?;
        Ok(Self { db })
    }

    pub fn connect(&self) -> Result<TursoConnection, turso::Error> {
        let conn = Arc::new(self.db.connect()?);
        Ok(TursoConnection {
            conn,
            cache_enabled: Arc::new(AtomicBool::new(true)),
            #[cfg(test)]
            counters: Arc::new(TestCounters::default()),
        })
    }
}

impl TursoConnection {
    /// Enable or disable routing through turso's prepared-statement cache
    /// for future `prepare_one` calls. turso has no public API to evict
    /// entries already cached on its connection, so flipping this to
    /// `false` cannot remove what's already there — it only steers
    /// subsequent calls to `Connection::prepare`. That's the right
    /// behaviour for diesel's `CacheSize::Disabled`: avoid producing new
    /// cached state, while not pretending to flush state that turso owns.
    pub fn set_cache_enabled(&self, enabled: bool) {
        self.cache_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Prepare a single statement, dispatching to turso's
    /// `prepare_cached` when allowed and `prepare` otherwise. turso's
    /// `prepare_cached` already validates schema compatibility on every
    /// lookup (`Program::is_compatible_with`) — so DDL through any path
    /// (including batches with leading `/* … */` comments) invalidates
    /// stale entries automatically. This means we don't need our own
    /// keyword inspection, our own cache map, or a manual flush after
    /// `execute_batch`.
    async fn prepare_one(&self, sql: &str, cacheable: bool) -> Result<Statement, turso::Error> {
        let use_cache = cacheable && self.cache_enabled.load(Ordering::Relaxed);
        #[cfg(test)]
        {
            let counter = if use_cache {
                &self.counters.cached_calls
            } else {
                &self.counters.uncached_calls
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if use_cache {
            self.conn.prepare_cached(sql).await
        } else {
            self.conn.prepare(sql).await
        }
    }

    pub async fn execute(
        &self,
        stmt: &TursoPreparedStatement,
    ) -> Result<TursoResult, turso::Error> {
        let mut prepared = self.prepare_one(&stmt.sql, stmt.cacheable).await?;

        // If the prepared statement produces result columns (SELECT, PRAGMA
        // with output, INSERT/UPDATE/DELETE … RETURNING, …) we can't call
        // `Statement::execute` on it — turso surfaces the first stepped row
        // as a `Misuse("unexpected row …")` error. Step through and
        // *discard* rows rather than materializing them: the caller of
        // `execute()` only consumes `changes`, so an
        // `UPDATE … RETURNING *` over a large table doesn't have to buffer
        // every row in memory.
        if !prepared.columns().is_empty() {
            let changes = Self::drain_rows(prepared, stmt.binds.clone()).await?;
            return Ok(TursoResult { changes });
        }

        let params: Vec<Value> = stmt.binds.clone();
        let rows_affected = prepared.execute(params).await?;

        let changes = usize::try_from(rows_affected).map_err(|_| {
            turso::Error::ConversionFailure(format!(
                "rows_affected ({rows_affected}) exceeds usize::MAX"
            ))
        })?;
        Ok(TursoResult { changes })
    }

    pub async fn execute_batch(&self, stmt: &TursoPreparedStatement) -> Result<(), turso::Error> {
        // Batch SQL is multi-statement and not a single prepared
        // statement, so it's never cached as a unit. Any DDL inside the
        // batch invalidates dependent cached statements automatically via
        // turso's `Program::is_compatible_with` check on the next lookup.
        self.conn.execute_batch(&stmt.sql).await
    }

    /// Open a live row iterator for `stmt`, returning the iterator and
    /// the resolved column names. Used by `load()` to back
    /// diesel-async's `Stream` associated type with a lazy stream
    /// (`stream::unfold` over `turso::Rows`) rather than a buffered
    /// `Vec` — large `SELECT`s no longer have to materialize the full
    /// result set up front.
    pub async fn open_stream(
        &self,
        stmt: &TursoPreparedStatement,
    ) -> Result<(turso::Rows, Arc<[String]>), turso::Error> {
        let mut prepared = self.prepare_one(&stmt.sql, stmt.cacheable).await?;
        let column_names: Arc<[String]> = prepared
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let rows = prepared.query(stmt.binds.clone()).await?;
        Ok((rows, column_names))
    }

    /// Step through a prepared row-producing statement to completion,
    /// discarding rows. Used by `execute()` when the statement happens to
    /// produce columns (e.g. `INSERT/UPDATE/DELETE … RETURNING`) — the
    /// caller only wants the affected-row count, so buffering rows would
    /// waste memory for `RETURNING *` over large updates.
    async fn drain_rows(
        mut prepared: Statement,
        params: Vec<Value>,
    ) -> Result<usize, turso::Error> {
        let mut rows_iter = prepared.query(params).await?;
        while rows_iter.next().await?.is_some() {}
        let changes = prepared.n_change();
        usize::try_from(changes).map_err(|_| {
            turso::Error::ConversionFailure(format!("rows_affected ({changes}) exceeds usize::MAX"))
        })
    }

    /// Returns `(cached_route, uncached_route)` — number of times
    /// `prepare_one` chose `Connection::prepare_cached` vs.
    /// `Connection::prepare` since this connection was created. Used by
    /// tests to assert that uncacheable statements bypass the cache.
    #[cfg(test)]
    pub(crate) fn cache_route_counts(&self) -> (u64, u64) {
        (
            self.counters.cached_calls.load(Ordering::Relaxed),
            self.counters.uncached_calls.load(Ordering::Relaxed),
        )
    }
}

impl TursoPreparedStatement {
    /// Construct a carrier for a SQL string. No preparation happens here —
    /// this is just a builder. Actual `Statement` creation (cached or
    /// not) is deferred to [`TursoConnection::prepare_one`].
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            binds: Vec::new(),
            cacheable: true,
        }
    }

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
