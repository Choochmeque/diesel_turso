use backend::TursoBackend;
use bind_collector::TursoBindCollector;
use binding::{TursoConnection, TursoDatabase};
use diesel::{
    connection::{
        get_default_instrumentation, CacheSize, Instrumentation, InstrumentationEvent,
        StrQueryHelper,
    },
    query_builder::{AsQuery, QueryFragment, QueryId},
    ConnectionResult, QueryResult,
};
use diesel_async::AnsiTransactionManager;
use diesel_async::{AsyncConnection, AsyncConnectionCore, SimpleAsyncConnection};
use futures_util::{
    future::BoxFuture,
    stream::{self, BoxStream},
    FutureExt, StreamExt,
};
use query_builder::TursoQueryBuilder;
use row::TursoRow;
use utils::TursoError;

pub mod backend;
mod bind_collector;
mod binding;
mod insert_with_default_for_turso;
mod insertable;
mod query_builder;
mod row;
mod types;
mod utils;
mod value;

pub struct AsyncTursoConnection {
    transaction_manager: AnsiTransactionManager,
    binding: TursoDatabase,
    pub(crate) connection: Option<TursoConnection>,
    instrumentation: Box<dyn Instrumentation>,
    /// Whether the prepared-statement cache is enabled. Diesel's default
    /// is `CacheSize::Unbounded`, so cache is on unless the user disables it.
    cache_enabled: bool,
}

impl AsyncTursoConnection {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let binding = TursoDatabase::new(path).await?;
        Ok(Self {
            transaction_manager: AnsiTransactionManager::default(),
            binding,
            connection: None,
            instrumentation: Box::new(get_default_instrumentation()),
            cache_enabled: true,
        })
    }

    pub(crate) async fn ensure_connection(
        &mut self,
    ) -> Result<&TursoConnection, diesel::result::Error> {
        if self.connection.is_none() {
            let conn = self.binding.connect().await.map_err(|e| {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(TursoError {
                        message: e.to_string(),
                    }),
                )
            })?;
            // Apply the user-requested cache setting to the new connection.
            conn.set_cache_enabled(self.cache_enabled);
            self.connection = Some(conn);
        }
        let Some(conn) = self.connection.as_ref() else {
            unreachable!("self.connection populated above")
        };
        Ok(conn)
    }
}

impl SimpleAsyncConnection for AsyncTursoConnection {
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        self.instrumentation()
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                query,
            )));

        let result = async {
            let conn = self.ensure_connection().await?;
            let stmt = conn.prepare(query);
            conn.execute_batch(&stmt).await.map_err(|e| {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(TursoError {
                        message: e.to_string(),
                    }),
                )
            })
        }
        .await;

        self.instrumentation()
            .on_connection_event(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(query),
                result.as_ref().err(),
            ));
        result
    }
}

impl AsyncConnectionCore for AsyncTursoConnection {
    type ExecuteFuture<'conn, 'query> = BoxFuture<'conn, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> = BoxFuture<'conn, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'conn, QueryResult<Self::Row<'conn, 'query>>>;
    type Row<'conn, 'query> = TursoRow;
    type Backend = TursoBackend;

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        let prep = (|| -> QueryResult<(String, Vec<turso::Value>, bool)> {
            let source = source.as_query();
            let mut query_builder = TursoQueryBuilder::default();
            source.to_sql(&mut query_builder, &TursoBackend)?;
            let binds = construct_bind_data(&source)?;
            let cacheable = source.is_safe_to_cache_prepared(&TursoBackend)?;
            Ok((query_builder.sql, binds, cacheable))
        })();

        async move {
            let (sql, binds, cacheable) = prep?;

            self.instrumentation()
                .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                    &sql,
                )));

            let opened = async {
                let conn = self.ensure_connection().await?;
                let mut stmt = conn.prepare(&sql);
                stmt.bind(binds);
                stmt.set_cacheable(cacheable);
                conn.open_stream(&stmt).await.map_err(|e| {
                    diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::Unknown,
                        Box::new(TursoError {
                            message: e.to_string(),
                        }),
                    )
                })
            }
            .await;

            // The instrumented "query" spans `prepare` + `open` — the row
            // stream that follows pulls lazily on consumer demand. Most
            // diesel-async drivers emit finish_query the same way (around
            // the open, not around full drain) since "query duration" for
            // a streaming load isn't well-defined.
            self.instrumentation()
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    opened.as_ref().err(),
                ));

            let (rows_iter, column_names) = opened?;

            let stream = stream::unfold(Some((rows_iter, column_names)), |state| async move {
                let (mut rows_iter, column_names) = state?;
                match rows_iter.next().await {
                    Ok(None) => None,
                    Err(e) => Some((
                        Err(diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::Unknown,
                            Box::new(TursoError {
                                message: e.to_string(),
                            }),
                        )),
                        None,
                    )),
                    Ok(Some(row)) => {
                        let column_count = column_names.len();
                        let mut values = Vec::with_capacity(column_count);
                        for idx in 0..column_count {
                            match row.get_value(idx) {
                                Ok(v) => values.push(v),
                                Err(e) => {
                                    return Some((
                                        Err(diesel::result::Error::DatabaseError(
                                            diesel::result::DatabaseErrorKind::Unknown,
                                            Box::new(TursoError {
                                                message: e.to_string(),
                                            }),
                                        )),
                                        None,
                                    ));
                                }
                            }
                        }
                        let item = TursoRow::from_turso_values(values, column_names.clone())
                            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)));
                        let next_state = match &item {
                            Ok(_) => Some((rows_iter, column_names)),
                            Err(_) => None,
                        };
                        Some((item, next_state))
                    }
                }
            });

            Ok(stream.boxed())
        }
        .boxed()
    }

    #[doc(hidden)]
    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        let prep = (|| -> QueryResult<(String, Vec<turso::Value>, bool)> {
            let mut query_builder = TursoQueryBuilder::default();
            source.to_sql(&mut query_builder, &TursoBackend)?;
            let binds = construct_bind_data(&source)?;
            let cacheable = source.is_safe_to_cache_prepared(&TursoBackend)?;
            Ok((query_builder.sql, binds, cacheable))
        })();

        async move {
            let (sql, binds, cacheable) = prep?;

            self.instrumentation()
                .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                    &sql,
                )));

            let result = async {
                let conn = self.ensure_connection().await?;
                let mut stmt = conn.prepare(&sql);
                stmt.bind(binds);
                stmt.set_cacheable(cacheable);
                conn.execute(&stmt).await.map(|r| r.changes).map_err(|e| {
                    diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::Unknown,
                        Box::new(TursoError {
                            message: e.to_string(),
                        }),
                    )
                })
            }
            .await;

            self.instrumentation()
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    result.as_ref().err(),
                ));

            result
        }
        .boxed()
    }
}

impl AsyncConnection for AsyncTursoConnection {
    type TransactionManager = AnsiTransactionManager;

    async fn establish(path: &str) -> ConnectionResult<Self> {
        Self::new(path)
            .await
            .map_err(|e| diesel::result::ConnectionError::BadConnection(e.to_string()))
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_manager
    }

    #[doc(hidden)]
    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    #[doc = " Set a specific [`Instrumentation`] implementation for this connection"]
    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Box::new(instrumentation);
    }

    #[doc = " Set the prepared statement cache size to [`CacheSize`] for this connection"]
    fn set_prepared_statement_cache_size(&mut self, size: CacheSize) {
        let enabled = cache_size_enabled(size).unwrap_or_else(|| {
            // `CacheSize` is `#[non_exhaustive]`. We can't return an
            // error from this trait method, so we default the unknown
            // variant to enabled (matches diesel's `Unbounded` default
            // and is the least-surprising behaviour). Surface the
            // fallback through instrumentation so the silent default is
            // at least observable to anyone with logging on. Use
            // `cache_query` since it's the closest semantic fit
            // (cache-related event).
            self.instrumentation()
                .on_connection_event(InstrumentationEvent::cache_query(
                    "[diesel-turso] unrecognized CacheSize variant; treating as Unbounded",
                ));
            true
        });
        self.cache_enabled = enabled;
        if let Some(conn) = self.connection.as_ref() {
            conn.set_cache_enabled(enabled);
        }
    }
}

/// Map diesel's [`CacheSize`] onto turso's enable/disable knob. turso's
/// bindings don't expose a bounded LRU, so we collapse the diesel knob to
/// a boolean.
///
/// `CacheSize` is `#[non_exhaustive]`. Returns `None` for any future
/// variant so the caller can decide how to surface the fallback (we emit
/// an instrumentation event in `set_prepared_statement_cache_size` and
/// then default to enabled, matching diesel's `Unbounded` default).
const fn cache_size_enabled(size: CacheSize) -> Option<bool> {
    match size {
        CacheSize::Unbounded => Some(true),
        CacheSize::Disabled => Some(false),
        _ => None,
    }
}

pub(crate) fn construct_bind_data<T>(query: &T) -> Result<Vec<turso::Value>, diesel::result::Error>
where
    T: QueryFragment<TursoBackend>,
{
    let mut bind_collector = TursoBindCollector::default();

    query.collect_binds(&mut bind_collector, &mut (), &TursoBackend)?;

    let values = bind_collector
        .binds
        .iter()
        .map(|(bind, _)| bind.to_turso_value())
        .collect::<Vec<turso::Value>>();
    Ok(values)
}

#[cfg(any(
    feature = "bb8",
    feature = "deadpool",
    feature = "mobc",
    feature = "r2d2"
))]
impl diesel_async::pooled_connection::PoolableConnection for AsyncTursoConnection {}

#[cfg(test)]
mod tests;
