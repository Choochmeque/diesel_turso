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
        let prep = (|| -> QueryResult<(String, Vec<turso::Value>)> {
            let source = source.as_query();
            let mut query_builder = TursoQueryBuilder::default();
            source.to_sql(&mut query_builder, &TursoBackend)?;
            let binds = construct_bind_data(&source)?;
            Ok((query_builder.sql, binds))
        })();

        async move {
            let (sql, binds) = prep?;
            let conn = self.ensure_connection().await?;

            let mut stmt = conn.prepare(&sql);
            stmt.bind(binds);

            let result = conn.query(&stmt).await.map_err(|e| {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(TursoError {
                        message: e.to_string(),
                    }),
                )
            })?;

            let column_names = result.column_names;
            let rows: Vec<QueryResult<TursoRow>> = result
                .rows
                .into_iter()
                .map(|values| {
                    TursoRow::from_turso_values(values, column_names.clone())
                        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
                })
                .collect();
            Ok(stream::iter(rows).boxed())
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
        let prep = (|| -> QueryResult<(String, Vec<turso::Value>)> {
            let mut query_builder = TursoQueryBuilder::default();
            source.to_sql(&mut query_builder, &TursoBackend)?;
            let binds = construct_bind_data(&source)?;
            Ok((query_builder.sql, binds))
        })();

        async move {
            let (sql, binds) = prep?;
            let conn = self.ensure_connection().await?;

            let mut stmt = conn.prepare(&sql);
            stmt.bind(binds);

            let result = conn.execute(&stmt).await.map_err(|e| {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(TursoError {
                        message: e.to_string(),
                    }),
                )
            })?;

            Ok(result.changes)
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
        // turso's bindings don't expose a bounded LRU; map diesel's two
        // settings to enable/disable.
        let enabled = matches!(size, CacheSize::Unbounded);
        self.cache_enabled = enabled;
        if let Some(conn) = self.connection.as_ref() {
            conn.set_cache_enabled(enabled);
        }
    }
}

fn construct_bind_data<T>(query: &T) -> Result<Vec<turso::Value>, diesel::result::Error>
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
