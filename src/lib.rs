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
}

impl AsyncTursoConnection {
    pub async fn new(path: &str) -> Result<Self, turso::Error> {
        let binding = TursoDatabase::new(path).await?;
        Ok(AsyncTursoConnection {
            transaction_manager: AnsiTransactionManager::default(),
            binding,
            connection: None,
            instrumentation: Box::new(get_default_instrumentation()),
        })
    }

    pub(crate) async fn ensure_connection(&mut self) -> Result<(), diesel::result::Error> {
        if self.connection.is_none() {
            self.connection = Some(self.binding.connect().await.map_err(|e| {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(TursoError {
                        message: e.to_string(),
                    }),
                )
            })?);
        }
        Ok(())
    }
}

impl SimpleAsyncConnection for AsyncTursoConnection {
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        self.ensure_connection().await?;

        self.instrumentation()
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                query,
            )));

        let conn = self.connection.as_ref().unwrap();
        let stmt = conn.prepare(query);

        let result = conn.execute_batch(&stmt).await.map_err(|e| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new(TursoError {
                    message: e.to_string(),
                }),
            )
        });

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
        let source = source.as_query();
        let mut query_builder = TursoQueryBuilder::default();
        source.to_sql(&mut query_builder, &TursoBackend).unwrap();
        let sql = query_builder.sql.clone();
        let binds = construct_bind_data(&source).unwrap();

        async move {
            self.ensure_connection().await?;
            let conn = self.connection.as_ref().unwrap();

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

            if let Some(error) = result.error() {
                return Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(TursoError { message: error }),
                ));
            }

            let results = result.results().unwrap_or_else(Vec::new);

            if results.is_empty() {
                return Ok(stream::iter(vec![]).boxed());
            }

            let field_keys: Vec<String> = if !results.is_empty() && !results[0].is_empty() {
                results[0].iter().map(|(key, _)| key.clone()).collect()
            } else {
                Vec::new()
            };

            let rows: Vec<QueryResult<TursoRow>> = results
                .iter()
                .map(|row| {
                    let values: Vec<turso::Value> = row.iter().map(|(_, v)| v.clone()).collect();
                    Ok(TursoRow::from_turso_values(values, field_keys.clone()))
                })
                .collect();
            let iter = stream::iter(rows).boxed();
            Ok(iter)
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
        let mut query_builder = TursoQueryBuilder::default();
        source.to_sql(&mut query_builder, &TursoBackend).unwrap();
        let sql = query_builder.sql.clone();
        let binds = construct_bind_data(&source).unwrap();

        async move {
            self.ensure_connection().await?;
            let conn = self.connection.as_ref().unwrap();

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

            if let Some(error) = result.error() {
                return Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(TursoError { message: error }),
                ));
            }

            let meta = result.meta();
            Ok(meta.changes)
        }
        .boxed()
    }
}

impl AsyncConnection for AsyncTursoConnection {
    type TransactionManager = AnsiTransactionManager;

    async fn establish(path: &str) -> ConnectionResult<Self> {
        AsyncTursoConnection::new(path)
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
    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {
        todo!()
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
