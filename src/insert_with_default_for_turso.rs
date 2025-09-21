use crate::backend::TursoBackend;
use crate::AsyncTursoConnection;
use diesel::insertable::{
    CanInsertInSingleQuery, ColumnInsertValue, DefaultableColumnInsertValue, InsertValues,
};
use diesel::prelude::*;
use diesel::query_builder::{AstPass, DebugQuery, QueryFragment, QueryId};
use diesel::query_builder::{BatchInsert, InsertStatement, ValuesClause};
use diesel_async::AsyncConnectionCore;
use std::fmt::{self, Debug, Display};

use diesel_async::methods::ExecuteDsl;

#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct Yes;

impl Default for Yes {
    fn default() -> Self {
        Yes
    }
}

#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct No;

impl Default for No {
    fn default() -> Self {
        No
    }
}

pub trait Any<Rhs> {
    type Out: Any<Yes> + Any<No>;
}

impl Any<No> for No {
    type Out = No;
}

impl Any<Yes> for No {
    type Out = Yes;
}

impl Any<No> for Yes {
    type Out = Yes;
}

impl Any<Yes> for Yes {
    type Out = Yes;
}

// TODO: do we need it?
#[allow(dead_code)]
pub trait ContainsDefaultableValue {
    type Out: Any<Yes> + Any<No>;
}

impl<C, B> ContainsDefaultableValue for ColumnInsertValue<C, B> {
    type Out = No;
}

impl<I> ContainsDefaultableValue for DefaultableColumnInsertValue<I> {
    type Out = Yes;
}

impl<I, const SIZE: usize> ContainsDefaultableValue for [I; SIZE]
where
    I: ContainsDefaultableValue,
{
    type Out = I::Out;
}

impl<I, T> ContainsDefaultableValue for ValuesClause<I, T>
where
    I: ContainsDefaultableValue,
{
    type Out = I::Out;
}

impl<T> ContainsDefaultableValue for &T
where
    T: ContainsDefaultableValue,
{
    type Out = T::Out;
}

// TODO: do we need it?
#[allow(dead_code)]
pub trait DebugQueryHelper<ContainsDefaultableValue> {
    fn fmt_debug(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
    fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl<T, V, QId, Op, Ret, const STATIC_QUERY_ID: bool> DebugQueryHelper<Yes>
    for DebugQuery<
        '_,
        InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op, Ret>,
        TursoBackend,
    >
where
    V: QueryFragment<TursoBackend>,
    T: Copy + QuerySource,
    Op: Copy,
    Ret: Copy,
    for<'b> InsertStatement<T, &'b ValuesClause<V, T>, Op, Ret>: QueryFragment<TursoBackend>,
{
    fn fmt_debug(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Since we can't access query field, we'll provide a generic implementation
        f.debug_struct("BatchInsertQuery")
            .field("backend", &"Turso")
            .finish()
    }

    fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Since we can't access query field, we'll provide a generic implementation
        writeln!(f, "-- Turso batch insert query")
    }
}

#[allow(unsafe_code)] // cast to transparent wrapper type
impl<'a, T, V, QId, Op, const STATIC_QUERY_ID: bool> DebugQueryHelper<No>
    for DebugQuery<
        'a,
        InsertStatement<T, BatchInsert<V, T, QId, STATIC_QUERY_ID>, Op>,
        TursoBackend,
    >
where
    T: Copy + QuerySource,
    Op: Copy,
    DebugQuery<
        'a,
        InsertStatement<T, TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>, Op>,
        TursoBackend,
    >: Debug + Display,
{
    fn fmt_debug(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = unsafe {
            // This cast is safe as `TursoBatchInsertWrapper` is #[repr(transparent)]
            &*(self as *const DebugQuery<
                'a,
                InsertStatement<T, BatchInsert<V, T, QId, STATIC_QUERY_ID>, Op>,
                TursoBackend,
            >
                as *const DebugQuery<
                    'a,
                    InsertStatement<T, TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>, Op>,
                    TursoBackend,
                >)
        };
        <_ as Debug>::fmt(value, f)
    }

    fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = unsafe {
            // This cast is safe as `TursoBatchInsertWrapper` is #[repr(transparent)]
            &*(self as *const DebugQuery<
                'a,
                InsertStatement<T, BatchInsert<V, T, QId, STATIC_QUERY_ID>, Op>,
                TursoBackend,
            >
                as *const DebugQuery<
                    'a,
                    InsertStatement<T, TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>, Op>,
                    TursoBackend,
                >)
        };
        <_ as Display>::fmt(value, f)
    }
}

// Note: Removed generic ExecuteDsl implementation to avoid conflicts with diesel_async generic impl
// The default diesel_async ExecuteDsl will handle normal cases now that QueryFragment is implemented

// ExecuteDsl implementation for (Yes, InsertStatement) for async transactions
impl<V, T, QId, Op, const STATIC_QUERY_ID: bool> ExecuteDsl<AsyncTursoConnection, TursoBackend>
    for (
        Yes,
        InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op>,
    )
where
    T: Table + Copy + QueryId + 'static + Send + Sync,
    T::FromClause: QueryFragment<TursoBackend>,
    Op: Copy + QueryId + QueryFragment<TursoBackend> + Send + Sync + 'static,
    V: InsertValues<TursoBackend, T>
        + CanInsertInSingleQuery<TursoBackend>
        + QueryId
        + Send
        + Sync
        + 'static,
    for<'a> InsertStatement<T, &'a ValuesClause<V, T>, Op>:
        ExecuteDsl<AsyncTursoConnection, TursoBackend>,
{
    fn execute<'conn, 'query>(
        (Yes, query): Self,
        conn: &'conn mut AsyncTursoConnection,
    ) -> <AsyncTursoConnection as AsyncConnectionCore>::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        Box::pin(async move {
            let mut result = 0;
            for record in &query.records.values {
                let stmt =
                    InsertStatement::new(query.target, record, query.operator, query.returning);
                result += diesel_async::RunQueryDsl::execute(stmt, conn).await?;
            }
            Ok(result)
        })
    }
}

#[repr(transparent)]
pub struct TursoBatchInsertWrapper<V, T, QId, const STATIC_QUERY_ID: bool>(
    BatchInsert<V, T, QId, STATIC_QUERY_ID>,
);

impl<V, Tab, QId, const STATIC_QUERY_ID: bool> QueryFragment<TursoBackend>
    for TursoBatchInsertWrapper<Vec<ValuesClause<V, Tab>>, Tab, QId, STATIC_QUERY_ID>
where
    ValuesClause<V, Tab>: QueryFragment<TursoBackend>,
    V: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        if !STATIC_QUERY_ID {
            out.unsafe_to_cache_prepared();
        }

        let mut values = self.0.values.iter();
        if let Some(value) = values.next() {
            value.walk_ast(out.reborrow())?;
        }
        for value in values {
            out.push_sql(", (");
            value.values.walk_ast(out.reborrow())?;
            out.push_sql(")");
        }
        Ok(())
    }
}

#[repr(transparent)]
pub struct TursoCanInsertInSingleQueryHelper<T: ?Sized>(T);

impl<V, T, QId, const STATIC_QUERY_ID: bool> CanInsertInSingleQuery<TursoBackend>
    for TursoBatchInsertWrapper<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>
where
    // We constrain that here on an internal helper type
    // to make sure that this does not accidentally leak
    // so that none does really implement normal batch
    // insert for inserts with default values here
    TursoCanInsertInSingleQueryHelper<V>: CanInsertInSingleQuery<TursoBackend>,
{
    fn rows_to_insert(&self) -> Option<usize> {
        Some(self.0.values.len())
    }
}

impl<T> CanInsertInSingleQuery<TursoBackend> for TursoCanInsertInSingleQueryHelper<T>
where
    T: CanInsertInSingleQuery<TursoBackend>,
{
    fn rows_to_insert(&self) -> Option<usize> {
        self.0.rows_to_insert()
    }
}

impl<V, T, QId, const STATIC_QUERY_ID: bool> QueryId
    for TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>
where
    BatchInsert<V, T, QId, STATIC_QUERY_ID>: QueryId,
{
    type QueryId = <BatchInsert<V, T, QId, STATIC_QUERY_ID> as QueryId>::QueryId;

    const HAS_STATIC_QUERY_ID: bool =
        <BatchInsert<V, T, QId, STATIC_QUERY_ID> as QueryId>::HAS_STATIC_QUERY_ID;
}

impl<V, T, QId, Op, const STATIC_QUERY_ID: bool> ExecuteDsl<AsyncTursoConnection, TursoBackend>
    for (
        No,
        InsertStatement<T, BatchInsert<V, T, QId, STATIC_QUERY_ID>, Op>,
    )
where
    T: Table + QueryId + 'static,
    T::FromClause: QueryFragment<TursoBackend>,
    Op: QueryFragment<TursoBackend> + QueryId,
    TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>:
        QueryFragment<TursoBackend> + QueryId + CanInsertInSingleQuery<TursoBackend>,
    InsertStatement<T, TursoBatchInsertWrapper<V, T, QId, STATIC_QUERY_ID>, Op>:
        ExecuteDsl<AsyncTursoConnection, TursoBackend>,
{
    fn execute<'conn, 'query>(
        (No, query): Self,
        conn: &'conn mut AsyncTursoConnection,
    ) -> <AsyncTursoConnection as AsyncConnectionCore>::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        let wrapped = InsertStatement::new(
            query.target,
            TursoBatchInsertWrapper(query.records),
            query.operator,
            query.returning,
        );
        diesel_async::RunQueryDsl::execute(wrapped, conn)
    }
}

// QueryFragment implementation for BatchInsert with TursoBackend and SqliteBatchInsert
impl<Tab, V, QId, const HAS_STATIC_QUERY_ID: bool>
    QueryFragment<TursoBackend, crate::backend::SqliteBatchInsert>
    for BatchInsert<Vec<ValuesClause<V, Tab>>, Tab, QId, HAS_STATIC_QUERY_ID>
where
    ValuesClause<V, Tab>: QueryFragment<TursoBackend>,
    V: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        if !HAS_STATIC_QUERY_ID {
            out.unsafe_to_cache_prepared();
        }

        let mut values = self.values.iter();
        if let Some(value) = values.next() {
            value.walk_ast(out.reborrow())?;
        }
        for value in values {
            out.push_sql(", (");
            value.values.walk_ast(out.reborrow())?;
            out.push_sql(")");
        }
        Ok(())
    }
}

// CanInsertInSingleQuery implementations for TursoBackend that does not support default keywords
impl<T, Table, QId, const HAS_STATIC_QUERY_ID: bool> CanInsertInSingleQuery<TursoBackend>
    for BatchInsert<T, Table, QId, HAS_STATIC_QUERY_ID>
where
    T: CanInsertInSingleQuery<TursoBackend>,
{
    fn rows_to_insert(&self) -> Option<usize> {
        self.values.rows_to_insert()
    }
}

impl<T, const N: usize> CanInsertInSingleQuery<TursoBackend> for [T; N] {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(N)
    }
}

impl<T, const N: usize> CanInsertInSingleQuery<TursoBackend> for Box<[T; N]> {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(N)
    }
}

impl<T> CanInsertInSingleQuery<TursoBackend> for [T] {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl<T> CanInsertInSingleQuery<TursoBackend> for Vec<T> {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(self.len())
    }
}
