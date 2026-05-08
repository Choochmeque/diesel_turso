//! Batch-insert support for inserts that may omit defaultable columns.
//!
//! turso (like sqlite) doesn't accept a `DEFAULT` keyword in multi-row
//! `VALUES` lists, so a `Vec<NewT>` where some rows have `Some(_)` and
//! others have `None` for an `Option<T>`-against-non-nullable-with-default
//! column can't be emitted as a single `INSERT … VALUES (…), (…)`.
//! diesel solves this for sqlite with a type-level dispatch that picks
//! per-row inserts when any field is defaultable. None of those types are
//! re-exported by diesel even under the third-party-backend feature, so
//! we keep a local copy of the dispatch infrastructure here:
//!
//! - [`Yes`] / [`No`] / [`Any`] / [`ContainsDefaultableValue`] — type-level
//!   "any field is defaultable?" predicate, reduced over the tuple of
//!   column-insert-values that the `Insertable` derive produces.
//! - The dispatcher impl on `InsertStatement<…BatchInsert<Vec<…>, …>>`
//!   that routes to `(Yes, …)` for per-row inserts or `(No, …)` for
//!   single-statement `VALUES (…), (…)` emission.
//! - [`TursoBatchInsertWrapper`] — a `#[repr(transparent)]` newtype that
//!   provides `QueryFragment<TursoBackend>` (and the related traits) for
//!   the batched `(No, …)` path. We keep this off the unwrapped
//!   `BatchInsert<…>` itself: if both impls existed, diesel-async's
//!   blanket `ExecuteDsl<Conn, DB> for T: QueryFragment<DB>` would
//!   intercept the dispatcher.
//!
//! Reused from diesel: `BatchInsert`, `ValuesClause`, `InsertStatement`,
//! `ColumnInsertValue`, `DefaultableColumnInsertValue`,
//! `CanInsertInSingleQuery`, `InsertValues`, `QueryFragment`, `QueryId`,
//! `AstPass`, and the `diesel_derives::__diesel_for_each_tuple!` macro.

use crate::backend::TursoBackend;
use crate::AsyncTursoConnection;
use diesel::insertable::{
    CanInsertInSingleQuery, ColumnInsertValue, DefaultableColumnInsertValue, InsertValues,
};
use diesel::prelude::*;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::query_builder::{BatchInsert, InsertStatement, ValuesClause};
use diesel_async::AsyncConnectionCore;

use diesel_async::methods::ExecuteDsl;

#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct Yes;

impl Default for Yes {
    fn default() -> Self {
        Self
    }
}

#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct No;

impl Default for No {
    fn default() -> Self {
        Self
    }
}

pub trait Any<Rhs> {
    type Out: Any<Yes> + Any<No>;
}

impl Any<Self> for No {
    type Out = Self;
}

impl Any<Yes> for No {
    type Out = Yes;
}

impl Any<No> for Yes {
    type Out = Self;
}

impl Any<Self> for Yes {
    type Out = Self;
}

/// Type-level "contains a defaultable column?" predicate. The
/// `Insertable` derive on a struct produces a tuple of
/// `ColumnInsertValue<…>` (every field is provided) and
/// `DefaultableColumnInsertValue<…>` (the field is `Option<T>` against a
/// non-nullable column with a DB default). This trait reduces that tuple
/// to a single `Yes` / `No` so the `ExecuteDsl` dispatcher below can pick
/// the appropriate batch-insert path.
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

impl<I, Tab> ContainsDefaultableValue for ValuesClause<I, Tab>
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

// `Insertable` derives produce a tuple of column-insert-values, one per
// struct field. ContainsDefaultableValue reduces a tuple to `Yes` if
// *any* element is `Yes`, otherwise `No`. The recursion is
// `<T1::Out as Any<<T2::Out as Any<<T3::Out as Any<…>>::Out>>::Out>>::Out`,
// driven by the macro from `diesel_derives` that expands across all
// supported tuple arities.
macro_rules! tuple_impls {
    ($(
        $Tuple:tt {
            $(($idx:tt) -> $T:ident, $ST:ident, $TT:ident,)+
        }
    )+) => {
        $(
            impl_contains_defaultable_value!($($T,)*);
        )*
    }
}

macro_rules! impl_contains_defaultable_value {
    (
        @build
        start_ts = [$($ST: ident,)*],
        ts = [$T1: ident,],
        bounds = [$($bounds: tt)*],
        out = [$($out: tt)*],
    ) => {
        impl<$($ST,)*> ContainsDefaultableValue for ($($ST,)*)
        where
            $($ST: ContainsDefaultableValue,)*
            $($bounds)*
            $T1::Out: Any<$($out)*>,
        {
            type Out = <$T1::Out as Any<$($out)*>>::Out;
        }
    };
    (
        @build
        start_ts = [$($ST: ident,)*],
        ts = [$T1: ident, $($T: ident,)+],
        bounds = [$($bounds: tt)*],
        out = [$($out: tt)*],
    ) => {
        impl_contains_defaultable_value! {
            @build
            start_ts = [$($ST,)*],
            ts = [$($T,)*],
            bounds = [$($bounds)* $T1::Out: Any<$($out)*>,],
            out = [<$T1::Out as Any<$($out)*>>::Out],
        }
    };
    ($T1: ident, $($T: ident,)+) => {
        impl_contains_defaultable_value! {
            @build
            start_ts = [$T1, $($T,)*],
            ts = [$($T,)*],
            bounds = [],
            out = [$T1::Out],
        }
    };
    ($T1: ident,) => {
        impl<$T1> ContainsDefaultableValue for ($T1,)
        where $T1: ContainsDefaultableValue,
        {
            type Out = <$T1 as ContainsDefaultableValue>::Out;
        }
    }
}

diesel_derives::__diesel_for_each_tuple!(tuple_impls);

// Dispatcher — see module-level doc. Picks `(Yes, …)` (per-row) or
// `(No, …)` (single-statement `VALUES (…), (…)`) based on whether any
// field of `V` is defaultable.
impl<V, T, QId, Op, O, const STATIC_QUERY_ID: bool> ExecuteDsl<AsyncTursoConnection, TursoBackend>
    for InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op>
where
    T: QuerySource,
    V: ContainsDefaultableValue<Out = O>,
    O: Default,
    (O, Self): ExecuteDsl<AsyncTursoConnection, TursoBackend>,
{
    fn execute<'conn, 'query>(
        query: Self,
        conn: &'conn mut AsyncTursoConnection,
    ) -> <AsyncTursoConnection as AsyncConnectionCore>::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        <(O, Self) as ExecuteDsl<AsyncTursoConnection, TursoBackend>>::execute(
            (O::default(), query),
            conn,
        )
    }
}

// `(Yes, …)` path: any field is defaultable, so each row may have its
// own column shape (`None` field → column omitted from that row's SQL).
// Emit one INSERT per row.
//
// Critical detail: SQL emission happens *before* the async block, so the
// future only captures owned `(String, Vec<turso::Value>)` data — no
// references to `V`. That keeps `V` lifetime-unconstrained, allowing
// `.values(&rows)` (borrowed `Vec`) to work without forcing `V: 'static`
// or running into the late-bound `'conn` collision that
// `V: 'conn` would cause.
impl<V, T, QId, Op, const STATIC_QUERY_ID: bool> ExecuteDsl<AsyncTursoConnection, TursoBackend>
    for (
        Yes,
        InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op>,
    )
where
    T: Table + Copy + QueryId + 'static,
    T::FromClause: QueryFragment<TursoBackend>,
    Op: Copy + QueryId + QueryFragment<TursoBackend>,
    V: InsertValues<TursoBackend, T> + CanInsertInSingleQuery<TursoBackend> + QueryId,
    for<'a> InsertStatement<T, &'a ValuesClause<V, T>, Op>: QueryFragment<TursoBackend>,
{
    fn execute<'conn, 'query>(
        (Yes, query): Self,
        conn: &'conn mut AsyncTursoConnection,
    ) -> <AsyncTursoConnection as AsyncConnectionCore>::ExecuteFuture<'conn, 'query>
    where
        Self: 'query,
    {
        // Emit (sql, binds) for each row up front, while V is still in
        // scope. After this, no `V` references survive into the future.
        let prepared: diesel::QueryResult<Vec<(String, Vec<turso::Value>)>> = query
            .records
            .values
            .iter()
            .map(|record| {
                let stmt =
                    InsertStatement::new(query.target, record, query.operator, query.returning);
                let mut qb = crate::query_builder::TursoQueryBuilder::default();
                <_ as QueryFragment<TursoBackend>>::to_sql(&stmt, &mut qb, &TursoBackend)?;
                let binds = crate::construct_bind_data(&stmt)?;
                Ok((qb.sql, binds))
            })
            .collect();

        Box::pin(async move {
            let prepared = prepared?;
            // Wrap the per-row inserts in a transaction so the batch is
            // statement-atomic — same failure semantics as a single
            // multi-row INSERT. Without this, a partial failure
            // (row 1 succeeds, row 2 errors) would leave row 1
            // committed. `AsyncConnection::transaction` issues a
            // `BEGIN`/`SAVEPOINT` automatically depending on nesting
            // depth and rolls back on any propagated error.
            <AsyncTursoConnection as diesel_async::AsyncConnection>::transaction::<
                usize,
                diesel::result::Error,
                _,
            >(conn, async move |conn| {
                let mut total = 0usize;
                for (sql, binds) in prepared {
                    let inner = conn.ensure_connection()?;
                    let mut prep = inner.prepare(&sql);
                    prep.bind(binds);
                    // Each per-row SQL is unique-ish (default columns
                    // vary) and we already paid the prepare cost above
                    // — no point populating turso's cache for these.
                    prep.set_cacheable(false);
                    let result = inner.execute(&prep).await.map_err(|e| {
                        diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::Unknown,
                            Box::new(crate::utils::TursoError {
                                message: e.to_string(),
                            }),
                        )
                    })?;
                    total += result.changes;
                }
                Ok(total)
            })
            .await
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

// `CanInsertInSingleQuery` impls for collection types. diesel core has
// these gated to `IsoSqlDefaultKeyword`; our backend uses
// `DoesNotSupportDefaultKeyword`, so the upstream impls don't apply and
// we provide local equivalents.
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
