use diesel::{
    backend::{
        sql_dialect::{self, returning_clause::DoesNotSupportReturningClause},
        Backend, DieselReserveSpecialization, SqlDialect, TrustedBackend,
    },
    sql_types::TypeMetadata,
};

use crate::{
    bind_collector::TursoBindCollector, query_builder::TursoQueryBuilder, value::TursoValue,
};

/// The SQLite backend
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Default)]
pub struct TursoBackend;

/// Determines how a bind parameter is given to SQLite
///
/// Diesel deals with bind parameters after serialization as opaque blobs of
/// bytes. However, SQLite instead has several functions where it expects the
/// relevant C types.
///
/// The variants of this struct determine what bytes are expected from
/// `ToSql` impls.
#[allow(missing_debug_implementations)]
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
// sqlite types
pub enum TursoType {
    Binary,
    Text,
    Double,
    Integer,
}

impl Backend for TursoBackend {
    type QueryBuilder = TursoQueryBuilder;
    type RawValue<'a> = TursoValue;
    type BindCollector<'a> = TursoBindCollector;
}

impl TypeMetadata for TursoBackend {
    type TypeMetadata = TursoType;
    type MetadataLookup = ();
}

impl SqlDialect for TursoBackend {
    // this is actually not true, but i would need to properly implement the ast for this, since the sqlite one is not exported
    type ReturningClause = DoesNotSupportReturningClause;

    type OnConflictClause = SqliteOnConflictClause;

    type InsertWithDefaultKeyword =
        sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;
    type BatchInsertSupport = SqliteBatchInsert;
    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;

    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;
    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;

    type WindowFrameClauseGroupSupport =
        sql_dialect::window_frame_clause_group_support::IsoGroupWindowFrameUnit;
    type WindowFrameExclusionSupport =
        sql_dialect::window_frame_exclusion_support::FrameExclusionSupport;
    type AggregateFunctionExpressions =
        sql_dialect::aggregate_function_expressions::PostgresLikeAggregateFunctionExpressions;
    type BuiltInWindowFunctionRequireOrder =
        sql_dialect::built_in_window_function_require_order::NoOrderRequired;
}

impl DieselReserveSpecialization for TursoBackend {}
impl TrustedBackend for TursoBackend {}

#[derive(Debug, Copy, Clone)]
pub struct SqliteOnConflictClause;

impl sql_dialect::on_conflict_clause::SupportsOnConflictClause for SqliteOnConflictClause {}
impl sql_dialect::on_conflict_clause::PgLikeOnConflictClause for SqliteOnConflictClause {}

#[derive(Debug, Copy, Clone)]
pub struct SqliteBatchInsert;

#[derive(Debug, Copy, Clone)]
pub struct SqliteReturningClause;

impl sql_dialect::returning_clause::SupportsReturningClause for SqliteReturningClause {}
