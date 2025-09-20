use diesel::expression::{AppearsOnTable, Expression};
use diesel::insertable::{ColumnInsertValue, DefaultableColumnInsertValue, InsertValues};
use diesel::query_builder::{AstPass, NoFromClause, QueryFragment};
use diesel::query_source::Column;
use diesel::result::QueryResult;

impl<Col, Expr> InsertValues<crate::backend::TursoBackend, Col::Table>
    for DefaultableColumnInsertValue<ColumnInsertValue<Col, Expr>>
where
    Col: Column,
    Expr: Expression<SqlType = Col::SqlType> + AppearsOnTable<NoFromClause>,
    Self: QueryFragment<crate::backend::TursoBackend>,
{
    fn column_names(
        &self,
        mut out: AstPass<'_, '_, crate::backend::TursoBackend>,
    ) -> QueryResult<()> {
        if let Self::Expression(..) = *self {
            out.push_identifier(Col::NAME)?;
        }
        Ok(())
    }
}

impl<Col, Expr>
    QueryFragment<
        crate::backend::TursoBackend,
        diesel::backend::sql_dialect::default_keyword_for_insert::DoesNotSupportDefaultKeyword,
    > for DefaultableColumnInsertValue<ColumnInsertValue<Col, Expr>>
where
    Expr: QueryFragment<crate::backend::TursoBackend>,
{
    fn walk_ast<'b>(
        &'b self,
        mut out: AstPass<'_, 'b, crate::backend::TursoBackend>,
    ) -> QueryResult<()> {
        if let Self::Expression(ref inner) = *self {
            inner.walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}
