use crate::backend::{SqliteReturningClause, TursoBackend};
use diesel::query_builder::ReturningClause;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;

impl<Expr> QueryFragment<TursoBackend, SqliteReturningClause> for ReturningClause<Expr>
where
    Expr: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        out.skip_from(true);
        out.push_sql(" RETURNING ");
        self.0.walk_ast(out.reborrow())?;
        Ok(())
    }
}
