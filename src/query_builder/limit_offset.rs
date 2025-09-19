use diesel::query_builder::{AstPass, IntoBoxedClause, QueryFragment};
use diesel::query_builder::{BoxedLimitOffsetClause, LimitOffsetClause};
use diesel::query_builder::{LimitClause, NoLimitClause};
use diesel::query_builder::{NoOffsetClause, OffsetClause};
use diesel::result::QueryResult;

use crate::backend::TursoBackend;

impl QueryFragment<TursoBackend> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    fn walk_ast<'b>(&'b self, _out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        Ok(())
    }
}

impl<L> QueryFragment<TursoBackend> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    LimitClause<L>: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<O> QueryFragment<TursoBackend> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    OffsetClause<O>: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        // Sqlite requires a limit clause in front of any offset clause
        // using `LIMIT -1` is the same as not having any limit clause
        // https://Sqlite.org/lang_select.html
        out.push_sql(" LIMIT -1 ");
        self.offset_clause.walk_ast(out)?;
        Ok(())
    }
}

impl<L, O> QueryFragment<TursoBackend> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    LimitClause<L>: QueryFragment<TursoBackend>,
    OffsetClause<O>: QueryFragment<TursoBackend>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        self.limit_clause.walk_ast(out.reborrow())?;
        self.offset_clause.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl QueryFragment<TursoBackend> for BoxedLimitOffsetClause<'_, TursoBackend> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, TursoBackend>) -> QueryResult<()> {
        match (self.limit.as_ref(), self.offset.as_ref()) {
            (Some(limit), Some(offset)) => {
                limit.walk_ast(out.reborrow())?;
                offset.walk_ast(out.reborrow())?;
            }
            (Some(limit), None) => {
                limit.walk_ast(out.reborrow())?;
            }
            (None, Some(offset)) => {
                // See the `QueryFragment` implementation for `LimitOffsetClause` for details.
                out.push_sql(" LIMIT -1 ");
                offset.walk_ast(out.reborrow())?;
            }
            (None, None) => {}
        }
        Ok(())
    }
}

// Have explicit impls here because we need to set `Some`/`None` for the clauses
// correspondingly, otherwise we cannot match on it in the `QueryFragment` impl
// above
impl<'a> IntoBoxedClause<'a, TursoBackend> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, TursoBackend>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: None,
        }
    }
}

impl<'a, L> IntoBoxedClause<'a, TursoBackend> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<TursoBackend> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, TursoBackend>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: None,
        }
    }
}

impl<'a, O> IntoBoxedClause<'a, TursoBackend> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<TursoBackend> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, TursoBackend>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, TursoBackend>
    for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<TursoBackend> + Send + 'a,
    O: QueryFragment<TursoBackend> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, TursoBackend>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}
