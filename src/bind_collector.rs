use diesel::{
    query_builder::BindCollector,
    serialize::{IsNull, Output},
    sql_types::HasSqlType,
};
use turso::Value;

use crate::backend::{TursoBackend, TursoType};
use crate::value::TursoValue;

#[derive(Default)]
pub struct TursoBindCollector {
    pub binds: Vec<(TursoValue, TursoType)>,
}

impl<'bind> BindCollector<'bind, TursoBackend> for TursoBindCollector {
    type Buffer = TursoValue;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'bind U,
        metadata_lookup: &mut <TursoBackend as diesel::sql_types::TypeMetadata>::MetadataLookup,
    ) -> diesel::QueryResult<()>
    where
        TursoBackend: diesel::backend::Backend + diesel::sql_types::HasSqlType<T>,
        U: diesel::serialize::ToSql<T, TursoBackend> + ?Sized + 'bind,
    {
        let value = TursoValue::from_turso_value(Value::Null); // start out with null
        let mut to_sql_output = Output::new(value, metadata_lookup);
        let is_null = bind
            .to_sql(&mut to_sql_output)
            .map_err(diesel::result::Error::SerializationError)?;

        let bind = if matches!(is_null, IsNull::No) {
            to_sql_output.into_inner()
        } else {
            TursoValue::from_turso_value(Value::Null)
        };

        let metadata = <TursoBackend as HasSqlType<T>>::metadata(metadata_lookup);
        self.binds.push((bind, metadata));
        Ok(())
    }
}
