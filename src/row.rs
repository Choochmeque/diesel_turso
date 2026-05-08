use std::fmt;
use std::sync::Arc;

use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};
use turso::Value;

use crate::{backend::TursoBackend, value::TursoValue};

pub struct TursoRow {
    values: Vec<Value>,
    fields: Arc<[String]>,
}

/// Reported when a row is constructed with a different number of values than
/// declared columns. The constructor enforces `values.len() == fields.len()`
/// so column lookups never have to fall back to `None` for "value missing
/// for declared column".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldCountMismatch {
    pub values: usize,
    pub fields: usize,
}

impl fmt::Display for FieldCountMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "row has {} values but the prepared statement declares {} columns",
            self.values, self.fields
        )
    }
}

impl std::error::Error for FieldCountMismatch {}

impl TursoRow {
    pub fn from_turso_values(
        values: Vec<Value>,
        fields: Arc<[String]>,
    ) -> Result<Self, FieldCountMismatch> {
        if values.len() != fields.len() {
            return Err(FieldCountMismatch {
                values: values.len(),
                fields: fields.len(),
            });
        }
        Ok(Self { values, fields })
    }
}

impl RowSealed for TursoRow {}

impl<'stmt> Row<'stmt, TursoBackend> for TursoRow {
    type Field<'f>
        = TursoField<'f>
    where
        'stmt: 'f,
        Self: 'f;

    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.fields.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'stmt: 'b,
        Self: diesel::row::RowIndex<I>,
    {
        let index = self.idx(idx)?;
        Some(TursoField {
            name: self.fields.get(index)?,
            value: self.values.get(index)?,
        })
    }

    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> diesel::row::PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for TursoRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.fields.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl RowIndex<&str> for TursoRow {
    fn idx(&self, field: &str) -> Option<usize> {
        self.fields.iter().position(|i| i == field)
    }
}

pub struct TursoField<'stmt> {
    name: &'stmt String,
    value: &'stmt Value,
}

impl<'stmt> Field<'stmt, TursoBackend> for TursoField<'stmt> {
    fn field_name(&self) -> Option<&str> {
        Some(self.name)
    }

    fn value(&self) -> Option<TursoValue> {
        match self.value {
            Value::Null => None,
            _ => Some(TursoValue::from_turso_value(self.value.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FieldCountMismatch, TursoRow};
    use std::sync::Arc;
    use turso::Value;

    fn fields(names: &[&str]) -> Arc<[String]> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn from_turso_values_accepts_matching_lengths() {
        let row = TursoRow::from_turso_values(
            vec![Value::Integer(1), Value::Text("a".into())],
            fields(&["id", "name"]),
        );
        assert!(row.is_ok());
    }

    #[test]
    fn from_turso_values_rejects_too_few_values() {
        let result = TursoRow::from_turso_values(vec![Value::Integer(1)], fields(&["id", "name"]));
        assert!(matches!(
            &result,
            Err(FieldCountMismatch {
                values: 1,
                fields: 2,
            }),
        ));
    }

    #[test]
    fn from_turso_values_rejects_too_many_values() {
        let result = TursoRow::from_turso_values(
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
            fields(&["only_one_column"]),
        );
        assert!(matches!(
            &result,
            Err(FieldCountMismatch {
                values: 3,
                fields: 1,
            }),
        ));
    }
}
