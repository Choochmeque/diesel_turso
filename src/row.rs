use std::sync::Arc;

use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};
use turso::Value;

use crate::{backend::TursoBackend, value::TursoValue};

pub struct TursoRow {
    values: Vec<Value>,
    fields: Arc<[String]>,
}

impl TursoRow {
    pub const fn from_turso_values(values: Vec<Value>, fields: Arc<[String]>) -> Self {
        Self { values, fields }
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
