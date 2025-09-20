use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};
use turso::Value;

use crate::{backend::TursoBackend, value::TursoValue};

pub struct TursoRow {
    values: Rc<RefCell<Vec<Value>>>,
    field_vec: Vec<String>,
}

// SAFETY: Turso values are thread-safe
unsafe impl Send for TursoRow {}
unsafe impl Sync for TursoRow {}

impl TursoRow {
    pub fn from_turso_values(values: Vec<Value>, field_vec: Vec<String>) -> Self {
        Self {
            values: Rc::new(RefCell::new(values)),
            field_vec,
        }
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
        self.field_vec.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'stmt: 'b,
        Self: diesel::row::RowIndex<I>,
    {
        let index = self.idx(idx)?;
        let name = self.field_vec.get(index)?;
        Some(TursoField {
            name: name.to_string(),
            values: self.values.borrow(),
            index,
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
        if idx < self.field_vec.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl RowIndex<&str> for TursoRow {
    fn idx(&self, field: &str) -> Option<usize> {
        self.field_vec.iter().position(|i| i == field)
    }
}

pub struct TursoField<'stmt> {
    values: Ref<'stmt, Vec<Value>>,
    name: String,
    index: usize,
}

impl<'stmt> Field<'stmt, TursoBackend> for TursoField<'stmt> {
    fn field_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn value(&self) -> Option<TursoValue> {
        let turso_value = self.values.get(self.index)?;
        match turso_value {
            Value::Null => None,
            _ => Some(TursoValue::from_turso_value(turso_value.clone())),
        }
    }
}
