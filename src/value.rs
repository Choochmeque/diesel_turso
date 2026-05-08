use diesel::deserialize;
use turso::Value;

#[derive(Debug)]
pub struct TursoValue {
    value: Value,
}

impl From<bool> for TursoValue {
    fn from(value: bool) -> Self {
        Self::from_turso_value(Value::Integer(i64::from(value)))
    }
}

impl From<f64> for TursoValue {
    fn from(value: f64) -> Self {
        Self::from_turso_value(Value::Real(value))
    }
}

impl From<i64> for TursoValue {
    fn from(value: i64) -> Self {
        Self::from_turso_value(Value::Integer(value))
    }
}

impl From<String> for TursoValue {
    fn from(value: String) -> Self {
        Self::from_turso_value(Value::Text(value))
    }
}

impl From<i16> for TursoValue {
    fn from(value: i16) -> Self {
        Self::from_turso_value(Value::Integer(i64::from(value)))
    }
}

impl From<i32> for TursoValue {
    fn from(value: i32) -> Self {
        Self::from_turso_value(Value::Integer(i64::from(value)))
    }
}

impl From<f32> for TursoValue {
    fn from(value: f32) -> Self {
        Self::from_turso_value(Value::Real(f64::from(value)))
    }
}

impl From<Vec<u8>> for TursoValue {
    fn from(value: Vec<u8>) -> Self {
        Self::from_turso_value(Value::Blob(value))
    }
}

impl From<()> for TursoValue {
    fn from(_value: ()) -> Self {
        Self::from_turso_value(Value::Null)
    }
}

impl From<&[u8]> for TursoValue {
    fn from(value: &[u8]) -> Self {
        Self::from_turso_value(Value::Blob(value.to_vec()))
    }
}

impl TursoValue {
    pub const fn from_turso_value(value: Value) -> Self {
        Self { value }
    }

    pub fn to_turso_value(&self) -> Value {
        self.value.clone()
    }

    pub(crate) fn read_string(&self) -> deserialize::Result<String> {
        match &self.value {
            Value::Text(s) => Ok(s.clone()),
            other => Err(format!("expected text value, got {other:?}").into()),
        }
    }

    pub(crate) fn read_bool(&self) -> deserialize::Result<bool> {
        match &self.value {
            Value::Integer(i) => Ok(*i != 0),
            other => Err(format!("expected boolean (integer) value, got {other:?}").into()),
        }
    }

    pub(crate) fn read_int(&self) -> deserialize::Result<i64> {
        match &self.value {
            Value::Integer(i) => Ok(*i),
            other => Err(format!("expected integer value, got {other:?}").into()),
        }
    }

    /// Returns float value. Integer values are widened to `f64` (lossy beyond 2^53).
    pub(crate) fn read_number(&self) -> deserialize::Result<f64> {
        match &self.value {
            Value::Real(f) => Ok(*f),
            #[allow(clippy::cast_precision_loss)]
            Value::Integer(i) => Ok(*i as f64),
            other => Err(format!("expected numeric value, got {other:?}").into()),
        }
    }

    pub(crate) fn read_blob(&self) -> deserialize::Result<Vec<u8>> {
        match &self.value {
            Value::Blob(b) => Ok(b.clone()),
            other => Err(format!("expected blob value, got {other:?}").into()),
        }
    }

    #[cfg(feature = "chrono")]
    pub(crate) fn parse_string<R>(
        &self,
        f: impl FnOnce(&str) -> deserialize::Result<R>,
    ) -> deserialize::Result<R> {
        match &self.value {
            Value::Text(s) => f(s),
            other => Err(format!("expected text value, got {other:?}").into()),
        }
    }

    pub const fn is_null(&self) -> bool {
        matches!(self.value, Value::Null)
    }
}
