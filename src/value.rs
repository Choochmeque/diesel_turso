use turso::Value;

#[derive(Debug)]
pub struct TursoValue {
    value: Value,
}

impl From<bool> for TursoValue {
    fn from(value: bool) -> Self {
        Self::from_turso_value(Value::Integer(if value { 1 } else { 0 }))
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
        Self::from_turso_value(Value::Integer(value as i64))
    }
}

impl From<i32> for TursoValue {
    fn from(value: i32) -> Self {
        Self::from_turso_value(Value::Integer(value as i64))
    }
}

impl From<f32> for TursoValue {
    fn from(value: f32) -> Self {
        Self::from_turso_value(Value::Real(value as f64))
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
    pub fn from_turso_value(value: Value) -> Self {
        Self { value }
    }

    pub fn to_turso_value(&self) -> Value {
        self.value.clone()
    }

    pub(crate) fn read_string(&self) -> String {
        match &self.value {
            Value::Text(s) => s.clone(),
            _ => panic!("Value is not a string, but {:?}", self.value),
        }
    }

    pub(crate) fn read_bool(&self) -> bool {
        match &self.value {
            Value::Integer(i) => *i != 0,
            _ => panic!("Value is not a bool, but {:?}", self.value),
        }
    }

    /// Returns float value
    pub(crate) fn read_number(&self) -> f64 {
        match &self.value {
            Value::Real(f) => *f,
            Value::Integer(i) => *i as f64,
            _ => panic!("Value is not a number, but {:?}", self.value),
        }
    }

    pub(crate) fn read_blob(&self) -> Vec<u8> {
        match &self.value {
            Value::Blob(b) => b.clone(),
            _ => panic!("Value is not a blob, but {:?}", self.value),
        }
    }

    pub(crate) fn parse_string<R>(&self, f: impl FnOnce(&str) -> R) -> R {
        match &self.value {
            Value::Text(s) => f(s),
            _ => panic!("Value is not a string, but {:?}", self.value),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value, Value::Null)
    }
}
