use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{self, HasSqlType},
};

use crate::{
    backend::{TursoBackend, TursoType},
    value::TursoValue,
};

mod date_and_time;

// VarChar is just an alias for Text in diesel, so we only need Text implementations

// Boolean
impl HasSqlType<sql_types::Bool> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Integer
    }
}

impl FromSql<sql_types::Bool, TursoBackend> for bool {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_bool())
    }
}

impl ToSql<sql_types::Bool, TursoBackend> for bool {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(i64::from(*self));
        Ok(IsNull::No)
    }
}

// SMALL INT

impl HasSqlType<sql_types::SmallInt> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Integer
    }
}

impl FromSql<sql_types::SmallInt, TursoBackend> for i16 {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Self::try_from(value.read_int()).map_err(Into::into)
    }
}

impl ToSql<sql_types::SmallInt, TursoBackend> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(i32::from(*self));
        Ok(IsNull::No)
    }
}

// ------

// Int

impl HasSqlType<sql_types::Integer> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Integer
    }
}

impl FromSql<sql_types::Integer, TursoBackend> for i32 {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Self::try_from(value.read_int()).map_err(Into::into)
    }
}

impl ToSql<sql_types::Integer, TursoBackend> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(i64::from(*self));
        Ok(IsNull::No)
    }
}

// ------

// BigInt

impl HasSqlType<sql_types::BigInt> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Integer
    }
}

impl FromSql<sql_types::BigInt, TursoBackend> for i64 {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_int())
    }
}

impl ToSql<sql_types::BigInt, TursoBackend> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self);
        Ok(IsNull::No)
    }
}

// ------

// Float

impl HasSqlType<sql_types::Float> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Double
    }
}

impl FromSql<sql_types::Float, TursoBackend> for f32 {
    #[allow(clippy::cast_possible_truncation)]
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_number() as Self)
    }
}

impl ToSql<sql_types::Float, TursoBackend> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(f64::from(*self));
        Ok(IsNull::No)
    }
}

// ------

// Double

impl HasSqlType<sql_types::Double> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Double
    }
}

impl FromSql<sql_types::Double, TursoBackend> for f64 {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_number())
    }
}

impl ToSql<sql_types::Double, TursoBackend> for f64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self);
        Ok(IsNull::No)
    }
}

// ------

// Text
impl HasSqlType<sql_types::Text> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Text
    }
}

impl ToSql<sql_types::Text, TursoBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_string())
    }
}

impl FromSql<sql_types::Binary, TursoBackend> for Vec<u8> {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_blob())
    }
}

impl ToSql<sql_types::Binary, TursoBackend> for [u8] {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.to_vec());
        Ok(IsNull::No)
    }
}

// ------

// Blob

impl HasSqlType<sql_types::Binary> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Binary
    }
}

// ------ Time related (simplified to only text)

impl HasSqlType<sql_types::Date> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Text
    }
}

impl HasSqlType<sql_types::Time> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Text
    }
}

impl HasSqlType<sql_types::Timestamp> for TursoBackend {
    fn metadata(_lookup: &mut ()) -> TursoType {
        TursoType::Text
    }
}
