use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{self, HasSqlType},
};

use crate::{
    backend::{TursoBackend, TursoType},
    value::TursoValue,
};

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
        out.set_value(*self);
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
        Ok(value.read_number() as i16)
    }
}

impl ToSql<sql_types::SmallInt, TursoBackend> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self as f64);
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
        Ok(value.read_number() as i32)
    }
}

impl ToSql<sql_types::Integer, TursoBackend> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self as i64);
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
        Ok(value.read_number() as i64)
    }
}

impl ToSql<sql_types::BigInt, TursoBackend> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self as f64);
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
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_number() as f32)
    }
}

impl ToSql<sql_types::Float, TursoBackend> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(*self as f64);
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

impl FromSql<sql_types::Text, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        Ok(value.read_string())
    }
}

// impl ToSql<sql_types::Text, TursoBackend> for String {
//     fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
//         out.set_value(self.clone());
//         Ok(IsNull::No)
//     }
// }

impl FromSql<sql_types::Binary, TursoBackend> for *const [u8] {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        let bytes = value.read_blob();
        Ok(bytes.as_slice() as *const _)
    }
}

impl ToSql<sql_types::Binary, TursoBackend> for [u8] {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self);
        Ok(IsNull::No)
    }
}

// impl FromSql<sql_types::Text, TursoBackend> for &str {
//     fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
//         let text = value.read_string();
//         Ok(text.as_ref())
//     }
// }

impl ToSql<sql_types::Text, TursoBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.to_string());
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

impl FromSql<sql_types::Date, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Date, TursoBackend> for chrono::NaiveDate {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        let text = value.read_string();
        let parsed = chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d")?;
        Ok(parsed)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Timestamp, TursoBackend> for chrono::NaiveDateTime {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        let text = value.read_string();
        let parsed = chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f"))?;
        Ok(parsed)
    }
}

impl ToSql<sql_types::Date, TursoBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        ToSql::<sql_types::Text, TursoBackend>::to_sql(self, out)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Date, TursoBackend> for chrono::NaiveDate {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format("%Y-%m-%d").to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Time, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Time, TursoBackend> for chrono::NaiveTime {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        let text = value.read_string();
        let parsed = chrono::NaiveTime::parse_from_str(&text, "%H:%M:%S")
            .or_else(|_| chrono::NaiveTime::parse_from_str(&text, "%H:%M:%S%.f"))?;
        Ok(parsed)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Time, TursoBackend> for chrono::NaiveTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format("%H:%M:%S").to_string());
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Timestamp, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Timestamp, TursoBackend> for chrono::NaiveDateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format("%Y-%m-%d %H:%M:%S").to_string());
        Ok(IsNull::No)
    }
}

impl ToSql<sql_types::Timestamp, TursoBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        ToSql::<sql_types::Text, TursoBackend>::to_sql(self, out)
    }
}
