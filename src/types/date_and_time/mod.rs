use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, Output, ToSql},
    sql_types::{self},
};

use crate::{backend::TursoBackend, value::TursoValue};

#[cfg(feature = "chrono")]
mod chrono;

impl FromSql<sql_types::Date, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

impl ToSql<sql_types::Date, TursoBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        ToSql::<sql_types::Text, TursoBackend>::to_sql(self, out)
    }
}

impl ToSql<sql_types::Date, TursoBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        <str as ToSql<sql_types::Date, TursoBackend>>::to_sql(self as &str, out)
    }
}

impl FromSql<sql_types::Time, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

impl ToSql<sql_types::Time, TursoBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        ToSql::<sql_types::Text, TursoBackend>::to_sql(self, out)
    }
}

impl ToSql<sql_types::Time, TursoBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        <str as ToSql<sql_types::Time, TursoBackend>>::to_sql(self as &str, out)
    }
}

impl FromSql<sql_types::Timestamp, TursoBackend> for String {
    fn from_sql(value: TursoValue) -> deserialize::Result<Self> {
        FromSql::<sql_types::Text, TursoBackend>::from_sql(value)
    }
}

impl ToSql<sql_types::Timestamp, TursoBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        ToSql::<sql_types::Text, TursoBackend>::to_sql(self, out)
    }
}

impl ToSql<sql_types::Timestamp, TursoBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        <str as ToSql<sql_types::Timestamp, TursoBackend>>::to_sql(self as &str, out)
    }
}
