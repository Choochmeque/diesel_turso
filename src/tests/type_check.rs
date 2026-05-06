use super::connection;
use super::AsyncTursoConnection;
use diesel::deserialize::FromSqlRow;
use diesel::expression::{AsExpression, ValidGrouping};
use diesel::prelude::*;
use diesel::query_builder::{NoFromClause, QueryFragment, QueryId};
use diesel::sql_types::{self, HasSqlType, SingleValue};
use diesel_async::AsyncConnectionCore;
use diesel_async::RunQueryDsl;
use std::fmt::Debug;

async fn type_check<T, ST>(conn: &mut AsyncTursoConnection, value: T)
where
    T: Clone
        + AsExpression<ST>
        + FromSqlRow<ST, <AsyncTursoConnection as AsyncConnectionCore>::Backend>
        + Send
        + PartialEq
        + Debug
        + 'static,
    T::Expression: ValidGrouping<()>
        + SelectableExpression<NoFromClause>
        + QueryFragment<<AsyncTursoConnection as AsyncConnectionCore>::Backend>
        + QueryId
        + Send,
    <AsyncTursoConnection as AsyncConnectionCore>::Backend: HasSqlType<ST>,
    ST: SingleValue,
{
    let res = diesel::select(value.clone().into_sql())
        .get_result::<T>(conn)
        .await;

    assert_eq!(Ok(value), res);
}

#[tokio::test]
async fn check_small_int() {
    let mut conn = connection().await;
    type_check::<_, sql_types::SmallInt>(&mut conn, 1_i16).await;
    type_check::<_, sql_types::SmallInt>(&mut conn, 1_i16).await;
    type_check::<_, sql_types::SmallInt>(&mut conn, i16::MIN).await;
    type_check::<_, sql_types::SmallInt>(&mut conn, i16::MAX).await;
    drop(conn);
}

#[tokio::test]
async fn check_int() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Integer>(&mut conn, 1_i32).await;
    type_check::<_, sql_types::Integer>(&mut conn, -1_i32).await;
    type_check::<_, sql_types::Integer>(&mut conn, i32::MIN).await;
    type_check::<_, sql_types::Integer>(&mut conn, i32::MAX).await;
    drop(conn);
}

#[tokio::test]
async fn check_big_int() {
    let mut conn = connection().await;
    type_check::<_, sql_types::BigInt>(&mut conn, 1_i64).await;
    type_check::<_, sql_types::BigInt>(&mut conn, -1_i64).await;
    type_check::<_, sql_types::BigInt>(&mut conn, i64::MIN).await;
    type_check::<_, sql_types::BigInt>(&mut conn, i64::MAX).await;
    drop(conn);
}

#[tokio::test]
async fn check_bool() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Bool>(&mut conn, false).await;
    type_check::<_, sql_types::Bool>(&mut conn, false).await;
    drop(conn);
}

#[tokio::test]
async fn check_f32() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Float4>(&mut conn, 1.0_f32).await;
    type_check::<_, sql_types::Float4>(&mut conn, f32::MIN_POSITIVE).await;
    type_check::<_, sql_types::Float4>(&mut conn, f32::MIN).await;
    type_check::<_, sql_types::Float4>(&mut conn, f32::MAX).await;
    drop(conn);
}

#[tokio::test]
async fn check_f64() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Float8>(&mut conn, 1.0_f64).await;
    type_check::<_, sql_types::Float8>(&mut conn, f64::MIN_POSITIVE).await;
    type_check::<_, sql_types::Float8>(&mut conn, f64::MIN).await;
    type_check::<_, sql_types::Float8>(&mut conn, f64::MAX).await;
    drop(conn);
}

#[tokio::test]
async fn check_string() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Text>(&mut conn, String::from("Test")).await;
    type_check::<_, sql_types::Text>(&mut conn, String::new()).await;
    type_check::<_, sql_types::Text>(&mut conn, String::from("üöä")).await;
    drop(conn);
}

#[tokio::test]
async fn check_option() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Nullable<sql_types::Integer>>(&mut conn, Some(42)).await;
    type_check::<_, sql_types::Nullable<sql_types::Integer>>(&mut conn, None::<i32>).await;

    type_check::<_, sql_types::Nullable<sql_types::Text>>(&mut conn, Some(String::new())).await;
    type_check::<_, sql_types::Nullable<sql_types::Text>>(&mut conn, None::<String>).await;
    drop(conn);
}

#[tokio::test]
async fn test_blob() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Blob>(&mut conn, b"foo".to_vec()).await;
    type_check::<_, sql_types::Blob>(&mut conn, Vec::new()).await;
    drop(conn);
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_timestamp() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Timestamp>(
        &mut conn,
        chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2021, 9, 27).expect("valid date 2021-09-27"),
            chrono::NaiveTime::from_hms_milli_opt(17, 44, 23, 0).expect("valid time 17:44:23"),
        ),
    )
    .await;
    drop(conn);
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_date() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Date>(
        &mut conn,
        chrono::NaiveDate::from_ymd_opt(2021, 9, 27).expect("valid date 2021-09-27"),
    )
    .await;
    drop(conn);
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn test_time() {
    let mut conn = connection().await;
    type_check::<_, sql_types::Time>(
        &mut conn,
        chrono::NaiveTime::from_hms_milli_opt(17, 44, 23, 0).expect("valid time 17:44:23"),
    )
    .await;
    drop(conn);
}
