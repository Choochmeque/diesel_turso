extern crate chrono;

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use turso::Value;

use self::chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::backend::TursoBackend;

const DATE_FORMAT: &str = "%F";

const ENCODE_TIME_FORMAT: &str = "%T%.f";

const TIME_FORMATS: [&str; 9] = [
    // Most likely formats
    "%T%.f", "%T", // All other valid formats in order of increasing specificity
    "%R", "%RZ", "%R%:z", "%TZ", "%T%:z", "%T%.fZ", "%T%.f%:z",
];

const ENCODE_NAIVE_DATETIME_FORMAT: &str = "%F %T%.f";

const NAIVE_DATETIME_FORMATS: [&str; 18] = [
    // Most likely formats
    "%F %T%.f",
    "%F %T%.f%:z",
    "%F %T",
    "%F %T%:z",
    // All other formats in order of increasing specificity
    "%F %R",
    "%F %RZ",
    "%F %R%:z",
    "%F %TZ",
    "%F %T%.fZ",
    "%FT%R",
    "%FT%RZ",
    "%FT%R%:z",
    "%FT%T",
    "%FT%TZ",
    "%FT%T%:z",
    "%FT%T%.f",
    "%FT%T%.fZ",
    "%FT%T%.f%:z",
];

fn parse_julian(julian_days: f64) -> Option<NaiveDateTime> {
    const EPOCH_IN_JULIAN_DAYS: f64 = 2_440_587.5;
    const SECONDS_IN_DAY: f64 = 86400.0;
    const NANOS_PER_SEC: u32 = 1_000_000_000;
    let timestamp = (julian_days - EPOCH_IN_JULIAN_DAYS) * SECONDS_IN_DAY;

    // `NaiveDateTime::from_timestamp_opt(secs, nanos)` requires `nanos`
    // to be a forward offset in `[0, 1_000_000_000)` from `secs`, even
    // when `secs` is negative. `f64::trunc` + `f64::fract` round toward
    // zero and so produce a *negative* fractional part for pre-epoch
    // timestamps (e.g. `-1.5` → `trunc=-1, fract=-0.5`), which then
    // becomes `0` after `as u32` and yields the wrong instant.
    //
    // Use `floor` instead so the fractional remainder is always in
    // `[0, 1)`, giving the canonical (`-2`, `500_000_000`) split for the
    // same `-1.5s` case.
    let secs_floor = timestamp.floor();
    #[allow(clippy::cast_possible_truncation)]
    let mut seconds = secs_floor as i64;
    let frac = timestamp - secs_floor;
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let mut nanos = (frac * f64::from(NANOS_PER_SEC)).round() as u32;
    // Guard against rounding the fractional part up to a full second
    // (e.g. `0.9999999999999999 * 1e9` → `1_000_000_000`).
    if nanos >= NANOS_PER_SEC {
        seconds = seconds.saturating_add(1);
        nanos -= NANOS_PER_SEC;
    }

    #[allow(deprecated)] // otherwise we would need to bump our minimal chrono version
    NaiveDateTime::from_timestamp_opt(seconds, nanos)
}

/// Decode a `NaiveDateTime` from any of the three `SQLite`/turso storage
/// classes for date/time values:
///
/// - `TEXT`: ISO-8601 string in any of the formats listed in
///   [`NAIVE_DATETIME_FORMATS`], or a Julian-day number serialised as text
///   (this last form handles `julianday()` results that landed as TEXT).
/// - `REAL`: a Julian-day number (matches `SQLite`'s `julianday()`).
/// - `INTEGER`: a Unix timestamp in seconds (matches `SQLite`'s
///   `unixepoch()` and `strftime('%s', …)`).
///
/// Used as the dispatch core for the `Date`, `Time`, and `Timestamp`
/// `FromSql` impls — the date/time impls slice off the appropriate
/// component of the resulting `NaiveDateTime`.
fn decode_naive_datetime(value: &turso::Value) -> deserialize::Result<NaiveDateTime> {
    match value {
        Value::Text(text) => {
            for format in NAIVE_DATETIME_FORMATS {
                if let Ok(dt) = NaiveDateTime::parse_from_str(text, format) {
                    return Ok(dt);
                }
            }
            if let Ok(julian_days) = text.parse::<f64>() {
                if let Some(dt) = parse_julian(julian_days) {
                    return Ok(dt);
                }
            }
            Err(format!("Invalid datetime {text}").into())
        }
        Value::Real(julian_days) => parse_julian(*julian_days)
            .ok_or_else(|| format!("Invalid Julian day {julian_days}").into()),
        Value::Integer(unix_seconds) => {
            #[allow(deprecated)] // would need a higher chrono MSRV otherwise
            NaiveDateTime::from_timestamp_opt(*unix_seconds, 0)
                .ok_or_else(|| format!("Invalid Unix timestamp {unix_seconds}").into())
        }
        other => Err(format!("expected datetime value, got {other:?}").into()),
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Date, TursoBackend> for NaiveDate {
    fn from_sql(value: <TursoBackend as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        match value.raw() {
            Value::Text(text) => Self::parse_from_str(text, DATE_FORMAT).map_err(Into::into),
            Value::Real(_) | Value::Integer(_) => Ok(decode_naive_datetime(value.raw())?.date()),
            other => Err(format!("expected date value, got {other:?}").into()),
        }
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Date, TursoBackend> for NaiveDate {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format(DATE_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Time, TursoBackend> for NaiveTime {
    fn from_sql(value: <TursoBackend as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        match value.raw() {
            Value::Text(text) => {
                for format in TIME_FORMATS {
                    if let Ok(time) = Self::parse_from_str(text, format) {
                        return Ok(time);
                    }
                }
                Err(format!("Invalid time {text}").into())
            }
            Value::Real(_) | Value::Integer(_) => Ok(decode_naive_datetime(value.raw())?.time()),
            other => Err(format!("expected time value, got {other:?}").into()),
        }
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Time, TursoBackend> for NaiveTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format(ENCODE_TIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Timestamp, TursoBackend> for NaiveDateTime {
    fn from_sql(value: <TursoBackend as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        decode_naive_datetime(value.raw())
    }
}

#[cfg(feature = "chrono")]
impl ToSql<sql_types::Timestamp, TursoBackend> for NaiveDateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, TursoBackend>) -> serialize::Result {
        out.set_value(self.format(ENCODE_NAIVE_DATETIME_FORMAT).to_string());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    extern crate chrono;

    use self::chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

    use diesel::dsl::{now, sql};
    //use diesel::prelude::*;
    use crate::tests::connection;
    use diesel::sql_types::{Text, Time, Timestamp};
    use diesel::{declare_sql_function, select, ExpressionMethods};
    use diesel_async::*;

    #[declare_sql_function]
    extern "SQL" {
        fn datetime(x: Text) -> Timestamp;
        fn time(x: Text) -> Time;
        fn date(x: Text) -> Date;
    }

    #[test]
    fn parse_julian_round_trips_pre_and_post_epoch() {
        // Unix epoch — anchor case.
        let epoch = super::parse_julian(2_440_587.5).expect("epoch decodes");
        assert_eq!(epoch.and_utc().timestamp(), 0);
        assert_eq!(epoch.and_utc().timestamp_subsec_nanos(), 0);

        // Pre-epoch: Julian day 0.0 ≈ -210866760000s = 4714 BC. Use a
        // gentler pre-epoch value: 1900-01-01 = JD 2_415_020.5.
        let pre_epoch = super::parse_julian(2_415_020.5).expect("1900-01-01 decodes");
        let expected = NaiveDate::from_ymd_opt(1900, 1, 1)
            .expect("date constructible")
            .and_hms_opt(0, 0, 0)
            .expect("time constructible");
        assert_eq!(pre_epoch, expected);

        // Pre-epoch with a sub-second component. JD with `.25` extra =
        // +0.25 days = +6h. So 1900-01-01 06:00:00.
        let pre_epoch_frac = super::parse_julian(2_415_020.75).expect("decodes");
        let expected_frac = NaiveDate::from_ymd_opt(1900, 1, 1)
            .expect("date constructible")
            .and_hms_opt(6, 0, 0)
            .expect("time constructible");
        assert_eq!(pre_epoch_frac, expected_frac);

        // Pre-epoch with a fractional second that exercises the
        // negative-`fract`+`as u32` saturation bug specifically:
        // JD = epoch − 0.5 / 86400 days ≈ 0.5s before epoch.
        // (The exact half-second can't be represented as `f64` Julian-day
        // arithmetic, so we test the structural invariants instead of an
        // exact nanosecond match.)
        let half_sec_before_epoch =
            super::parse_julian(2_440_587.5 - 0.5 / 86_400.0).expect("0.5s before epoch decodes");
        assert_eq!(half_sec_before_epoch.and_utc().timestamp(), -1);
        let nanos = half_sec_before_epoch.and_utc().timestamp_subsec_nanos();
        // Pre-fix this would have been 0 (negative `fract` × 1e9 cast to
        // `u32` saturates to 0). Post-fix it's the canonical positive
        // forward-from-floor remainder, somewhere near 5e8 ns for a
        // ~0.5s offset.
        assert!(
            (400_000_000..1_000_000_000).contains(&nanos),
            "expected positive forward-from-floor nanos near 5e8 for ~0.5s \
             before epoch; got {nanos} (pre-fix the bug produced 0)",
        );
    }

    // SQLite/turso return `julianday(…)` results as REAL and bare
    // numeric literals (`SELECT 2440587.5`) as REAL. Decoding must
    // dispatch on storage class, not just TEXT.
    #[tokio::test]
    async fn decodes_julian_day_stored_as_real() {
        let mut connection = connection().await;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("1970-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        // Unquoted → REAL storage class (vs `'2440587.5'` which is TEXT).
        let query = select(sql::<Timestamp>("2440587.5"));
        assert_eq!(
            Ok(epoch),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );
    }

    // SQLite/turso return `unixepoch(…)` results as INTEGER and bare
    // integer literals (`SELECT 0`) as INTEGER. Decoding must dispatch
    // on storage class, not just TEXT.
    #[tokio::test]
    async fn decodes_unix_seconds_stored_as_integer() {
        let mut connection = connection().await;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("1970-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let query = select(sql::<Timestamp>("0"));
        assert_eq!(
            Ok(epoch),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );

        // Non-zero Unix timestamp: 2000-01-01 00:00:00 UTC = 946684800.
        let y2k = NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("2000-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let query = select(sql::<Timestamp>("946684800"));
        assert_eq!(
            Ok(y2k),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );
    }

    #[tokio::test]
    async fn unix_epoch_encodes_correctly() {
        let mut connection = connection().await;
        let time = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("1970-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let query = select(datetime("1970-01-01 00:00:00.000000").eq(time));
        assert_eq!(Ok(true), query.get_result(&mut connection).await);
    }

    #[tokio::test]
    async fn unix_epoch_decodes_correctly_in_all_possible_formats() {
        let mut connection = connection().await;
        let time = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("1970-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let valid_epoch_formats = vec![
            "1970-01-01 00:00",
            "1970-01-01 00:00:00",
            "1970-01-01 00:00:00.000",
            "1970-01-01 00:00:00.000000",
            "1970-01-01T00:00",
            "1970-01-01T00:00:00",
            "1970-01-01T00:00:00.000",
            "1970-01-01T00:00:00.000000",
            "1970-01-01 00:00Z",
            "1970-01-01 00:00:00Z",
            "1970-01-01 00:00:00.000Z",
            "1970-01-01 00:00:00.000000Z",
            "1970-01-01T00:00Z",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000Z",
            "1970-01-01T00:00:00.000000Z",
            "1970-01-01 00:00+00:00",
            "1970-01-01 00:00:00+00:00",
            "1970-01-01 00:00:00.000+00:00",
            "1970-01-01 00:00:00.000000+00:00",
            "1970-01-01T00:00+00:00",
            "1970-01-01T00:00:00+00:00",
            "1970-01-01T00:00:00.000+00:00",
            "1970-01-01T00:00:00.000000+00:00",
            "1970-01-01 00:00+01:00",
            "1970-01-01 00:00:00+01:00",
            "1970-01-01 00:00:00.000+01:00",
            "1970-01-01 00:00:00.000000+01:00",
            "1970-01-01T00:00+01:00",
            "1970-01-01T00:00:00+01:00",
            "1970-01-01T00:00:00.000+01:00",
            "1970-01-01T00:00:00.000000+01:00",
            "1970-01-01T00:00-01:00",
            "1970-01-01T00:00:00-01:00",
            "1970-01-01T00:00:00.000-01:00",
            "1970-01-01T00:00:00.000000-01:00",
            "1970-01-01T00:00-01:00",
            "1970-01-01T00:00:00-01:00",
            "1970-01-01T00:00:00.000-01:00",
            "1970-01-01T00:00:00.000000-01:00",
            "2440587.5",
        ];

        for s in valid_epoch_formats {
            let epoch_from_sql = select(sql::<Timestamp>(&format!("'{s}'")))
                .get_result(&mut connection)
                .await;
            assert_eq!(Ok(time), epoch_from_sql, "format {s} failed");
        }
    }

    #[tokio::test]
    async fn times_relative_to_now_encode_correctly() {
        let mut connection = connection().await;
        let time = Utc::now().naive_utc()
            + Duration::try_seconds(60).expect("60 seconds fits in Duration");
        let query = select(now.lt(time));
        assert_eq!(Ok(true), query.get_result(&mut connection).await);

        let time = Utc::now().naive_utc()
            - Duration::try_seconds(600).expect("600 seconds fits in Duration");
        let query = select(now.gt(time));
        assert_eq!(Ok(true), query.get_result(&mut connection).await);
    }

    #[tokio::test]
    async fn times_of_day_encode_correctly() {
        let mut connection = connection().await;

        let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("00:00:00 is a valid time");
        let query = select(time("00:00:00.000000").eq(midnight));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("midnight equality query should run"));

        let noon = NaiveTime::from_hms_opt(12, 0, 0).expect("12:00:00 is a valid time");
        let query = select(time("12:00:00.000000").eq(noon));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("noon equality query should run"));

        let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200)
            .expect("23:37:04.002200 is a valid time");
        let query = select(sql::<Time>("'23:37:04.002200'").eq(roughly_half_past_eleven));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("23:37:04.002200 equality query should run"));
    }

    #[tokio::test]
    async fn times_of_day_decode_correctly() {
        let mut connection = connection().await;
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("00:00:00 is a valid time");
        let valid_midnight_formats = &[
            "00:00",
            "00:00:00",
            "00:00:00.000",
            "00:00:00.000000",
            "00:00Z",
            "00:00:00Z",
            "00:00:00.000Z",
            "00:00:00.000000Z",
            "00:00+00:00",
            "00:00:00+00:00",
            "00:00:00.000+00:00",
            "00:00:00.000000+00:00",
            "00:00+01:00",
            "00:00:00+01:00",
            "00:00:00.000+01:00",
            "00:00:00.000000+01:00",
            "00:00-01:00",
            "00:00:00-01:00",
            "00:00:00.000-01:00",
            "00:00:00.000000-01:00",
        ];
        for format in valid_midnight_formats {
            let query = select(sql::<Time>(&format!("'{format}'")));
            assert_eq!(
                Ok(midnight),
                query.get_result::<NaiveTime>(&mut connection).await,
                "format {format} failed"
            );
        }

        let noon = NaiveTime::from_hms_opt(12, 0, 0).expect("12:00:00 is a valid time");
        let query = select(sql::<Time>("'12:00:00'"));
        assert_eq!(
            Ok(noon),
            query.get_result::<NaiveTime>(&mut connection).await
        );

        let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200)
            .expect("23:37:04.002200 is a valid time");
        let query = select(sql::<Time>("'23:37:04.002200'"));
        assert_eq!(
            Ok(roughly_half_past_eleven),
            query.get_result::<NaiveTime>(&mut connection).await
        );
    }

    #[tokio::test]
    async fn dates_encode_correctly() {
        let mut connection = connection().await;
        let january_first_2000 =
            NaiveDate::from_ymd_opt(2000, 1, 1).expect("2000-01-01 is a valid date");
        let query = select(date("2000-01-01").eq(january_first_2000));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("2000-01-01 equality query should run"));

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11).expect("0000-04-11 is a valid date");
        let query = select(date("0000-04-11").eq(distant_past));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("0000-04-11 equality query should run"));

        let january_first_2018 =
            NaiveDate::from_ymd_opt(2018, 1, 1).expect("2018-01-01 is a valid date");
        let query = select(date("2018-01-01").eq(january_first_2018));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("2018-01-01 equality query should run"));

        let distant_future =
            NaiveDate::from_ymd_opt(9999, 1, 8).expect("9999-01-08 is a valid date");
        let query = select(date("9999-01-08").eq(distant_future));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("9999-01-08 equality query should run"));
    }

    #[tokio::test]
    async fn dates_decode_correctly() {
        let mut connection = connection().await;
        let january_first_2000 =
            NaiveDate::from_ymd_opt(2000, 1, 1).expect("2000-01-01 is a valid date");
        let query = select(date("2000-01-01"));
        assert_eq!(
            Ok(january_first_2000),
            query.get_result::<NaiveDate>(&mut connection).await
        );

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11).expect("0000-04-11 is a valid date");
        let query = select(date("0000-04-11"));
        assert_eq!(
            Ok(distant_past),
            query.get_result::<NaiveDate>(&mut connection).await
        );

        let january_first_2018 =
            NaiveDate::from_ymd_opt(2018, 1, 1).expect("2018-01-01 is a valid date");
        let query = select(date("2018-01-01"));
        assert_eq!(
            Ok(january_first_2018),
            query.get_result::<NaiveDate>(&mut connection).await
        );

        let distant_future =
            NaiveDate::from_ymd_opt(9999, 1, 8).expect("9999-01-08 is a valid date");
        let query = select(date("9999-01-08"));
        assert_eq!(
            Ok(distant_future),
            query.get_result::<NaiveDate>(&mut connection).await
        );
    }

    #[tokio::test]
    async fn datetimes_decode_correctly() {
        let mut connection = connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("2000-01-01 is a valid date")
            .and_hms_opt(1, 1, 1)
            .expect("01:01:01 is a valid time");
        let query = select(datetime("2000-01-01 01:01:01.000000"));
        assert_eq!(
            Ok(january_first_2000),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11)
            .expect("0000-04-11 is a valid date")
            .and_hms_opt(2, 2, 2)
            .expect("02:02:02 is a valid time");
        let query = select(datetime("0000-04-11 02:02:02.000000"));
        assert_eq!(
            Ok(distant_past),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );

        let january_first_2018 =
            NaiveDate::from_ymd_opt(2018, 1, 1).expect("2018-01-01 is a valid date");
        let query = select(date("2018-01-01"));
        assert_eq!(
            Ok(january_first_2018),
            query.get_result::<NaiveDate>(&mut connection).await
        );

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8)
            .expect("9999-01-08 is a valid date")
            .and_hms_opt(23, 59, 59)
            .expect("23:59:59 is a valid time")
            .with_nanosecond(100_000)
            .expect("100_000 ns < 2_000_000_000 (leap second cap)");
        let query = select(sql::<Timestamp>("'9999-01-08 23:59:59.000100'"));
        assert_eq!(
            Ok(distant_future),
            query.get_result::<NaiveDateTime>(&mut connection).await
        );
    }

    #[tokio::test]
    async fn datetimes_encode_correctly() {
        let mut connection = connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("2000-01-01 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let query = select(datetime("2000-01-01 00:00:00.000000").eq(january_first_2000));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("2000-01-01 00:00:00 equality query should run"));

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11)
            .expect("0000-04-11 is a valid date")
            .and_hms_opt(20, 0, 20)
            .expect("20:00:20 is a valid time");
        let query = select(datetime("0000-04-11 20:00:20.000000").eq(distant_past));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("0000-04-11 20:00:20 equality query should run"));

        let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1)
            .expect("2018-01-01 is a valid date")
            .and_hms_opt(12, 0, 0)
            .expect("12:00:00 is a valid time")
            .with_nanosecond(500_000)
            .expect("500_000 ns < 2_000_000_000 (leap second cap)");
        let query = select(sql::<Timestamp>("'2018-01-01 12:00:00.000500'").eq(january_first_2018));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("2018-01-01 12:00:00.000500 equality query should run"));

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8)
            .expect("9999-01-08 is a valid date")
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is a valid time");
        let query = select(datetime("9999-01-08 00:00:00.000000").eq(distant_future));
        assert!(query
            .get_result::<bool>(&mut connection)
            .await
            .expect("9999-01-08 00:00:00 equality query should run"));
    }
}
