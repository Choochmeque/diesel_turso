extern crate chrono;

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;

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
    let timestamp = (julian_days - EPOCH_IN_JULIAN_DAYS) * SECONDS_IN_DAY;
    #[allow(clippy::cast_possible_truncation)] // we want to truncate
    let seconds = timestamp.trunc() as i64;
    // that's not true, `fract` is always > 0
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let nanos = (timestamp.fract() * 1E9) as u32;
    #[allow(deprecated)] // otherwise we would need to bump our minimal chrono version
    NaiveDateTime::from_timestamp_opt(seconds, nanos)
}

#[cfg(feature = "chrono")]
impl FromSql<sql_types::Date, TursoBackend> for NaiveDate {
    fn from_sql(value: <TursoBackend as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        value
            .parse_string(|s| Self::parse_from_str(s, DATE_FORMAT))
            .map_err(Into::into)
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
        value.parse_string(|text| {
            for format in TIME_FORMATS {
                if let Ok(time) = Self::parse_from_str(text, format) {
                    return Ok(time);
                }
            }

            Err(format!("Invalid time {text}").into())
        })
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
        value.parse_string(|text| {
            for format in NAIVE_DATETIME_FORMATS {
                if let Ok(dt) = Self::parse_from_str(text, format) {
                    return Ok(dt);
                }
            }

            if let Ok(julian_days) = text.parse::<f64>() {
                if let Some(timestamp) = parse_julian(julian_days) {
                    return Ok(timestamp);
                }
            }

            Err(format!("Invalid datetime {text}").into())
        })
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
#[allow(clippy::unwrap_used)]
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

    #[tokio::test]
    async fn unix_epoch_encodes_correctly() {
        let connection = &mut connection().await;
        let time = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let query = select(datetime("1970-01-01 00:00:00.000000").eq(time));
        assert_eq!(Ok(true), query.get_result(connection).await);
    }

    #[tokio::test]
    async fn unix_epoch_decodes_correctly_in_all_possible_formats() {
        let connection = &mut connection().await;
        let time = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
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
                .get_result(connection)
                .await;
            assert_eq!(Ok(time), epoch_from_sql, "format {s} failed");
        }
    }

    #[tokio::test]
    async fn times_relative_to_now_encode_correctly() {
        let connection = &mut connection().await;
        let time = Utc::now().naive_utc() + Duration::try_seconds(60).unwrap();
        let query = select(now.lt(time));
        assert_eq!(Ok(true), query.get_result(connection).await);

        let time = Utc::now().naive_utc() - Duration::try_seconds(600).unwrap();
        let query = select(now.gt(time));
        assert_eq!(Ok(true), query.get_result(connection).await);
    }

    #[tokio::test]
    async fn times_of_day_encode_correctly() {
        let connection = &mut connection().await;

        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let query = select(time("00:00:00.000000").eq(midnight));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let query = select(time("12:00:00.000000").eq(noon));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200).unwrap();
        let query = select(sql::<Time>("'23:37:04.002200'").eq(roughly_half_past_eleven));
        assert!(query.get_result::<bool>(connection).await.unwrap());
    }

    #[tokio::test]
    async fn times_of_day_decode_correctly() {
        let connection = &mut connection().await;
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
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
                query.get_result::<NaiveTime>(connection).await,
                "format {format} failed"
            );
        }

        let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let query = select(sql::<Time>("'12:00:00'"));
        assert_eq!(Ok(noon), query.get_result::<NaiveTime>(connection).await);

        let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200).unwrap();
        let query = select(sql::<Time>("'23:37:04.002200'"));
        assert_eq!(
            Ok(roughly_half_past_eleven),
            query.get_result::<NaiveTime>(connection).await
        );
    }

    #[tokio::test]
    async fn dates_encode_correctly() {
        let connection = &mut connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let query = select(date("2000-01-01").eq(january_first_2000));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11).unwrap();
        let query = select(date("0000-04-11").eq(distant_past));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
        let query = select(date("2018-01-01").eq(january_first_2018));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8).unwrap();
        let query = select(date("9999-01-08").eq(distant_future));
        assert!(query.get_result::<bool>(connection).await.unwrap());
    }

    #[tokio::test]
    async fn dates_decode_correctly() {
        let connection = &mut connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let query = select(date("2000-01-01"));
        assert_eq!(
            Ok(january_first_2000),
            query.get_result::<NaiveDate>(connection).await
        );

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11).unwrap();
        let query = select(date("0000-04-11"));
        assert_eq!(
            Ok(distant_past),
            query.get_result::<NaiveDate>(connection).await
        );

        let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
        let query = select(date("2018-01-01"));
        assert_eq!(
            Ok(january_first_2018),
            query.get_result::<NaiveDate>(connection).await
        );

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8).unwrap();
        let query = select(date("9999-01-08"));
        assert_eq!(
            Ok(distant_future),
            query.get_result::<NaiveDate>(connection).await
        );
    }

    #[tokio::test]
    async fn datetimes_decode_correctly() {
        let connection = &mut connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(1, 1, 1)
            .unwrap();
        let query = select(datetime("2000-01-01 01:01:01.000000"));
        assert_eq!(
            Ok(january_first_2000),
            query.get_result::<NaiveDateTime>(connection).await
        );

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11)
            .unwrap()
            .and_hms_opt(2, 2, 2)
            .unwrap();
        let query = select(datetime("0000-04-11 02:02:02.000000"));
        assert_eq!(
            Ok(distant_past),
            query.get_result::<NaiveDateTime>(connection).await
        );

        let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
        let query = select(date("2018-01-01"));
        assert_eq!(
            Ok(january_first_2018),
            query.get_result::<NaiveDate>(connection).await
        );

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap()
            .with_nanosecond(100_000)
            .unwrap();
        let query = select(sql::<Timestamp>("'9999-01-08 23:59:59.000100'"));
        assert_eq!(
            Ok(distant_future),
            query.get_result::<NaiveDateTime>(connection).await
        );
    }

    #[tokio::test]
    async fn datetimes_encode_correctly() {
        let connection = &mut connection().await;
        let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let query = select(datetime("2000-01-01 00:00:00.000000").eq(january_first_2000));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let distant_past = NaiveDate::from_ymd_opt(0, 4, 11)
            .unwrap()
            .and_hms_opt(20, 00, 20)
            .unwrap();
        let query = select(datetime("0000-04-11 20:00:20.000000").eq(distant_past));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1)
            .unwrap()
            .and_hms_opt(12, 00, 00)
            .unwrap()
            .with_nanosecond(500_000)
            .unwrap();
        let query = select(sql::<Timestamp>("'2018-01-01 12:00:00.000500'").eq(january_first_2018));
        assert!(query.get_result::<bool>(connection).await.unwrap());

        let distant_future = NaiveDate::from_ymd_opt(9999, 1, 8)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let query = select(datetime("9999-01-08 00:00:00.000000").eq(distant_future));
        assert!(query.get_result::<bool>(connection).await.unwrap());
    }
}
