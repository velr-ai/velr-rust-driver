use crate::error::{DecodeError, EncodeError};
use crate::property::{PropertyValue, PropertyValueRef};
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{Offset, TimeZone};
use jiff::{Span, Timestamp};
use std::fmt;
use std::str::FromStr;

pub const NANOS_PER_SECOND: i64 = 1_000_000_000;
pub const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
pub const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
pub const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
const DAYS_PER_AVERAGE_YEAR: f64 = 365.2425;
const DAYS_PER_AVERAGE_MONTH: f64 = DAYS_PER_AVERAGE_YEAR / 12.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentTemporalKind {
    Date,
    LocalTime,
    Time,
    LocalDateTime,
    DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateValue {
    inner: Date,
}

impl DateValue {
    pub fn new(year: i32, month: u8, day: u8) -> Result<Self, EncodeError> {
        let year = i16::try_from(year)
            .map_err(|_| EncodeError::InvalidTemporal("year must be in -9999..=9999"))?;
        let month = i8::try_from(month)
            .map_err(|_| EncodeError::InvalidTemporal("month must be in 1..=12"))?;
        let day =
            i8::try_from(day).map_err(|_| EncodeError::InvalidTemporal("day must be in 1..=31"))?;
        let inner = Date::new(year, month, day)
            .map_err(|_| EncodeError::InvalidTemporal("invalid calendar date"))?;
        Ok(Self { inner })
    }

    pub fn from_calendar(
        year: i32,
        month: Option<u8>,
        day: Option<u8>,
    ) -> Result<Self, EncodeError> {
        Self::new(year, month.unwrap_or(1), day.unwrap_or(1))
    }

    pub fn from_iso_week(
        year: i32,
        week: u8,
        day_of_week: Option<u8>,
    ) -> Result<Self, EncodeError> {
        if week == 0 || week > 53 {
            return Err(EncodeError::InvalidTemporal("week must be in 1..=53"));
        }
        let day_of_week = day_of_week.unwrap_or(1);
        if !(1..=7).contains(&day_of_week) {
            return Err(EncodeError::InvalidTemporal("dayOfWeek must be in 1..=7"));
        }

        let start = iso_week_one_monday_epoch_day(year)?;
        let day = start + (i64::from(week) - 1) * 7 + (i64::from(day_of_week) - 1);
        let value = Self::from_epoch_day(day)?;
        if value.iso_week_year()? != year {
            return Err(EncodeError::InvalidTemporal("invalid ISO week date"));
        }
        Ok(value)
    }

    pub fn from_ordinal_day(year: i32, ordinal_day: u16) -> Result<Self, EncodeError> {
        let jan1 = Self::new(year, 1, 1)?;
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if ordinal_day == 0 || ordinal_day > days_in_year {
            return Err(EncodeError::InvalidTemporal(
                "ordinalDay must be valid for year",
            ));
        }
        Self::from_epoch_day(jan1.epoch_day() + i64::from(ordinal_day) - 1)
    }

    pub fn from_quarter(
        year: i32,
        quarter: u8,
        day_of_quarter: Option<u16>,
    ) -> Result<Self, EncodeError> {
        if !(1..=4).contains(&quarter) {
            return Err(EncodeError::InvalidTemporal("quarter must be in 1..=4"));
        }
        let day_of_quarter = day_of_quarter.unwrap_or(1);
        if day_of_quarter == 0 {
            return Err(EncodeError::InvalidTemporal(
                "dayOfQuarter must be positive",
            ));
        }

        let month = (quarter - 1) * 3 + 1;
        let start = Self::new(year, month, 1)?;
        let value = Self::from_epoch_day(start.epoch_day() + i64::from(day_of_quarter) - 1)?;
        if value.year() != year || ((value.month() - 1) / 3) + 1 != quarter {
            return Err(EncodeError::InvalidTemporal(
                "dayOfQuarter must be valid for quarter",
            ));
        }
        Ok(value)
    }

    pub fn from_epoch_day(epoch_day: i64) -> Result<Self, EncodeError> {
        let (year, month, day) = ymd_from_epoch_day(epoch_day)?;
        Self::new(year, month, day)
    }

    pub fn year(&self) -> i32 {
        i32::from(self.inner.year())
    }

    pub fn month(&self) -> u8 {
        self.inner.month() as u8
    }

    pub fn day(&self) -> u8 {
        self.inner.day() as u8
    }

    pub fn as_jiff(&self) -> Date {
        self.inner
    }

    pub fn epoch_day(&self) -> i64 {
        epoch_day_from_ymd(self.year(), self.month(), self.day())
    }

    pub fn iso_week_year(&self) -> Result<i32, EncodeError> {
        let day = self.epoch_day();
        let year = self.year();
        if day < iso_week_one_monday_epoch_day(year)? {
            Ok(year - 1)
        } else if day >= iso_week_one_monday_epoch_day(year + 1)? {
            Ok(year + 1)
        } else {
            Ok(year)
        }
    }

    pub fn iso_day_of_week(&self) -> u8 {
        day_of_week_from_epoch_day(self.epoch_day())
    }

    pub fn iso_week(&self) -> Result<u8, EncodeError> {
        let week_year = self.iso_week_year()?;
        let week_one = iso_week_one_monday_epoch_day(week_year)?;
        Ok(((self.epoch_day() - week_one) / 7 + 1) as u8)
    }

    pub fn quarter(&self) -> u8 {
        (self.month() - 1) / 3 + 1
    }

    pub fn ordinal_day(&self) -> Result<u16, EncodeError> {
        let jan1 = Self::new(self.year(), 1, 1)?;
        Ok((self.epoch_day() - jan1.epoch_day() + 1) as u16)
    }

    pub fn day_of_quarter(&self) -> Result<u16, EncodeError> {
        let start = Self::new(self.year(), (self.quarter() - 1) * 3 + 1, 1)?;
        Ok((self.epoch_day() - start.epoch_day() + 1) as u16)
    }

    pub fn checked_add_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_duration_groups(
            duration.total_months(),
            duration.total_days(),
            duration.total_time_nanos(),
        )
    }

    pub fn checked_sub_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_duration_groups(
            checked_i64_from_i128(-i128::from(duration.total_months()))?,
            checked_i64_from_i128(-i128::from(duration.total_days()))?,
            duration
                .total_time_nanos()
                .checked_neg()
                .ok_or(EncodeError::InvalidTemporal("duration arithmetic overflow"))?,
        )
    }

    fn checked_add_duration_groups(
        &self,
        months: i64,
        days: i64,
        time_nanos: i128,
    ) -> Result<Self, EncodeError> {
        let date = self.checked_add_months(months)?;
        let time_days = checked_i64_from_i128(time_nanos / i128::from(NANOS_PER_DAY))?;
        date.checked_add_days(
            days.checked_add(time_days)
                .ok_or(EncodeError::InvalidTemporal(
                    "date duration arithmetic overflow",
                ))?,
        )
    }

    fn checked_add_months(&self, months: i64) -> Result<Self, EncodeError> {
        let month_index = i128::from(self.year()) * 12 + i128::from(self.month()) - 1;
        let target =
            month_index
                .checked_add(i128::from(months))
                .ok_or(EncodeError::InvalidTemporal(
                    "date duration arithmetic overflow",
                ))?;
        let year = checked_i32_from_i128(target.div_euclid(12))?;
        let month = u8::try_from(target.rem_euclid(12) + 1)
            .map_err(|_| EncodeError::InvalidTemporal("month must be in 1..=12"))?;
        let day = self.day().min(days_in_month(year, month));
        Self::new(year, month, day)
    }

    fn checked_add_days(&self, days: i64) -> Result<Self, EncodeError> {
        let epoch_day = i128::from(self.epoch_day())
            .checked_add(i128::from(days))
            .ok_or(EncodeError::InvalidTemporal(
                "date duration arithmetic overflow",
            ))?;
        Self::from_epoch_day(checked_i64_from_i128(epoch_day)?)
    }
}

impl fmt::Display for DateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl FromStr for DateValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_open_cypher_date(s)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalTimeValue {
    inner: Time,
}

impl LocalTimeValue {
    pub fn new(hour: u8, minute: u8, second: u8, nanos: u32) -> Result<Self, EncodeError> {
        let hour = i8::try_from(hour)
            .map_err(|_| EncodeError::InvalidTemporal("hour must be in 0..=23"))?;
        let minute = i8::try_from(minute)
            .map_err(|_| EncodeError::InvalidTemporal("minute must be in 0..=59"))?;
        let second = i8::try_from(second)
            .map_err(|_| EncodeError::InvalidTemporal("second must be in 0..=59"))?;
        let nanos = i32::try_from(nanos)
            .map_err(|_| EncodeError::InvalidTemporal("nanos must be < 1_000_000_000"))?;
        let inner = Time::new(hour, minute, second, nanos)
            .map_err(|_| EncodeError::InvalidTemporal("invalid wall clock time"))?;
        Ok(Self { inner })
    }

    pub fn hour(&self) -> u8 {
        self.inner.hour() as u8
    }

    pub fn minute(&self) -> u8 {
        self.inner.minute() as u8
    }

    pub fn second(&self) -> u8 {
        self.inner.second() as u8
    }

    pub fn nanos(&self) -> u32 {
        self.inner.subsec_nanosecond() as u32
    }

    pub fn as_jiff(&self) -> Time {
        self.inner
    }

    pub fn nanos_since_midnight(&self) -> i64 {
        (((self.hour() as i64 * 60 + self.minute() as i64) * 60 + self.second() as i64)
            * NANOS_PER_SECOND)
            + self.nanos() as i64
    }

    pub fn from_nanos_since_midnight(nanos: i64) -> Result<Self, EncodeError> {
        if !(0..NANOS_PER_DAY).contains(&nanos) {
            return Err(EncodeError::InvalidTemporal(
                "nanos since midnight must be in 0..<24h",
            ));
        }
        let hour = nanos / NANOS_PER_HOUR;
        let nanos = nanos % NANOS_PER_HOUR;
        let minute = nanos / NANOS_PER_MINUTE;
        let nanos = nanos % NANOS_PER_MINUTE;
        let second = nanos / NANOS_PER_SECOND;
        let nanos = nanos % NANOS_PER_SECOND;
        Self::new(hour as u8, minute as u8, second as u8, nanos as u32)
    }

    pub fn checked_add_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_time_nanos(duration.total_time_nanos())
    }

    pub fn checked_sub_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_time_nanos(
            duration
                .total_time_nanos()
                .checked_neg()
                .ok_or(EncodeError::InvalidTemporal("duration arithmetic overflow"))?,
        )
    }

    fn checked_add_time_nanos(&self, nanos: i128) -> Result<Self, EncodeError> {
        let shifted = i128::from(self.nanos_since_midnight())
            .checked_add(nanos)
            .ok_or(EncodeError::InvalidTemporal(
                "time duration arithmetic overflow",
            ))?;
        let wrapped = shifted.rem_euclid(i128::from(NANOS_PER_DAY));
        Self::from_nanos_since_midnight(checked_i64_from_i128(wrapped)?)
    }
}

impl fmt::Display for LocalTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl FromStr for LocalTimeValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = s
            .parse::<Time>()
            .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))?;
        Ok(Self { inner })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UtcOffsetValue {
    seconds_east: i32,
}

impl UtcOffsetValue {
    pub fn utc() -> Self {
        Self { seconds_east: 0 }
    }

    pub fn new(seconds_east: i32) -> Result<Self, EncodeError> {
        Offset::from_seconds(seconds_east)
            .map_err(|_| EncodeError::InvalidTemporal("offset out of range"))?;
        Ok(Self { seconds_east })
    }

    pub fn seconds_east(&self) -> i32 {
        self.seconds_east
    }

    pub fn is_utc(&self) -> bool {
        self.seconds_east == 0
    }

    pub fn as_jiff(&self) -> Offset {
        Offset::from_seconds(self.seconds_east)
            .expect("UtcOffsetValue should always store a validated offset")
    }
}

impl fmt::Display for UtcOffsetValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.seconds_east == 0 {
            return f.write_str("Z");
        }
        let sign = if self.seconds_east < 0 { '-' } else { '+' };
        let abs = self.seconds_east.abs();
        let hours = abs / 3600;
        let minutes = (abs % 3600) / 60;
        let seconds = abs % 60;
        if seconds == 0 {
            write!(f, "{sign}{hours:02}:{minutes:02}")
        } else {
            write!(f, "{sign}{hours:02}:{minutes:02}:{seconds:02}")
        }
    }
}

impl FromStr for UtcOffsetValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_offset(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZonedTimeValue {
    pub time: LocalTimeValue,
    pub offset: UtcOffsetValue,
}

impl ZonedTimeValue {
    pub fn new(time: LocalTimeValue, offset: UtcOffsetValue) -> Self {
        Self { time, offset }
    }

    pub fn utc_nanos_since_midnight(&self) -> i64 {
        let local = self.time.nanos_since_midnight();
        let offset = self.offset.seconds_east() as i64 * NANOS_PER_SECOND;
        (local - offset).rem_euclid(NANOS_PER_DAY)
    }

    pub fn with_offset_same_instant(&self, offset: UtcOffsetValue) -> Result<Self, EncodeError> {
        let local =
            self.utc_nanos_since_midnight() + offset.seconds_east() as i64 * NANOS_PER_SECOND;
        Ok(Self {
            time: LocalTimeValue::from_nanos_since_midnight(local.rem_euclid(NANOS_PER_DAY))?,
            offset,
        })
    }

    pub fn checked_add_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        Ok(Self {
            time: self.time.checked_add_duration(duration)?,
            offset: self.offset,
        })
    }

    pub fn checked_sub_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        Ok(Self {
            time: self.time.checked_sub_duration(duration)?,
            offset: self.offset,
        })
    }
}

impl fmt::Display for ZonedTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.time, self.offset)
    }
}

impl FromStr for ZonedTimeValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let idx = find_offset_start(s, false)
            .ok_or_else(|| DecodeError::InvalidTemporal("missing zone offset".into()))?;
        Ok(Self {
            time: s[..idx].parse()?,
            offset: s[idx..].parse()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalDateTimeValue {
    inner: DateTime,
}

impl LocalDateTimeValue {
    pub fn new(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanos: u32,
    ) -> Result<Self, EncodeError> {
        let year = i16::try_from(year)
            .map_err(|_| EncodeError::InvalidTemporal("year must be in -9999..=9999"))?;
        let month = i8::try_from(month)
            .map_err(|_| EncodeError::InvalidTemporal("month must be in 1..=12"))?;
        let day =
            i8::try_from(day).map_err(|_| EncodeError::InvalidTemporal("day must be in 1..=31"))?;
        let hour = i8::try_from(hour)
            .map_err(|_| EncodeError::InvalidTemporal("hour must be in 0..=23"))?;
        let minute = i8::try_from(minute)
            .map_err(|_| EncodeError::InvalidTemporal("minute must be in 0..=59"))?;
        let second = i8::try_from(second)
            .map_err(|_| EncodeError::InvalidTemporal("second must be in 0..=59"))?;
        let nanos = i32::try_from(nanos)
            .map_err(|_| EncodeError::InvalidTemporal("nanos must be < 1_000_000_000"))?;
        let inner = DateTime::new(year, month, day, hour, minute, second, nanos)
            .map_err(|_| EncodeError::InvalidTemporal("invalid local datetime"))?;
        Ok(Self { inner })
    }

    pub fn from_date_and_time(date: DateValue, time: LocalTimeValue) -> Result<Self, EncodeError> {
        Self::new(
            date.year(),
            date.month(),
            date.day(),
            time.hour(),
            time.minute(),
            time.second(),
            time.nanos(),
        )
    }

    pub fn date(&self) -> DateValue {
        DateValue {
            inner: self.inner.date(),
        }
    }

    pub fn time(&self) -> LocalTimeValue {
        LocalTimeValue {
            inner: self.inner.time(),
        }
    }

    pub fn as_jiff(&self) -> DateTime {
        self.inner
    }

    pub fn local_epoch_day_and_nanos(&self) -> (i64, i64) {
        let date = self.date();
        let time = self.time();
        (date.epoch_day(), time.nanos_since_midnight())
    }

    pub fn checked_add_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_duration_groups(
            duration.total_months(),
            duration.total_days(),
            duration.total_time_nanos(),
        )
    }

    pub fn checked_sub_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        self.checked_add_duration_groups(
            checked_i64_from_i128(-i128::from(duration.total_months()))?,
            checked_i64_from_i128(-i128::from(duration.total_days()))?,
            duration
                .total_time_nanos()
                .checked_neg()
                .ok_or(EncodeError::InvalidTemporal("duration arithmetic overflow"))?,
        )
    }

    fn checked_add_duration_groups(
        &self,
        months: i64,
        days: i64,
        time_nanos: i128,
    ) -> Result<Self, EncodeError> {
        let date = self.date().checked_add_months(months)?;
        let shifted_time = i128::from(self.time().nanos_since_midnight())
            .checked_add(time_nanos)
            .ok_or(EncodeError::InvalidTemporal(
                "datetime duration arithmetic overflow",
            ))?;
        let time_days = shifted_time.div_euclid(i128::from(NANOS_PER_DAY));
        let time_nanos = shifted_time.rem_euclid(i128::from(NANOS_PER_DAY));
        let epoch_day = i128::from(date.epoch_day())
            .checked_add(i128::from(days))
            .and_then(|value| value.checked_add(time_days))
            .ok_or(EncodeError::InvalidTemporal(
                "datetime duration arithmetic overflow",
            ))?;
        let date = DateValue::from_epoch_day(checked_i64_from_i128(epoch_day)?)?;
        let time = LocalTimeValue::from_nanos_since_midnight(checked_i64_from_i128(time_nanos)?)?;
        Self::from_date_and_time(date, time)
    }
}

impl fmt::Display for LocalDateTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl FromStr for LocalDateTimeValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<DateTime>() {
            Ok(inner) => Ok(Self { inner }),
            Err(parse_err) => {
                let (date, time) = s.split_once('T').ok_or_else(|| {
                    DecodeError::InvalidTemporal(format!("invalid local datetime: {parse_err}"))
                })?;
                Self::from_date_and_time(date.parse()?, time.parse()?)
                    .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZonedDateTimeValue {
    pub datetime: LocalDateTimeValue,
    pub offset: UtcOffsetValue,
    pub zone_id: Option<Box<str>>,
}

impl ZonedDateTimeValue {
    pub fn new(
        datetime: LocalDateTimeValue,
        offset: UtcOffsetValue,
        zone_id: Option<impl Into<Box<str>>>,
    ) -> Result<Self, EncodeError> {
        let zone_id = zone_id.map(Into::into);
        if let Some(zone_id_str) = zone_id.as_deref() {
            validate_zoned_components(&datetime, &offset, zone_id_str)
                .map_err(|_| EncodeError::InvalidTemporal("invalid zone id or offset mismatch"))?;
        }
        Ok(Self {
            datetime,
            offset,
            zone_id,
        })
    }

    pub fn in_named_zone(
        datetime: LocalDateTimeValue,
        zone_id: impl Into<Box<str>>,
    ) -> Result<Self, EncodeError> {
        let zone_id = zone_id.into();
        let zoned = datetime
            .as_jiff()
            .in_tz(zone_id.as_ref())
            .map_err(|_| EncodeError::InvalidTemporal("invalid zone id or local datetime"))?;
        Ok(Self {
            datetime,
            offset: UtcOffsetValue::new(zoned.offset().seconds())?,
            zone_id: Some(zone_id),
        })
    }

    pub fn from_epoch(seconds: i64, nanoseconds: u32) -> Result<Self, EncodeError> {
        let nanoseconds = i32::try_from(nanoseconds)
            .map_err(|_| EncodeError::InvalidTemporal("nanoseconds out of range"))?;
        let timestamp = Timestamp::new(seconds, nanoseconds)
            .map_err(|_| EncodeError::InvalidTemporal("epoch datetime out of range"))?;
        Ok(Self::from_utc_timestamp(timestamp))
    }

    pub fn from_epoch_millis(milliseconds: i64) -> Result<Self, EncodeError> {
        let timestamp = Timestamp::from_millisecond(milliseconds)
            .map_err(|_| EncodeError::InvalidTemporal("epoch datetime out of range"))?;
        Ok(Self::from_utc_timestamp(timestamp))
    }

    fn from_utc_timestamp(timestamp: Timestamp) -> Self {
        let zoned = timestamp.to_zoned(TimeZone::UTC);
        Self {
            datetime: LocalDateTimeValue {
                inner: zoned.datetime(),
            },
            offset: UtcOffsetValue::utc(),
            zone_id: None,
        }
    }

    pub fn utc_epoch_day_and_nanos(&self) -> (i64, i64) {
        let (day, nanos) = self.datetime.local_epoch_day_and_nanos();
        let offset = self.offset.seconds_east() as i64 * NANOS_PER_SECOND;
        let shifted = nanos - offset;
        (
            day + shifted.div_euclid(NANOS_PER_DAY),
            shifted.rem_euclid(NANOS_PER_DAY),
        )
    }

    pub fn with_offset_same_instant(&self, offset: UtcOffsetValue) -> Result<Self, EncodeError> {
        let (day, nanos) = self.utc_epoch_day_and_nanos();
        Self::from_utc_epoch_day_and_nanos(day, nanos, offset, None::<Box<str>>)
    }

    #[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
    pub fn with_named_zone_same_instant(
        &self,
        zone_id: impl Into<Box<str>>,
    ) -> Result<Self, EncodeError> {
        let zone_id = zone_id.into();
        let (day, nanos) = self.utc_epoch_day_and_nanos();
        let timestamp = timestamp_from_epoch_day_and_nanos(day, nanos)?;
        let zoned = timestamp
            .in_tz(zone_id.as_ref())
            .map_err(|_| EncodeError::InvalidTemporal("invalid zone id"))?;
        Ok(Self {
            datetime: LocalDateTimeValue {
                inner: zoned.datetime(),
            },
            offset: UtcOffsetValue::new(zoned.offset().seconds())?,
            zone_id: Some(zone_id),
        })
    }

    #[cfg(not(any(feature = "tzdb-system", feature = "tzdb-bundled")))]
    pub fn with_named_zone_same_instant(
        &self,
        _zone_id: impl Into<Box<str>>,
    ) -> Result<Self, EncodeError> {
        Err(EncodeError::InvalidTemporal("named zones are not enabled"))
    }

    fn from_utc_epoch_day_and_nanos(
        day: i64,
        nanos: i64,
        offset: UtcOffsetValue,
        zone_id: Option<impl Into<Box<str>>>,
    ) -> Result<Self, EncodeError> {
        let shifted = i128::from(day) * i128::from(NANOS_PER_DAY)
            + i128::from(nanos)
            + i128::from(offset.seconds_east()) * i128::from(NANOS_PER_SECOND);
        let local_day = shifted.div_euclid(i128::from(NANOS_PER_DAY));
        let local_nanos = shifted.rem_euclid(i128::from(NANOS_PER_DAY));
        let date = DateValue::from_epoch_day(checked_i64_from_i128(local_day)?)?;
        let time = LocalTimeValue::from_nanos_since_midnight(checked_i64_from_i128(local_nanos)?)?;
        Ok(Self {
            datetime: LocalDateTimeValue::from_date_and_time(date, time)?,
            offset,
            zone_id: zone_id.map(Into::into),
        })
    }

    pub fn checked_add_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        if self.zone_id.is_some() {
            return Err(EncodeError::InvalidTemporal(
                "named-zone datetime duration arithmetic is not implemented",
            ));
        }
        Ok(Self {
            datetime: self.datetime.checked_add_duration(duration)?,
            offset: self.offset,
            zone_id: None,
        })
    }

    pub fn checked_sub_duration(&self, duration: &DurationValue) -> Result<Self, EncodeError> {
        if self.zone_id.is_some() {
            return Err(EncodeError::InvalidTemporal(
                "named-zone datetime duration arithmetic is not implemented",
            ));
        }
        Ok(Self {
            datetime: self.datetime.checked_sub_duration(duration)?,
            offset: self.offset,
            zone_id: None,
        })
    }
}

impl fmt::Display for ZonedDateTimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.datetime, self.offset)?;
        if let Some(zone_id) = &self.zone_id {
            write!(f, "[{zone_id}]")?;
        }
        Ok(())
    }
}

impl FromStr for ZonedDateTimeValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (main, zone_id) = split_zone_suffix(s)?;
        let value = match find_offset_start(main, true) {
            Some(idx) => {
                #[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
                if zone_id.is_some() {
                    return parse_zoned_datetime_with_jiff(s);
                }

                let value = Self {
                    datetime: main[..idx].parse()?,
                    offset: main[idx..].parse()?,
                    zone_id: zone_id.map(Into::into),
                };
                if let Some(zone_id) = value.zone_id.as_deref() {
                    validate_zoned_components(&value.datetime, &value.offset, zone_id)
                        .map_err(DecodeError::InvalidTemporal)?;
                }
                value
            }
            None => {
                let Some(zone_id) = zone_id else {
                    return Err(DecodeError::InvalidTemporal(
                        "missing datetime offset".into(),
                    ));
                };
                Self::in_named_zone(main.parse()?, zone_id)
                    .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))?
            }
        };
        Ok(value)
    }
}

pub fn current_temporal_value(
    kind: CurrentTemporalKind,
    epoch_millis: i64,
    timezone: Option<&str>,
) -> Result<PropertyValue, EncodeError> {
    let timestamp = Timestamp::from_millisecond(epoch_millis)
        .map_err(|_| EncodeError::InvalidTemporal("epoch datetime out of range"))?;
    let (timezone, zone_id) = current_time_zone(timezone)?;
    let zoned = timestamp.to_zoned(timezone);
    let date = DateValue {
        inner: zoned.date(),
    };
    let time = LocalTimeValue {
        inner: zoned.time(),
    };
    let offset = UtcOffsetValue::new(zoned.offset().seconds())?;
    Ok(match kind {
        CurrentTemporalKind::Date => PropertyValue::Date(date),
        CurrentTemporalKind::LocalTime => PropertyValue::LocalTime(time),
        CurrentTemporalKind::Time => PropertyValue::ZonedTime(ZonedTimeValue::new(time, offset)),
        CurrentTemporalKind::LocalDateTime => PropertyValue::LocalDateTime(LocalDateTimeValue {
            inner: zoned.datetime(),
        }),
        CurrentTemporalKind::DateTime => PropertyValue::ZonedDateTime(ZonedDateTimeValue {
            datetime: LocalDateTimeValue {
                inner: zoned.datetime(),
            },
            offset,
            zone_id,
        }),
    })
}

fn current_time_zone(timezone: Option<&str>) -> Result<(TimeZone, Option<Box<str>>), EncodeError> {
    let Some(timezone) = timezone else {
        let timezone = TimeZone::system();
        let zone_id = timezone.iana_name().map(Box::<str>::from);
        return Ok((timezone, zone_id));
    };

    if let Ok(offset) = timezone.parse::<UtcOffsetValue>() {
        return Ok((TimeZone::fixed(offset.as_jiff()), None));
    }

    let timezone =
        TimeZone::get(timezone).map_err(|_| EncodeError::InvalidTemporal("invalid zone id"))?;
    let zone_id = timezone.iana_name().map(Box::<str>::from);
    Ok((timezone, zone_id))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DurationMapParts {
    pub years: f64,
    pub months: f64,
    pub weeks: f64,
    pub days: f64,
    pub hours: f64,
    pub minutes: f64,
    pub seconds: f64,
    pub milliseconds: f64,
    pub microseconds: f64,
    pub nanoseconds: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct DurationIntegerParts {
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
}

#[derive(Debug, Clone)]
pub struct DurationValue {
    inner: Span,
    months: i64,
    days: i64,
    time_nanos: i128,
}

impl DurationValue {
    pub fn zero() -> Self {
        Self {
            inner: Span::default(),
            months: 0,
            days: 0,
            time_nanos: 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        years: i64,
        months: i64,
        weeks: i64,
        days: i64,
        hours: i64,
        minutes: i64,
        seconds: i64,
        milliseconds: i64,
        microseconds: i64,
        nanoseconds: i64,
    ) -> Result<Self, EncodeError> {
        let months_total = checked_i64_from_i128(i128::from(years) * 12_i128 + i128::from(months))?;
        let days_total = checked_i64_from_i128(i128::from(weeks) * 7_i128 + i128::from(days))?;
        let time_nanos = i128::from(hours) * i128::from(NANOS_PER_HOUR)
            + i128::from(minutes) * i128::from(NANOS_PER_MINUTE)
            + i128::from(seconds) * i128::from(NANOS_PER_SECOND)
            + i128::from(milliseconds) * 1_000_000
            + i128::from(microseconds) * 1_000
            + i128::from(nanoseconds);
        // jiff::Span has narrower field ranges than openCypher durations.
        // The group totals below are authoritative for display, encoding,
        // equality, and arithmetic; keep Span only as best-effort compatibility.
        let inner = Span::new()
            .try_years(years)
            .and_then(|s| s.try_months(months))
            .and_then(|s| s.try_weeks(weeks))
            .and_then(|s| s.try_days(days))
            .and_then(|s| s.try_hours(hours))
            .and_then(|s| s.try_minutes(minutes))
            .and_then(|s| s.try_seconds(seconds))
            .and_then(|s| s.try_milliseconds(milliseconds))
            .and_then(|s| s.try_microseconds(microseconds))
            .and_then(|s| s.try_nanoseconds(nanoseconds))
            .unwrap_or_else(|_| Span::default());
        Ok(Self {
            inner,
            months: months_total,
            days: days_total,
            time_nanos,
        })
    }

    pub fn from_open_cypher_parts(parts: DurationMapParts) -> Result<Self, EncodeError> {
        let (years, years_fraction) = split_whole_fraction(parts.years)?;
        let months_value = parts.months + years_fraction * 12.0;
        let (months, months_fraction) = split_whole_fraction(months_value)?;

        let days_value = months_fraction * DAYS_PER_AVERAGE_MONTH + parts.weeks * 7.0 + parts.days;
        let (whole_days, day_fraction) = split_whole_fraction(days_value)?;

        let nanos_value = day_fraction * NANOS_PER_DAY as f64
            + parts.hours * NANOS_PER_HOUR as f64
            + parts.minutes * NANOS_PER_MINUTE as f64
            + parts.seconds * NANOS_PER_SECOND as f64
            + parts.milliseconds * 1_000_000.0
            + parts.microseconds * 1_000.0
            + parts.nanoseconds;
        if !nanos_value.is_finite() {
            return Err(EncodeError::InvalidTemporal(
                "duration field must be finite",
            ));
        }
        let total_nanos = checked_i128_from_rounded_f64(nanos_value)?;
        let days = whole_days;
        let (hours, minutes, seconds, nanoseconds) = split_time_nanos(total_nanos)?;

        Self::from_parts(
            years,
            months,
            0,
            days,
            hours,
            minutes,
            seconds,
            0,
            0,
            nanoseconds,
        )
    }

    pub fn years(&self) -> i64 {
        self.months / 12
    }

    pub fn months(&self) -> i64 {
        self.months % 12
    }

    pub fn weeks(&self) -> i64 {
        0
    }

    pub fn days(&self) -> i64 {
        self.days
    }

    pub fn hours(&self) -> i64 {
        split_time_nanos(self.time_nanos)
            .expect("DurationValue should store checked time")
            .0
    }

    pub fn minutes(&self) -> i64 {
        split_time_nanos(self.time_nanos)
            .expect("DurationValue should store checked time")
            .1
    }

    pub fn seconds(&self) -> i64 {
        split_time_nanos(self.time_nanos)
            .expect("DurationValue should store checked time")
            .2
    }

    pub fn milliseconds(&self) -> i64 {
        self.nanoseconds() / 1_000_000
    }

    pub fn microseconds(&self) -> i64 {
        self.nanoseconds() / 1_000
    }

    pub fn nanoseconds(&self) -> i64 {
        split_time_nanos(self.time_nanos)
            .expect("DurationValue should store checked time")
            .3
    }

    pub fn total_months(&self) -> i64 {
        self.months
    }

    pub fn total_quarters(&self) -> i64 {
        self.total_months() / 3
    }

    pub fn total_weeks(&self) -> i64 {
        self.total_days() / 7
    }

    pub fn total_days(&self) -> i64 {
        self.days
    }

    pub fn total_time_nanos(&self) -> i128 {
        self.time_nanos
    }

    pub fn as_jiff(&self) -> Option<&Span> {
        Self::from_jiff_span(self.inner.clone())
            .ok()
            .filter(|value| value == self)
            .map(|_| &self.inner)
    }

    pub fn checked_add_duration(&self, other: &DurationValue) -> Result<Self, EncodeError> {
        let months = checked_i64_from_i128(i128::from(self.months) + i128::from(other.months))?;
        let days = checked_i64_from_i128(i128::from(self.days) + i128::from(other.days))?;
        let time_nanos = self
            .time_nanos
            .checked_add(other.time_nanos)
            .ok_or(EncodeError::InvalidTemporal("duration arithmetic overflow"))?;
        Self::from_groups(months, days, time_nanos)
    }

    pub fn checked_sub_duration(&self, other: &DurationValue) -> Result<Self, EncodeError> {
        let months = checked_i64_from_i128(i128::from(self.months) - i128::from(other.months))?;
        let days = checked_i64_from_i128(i128::from(self.days) - i128::from(other.days))?;
        let time_nanos = self
            .time_nanos
            .checked_sub(other.time_nanos)
            .ok_or(EncodeError::InvalidTemporal("duration arithmetic overflow"))?;
        Self::from_groups(months, days, time_nanos)
    }

    pub fn checked_mul_number(&self, factor: f64) -> Result<Self, EncodeError> {
        self.checked_scale(factor)
    }

    pub fn checked_div_number(&self, divisor: f64) -> Result<Self, EncodeError> {
        if divisor == 0.0 {
            return Err(EncodeError::InvalidTemporal("duration division by zero"));
        }
        self.checked_scale(1.0 / divisor)
    }

    fn checked_scale(&self, factor: f64) -> Result<Self, EncodeError> {
        if !factor.is_finite() {
            return Err(EncodeError::InvalidTemporal(
                "duration arithmetic factor must be finite",
            ));
        }

        let months_value = self.months as f64 * factor;
        let (months, months_fraction) = split_whole_fraction(months_value)?;
        let days_value = self.days as f64 * factor + months_fraction * DAYS_PER_AVERAGE_MONTH;
        let (days, day_fraction) = split_whole_fraction(days_value)?;
        let nanos_value = self.time_nanos as f64 * factor + day_fraction * NANOS_PER_DAY as f64;
        if !nanos_value.is_finite() {
            return Err(EncodeError::InvalidTemporal(
                "duration field must be finite",
            ));
        }
        let time_nanos = checked_i128_from_rounded_f64(nanos_value)?;
        Self::from_groups(months, days, time_nanos)
    }

    fn from_groups(months: i64, days: i64, time_nanos: i128) -> Result<Self, EncodeError> {
        let years = months / 12;
        let months = months % 12;
        let (hours, minutes, seconds, nanoseconds) = split_time_nanos(time_nanos)?;
        Self::from_parts(
            years,
            months,
            0,
            days,
            hours,
            minutes,
            seconds,
            0,
            0,
            nanoseconds,
        )
    }

    fn from_jiff_span(inner: Span) -> Result<Self, EncodeError> {
        let months = i64::from(inner.get_years()) * 12 + i64::from(inner.get_months());
        let days = i64::from(inner.get_weeks()) * 7 + i64::from(inner.get_days());
        let time_nanos = i128::from(inner.get_hours()) * i128::from(NANOS_PER_HOUR)
            + i128::from(inner.get_minutes()) * i128::from(NANOS_PER_MINUTE)
            + i128::from(inner.get_seconds()) * i128::from(NANOS_PER_SECOND)
            + i128::from(inner.get_milliseconds()) * 1_000_000
            + i128::from(inner.get_microseconds()) * 1_000
            + i128::from(inner.get_nanoseconds());
        Ok(Self {
            inner,
            months,
            days,
            time_nanos,
        })
    }
}

impl fmt::Display for DurationValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let months = self.total_months();
        let days = self.total_days();
        let nanos = self.total_time_nanos();

        if months == 0 && days == 0 && nanos == 0 {
            return f.write_str("PT0S");
        }

        f.write_str("P")?;

        let years = months / 12;
        let months_of_year = months % 12;
        if years != 0 {
            write!(f, "{years}Y")?;
        }
        if months_of_year != 0 {
            write!(f, "{months_of_year}M")?;
        }
        if days != 0 {
            write!(f, "{days}D")?;
        }

        let (hours, minutes, seconds, nanoseconds) =
            split_time_nanos(nanos).map_err(|_| fmt::Error)?;
        if hours != 0 || minutes != 0 || seconds != 0 || nanoseconds != 0 {
            f.write_str("T")?;
            if hours != 0 {
                write!(f, "{hours}H")?;
            }
            if minutes != 0 {
                write!(f, "{minutes}M")?;
            }
            if seconds != 0 || nanoseconds != 0 {
                write_duration_seconds(f, seconds, nanoseconds)?;
            }
        }

        Ok(())
    }
}

impl FromStr for DurationValue {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_open_cypher_duration(s).or_else(|_| {
            let inner = s
                .parse::<Span>()
                .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))?;
            Self::from_jiff_span(inner).map_err(|err| DecodeError::InvalidTemporal(err.to_string()))
        })
    }
}

impl PartialEq for DurationValue {
    fn eq(&self, other: &Self) -> bool {
        self.total_months() == other.total_months()
            && self.total_days() == other.total_days()
            && self.total_time_nanos() == other.total_time_nanos()
    }
}

impl Eq for DurationValue {}

pub fn temporal_component(
    value: PropertyValueRef<'_>,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    match value {
        PropertyValueRef::Null => Ok(Some(PropertyValue::Null)),
        PropertyValueRef::Date(value) => date_component(value, key),
        PropertyValueRef::LocalTime(value) => local_time_component(value, key),
        PropertyValueRef::ZonedTime(value) => zoned_time_component(value, key),
        PropertyValueRef::LocalDateTime(value) => local_datetime_component(value, key),
        PropertyValueRef::ZonedDateTime(value) => zoned_datetime_component(value, key),
        PropertyValueRef::Duration(value) => duration_component(value, key),
        _ => Ok(None),
    }
}

fn integer(value: impl Into<i64>) -> PropertyValue {
    PropertyValue::Integer(value.into())
}

fn string(value: impl Into<String>) -> PropertyValue {
    PropertyValue::String(value.into())
}

fn date_component(value: &DateValue, key: &str) -> Result<Option<PropertyValue>, EncodeError> {
    let component = match key {
        "year" => integer(value.year() as i64),
        "quarter" => integer(value.quarter() as i64),
        "month" => integer(value.month() as i64),
        "week" => integer(value.iso_week()? as i64),
        "weekYear" => integer(value.iso_week_year()? as i64),
        "day" => integer(value.day() as i64),
        "ordinalDay" => integer(value.ordinal_day()? as i64),
        "weekDay" => integer(value.iso_day_of_week() as i64),
        "dayOfQuarter" => integer(value.day_of_quarter()? as i64),
        _ => return Ok(None),
    };
    Ok(Some(component))
}

fn local_time_component(
    value: &LocalTimeValue,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    let nanos = value.nanos();
    let component = match key {
        "hour" => integer(value.hour() as i64),
        "minute" => integer(value.minute() as i64),
        "second" => integer(value.second() as i64),
        "millisecond" => integer((nanos / 1_000_000) as i64),
        "microsecond" => integer((nanos / 1_000) as i64),
        "nanosecond" => integer(nanos as i64),
        _ => return Ok(None),
    };
    Ok(Some(component))
}

fn zoned_time_component(
    value: &ZonedTimeValue,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    if let Some(component) = local_time_component(&value.time, key)? {
        return Ok(Some(component));
    }
    let component = match key {
        "timezone" | "offset" => string(value.offset.to_string()),
        "offsetMinutes" => integer(i64::from(value.offset.seconds_east() / 60)),
        "offsetSeconds" => integer(i64::from(value.offset.seconds_east())),
        _ => return Ok(None),
    };
    Ok(Some(component))
}

fn local_datetime_component(
    value: &LocalDateTimeValue,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    if let Some(component) = date_component(&value.date(), key)? {
        return Ok(Some(component));
    }
    local_time_component(&value.time(), key)
}

fn zoned_datetime_component(
    value: &ZonedDateTimeValue,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    if let Some(component) = local_datetime_component(&value.datetime, key)? {
        return Ok(Some(component));
    }
    let component = match key {
        "timezone" => string(
            value
                .zone_id
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| value.offset.to_string()),
        ),
        "offset" => string(value.offset.to_string()),
        "offsetMinutes" => integer(i64::from(value.offset.seconds_east() / 60)),
        "offsetSeconds" => integer(i64::from(value.offset.seconds_east())),
        "epochSeconds" => integer(zoned_datetime_epoch_seconds(value)?),
        "epochMillis" => integer(zoned_datetime_epoch_millis(value)?),
        _ => return Ok(None),
    };
    Ok(Some(component))
}

fn zoned_datetime_epoch_seconds(value: &ZonedDateTimeValue) -> Result<i64, EncodeError> {
    let (day, nanos) = value.utc_epoch_day_and_nanos();
    checked_i64_from_i128(i128::from(day) * 86_400 + i128::from(nanos / NANOS_PER_SECOND))
}

fn zoned_datetime_epoch_millis(value: &ZonedDateTimeValue) -> Result<i64, EncodeError> {
    let (day, nanos) = value.utc_epoch_day_and_nanos();
    checked_i64_from_i128(i128::from(day) * 86_400_000 + i128::from(nanos / 1_000_000))
}

fn duration_component(
    value: &DurationValue,
    key: &str,
) -> Result<Option<PropertyValue>, EncodeError> {
    let months = value.total_months();
    let days = value.total_days();
    let nanos = value.total_time_nanos();
    let nanos_per_hour = i128::from(NANOS_PER_HOUR);
    let nanos_per_minute = i128::from(NANOS_PER_MINUTE);
    let nanos_per_second = i128::from(NANOS_PER_SECOND);
    let component = match key {
        "years" => integer(months / 12),
        "quarters" => integer(value.total_quarters()),
        "months" => integer(months),
        "weeks" => integer(value.total_weeks()),
        "days" => integer(days),
        "hours" => integer(checked_i64_from_i128(nanos.div_euclid(nanos_per_hour))?),
        "minutes" => integer(checked_i64_from_i128(nanos.div_euclid(nanos_per_minute))?),
        "seconds" => integer(checked_i64_from_i128(nanos.div_euclid(nanos_per_second))?),
        "milliseconds" => integer(checked_i64_from_i128(nanos.div_euclid(1_000_000))?),
        "microseconds" => integer(checked_i64_from_i128(nanos.div_euclid(1_000))?),
        "nanoseconds" => integer(checked_i64_from_i128(nanos)?),
        "quartersOfYear" => integer((months % 12) / 3),
        "monthsOfQuarter" => integer(months % 3),
        "monthsOfYear" => integer(months % 12),
        "daysOfWeek" => integer(days % 7),
        "minutesOfHour" => integer(checked_i64_from_i128(
            nanos.div_euclid(nanos_per_minute).rem_euclid(60),
        )?),
        "secondsOfMinute" => integer(checked_i64_from_i128(
            nanos.div_euclid(nanos_per_second).rem_euclid(60),
        )?),
        "millisecondsOfSecond" => integer(checked_i64_from_i128(
            nanos.div_euclid(1_000_000).rem_euclid(1000),
        )?),
        "microsecondsOfSecond" => integer(checked_i64_from_i128(
            nanos.div_euclid(1_000).rem_euclid(1_000_000),
        )?),
        "nanosecondsOfSecond" => {
            integer(checked_i64_from_i128(nanos.rem_euclid(nanos_per_second))?)
        }
        _ => return Ok(None),
    };
    Ok(Some(component))
}

fn epoch_day_from_ymd(year: i32, month: u8, day: u8) -> i64 {
    let month = month as i64;
    let day = day as i64;
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn ymd_from_epoch_day(epoch_day: i64) -> Result<(i32, u8, u8), EncodeError> {
    let z = epoch_day + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);

    let year = i32::try_from(year)
        .map_err(|_| EncodeError::InvalidTemporal("year must be in -9999..=9999"))?;
    let month =
        u8::try_from(month).map_err(|_| EncodeError::InvalidTemporal("month must be in 1..=12"))?;
    let day =
        u8::try_from(day).map_err(|_| EncodeError::InvalidTemporal("day must be in 1..=31"))?;
    Ok((year, month, day))
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

fn day_of_week_from_epoch_day(epoch_day: i64) -> u8 {
    (epoch_day + 3).rem_euclid(7) as u8 + 1
}

fn iso_week_one_monday_epoch_day(year: i32) -> Result<i64, EncodeError> {
    let jan4 = DateValue::new(year, 1, 4)?.epoch_day();
    Ok(jan4 - (i64::from(day_of_week_from_epoch_day(jan4)) - 1))
}

fn split_whole_fraction(value: f64) -> Result<(i64, f64), EncodeError> {
    if !value.is_finite() {
        return Err(EncodeError::InvalidTemporal(
            "duration field must be finite",
        ));
    }
    let whole = value.trunc();
    if whole < i64::MIN as f64 || whole > i64::MAX as f64 {
        return Err(EncodeError::InvalidTemporal(
            "duration field outside supported range",
        ));
    }
    Ok((whole as i64, value - whole))
}

fn split_time_nanos(value: i128) -> Result<(i64, i64, i64, i64), EncodeError> {
    let nanos_per_hour = i128::from(NANOS_PER_HOUR);
    let nanos_per_minute = i128::from(NANOS_PER_MINUTE);
    let nanos_per_second = i128::from(NANOS_PER_SECOND);

    let hours = value / nanos_per_hour;
    let rem = value % nanos_per_hour;
    let minutes = rem / nanos_per_minute;
    let rem = rem % nanos_per_minute;
    let seconds = rem / nanos_per_second;
    let nanoseconds = rem % nanos_per_second;

    Ok((
        checked_i64_from_i128(hours)?,
        checked_i64_from_i128(minutes)?,
        checked_i64_from_i128(seconds)?,
        checked_i64_from_i128(nanoseconds)?,
    ))
}

fn write_duration_seconds(
    f: &mut fmt::Formatter<'_>,
    seconds: i64,
    nanoseconds: i64,
) -> fmt::Result {
    let total = i128::from(seconds) * i128::from(NANOS_PER_SECOND) + i128::from(nanoseconds);
    let sign = if total < 0 { "-" } else { "" };
    let abs = total.abs();
    let whole = abs / i128::from(NANOS_PER_SECOND);
    let frac = abs % i128::from(NANOS_PER_SECOND);
    if frac == 0 {
        write!(f, "{sign}{whole}S")
    } else {
        let mut frac = format!("{frac:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        write!(f, "{sign}{whole}.{frac}S")
    }
}

fn checked_i64_from_i128(value: i128) -> Result<i64, EncodeError> {
    i64::try_from(value)
        .map_err(|_| EncodeError::InvalidTemporal("duration field outside supported range"))
}

fn round_ties_even_with_float_tolerance(value: f64) -> f64 {
    let whole = value.trunc();
    let fraction = value - whole;
    let half_distance = (fraction.abs() - 0.5).abs();
    let tolerance = f64::EPSILON * value.abs().max(1.0) * 8.0;
    if half_distance <= tolerance {
        let whole_i128 = whole as i128;
        if whole_i128 % 2 == 0 {
            whole
        } else {
            whole + fraction.signum()
        }
    } else {
        value.round()
    }
}

fn checked_i128_from_rounded_f64(value: f64) -> Result<i128, EncodeError> {
    if !value.is_finite() {
        return Err(EncodeError::InvalidTemporal(
            "duration field must be finite",
        ));
    }
    let rounded = round_ties_even_with_float_tolerance(value);
    if rounded < i128::MIN as f64 || rounded > i128::MAX as f64 {
        return Err(EncodeError::InvalidTemporal(
            "duration field outside supported range",
        ));
    }
    Ok(rounded as i128)
}

fn checked_i32_from_i128(value: i128) -> Result<i32, EncodeError> {
    i32::try_from(value)
        .map_err(|_| EncodeError::InvalidTemporal("duration field outside supported range"))
}

fn timestamp_from_epoch_day_and_nanos(day: i64, nanos: i64) -> Result<Timestamp, EncodeError> {
    if !(0..NANOS_PER_DAY).contains(&nanos) {
        return Err(EncodeError::InvalidTemporal(
            "nanos since midnight must be in 0..<24h",
        ));
    }
    let total_seconds = i128::from(day) * 86_400 + i128::from(nanos.div_euclid(NANOS_PER_SECOND));
    let seconds = checked_i64_from_i128(total_seconds)?;
    let subsec_nanos = i32::try_from(nanos.rem_euclid(NANOS_PER_SECOND))
        .map_err(|_| EncodeError::InvalidTemporal("nanoseconds out of range"))?;
    Timestamp::new(seconds, subsec_nanos)
        .map_err(|_| EncodeError::InvalidTemporal("epoch datetime out of range"))
}

fn parse_open_cypher_date(s: &str) -> Result<DateValue, DecodeError> {
    if let Ok(inner) = s.parse::<Date>() {
        return Ok(DateValue { inner });
    }

    if let Some(w_pos) = s.find('W') {
        let year_part = s[..w_pos].strip_suffix('-').unwrap_or(&s[..w_pos]);
        let year = parse_i32_digits(year_part, "invalid ISO week year")?;
        let tail = &s[w_pos + 1..];
        let (week, day) = match tail.split_once('-') {
            Some((week, day)) => (
                parse_u8_digits(week, "invalid ISO week")?,
                Some(parse_u8_digits(day, "invalid ISO day")?),
            ),
            None if tail.len() == 2 => (parse_u8_digits(tail, "invalid ISO week")?, None),
            None if tail.len() == 3 => (
                parse_u8_digits(&tail[..2], "invalid ISO week")?,
                Some(parse_u8_digits(&tail[2..], "invalid ISO day")?),
            ),
            _ => return Err(DecodeError::InvalidTemporal("invalid ISO week date".into())),
        };
        return DateValue::from_iso_week(year, week, day)
            .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    if s.len() == 4 && is_ascii_digits(s) {
        return DateValue::from_calendar(parse_i32_digits(s, "invalid year")?, None, None)
            .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    if s.len() == 6 && is_ascii_digits(s) {
        return DateValue::from_calendar(
            parse_i32_digits(&s[..4], "invalid year")?,
            Some(parse_u8_digits(&s[4..], "invalid month")?),
            None,
        )
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    if s.len() == 7 && is_ascii_digits(s) {
        return DateValue::from_ordinal_day(
            parse_i32_digits(&s[..4], "invalid year")?,
            parse_u16_digits(&s[4..], "invalid ordinal day")?,
        )
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    if s.len() == 7 && &s[4..5] == "-" {
        return DateValue::from_calendar(
            parse_i32_digits(&s[..4], "invalid year")?,
            Some(parse_u8_digits(&s[5..], "invalid month")?),
            None,
        )
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    if s.len() == 8 && &s[4..5] == "-" {
        return DateValue::from_ordinal_day(
            parse_i32_digits(&s[..4], "invalid year")?,
            parse_u16_digits(&s[5..], "invalid ordinal day")?,
        )
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    Err(DecodeError::InvalidTemporal("invalid date".into()))
}

fn parse_open_cypher_duration(s: &str) -> Result<DurationValue, DecodeError> {
    let rest = s
        .strip_prefix('P')
        .ok_or_else(|| DecodeError::InvalidTemporal("duration must start with P".into()))?;
    if rest.contains('-') {
        if let Ok(value) = parse_alternative_duration(rest) {
            return Ok(value);
        }
    }

    let mut parts = DurationMapParts::default();
    let mut integer_parts = DurationIntegerParts::default();
    let mut integer_only = true;
    let mut in_time = false;
    let mut number_start: Option<usize> = None;

    for (idx, ch) in rest.char_indices() {
        if ch == 'T' {
            if number_start.is_some() {
                return Err(DecodeError::InvalidTemporal(
                    "missing duration designator".into(),
                ));
            }
            in_time = true;
            continue;
        }

        if ch.is_ascii_digit() || ch == '.' || ((ch == '+' || ch == '-') && number_start.is_none())
        {
            number_start.get_or_insert(idx);
            continue;
        }

        let start = number_start
            .take()
            .ok_or_else(|| DecodeError::InvalidTemporal("missing duration field value".into()))?;
        let field = &rest[start..idx];
        if field.contains('.') {
            integer_only = false;
        } else {
            let value = parse_i64_duration_field(field, "invalid duration field")?;
            match (ch, in_time) {
                ('Y', false) => integer_parts.years = value,
                ('M', false) => integer_parts.months = value,
                ('W', false) => integer_parts.weeks = value,
                ('D', false) => integer_parts.days = value,
                ('H', true) => integer_parts.hours = value,
                ('M', true) => integer_parts.minutes = value,
                ('S', true) => integer_parts.seconds = value,
                _ => {}
            }
        }

        let value = parse_f64_digits(field, "invalid duration field")?;
        match (ch, in_time) {
            ('Y', false) => parts.years = value,
            ('M', false) => parts.months = value,
            ('W', false) => parts.weeks = value,
            ('D', false) => parts.days = value,
            ('H', true) => parts.hours = value,
            ('M', true) => parts.minutes = value,
            ('S', true) => parts.seconds = value,
            _ => {
                return Err(DecodeError::InvalidTemporal(
                    "invalid duration designator".into(),
                ))
            }
        }
    }

    if number_start.is_some() {
        return Err(DecodeError::InvalidTemporal(
            "missing duration designator".into(),
        ));
    }

    if integer_only {
        return DurationValue::from_parts(
            integer_parts.years,
            integer_parts.months,
            integer_parts.weeks,
            integer_parts.days,
            integer_parts.hours,
            integer_parts.minutes,
            integer_parts.seconds,
            0,
            0,
            0,
        )
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()));
    }

    DurationValue::from_open_cypher_parts(parts)
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))
}

fn parse_alternative_duration(rest: &str) -> Result<DurationValue, DecodeError> {
    let (date, time) = rest
        .split_once('T')
        .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration".into()))?;
    let mut date_parts = date.split('-');
    let years = parse_f64_digits(
        date_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration year".into()))?,
        "invalid duration year",
    )?;
    let months = parse_f64_digits(
        date_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration month".into()))?,
        "invalid duration month",
    )?;
    let days = parse_f64_digits(
        date_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration day".into()))?,
        "invalid duration day",
    )?;
    if date_parts.next().is_some() {
        return Err(DecodeError::InvalidTemporal("invalid duration date".into()));
    }

    let mut time_parts = time.split(':');
    let hours = parse_f64_digits(
        time_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration hour".into()))?,
        "invalid duration hour",
    )?;
    let minutes = parse_f64_digits(
        time_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration minute".into()))?,
        "invalid duration minute",
    )?;
    let seconds = parse_f64_digits(
        time_parts
            .next()
            .ok_or_else(|| DecodeError::InvalidTemporal("invalid duration second".into()))?,
        "invalid duration second",
    )?;
    if time_parts.next().is_some() {
        return Err(DecodeError::InvalidTemporal("invalid duration time".into()));
    }

    DurationValue::from_open_cypher_parts(DurationMapParts {
        years,
        months,
        days,
        hours,
        minutes,
        seconds,
        ..DurationMapParts::default()
    })
    .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))
}

fn is_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

fn parse_i32_digits(value: &str, msg: &str) -> Result<i32, DecodeError> {
    if !is_ascii_digits(value) {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    value
        .parse::<i32>()
        .map_err(|_| DecodeError::InvalidTemporal(msg.into()))
}

fn parse_u8_digits(value: &str, msg: &str) -> Result<u8, DecodeError> {
    if !is_ascii_digits(value) {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    value
        .parse::<u8>()
        .map_err(|_| DecodeError::InvalidTemporal(msg.into()))
}

fn parse_u16_digits(value: &str, msg: &str) -> Result<u16, DecodeError> {
    if !is_ascii_digits(value) {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    value
        .parse::<u16>()
        .map_err(|_| DecodeError::InvalidTemporal(msg.into()))
}

fn parse_i64_duration_field(value: &str, msg: &str) -> Result<i64, DecodeError> {
    if value.is_empty()
        || matches!(value, "+" | "-")
        || !value
            .trim_start_matches(['+', '-'])
            .bytes()
            .all(|b| b.is_ascii_digit())
    {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    value
        .parse::<i64>()
        .map_err(|_| DecodeError::InvalidTemporal(msg.into()))
}

fn parse_f64_digits(value: &str, msg: &str) -> Result<f64, DecodeError> {
    if value.is_empty() {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    let parsed = value
        .parse::<f64>()
        .map_err(|_| DecodeError::InvalidTemporal(msg.into()))?;
    if !parsed.is_finite() {
        return Err(DecodeError::InvalidTemporal(msg.into()));
    }
    Ok(parsed)
}

fn parse_offset(s: &str) -> Result<UtcOffsetValue, DecodeError> {
    if s == "Z" {
        return Ok(UtcOffsetValue::utc());
    }
    if !matches!(s.len(), 3 | 5 | 6 | 7 | 9) {
        return Err(DecodeError::InvalidTemporal(
            "offset must be Z, ±HH, ±HHMM, ±HH:MM, ±HHMMSS or ±HH:MM:SS".into(),
        ));
    }
    let sign = match &s[0..1] {
        "+" => 1,
        "-" => -1,
        _ => {
            return Err(DecodeError::InvalidTemporal(
                "offset must start with '+' or '-'".into(),
            ))
        }
    };
    let hours = s[1..3]
        .parse::<i32>()
        .map_err(|_| DecodeError::InvalidTemporal("invalid offset hour".into()))?;
    let mut minutes = 0;
    let mut seconds = 0;
    if s.len() == 5 || s.len() == 7 {
        minutes = s[3..5]
            .parse::<i32>()
            .map_err(|_| DecodeError::InvalidTemporal("invalid offset minute".into()))?;
        if s.len() == 7 {
            seconds = s[5..7]
                .parse::<i32>()
                .map_err(|_| DecodeError::InvalidTemporal("invalid offset second".into()))?;
        }
    } else if s.len() >= 6 {
        if &s[3..4] != ":" {
            return Err(DecodeError::InvalidTemporal(
                "offset separator must be ':'".into(),
            ));
        }
        minutes = s[4..6]
            .parse::<i32>()
            .map_err(|_| DecodeError::InvalidTemporal("invalid offset minute".into()))?;
    }
    if s.len() == 9 {
        if &s[6..7] != ":" {
            return Err(DecodeError::InvalidTemporal(
                "offset separator must be ':'".into(),
            ));
        }
        seconds = s[7..9]
            .parse::<i32>()
            .map_err(|_| DecodeError::InvalidTemporal("invalid offset second".into()))?;
    }
    if minutes > 59 || seconds > 59 {
        return Err(DecodeError::InvalidTemporal(
            "offset minute and second must be in 0..=59".into(),
        ));
    }
    let seconds_east = sign * (hours * 3600 + minutes * 60 + seconds);
    UtcOffsetValue::new(seconds_east).map_err(|err| DecodeError::InvalidTemporal(err.to_string()))
}

fn split_zone_suffix(s: &str) -> Result<(&str, Option<&str>), DecodeError> {
    match s.rsplit_once('[') {
        Some((prefix, zone)) if zone.ends_with(']') => Ok((prefix, Some(&zone[..zone.len() - 1]))),
        Some(_) => Err(DecodeError::InvalidTemporal(
            "zone id suffix must end with ']'".into(),
        )),
        None => Ok((s, None)),
    }
}

fn find_offset_start(s: &str, has_date: bool) -> Option<usize> {
    if s.ends_with('Z') {
        return Some(s.len() - 1);
    }
    let start = if has_date {
        s.find('T').map(|idx| idx + 1)?
    } else {
        1
    };
    let bytes = s.as_bytes();
    for idx in start..bytes.len() {
        if bytes[idx] == b'+' || bytes[idx] == b'-' {
            return Some(idx);
        }
    }
    None
}

#[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
fn parse_zoned_datetime_with_jiff(s: &str) -> Result<ZonedDateTimeValue, DecodeError> {
    let zoned = s
        .parse::<jiff::Zoned>()
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))?;
    let zone_id = zoned
        .time_zone()
        .iana_name()
        .map(|name| Box::<str>::from(name.to_owned()));
    let offset = zoned
        .offset()
        .to_string()
        .parse::<UtcOffsetValue>()
        .map_err(|err| DecodeError::InvalidTemporal(err.to_string()))?;
    Ok(ZonedDateTimeValue {
        datetime: LocalDateTimeValue {
            inner: zoned.datetime(),
        },
        offset,
        zone_id,
    })
}

#[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
fn validate_zoned_components(
    datetime: &LocalDateTimeValue,
    offset: &UtcOffsetValue,
    zone_id: &str,
) -> Result<(), String> {
    let candidate = format!("{}{}[{zone_id}]", datetime, offset);
    candidate
        .parse::<jiff::Zoned>()
        .map(|_| ())
        .map_err(|err| err.to_string())
}

#[cfg(not(any(feature = "tzdb-system", feature = "tzdb-bundled")))]
fn validate_zoned_components(
    _datetime: &LocalDateTimeValue,
    _offset: &UtcOffsetValue,
    _zone_id: &str,
) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage_display(value: crate::property::PropertyValue) -> String {
        let storage = crate::codec::encode_property_value(&value).unwrap();
        crate::render::storage_value_to_display_text(storage.as_ref()).unwrap()
    }

    fn storage_roundtrip(value: crate::property::PropertyValue) -> crate::property::PropertyValue {
        let storage = crate::codec::encode_property_value(&value).unwrap();
        crate::codec::decode_property_value(storage.as_ref()).unwrap()
    }

    #[test]
    fn parses_and_formats_date() {
        let value = DateValue::new(2026, 4, 6).unwrap();
        assert_eq!(value.to_string(), "2026-04-06");
        assert_eq!("2026-04-06".parse::<DateValue>().unwrap(), value);
    }

    #[test]
    fn parses_and_formats_local_time() {
        let value = LocalTimeValue::new(12, 0, 1, 120_000_000).unwrap();
        assert_eq!(value.to_string(), "12:00:01.12");
        assert_eq!("12:00:01.12".parse::<LocalTimeValue>().unwrap(), value);
    }

    #[test]
    fn parses_and_formats_duration() {
        let value = "P2M10DT2H30M".parse::<DurationValue>().unwrap();
        assert_eq!(value.to_string(), "P2M10DT2H30M");
        assert!(value.as_jiff().is_some());
    }

    #[test]
    fn parses_offset() {
        let value = "+01:00".parse::<UtcOffsetValue>().unwrap();
        assert_eq!(value.seconds_east(), 3600);
        assert_eq!(value.to_string(), "+01:00");
    }

    #[test]
    fn parses_compact_hour_offset_and_canonicalizes() {
        let value = "+01".parse::<UtcOffsetValue>().unwrap();
        assert_eq!(value.seconds_east(), 3600);
        assert_eq!(value.to_string(), "+01:00");
    }

    #[test]
    fn parses_negative_half_hour_offset() {
        let value = "-05:30".parse::<UtcOffsetValue>().unwrap();
        assert_eq!(value.seconds_east(), -(5 * 3600 + 30 * 60));
        assert_eq!(value.to_string(), "-05:30");
    }

    #[test]
    fn parses_utc_and_second_precision_offsets() {
        let utc = "Z".parse::<UtcOffsetValue>().unwrap();
        assert_eq!(utc.seconds_east(), 0);
        assert_eq!(utc.to_string(), "Z");

        let value = "+02:05:59".parse::<UtcOffsetValue>().unwrap();
        assert_eq!(value.seconds_east(), 2 * 3600 + 5 * 60 + 59);
        assert_eq!(value.to_string(), "+02:05:59");
    }

    #[test]
    fn parses_zoned_time() {
        let value = "12:00:00+01:00".parse::<ZonedTimeValue>().unwrap();
        assert_eq!(value.to_string(), "12:00:00+01:00");
    }

    #[test]
    fn parses_compact_zoned_time_and_canonicalizes() {
        let value = "12:00:00+01".parse::<ZonedTimeValue>().unwrap();
        assert_eq!(value.to_string(), "12:00:00+01:00");
    }

    #[test]
    fn parses_offset_only_zoned_datetime() {
        let value = "2026-04-06T12:00:00+01:00"
            .parse::<ZonedDateTimeValue>()
            .unwrap();
        assert_eq!(value.to_string(), "2026-04-06T12:00:00+01:00");
    }

    #[test]
    fn constructs_temporal1_date_forms() {
        assert_eq!(
            DateValue::from_iso_week(1817, 2, Some(2))
                .unwrap()
                .to_string(),
            "1817-01-07"
        );
        assert_eq!(
            DateValue::from_ordinal_day(1984, 202).unwrap().to_string(),
            "1984-07-20"
        );
        assert_eq!(
            DateValue::from_quarter(1984, 3, Some(45))
                .unwrap()
                .to_string(),
            "1984-08-14"
        );
    }

    #[test]
    fn constructs_epoch_datetime_and_open_cypher_duration() {
        assert_eq!(
            ZonedDateTimeValue::from_epoch(416_779, 999_999_999)
                .unwrap()
                .to_string(),
            "1970-01-05T19:46:19.999999999Z"
        );
        assert_eq!(
            ZonedDateTimeValue::from_epoch_millis(237_821_673_987)
                .unwrap()
                .to_string(),
            "1977-07-15T13:34:33.987Z"
        );

        let duration = DurationValue::from_open_cypher_parts(DurationMapParts {
            months: 0.75,
            ..DurationMapParts::default()
        })
        .unwrap();
        assert_eq!(duration.to_string(), "P22DT19H51M49.5S");
    }

    #[test]
    fn parses_temporal2_date_string_forms() {
        assert_eq!(
            "2015-07-01",
            "2015-07".parse::<DateValue>().unwrap().to_string()
        );
        assert_eq!(
            "2015-07-01",
            "201507".parse::<DateValue>().unwrap().to_string()
        );
        assert_eq!(
            "2015-07-21",
            "2015-W30-2".parse::<DateValue>().unwrap().to_string()
        );
        assert_eq!(
            "2015-07-20",
            "2015W30".parse::<DateValue>().unwrap().to_string()
        );
        assert_eq!(
            "2015-07-21",
            "2015-202".parse::<DateValue>().unwrap().to_string()
        );
        assert_eq!(
            "2015-01-01",
            "2015".parse::<DateValue>().unwrap().to_string()
        );
    }

    #[test]
    fn parses_temporal2_datetime_string_forms() {
        assert_eq!(
            "2015-07-21T21:40:32.142",
            storage_display(crate::property::PropertyValue::LocalDateTime(
                "2015-W30-2T214032.142"
                    .parse::<LocalDateTimeValue>()
                    .unwrap()
            ))
        );
        assert_eq!(
            "2015-07-21T21:40:32+01:00",
            storage_display(crate::property::PropertyValue::ZonedDateTime(
                "2015-202T21:40:32+0100"
                    .parse::<ZonedDateTimeValue>()
                    .unwrap()
            ))
        );
        assert_eq!(
            "2015-07-21T21:40",
            storage_display(crate::property::PropertyValue::LocalDateTime(
                "20150721T21:40".parse::<LocalDateTimeValue>().unwrap()
            ))
        );
        assert_eq!(
            "2015-07-20T21:40-02:00",
            storage_display(crate::property::PropertyValue::ZonedDateTime(
                "2015-W30T2140-02".parse::<ZonedDateTimeValue>().unwrap()
            ))
        );
    }

    #[test]
    fn parses_temporal2_duration_string_forms() {
        assert_eq!(
            "P5M1DT12H",
            "P5M1.5D".parse::<DurationValue>().unwrap().to_string()
        );
        assert_eq!(
            "P22DT19H51M49.5S",
            "P0.75M".parse::<DurationValue>().unwrap().to_string()
        );
        assert_eq!(
            "PT45S",
            "PT0.75M".parse::<DurationValue>().unwrap().to_string()
        );
        assert_eq!(
            "P17DT12H",
            "P2.5W".parse::<DurationValue>().unwrap().to_string()
        );
        assert_eq!(
            "P12Y5M14DT16H13M10S",
            "P12Y5M14DT16H12M70S"
                .parse::<DurationValue>()
                .unwrap()
                .to_string()
        );
        assert_eq!(
            "P13DT40H13M10S",
            "P13DT40H13M10S"
                .parse::<DurationValue>()
                .unwrap()
                .to_string()
        );
        assert_eq!(
            "P2012Y2M2DT14H37M21.545S",
            "P2012-02-02T14:37:21.545"
                .parse::<DurationValue>()
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn duration_preserves_open_cypher_component_groups() {
        let base = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.0,
            months: 5.0,
            days: 14.0,
            hours: 16.0,
            minutes: 12.0,
            seconds: 70.0,
            ..DurationMapParts::default()
        })
        .unwrap();
        let same_time_group = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.0,
            months: 5.0,
            days: 14.0,
            hours: 16.0,
            minutes: 13.0,
            seconds: 10.0,
            ..DurationMapParts::default()
        })
        .unwrap();
        let different_day_group = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.0,
            months: 5.0,
            days: 13.0,
            hours: 40.0,
            minutes: 13.0,
            seconds: 10.0,
            ..DurationMapParts::default()
        })
        .unwrap();

        assert_eq!(base, same_time_group);
        assert_ne!(base, different_day_group);
        assert_eq!(base.to_string(), "P12Y5M14DT16H13M10S");
        assert_eq!(different_day_group.to_string(), "P12Y5M13DT40H13M10S");
    }

    #[test]
    fn negative_duration_components_use_floor_seconds() {
        let duration =
            DurationValue::from_parts(0, 0, 0, 0, -23, -59, -59, 0, 0, -900_000_000).unwrap();
        let value = crate::property::PropertyValue::Duration(duration);

        assert_eq!(storage_display(value.clone()), "PT-23H-59M-59.9S");
        assert_eq!(
            temporal_component(value.as_ref(), "seconds").unwrap(),
            Some(crate::property::PropertyValue::Integer(-86_400))
        );
        assert_eq!(
            temporal_component(value.as_ref(), "nanosecondsOfSecond").unwrap(),
            Some(crate::property::PropertyValue::Integer(100_000_000))
        );
        assert_eq!(
            temporal_component(value.as_ref(), "secondsOfMinute").unwrap(),
            Some(crate::property::PropertyValue::Integer(0))
        );
        assert_eq!(
            temporal_component(value.as_ref(), "millisecondsOfSecond").unwrap(),
            Some(crate::property::PropertyValue::Integer(100))
        );
    }

    #[test]
    fn duration_supports_large_open_cypher_groups() {
        let calendar =
            DurationValue::from_parts(1_999_999_998, 11, 0, 30, 0, 0, 0, 0, 0, 0).unwrap();
        assert_eq!(calendar.years(), 1_999_999_998);
        assert_eq!(calendar.days(), 30);
        assert!(calendar.as_jiff().is_none());
        let calendar_value = crate::property::PropertyValue::Duration(calendar);
        assert_eq!(
            storage_display(calendar_value.clone()),
            "P1999999998Y11M30D"
        );
        assert_eq!(storage_roundtrip(calendar_value.clone()), calendar_value);

        let seconds =
            DurationValue::from_parts(0, 0, 0, 0, 17_531_639_991_215, 59, 59, 0, 0, 0).unwrap();
        assert_eq!(seconds.hours(), 17_531_639_991_215);
        assert!(seconds.as_jiff().is_none());
        let seconds_value = crate::property::PropertyValue::Duration(seconds);
        assert_eq!(
            storage_display(seconds_value.clone()),
            "PT17531639991215H59M59S"
        );
        assert_eq!(storage_roundtrip(seconds_value.clone()), seconds_value);
    }

    #[test]
    fn duration_fractional_nanoseconds_use_ties_even_rounding() {
        for (nanoseconds, expected) in [
            (0.5, "PT0S"),
            (0.6, "PT0.000000001S"),
            (1.5, "PT0.000000002S"),
            (-0.5, "PT0S"),
            (-0.6, "PT-0.000000001S"),
            (-1.5, "PT-0.000000002S"),
        ] {
            let duration = DurationValue::from_open_cypher_parts(DurationMapParts {
                nanoseconds,
                ..DurationMapParts::default()
            })
            .unwrap();
            assert_eq!(duration.to_string(), expected);
        }
    }

    #[test]
    fn duration_fractional_years_cascade_through_months() {
        let duration = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.5,
            months: 5.5,
            days: 14.5,
            hours: 16.5,
            minutes: 12.5,
            seconds: 70.5,
            nanoseconds: 3.0,
            ..DurationMapParts::default()
        })
        .unwrap();

        assert_eq!(duration.to_string(), "P12Y11M29DT33H58M13.500000003S");
    }

    #[test]
    fn temporal8_date_time_and_datetime_arithmetic() {
        let duration = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.5,
            months: 5.5,
            days: 14.5,
            hours: 16.5,
            minutes: 12.5,
            seconds: 70.5,
            nanoseconds: 3.0,
            ..DurationMapParts::default()
        })
        .unwrap();

        let date = DateValue::new(1984, 10, 11).unwrap();
        assert_eq!(
            date.checked_add_duration(&duration).unwrap().to_string(),
            "1997-10-11"
        );
        assert_eq!(
            date.checked_sub_duration(&duration).unwrap().to_string(),
            "1971-10-12"
        );

        let time = LocalTimeValue::new(12, 31, 14, 1).unwrap();
        assert_eq!(
            time.checked_add_duration(&duration).unwrap().to_string(),
            "22:29:27.500000004"
        );
        assert_eq!(
            time.checked_sub_duration(&duration).unwrap().to_string(),
            "02:33:00.499999998"
        );

        let datetime = LocalDateTimeValue::new(1984, 10, 11, 12, 31, 14, 1).unwrap();
        assert_eq!(
            datetime
                .checked_add_duration(&duration)
                .unwrap()
                .to_string(),
            "1997-10-11T22:29:27.500000004"
        );
        assert_eq!(
            datetime
                .checked_sub_duration(&duration)
                .unwrap()
                .to_string(),
            "1971-10-12T02:33:00.499999998"
        );
    }

    #[test]
    fn temporal8_duration_subtract_multiply_and_divide() {
        let base = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.0,
            months: 5.0,
            days: 14.0,
            hours: 16.0,
            minutes: 12.0,
            seconds: 70.0,
            nanoseconds: 1.0,
            ..DurationMapParts::default()
        })
        .unwrap();
        let fractional = DurationValue::from_open_cypher_parts(DurationMapParts {
            years: 12.5,
            months: 5.5,
            days: 14.5,
            hours: 16.5,
            minutes: 12.5,
            seconds: 70.5,
            nanoseconds: 3.0,
            ..DurationMapParts::default()
        })
        .unwrap();

        assert_eq!(
            base.checked_sub_duration(&fractional).unwrap().to_string(),
            "P-6M-15DT-17H-45M-3.500000002S"
        );
        assert_eq!(
            base.checked_mul_number(0.5).unwrap().to_string(),
            "P6Y2M22DT13H21M8S"
        );
        assert_eq!(
            base.checked_div_number(2.0).unwrap().to_string(),
            "P6Y2M22DT13H21M8S"
        );
    }

    #[test]
    fn duration_formats_negative_time_components_without_day_carry() {
        let cases = [
            (
                DurationMapParts {
                    seconds: -2.0,
                    milliseconds: 1.0,
                    ..DurationMapParts::default()
                },
                "PT-1.999S",
            ),
            (
                DurationMapParts {
                    days: 1.0,
                    milliseconds: -1.0,
                    ..DurationMapParts::default()
                },
                "P1DT-0.001S",
            ),
            (
                DurationMapParts {
                    seconds: -60.0,
                    milliseconds: -1.0,
                    ..DurationMapParts::default()
                },
                "PT-1M-0.001S",
            ),
        ];

        for (parts, expected) in cases {
            assert_eq!(
                DurationValue::from_open_cypher_parts(parts)
                    .unwrap()
                    .to_string(),
                expected
            );
        }
    }

    #[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
    #[test]
    fn parses_named_zone_zoned_datetime() {
        let value = "2026-04-06T12:00:00+01:00[Europe/Lisbon]"
            .parse::<ZonedDateTimeValue>()
            .unwrap();
        assert_eq!(
            value.to_string(),
            "2026-04-06T12:00:00+01:00[Europe/Lisbon]"
        );
        assert_eq!(value.zone_id.as_deref(), Some("Europe/Lisbon"));
    }

    #[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
    #[test]
    fn rejects_named_zone_offset_mismatch() {
        let err = "2026-04-06T12:00:00+00:00[Europe/Lisbon]"
            .parse::<ZonedDateTimeValue>()
            .unwrap_err();
        assert!(matches!(err, DecodeError::InvalidTemporal(_)));
    }
}
