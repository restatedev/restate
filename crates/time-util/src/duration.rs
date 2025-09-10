// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use jiff::fmt::{friendly, temporal};
use jiff::{Span, SpanRelativeTo, SpanRound};

static VERBOSE_PRINTER: friendly::SpanPrinter = friendly::SpanPrinter::new()
    .designator(friendly::Designator::Verbose)
    .spacing(friendly::Spacing::BetweenUnitsAndDesignators);
static COMPACT_PRINTER: friendly::SpanPrinter =
    friendly::SpanPrinter::new().designator(friendly::Designator::Compact);

#[inline]
fn print_inner(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let printer = if f.alternate() {
        &VERBOSE_PRINTER
    } else {
        &COMPACT_PRINTER
    };

    printer
        .print_span(span, jiff::fmt::StdFmtWrite(f))
        .map_err(|_| std::fmt::Error)
}

/// Extension trait for [`std::time::Duration`] that provides abstractions for human-friendly
/// printing.
pub trait DurationExt {
    /// zero-cost conversion to [`FriendlyDuration`]
    ///
    /// [`FriendlyDuration`] provides human-friendly options to formatting a duration.
    fn friendly(&self) -> FriendlyDuration;

    fn friendly_non_zero(&self) -> Option<NonZeroFriendlyDuration> {
        self.friendly().to_non_zero()
    }
}

/// Displays a time span with 'days' as the maximum unit.
pub struct Days;
/// Displays a time span with 'seconds' as the maximum unit.
pub struct Seconds;
/// Displays a time span in 'HH::MM::SS[.fff]' format.
pub struct Hms;
/// Displays a time span in ISO 8601 format.
pub struct Iso8601;

mod private {
    pub trait Sealed {}
    impl Sealed for super::Days {}
    impl Sealed for super::Seconds {}
    impl Sealed for super::Hms {}
    impl Sealed for super::Iso8601 {}
}

/// A sealed trait for the different displayable time-span styles.
pub trait Style: private::Sealed {
    fn print_span(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

impl Style for Days {
    fn print_span(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        print_inner(span, f)
    }
}

impl Style for Seconds {
    fn print_span(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        print_inner(span, f)
    }
}

impl Style for Hms {
    fn print_span(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        static HMS_PRINTER: friendly::SpanPrinter =
            friendly::SpanPrinter::new().hours_minutes_seconds(true);

        HMS_PRINTER
            .print_span(span, jiff::fmt::StdFmtWrite(f))
            .map_err(|_| std::fmt::Error)
    }
}

impl Style for Iso8601 {
    fn print_span(span: &Span, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        static ISO_PRINTER: temporal::SpanPrinter = temporal::SpanPrinter::new();

        ISO_PRINTER
            .print_span(span, jiff::fmt::StdFmtWrite(f))
            .map_err(|_| std::fmt::Error)
    }
}

impl DurationExt for StdDuration {
    /// panics if the duration is negative or too large to fit into i64
    fn friendly(&self) -> FriendlyDuration {
        FriendlyDuration::from(*self)
    }

    /// Returns None if duration is zero
    fn friendly_non_zero(&self) -> Option<NonZeroFriendlyDuration> {
        FriendlyDuration::from(*self).to_non_zero()
    }
}

/// A wrapper around [`std::time::Duration`] that provides human-friendly display options.
///
/// # Display
///
/// The default display behaves the same as `to_days_span()`. But you can also use the following
/// conversions to customize the display behaviour:
///
/// - Use [`FriendlyDuration::to_days_span`] to display a duration as a span with its maximum unit
///   set to days.
/// - Use [`FriendlyDuration::to_seconds_span`] to display a duration as a span with its maximum
///   unit set to seconds.
/// - Use [`FriendlyDuration::to_hms_span`] to display a duration as a span that's displayed as
///   `HH:MM:SS`.
/// - Use [`FriendlyDuration::to_iso8601_span`] to display a duration as a span that's displayed as
///   `HH:MM:SS`.
///
/// # Parsing
///
/// This uses [`jiff::Span`]'s parsing to support both human-friendly and ISO8601
/// inputs. Days are the largest supported unit of time, and are interpreted as 24 hours long when
/// converting the parsed span into an actual duration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Duration<const CAN_BE_ZERO: bool = true>(StdDuration);

/// A duration that's allowed to be zero
pub type FriendlyDuration = Duration<true>;

/// A non-zero friendly duration is just a duration with a zero check
pub type NonZeroFriendlyDuration = Duration<false>;

impl<const CAN_BE_ZERO: bool> AsRef<StdDuration> for Duration<CAN_BE_ZERO> {
    fn as_ref(&self) -> &StdDuration {
        &self.0
    }
}

impl<const CAN_BE_ZERO: bool> std::ops::Deref for Duration<CAN_BE_ZERO> {
    type Target = StdDuration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const CAN_BE_ZERO: bool> PartialEq<StdDuration> for Duration<CAN_BE_ZERO> {
    fn eq(&self, other: &StdDuration) -> bool {
        &self.0 == other
    }
}

impl<const CAN_BE_ZERO: bool> PartialEq<Duration<CAN_BE_ZERO>> for StdDuration {
    fn eq(&self, other: &Duration<CAN_BE_ZERO>) -> bool {
        self == &other.0
    }
}

impl<const CAN_BE_ZERO: bool> PartialOrd<StdDuration> for Duration<CAN_BE_ZERO> {
    fn partial_cmp(&self, other: &StdDuration) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl<const CAN_BE_ZERO: bool> PartialOrd<Duration<CAN_BE_ZERO>> for StdDuration {
    fn partial_cmp(&self, other: &Duration<CAN_BE_ZERO>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl TryFrom<StdDuration> for NonZeroFriendlyDuration {
    type Error = DurationError;

    fn try_from(value: StdDuration) -> Result<Self, Self::Error> {
        if value.is_zero() {
            Err(DurationError(InnerError::ZeroNotAllowed))
        } else {
            Ok(Duration(value))
        }
    }
}

impl<const CAN_BE_ZERO: bool> From<Duration<CAN_BE_ZERO>> for StdDuration {
    fn from(d: Duration<CAN_BE_ZERO>) -> StdDuration {
        d.0
    }
}

/// Lossless conversion from non-zero duration to zero duration
impl From<NonZeroFriendlyDuration> for FriendlyDuration {
    fn from(value: NonZeroFriendlyDuration) -> Self {
        Self(value.0)
    }
}

impl From<StdDuration> for FriendlyDuration {
    fn from(d: StdDuration) -> FriendlyDuration {
        Duration(d)
    }
}

impl<const CAN_BE_ZERO: bool> std::fmt::Display for Duration<CAN_BE_ZERO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_days_span().print(f)
    }
}

// impl for durations that allow ZERO
impl FriendlyDuration {
    pub const ZERO: FriendlyDuration = Duration(StdDuration::ZERO);

    pub const fn new(duration: StdDuration) -> Self {
        Self(duration)
    }

    /// Creates a new [`FriendlyDuration`] from the given number of milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(StdDuration::from_millis(millis))
    }

    /// Creates a new [`FriendlyDuration`] from the given number of seconds.
    pub const fn from_secs(secs: u64) -> Self {
        Self(StdDuration::from_secs(secs))
    }

    /// Converts a duration into None if the duration is zero.
    pub const fn to_non_zero_std(self) -> Option<StdDuration> {
        if self.0.is_zero() { None } else { Some(self.0) }
    }

    pub const fn to_non_zero(self) -> Option<NonZeroFriendlyDuration> {
        if self.0.is_zero() {
            None
        } else {
            Some(Duration::<false>(self.0))
        }
    }
}

// impl for durations that doesn't allow ZERO
impl NonZeroFriendlyDuration {
    /// Panics if the duration is zero
    pub const fn new_unchecked(duration: StdDuration) -> Self {
        assert!(!duration.is_zero());
        Self(duration)
    }

    /// Creates a new [`NonZeroFriendlyDuration`] from the given number of milliseconds.
    ///
    /// Panics if the millis is zero.
    pub fn from_millis_unchecked(millis: u64) -> Self {
        assert!(millis > 0);
        Self(StdDuration::from_millis(millis))
    }

    /// Creates a new [`NonZeroFriendlyDuration`] from the given number of seconds.
    /// Panics if the secs is zero.
    pub fn from_secs_unchecked(secs: u64) -> Self {
        assert!(secs > 0);
        Self(StdDuration::from_secs(secs))
    }
}

// generic impls
impl<const CAN_BE_ZERO: bool> Duration<CAN_BE_ZERO> {
    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Returns a span with its maximum unit set to days.
    pub fn to_days_span(&self) -> TimeSpan<Days> {
        TimeSpan::<Days>::new(Span::try_from(self.0).unwrap())
    }

    /// Returns a span with its maximum unit set to seconds.
    pub fn to_seconds_span(&self) -> TimeSpan<Seconds> {
        TimeSpan::<Seconds>::new(Span::try_from(self.0).unwrap())
    }

    /// Returns a span that's displayed as `HH:MM:SS`
    pub fn to_hms_span(&self) -> TimeSpan<Hms> {
        TimeSpan::<Hms>::new(Span::try_from(self.0).unwrap())
    }

    /// Returns a span that's displayed as `HH:MM:SS`
    pub fn to_iso8601_span(&self) -> TimeSpan<Iso8601> {
        TimeSpan::<Iso8601>::new(Span::try_from(self.0).unwrap())
    }

    pub const fn as_std(&self) -> &StdDuration {
        &self.0
    }

    pub const fn to_std(self) -> StdDuration {
        self.0
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error(transparent)]
pub struct DurationError(#[from] InnerError);

#[derive(Debug, Clone, thiserror::Error)]
enum InnerError {
    #[error(transparent)]
    Jiff(#[from] jiff::Error),
    #[error("please use units of days or smaller")]
    BadUnits,
    #[error("duration cannot be zero")]
    ZeroNotAllowed,
}

impl<const CAN_BE_ZERO: bool> FromStr for Duration<CAN_BE_ZERO> {
    type Err = DurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parse_fn = || {
            // jiff cannot parse "0" without "0s" which is mildly annoying. Handling this case to remove
            // generational user pain.
            if s == "0" {
                return Ok(Duration(StdDuration::ZERO));
            }

            let span: Span = s.parse().map_err(InnerError::Jiff)?;
            if span.get_years() > 0 || span.get_months() > 0 {
                return Err(InnerError::BadUnits);
            }

            span.to_duration(SpanRelativeTo::days_are_24_hours())
                .and_then(StdDuration::try_from)
                .map(Duration)
                .map_err(InnerError::Jiff)
        };
        let parsed = parse_fn()?;

        if CAN_BE_ZERO {
            Ok(parsed)
        } else if parsed.is_zero() {
            Err(DurationError(InnerError::ZeroNotAllowed))
        } else {
            Ok(parsed)
        }
    }
}

/// A zero-cost abstraction of a time span with a maximum unit of time.
#[derive(Clone, Copy)]
pub struct TimeSpan<T: Style>(Span, PhantomData<T>);

impl<T: Style> TimeSpan<T> {
    /// Makes this span represent a duration in the opposite direction.
    /// By default, the span is positive.
    ///
    /// When a span is negative, it's display representation will reflect that. For instance,
    /// a duration of `5s` will be displayed as `5s ago` when formatted with [`TimeSpan<Seconds>`]
    /// or [`TimeSpan<Days>`].
    ///
    pub fn negated(self) -> TimeSpan<T> {
        TimeSpan(self.0.negate(), PhantomData)
    }

    #[inline]
    pub fn print(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        T::print_span(&self.0, f)
    }

    /// Returns `true` if the duration is negative.
    pub fn is_negative(&self) -> bool {
        self.0.is_negative()
    }

    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub fn days(&self) -> i32 {
        self.0.get_days()
    }

    pub fn hours(&self) -> i32 {
        self.0.get_hours()
    }

    pub fn minutes(&self) -> i64 {
        self.0.get_minutes()
    }

    pub fn seconds(&self) -> i64 {
        self.0.get_seconds()
    }

    pub fn milliseconds(&self) -> i64 {
        self.0.get_milliseconds()
    }

    pub fn microseconds(&self) -> i64 {
        self.0.get_microseconds()
    }

    pub fn nanoseconds(&self) -> i64 {
        self.0.get_nanoseconds()
    }
}

impl TimeSpan<Seconds> {
    fn new(span: Span) -> TimeSpan<Seconds> {
        let span = span
            .round(
                SpanRound::new()
                    .largest(jiff::Unit::Second)
                    .days_are_24_hours(),
            )
            .unwrap();
        TimeSpan(span, PhantomData)
    }
}

impl TimeSpan<Days> {
    fn new(span: Span) -> TimeSpan<Days> {
        let span = span
            .round(
                SpanRound::new()
                    .largest(jiff::Unit::Day)
                    .days_are_24_hours(),
            )
            .unwrap();
        TimeSpan(span, PhantomData)
    }
}

impl TimeSpan<Hms> {
    fn new(span: Span) -> TimeSpan<Hms> {
        let span = span
            .round(
                SpanRound::new()
                    .largest(jiff::Unit::Hour)
                    .days_are_24_hours(),
            )
            .unwrap();
        TimeSpan(span, PhantomData)
    }
}

impl TimeSpan<Iso8601> {
    fn new(span: Span) -> TimeSpan<Iso8601> {
        let span = span
            .round(
                SpanRound::new()
                    .largest(jiff::Unit::Day)
                    .days_are_24_hours(),
            )
            .unwrap();
        TimeSpan(span, PhantomData)
    }
}

impl<T: Style> std::fmt::Display for TimeSpan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f)
    }
}

impl<T: Style> From<TimeSpan<T>> for StdDuration {
    fn from(value: TimeSpan<T>) -> Self {
        let inner = if value.0.is_negative() {
            value.0.negate()
        } else {
            value.0
        };

        inner
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .and_then(StdDuration::try_from)
            .expect("duration is positive and fits in std::time::Duration")
    }
}

impl From<jiff::SignedDuration> for TimeSpan<Seconds> {
    fn from(value: jiff::SignedDuration) -> Self {
        let span = Span::try_from(value).unwrap();
        Self::new(span)
    }
}

impl From<jiff::SignedDuration> for TimeSpan<Days> {
    fn from(value: jiff::SignedDuration) -> Self {
        let span = Span::try_from(value).unwrap();
        Self::new(span)
    }
}

impl From<jiff::SignedDuration> for TimeSpan<Hms> {
    fn from(value: jiff::SignedDuration) -> Self {
        let span = Span::try_from(value).unwrap();
        Self::new(span)
    }
}

impl From<jiff::SignedDuration> for TimeSpan<Iso8601> {
    fn from(value: jiff::SignedDuration) -> Self {
        let span = Span::try_from(value).unwrap();
        Self::new(span)
    }
}

#[cfg(feature = "serde_with")]
impl<'de, const CAN_BE_ZERO: bool> serde_with::DeserializeAs<'de, StdDuration>
    for Duration<CAN_BE_ZERO>
{
    fn deserialize_as<D>(deserializer: D) -> Result<StdDuration, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer
            .deserialize_str::<helpers::DurationVisitor<CAN_BE_ZERO>>(helpers::DurationVisitor)
    }
}

#[cfg(feature = "serde_with")]
/// NOTE: Unlike when serializing `FriendlyDuration`, using serde_with will **always**
/// serialize into a human-friendly duration string and will never use numeric/binary
/// representation.
impl<const CAN_BE_ZERO: bool> serde_with::SerializeAs<std::time::Duration>
    for Duration<CAN_BE_ZERO>
{
    fn serialize_as<S>(source: &std::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.collect_str(&source.friendly().to_days_span())
    }
}

#[cfg(feature = "serde")]
impl<const CAN_BE_ZERO: bool> serde::ser::Serialize for Duration<CAN_BE_ZERO> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(&self.0.friendly().to_days_span())
        } else {
            // raw u64 num of bytes
            self.0.serialize(serializer)
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, const CAN_BE_ZERO: bool> serde::de::Deserialize<'de> for Duration<CAN_BE_ZERO> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer
                .deserialize_str::<helpers::DurationVisitor<CAN_BE_ZERO>>(helpers::DurationVisitor)
                .map(Duration)
        } else {
            let dur = StdDuration::deserialize(deserializer)?;
            if CAN_BE_ZERO || !dur.is_zero() {
                Ok(Duration(dur))
            } else {
                Err(serde::de::Error::custom(DurationError(
                    InnerError::ZeroNotAllowed,
                )))
            }
        }
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for FriendlyDuration {
    fn schema_name() -> String {
        "FriendlyDuration".to_owned()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "FriendlyDuration").to_owned(),
        )
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema: schemars::schema::SchemaObject = generator.subschema_for::<String>().into();
        let validation = schema.string();
        validation.min_length = Some(1);
        let metadata = schema.metadata();
        metadata.title = Some("Human-readable duration".to_owned());
        metadata.description = Some("Duration string in either jiff human friendly or ISO8601 format. Check https://docs.rs/jiff/latest/jiff/struct.Span.html#parsing-and-printing for more details.".to_owned());
        metadata.examples = vec![
            serde_json::Value::String("10 hours".to_owned()),
            serde_json::Value::String("5 days".to_owned()),
            serde_json::Value::String("5d".to_owned()),
            serde_json::Value::String("1h 4m".to_owned()),
            serde_json::Value::String("P40D".to_owned()),
            serde_json::Value::String("0".to_owned()),
        ];
        schema.into()
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for NonZeroFriendlyDuration {
    fn schema_name() -> String {
        "NonZeroFriendlyDuration".to_owned()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "NonZeroFriendlyDuration").to_owned(),
        )
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema: schemars::schema::SchemaObject = generator.subschema_for::<String>().into();
        let validation = schema.string();
        validation.min_length = Some(1);
        let metadata = schema.metadata();
        metadata.title = Some("Non-zero human-readable duration".to_owned());
        metadata.description = Some("Non-zero duration string in either jiff human friendly or ISO8601 format. Check https://docs.rs/jiff/latest/jiff/struct.Span.html#parsing-and-printing for more details.".to_owned());
        metadata.examples = vec![
            serde_json::Value::String("10 hours".to_owned()),
            serde_json::Value::String("5 days".to_owned()),
            serde_json::Value::String("5d".to_owned()),
            serde_json::Value::String("1h 4m".to_owned()),
            serde_json::Value::String("P40D".to_owned()),
        ];
        schema.into()
    }
}

#[cfg(feature = "serde")]
mod helpers {
    use std::str::FromStr;

    use serde::de::Error;

    use super::Duration;

    pub struct DurationVisitor<const CAN_BE_ZERO: bool>;

    impl<const CAN_BE_ZERO: bool> serde::de::Visitor<'_> for DurationVisitor<CAN_BE_ZERO> {
        type Value = std::time::Duration;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a human-friendly duration string")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Duration::<CAN_BE_ZERO>::from_str(value)
                .map_err(Error::custom)
                .map(|d| d.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn friendly_conversion() {
        let friendly = StdDuration::from_nanos(22).friendly();
        assert_eq!("22ns", friendly.to_days_span().to_string());
    }

    #[test]
    fn duration_display() {
        // let dur = FriendlyDuration::from_str("36h 4m 2s").unwrap();
        let dur = FriendlyDuration::from_str("36h 4m 2s").unwrap();
        assert_eq!(dur, StdDuration::from_secs(129842));
        assert_eq!("129842s", dur.to_seconds_span().to_string());
        // alternate format is more verbose
        assert_eq!("129842 seconds", format!("{:#}", dur.to_seconds_span()));

        // days
        assert_eq!("1d 12h 4m 2s", dur.to_days_span().to_string());
        // alternate format is more verbose
        assert_eq!(
            "1 day 12 hours 4 minutes 2 seconds",
            format!("{:#}", dur.to_days_span())
        );

        // negated
        assert_eq!("1d 12h 4m 2s ago", dur.to_days_span().negated().to_string());

        assert_eq!(
            "1 day 12 hours 4 minutes 2 seconds ago",
            format!("{:#}", dur.to_days_span().negated())
        );
    }

    #[test]
    fn hms_duration() {
        let dur = FriendlyDuration::from_str("3h 4m 2s").unwrap();
        assert_eq!(11042, dur.as_std().as_secs());
        assert_eq!("03:04:02", dur.to_hms_span().to_string());

        // with microseconds
        let dur = FriendlyDuration::from_str("1h 24us").unwrap();
        assert_eq!(3600000024, dur.as_std().as_micros());
        assert_eq!("01:00:00.000024", dur.to_hms_span().to_string());
    }

    #[test]
    fn iso8601_duration() {
        let dur = FriendlyDuration::from_str("3h 4m 2s").unwrap();
        assert_eq!(11042, dur.as_std().as_secs());
        assert_eq!("PT3H4M2S", dur.to_iso8601_span().to_string());
    }

    #[test]
    fn parse_friendly_duration_input_formats() {
        // parsing ZERO
        let duration = FriendlyDuration::from_str("0").unwrap();
        assert_eq!(StdDuration::ZERO, duration);
        let duration = FriendlyDuration::from_str("0s").unwrap();
        assert_eq!(StdDuration::ZERO, duration);

        let duration = FriendlyDuration::from_str("10 min").unwrap();
        assert_eq!(StdDuration::from_secs(600), duration);

        // we don't support "1 month" as months have variable length
        let duration = FriendlyDuration::from_str("P1M");
        assert_eq!(
            "please use units of days or smaller",
            duration.unwrap_err().to_string()
        );

        // we can, however, use "30 days" instead - we fix those to 24 hours
        let duration = FriendlyDuration::from_str("P30D");
        assert_eq!(StdDuration::from_secs(30 * 24 * 3600), duration.unwrap());

        // we should not render larger units which are unsupported as inputs
        let duration = FriendlyDuration::from_str("30 days 48 hours").unwrap();
        assert_eq!(StdDuration::from_secs(30 * 24 * 3600 + 48 * 3600), duration);
        assert_eq!("32d", duration.to_days_span().to_string());

        // more complex inputs are also supported - but will be serialized as a more humane output
        let duration = FriendlyDuration::from_str("P30DT10H30M15S");
        assert_eq!(
            StdDuration::from_secs(30 * 24 * 3600 + 10 * 3600 + 30 * 60 + 15),
            duration.unwrap()
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serialize_friendly() {
        let friendly_output = serde_json::from_str::<String>(
            &serde_json::to_string(&FriendlyDuration::new(StdDuration::from_secs(60 * 23)))
                .unwrap(),
        )
        .unwrap();

        assert_eq!(friendly_output, "23m");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialize_iso8601() {
        let d = StdDuration::from_secs(10);

        assert_eq!(
            serde_json::from_value::<FriendlyDuration>(serde_json::Value::String(
                "PT10S".to_owned()
            ))
            .unwrap()
            .0,
            d
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_roundtrip() {
        let duration = serde_json::from_value::<FriendlyDuration>(serde_json::Value::String(
            "P30D".to_owned(),
        ))
        .unwrap()
        .0;

        assert_eq!(
            serde_json::from_str::<String>(
                &serde_json::to_string(&FriendlyDuration::new(duration)).unwrap()
            )
            .unwrap(),
            "30d"
        );

        assert_eq!(
            serde_json::from_value::<NonZeroFriendlyDuration>(serde_json::Value::String(
                "0".to_owned()
            ),)
            .unwrap_err()
            .to_string(),
            "duration cannot be zero"
        );

        assert_eq!(
            serde_json::from_str::<String>(
                &serde_json::to_string(&NonZeroFriendlyDuration::try_from(duration).unwrap())
                    .unwrap()
            )
            .unwrap(),
            "30d"
        );
    }
}
