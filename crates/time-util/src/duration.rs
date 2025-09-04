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
use std::time::Duration;

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

impl DurationExt for Duration {
    /// panics if the duration is negative or too large to fit into i64
    fn friendly(&self) -> FriendlyDuration {
        FriendlyDuration::from(*self)
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
pub struct FriendlyDuration(Duration);

impl PartialEq<Duration> for FriendlyDuration {
    fn eq(&self, other: &Duration) -> bool {
        &self.0 == other
    }
}

impl PartialEq<FriendlyDuration> for Duration {
    fn eq(&self, other: &FriendlyDuration) -> bool {
        self == &other.0
    }
}

impl PartialOrd<Duration> for FriendlyDuration {
    fn partial_cmp(&self, other: &Duration) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<FriendlyDuration> for Duration {
    fn partial_cmp(&self, other: &FriendlyDuration) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl From<Duration> for FriendlyDuration {
    fn from(d: Duration) -> FriendlyDuration {
        FriendlyDuration(d)
    }
}

impl From<FriendlyDuration> for Duration {
    fn from(d: FriendlyDuration) -> Duration {
        d.0
    }
}

impl std::fmt::Display for FriendlyDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_days_span().print(f)
    }
}

impl FriendlyDuration {
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

    pub fn as_std(&self) -> &Duration {
        &self.0
    }

    pub fn to_std(self) -> Duration {
        self.0
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error(transparent)]
pub struct DurationParseError(#[from] jiff::Error);

impl FromStr for FriendlyDuration {
    type Err = DurationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let span: Span = s.parse()?;
        if span.get_years() > 0 || span.get_months() > 0 {
            return Err(DurationParseError(jiff::Error::from_args(format_args!(
                "Please use units of days or smaller"
            ))));
        }

        span.to_duration(SpanRelativeTo::days_are_24_hours())
            .and_then(Duration::try_from)
            .map(FriendlyDuration)
            .map_err(DurationParseError)
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

impl<T: Style> From<TimeSpan<T>> for Duration {
    fn from(value: TimeSpan<T>) -> Self {
        let inner = if value.0.is_negative() {
            value.0.negate()
        } else {
            value.0
        };

        inner
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .and_then(Duration::try_from)
            .expect("duration is positive and fits in std::time::Duration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn friendly_conversion() {
        let friendly = Duration::from_nanos(22).friendly();
        assert_eq!("22ns", friendly.to_days_span().to_string());
    }

    #[test]
    fn duration_display() {
        let dur = FriendlyDuration::from_str("36h 4m 2s").unwrap();
        assert_eq!(dur, Duration::from_secs(129842));
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
        let duration = FriendlyDuration::from_str("10 min").unwrap();
        assert_eq!(Duration::from_secs(600), duration);

        // we don't support "1 month" as months have variable length
        let duration = FriendlyDuration::from_str("P1M");
        assert_eq!(
            "Please use units of days or smaller",
            duration.unwrap_err().to_string()
        );

        // we can, however, use "30 days" instead - we fix those to 24 hours
        let duration = FriendlyDuration::from_str("P30D");
        assert_eq!(Duration::from_secs(30 * 24 * 3600), duration.unwrap());

        // we should not render larger units which are unsupported as inputs
        let duration = FriendlyDuration::from_str("30 days 48 hours").unwrap();
        assert_eq!(Duration::from_secs(30 * 24 * 3600 + 48 * 3600), duration);
        assert_eq!("32d", duration.to_days_span().to_string());

        // more complex inputs are also supported - but will be serialized as a more humane output
        let duration = FriendlyDuration::from_str("P30DT10H30M15S");
        assert_eq!(
            Duration::from_secs(30 * 24 * 3600 + 10 * 3600 + 30 * 60 + 15),
            duration.unwrap()
        );
    }
}
