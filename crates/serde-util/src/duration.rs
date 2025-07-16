// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use jiff::fmt::friendly::{Designator, SpanPrinter};
use jiff::{Span, SpanRelativeTo};
use serde::de::{Error, IntoDeserializer};
use serde::{Deserialize, Deserializer, Serializer};
use serde_with::{DeserializeAs, SerializeAs};

/// Serializable/Deserializable duration for use with serde_with.
///
/// Deserialization uses [`jiff::Span`]'s parsing to support both human-friendly and ISO8601
/// inputs. Days are the largest supported unit of time, and are interpreted as 24 hours long when
/// converting the parsed span into an actual duration.
///
/// Serialization is performed using [`jiff::fmt::friendly`] for output.
pub struct DurationString;

impl DurationString {
    /// Parse a duration from a string using the [`jiff::fmt::friendly`] format, allowing units of
    /// up to days to be used.
    pub fn parse_duration(s: &str) -> Result<Duration, serde::de::value::Error> {
        serde_with::As::<DurationString>::deserialize(s.into_deserializer())
    }

    /// Prints a duration as a string using the [`jiff::fmt::friendly`] format, using units up to
    /// days.
    pub fn display(duration: Duration) -> String {
        // jiff's SignedDuration pretty-print will add up units up to hours but not to days,
        // so we use a Span to get finer control over the display output and use calendar units
        // up to days (which we consider to be 24 hours in duration)
        let mut span = Span::try_from(duration).expect("fits i64");
        if span.get_seconds() >= 60 {
            let minutes = span.get_seconds() / 60;
            let seconds = span.get_seconds() % 60;
            span = span.minutes(minutes).seconds(seconds);
        }
        if span.get_minutes() >= 60 {
            let hours = span.get_minutes() / 60;
            let minutes = span.get_minutes() % 60;
            span = span.hours(hours).minutes(minutes);
        }
        if span.get_hours() >= 24 {
            let days = span.get_hours() / 24;
            let hours = span.get_hours() % 24;
            span = span.days(days).hours(hours);
        };

        let printer = SpanPrinter::new().designator(Designator::Compact);
        printer.span_to_string(&span)
    }
}

impl<'de> DeserializeAs<'de, std::time::Duration> for DurationString {
    fn deserialize_as<D>(deserializer: D) -> Result<std::time::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let span: Span = String::deserialize(deserializer)?
            .parse()
            .map_err(Error::custom)?;
        if span.get_years() > 0 || span.get_months() > 0 {
            return Err(Error::custom("Please use units of days or smaller"));
        }
        let signed_duration = span
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .map_err(Error::custom)?;
        Duration::try_from(signed_duration).map_err(Error::custom)
    }
}

impl SerializeAs<std::time::Duration> for DurationString {
    fn serialize_as<S>(source: &std::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&DurationString::display(*source))
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DurationString {
    fn schema_name() -> String {
        "DurationString".to_owned()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema: schemars::schema::SchemaObject = generator.subschema_for::<String>().into();
        let validation = schema.string();
        validation.min_length = Some(1);
        let metadata = schema.metadata();
        metadata.title = Some("DurationString".to_owned());
        metadata.description = Some("Duration string in either jiff human friendly or ISO8601 format. Check https://docs.rs/jiff/latest/jiff/struct.Span.html#parsing-and-printing for more details.".to_owned());
        schema.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use jiff::{SignedDuration, ToSpan};
    use serde::{Deserialize, Serialize};

    use serde_with::serde_as;

    #[serde_as]
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    struct MyDuration(#[serde_as(as = "DurationString")] std::time::Duration);

    #[test]
    fn parse_duration_input_formats() {
        let duration = DurationString::parse_duration("10 min");
        assert_eq!(Ok(Duration::from_secs(600)), duration);

        // we don't support "1 month" as months have variable length
        let duration = DurationString::parse_duration("P1M");
        assert_eq!(
            Err(serde::de::Error::custom(
                "Please use units of days or smaller"
            )),
            duration
        );

        // we can, however, use "30 days" instead - we fix those to 24 hours
        let duration = DurationString::parse_duration("P30D");
        assert_eq!(Ok(Duration::from_secs(30 * 24 * 3600)), duration);

        // we should not render larger units which are unsupported as inputs
        let duration = DurationString::parse_duration("30 days 48 hours").unwrap();
        assert_eq!(
            30.days()
                .hours(48)
                .to_duration(SpanRelativeTo::days_are_24_hours())
                .unwrap(),
            SignedDuration::try_from(duration).unwrap()
        );
        let duration_str = DurationString::display(duration);
        assert_eq!("32d", duration_str);

        // more complex inputs are also supported - but will be serialized as a more humane output
        let duration = DurationString::parse_duration("P30DT10H30M15S");
        assert_eq!(
            Ok(Duration::from_secs(
                30 * 24 * 3600 + 10 * 3600 + 30 * 60 + 15
            )),
            duration
        );
        assert_eq!(
            serde_json::from_str::<String>(
                &serde_json::to_string(&MyDuration(duration.unwrap())).unwrap()
            )
            .unwrap(),
            "30d 10h 30m 15s"
        );
    }

    #[test]
    fn serialize_friendly() {
        let friendly_output = serde_json::from_str::<String>(
            &serde_json::to_string(&MyDuration(std::time::Duration::from_secs(60 * 23))).unwrap(),
        )
        .unwrap();

        assert_eq!(friendly_output, "23m");
    }

    #[test]
    fn deserialize_iso8601() {
        let d = std::time::Duration::from_secs(10);

        assert_eq!(
            serde_json::from_value::<MyDuration>(serde_json::Value::String("PT10S".to_owned()))
                .unwrap()
                .0,
            d
        );
    }

    #[test]
    fn serde_roundtrip() {
        let duration =
            serde_json::from_value::<MyDuration>(serde_json::Value::String("P30D".to_owned()))
                .unwrap()
                .0;

        assert_eq!(
            serde_json::from_str::<String>(&serde_json::to_string(&MyDuration(duration)).unwrap())
                .unwrap(),
            "30d"
        );
    }
}
