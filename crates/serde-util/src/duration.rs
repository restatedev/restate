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

use jiff::{Span, SpanRelativeTo};
use serde::de::{Error, IntoDeserializer};
use serde::ser;
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
    pub fn parse_duration(s: &str) -> Result<Duration, serde::de::value::Error> {
        serde_with::As::<DurationString>::deserialize(s.into_deserializer())
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
        let span = Span::try_from(*source).map_err(ser::Error::custom)?;
        let signed_duration = span
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .map_err(ser::Error::custom)?;
        serializer.collect_str(&format!("{signed_duration:#}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                "could not compute normalized relative span from P1M when all days are assumed to \
                be 24 hours: using unit 'month' in span or configuration requires that a relative \
                reference time be given (`SpanRelativeTo::days_are_24_hours()` was given but this \
                only permits using days and weeks without a relative reference time)"
            )),
            duration
        );

        // we can, however, use "30 days" instead - we fix those to 24 hours
        let duration = DurationString::parse_duration("P30D");
        assert_eq!(Ok(Duration::from_secs(30 * 24 * 3600)), duration);

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
            "730h 30m 15s"
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
            "720h"
        );
    }
}
