// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr as _;
use std::time::Duration;

use restate_time_util::{DurationExt, FriendlyDuration};

use serde::de::Error;
use serde::{Deserializer, Serializer};
use serde_with::{DeserializeAs, SerializeAs};

/// Serializable/Deserializable duration for use with serde_with.
///
/// Deserialization uses [`restate_time_util::FriendlyDuration`]'s parsing to support both human-friendly and ISO8601
/// inputs. Days are the largest supported unit of time, and are interpreted as 24 hours long when
/// converting the parsed span into an actual duration.
///
/// Serialization is performed using [`restate_time_util::FriendlyDuration::to_days_span`] for output.
pub struct DurationString;

impl<'de> DeserializeAs<'de, Duration> for DurationString {
    fn deserialize_as<D>(deserializer: D) -> Result<std::time::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Helper;
        impl serde::de::Visitor<'_> for Helper {
            type Value = Duration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a human-friendly duration string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                FriendlyDuration::from_str(value)
                    .map_err(Error::custom)
                    .map(|d| d.to_std())
            }
        }

        deserializer.deserialize_str(Helper)
    }
}

impl SerializeAs<std::time::Duration> for DurationString {
    fn serialize_as<S>(source: &std::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&source.friendly().to_days_span())
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
