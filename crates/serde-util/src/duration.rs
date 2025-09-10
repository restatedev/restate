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

use restate_time_util::{DurationExt, FriendlyDuration};

use serde::de::Error;
use serde::{Deserializer, Serializer};

/// A human-friendly Serializable/Deserializable duration
///
/// Deserialization uses [`restate_time_util::FriendlyDuration`]'s parsing to support both human-friendly and ISO8601
/// inputs. Days are the largest supported unit of time, and are interpreted as 24 hours long when
/// converting the parsed span into an actual duration.
///
/// Serialization is performed using [`restate_time_util::FriendlyDuration::to_days_span`] for output if the serializer asks
/// for human-friendly output, otherwise it falls back to the default serializer/deserializer of [`std::time::Duration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Ord, PartialOrd)]
pub struct DurationString<const CAN_BE_ZERO: bool = true>(Duration);

pub type NonZeroDurationString = DurationString<false>;

// impl for durations that allow ZERO
impl DurationString<true> {
    pub const ZERO: DurationString<true> = DurationString(Duration::ZERO);

    pub const fn new(duration: Duration) -> Self {
        Self(duration)
    }

    /// Converts a duration into None if the duration is zero.
    pub const fn to_non_zero(self) -> Option<Duration> {
        if self.0.is_zero() { None } else { Some(self.0) }
    }
}

// impl for durations that doesn't allow ZERO
impl DurationString<false> {
    /// Panics if the duration is zero
    pub const fn new_unchecked(duration: Duration) -> Self {
        assert!(!duration.is_zero());
        Self(duration)
    }
}

// generic impls
impl<const CAN_BE_ZERO: bool> DurationString<CAN_BE_ZERO> {
    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub fn inner(&self) -> &Duration {
        &self.0
    }

    pub fn into_inner(self) -> Duration {
        self.0
    }
}

/// Lossless conversion from non-zero duration to zero duration
impl From<DurationString<false>> for DurationString<true> {
    fn from(value: DurationString<false>) -> Self {
        Self(value.0)
    }
}

impl<const CAN_BE_ZERO: bool> std::fmt::Display for DurationString<CAN_BE_ZERO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.friendly().to_days_span().print(f)
    }
}

impl<const CAN_BE_ZERO: bool> AsRef<Duration> for DurationString<CAN_BE_ZERO> {
    fn as_ref(&self) -> &Duration {
        &self.0
    }
}

impl<const CAN_BE_ZERO: bool> std::ops::Deref for DurationString<CAN_BE_ZERO> {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const CAN_BE_ZERO: bool> From<DurationString<CAN_BE_ZERO>> for Duration {
    fn from(value: DurationString<CAN_BE_ZERO>) -> Self {
        value.0
    }
}

impl<const CAN_BE_ZERO: bool> From<DurationString<CAN_BE_ZERO>> for FriendlyDuration {
    fn from(value: DurationString<CAN_BE_ZERO>) -> Self {
        value.0.friendly()
    }
}

impl<const CAN_BE_ZERO: bool> PartialEq<Duration> for DurationString<CAN_BE_ZERO> {
    fn eq(&self, other: &Duration) -> bool {
        &self.0 == other
    }
}

impl<const CAN_BE_ZERO: bool> PartialEq<DurationString<CAN_BE_ZERO>> for Duration {
    fn eq(&self, other: &DurationString<CAN_BE_ZERO>) -> bool {
        self == &other.0
    }
}

impl<const CAN_BE_ZERO: bool> PartialOrd<Duration> for DurationString<CAN_BE_ZERO> {
    fn partial_cmp(&self, other: &Duration) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl<const CAN_BE_ZERO: bool> PartialOrd<DurationString<CAN_BE_ZERO>> for Duration {
    fn partial_cmp(&self, other: &DurationString<CAN_BE_ZERO>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationError;

impl std::fmt::Display for DurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "duration cannot be zero")
    }
}
impl std::error::Error for DurationError {}

impl<const CAN_BE_ZERO: bool> TryFrom<Duration> for DurationString<CAN_BE_ZERO> {
    type Error = DurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        if value.is_zero() && !CAN_BE_ZERO {
            Err(DurationError)
        } else {
            Ok(DurationString(value))
        }
    }
}

impl<const CAN_BE_ZERO: bool> std::str::FromStr for DurationString<CAN_BE_ZERO> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dur = FriendlyDuration::from_str(s).map_err(|_| s.to_owned())?;

        Self::try_from(dur.to_std()).map_err(|_| s.to_owned())
    }
}

impl<'de> serde_with::DeserializeAs<'de, Duration> for DurationString {
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
                <FriendlyDuration as std::str::FromStr>::from_str(value)
                    .map_err(Error::custom)
                    .map(|d| d.to_std())
            }
        }

        deserializer.deserialize_str(Helper)
    }
}

impl serde_with::SerializeAs<std::time::Duration> for DurationString {
    fn serialize_as<S>(source: &std::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&source.friendly().to_days_span())
    }
}

impl<const CAN_BE_ZERO: bool> serde::ser::Serialize for DurationString<CAN_BE_ZERO> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(&self.0.friendly().to_days_span())
        } else {
            // raw u64 num of bytes
            self.0.serialize(serializer)
        }
    }
}

impl<'de, const CAN_BE_ZERO: bool> serde::de::Deserialize<'de> for DurationString<CAN_BE_ZERO> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer
                .deserialize_str::<helpers::DurationVisitor<CAN_BE_ZERO>>(helpers::DurationVisitor)
                .map(DurationString)
        } else {
            let dur = Duration::deserialize(deserializer)?;
            DurationString::<CAN_BE_ZERO>::try_from(dur)
                .map_err(|_| Error::custom("duration cannot be zero"))
        }
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for DurationString<true> {
    fn schema_name() -> String {
        "DurationString".to_owned()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "DurationString").to_owned(),
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
impl schemars::JsonSchema for DurationString<false> {
    fn schema_name() -> String {
        "NonZeroDurationString".to_owned()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "NonZeroDurationString").to_owned(),
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

mod helpers {
    use serde::de::Error;
    use std::str::FromStr;

    use restate_time_util::FriendlyDuration;

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
            FriendlyDuration::from_str(value)
                .map_err(Error::custom)
                .and_then(|d| {
                    let std_dur = d.to_std();
                    if std_dur.is_zero() && !CAN_BE_ZERO {
                        Err(Error::custom("duration cannot be zero"))
                    } else {
                        Ok(std_dur)
                    }
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_friendly() {
        let friendly_output = serde_json::from_str::<String>(
            &serde_json::to_string(&DurationString::new(std::time::Duration::from_secs(
                60 * 23,
            )))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(friendly_output, "23m");
    }

    #[test]
    fn deserialize_iso8601() {
        let d = std::time::Duration::from_secs(10);

        assert_eq!(
            serde_json::from_value::<DurationString>(serde_json::Value::String("PT10S".to_owned()))
                .unwrap()
                .0,
            d
        );
    }

    #[test]
    fn serde_roundtrip() {
        let duration =
            serde_json::from_value::<DurationString>(serde_json::Value::String("P30D".to_owned()))
                .unwrap()
                .0;

        assert_eq!(
            serde_json::from_str::<String>(
                &serde_json::to_string(&DurationString::new(duration)).unwrap()
            )
            .unwrap(),
            "30d"
        );

        assert_eq!(
            serde_json::from_value::<NonZeroDurationString>(serde_json::Value::String(
                "0".to_owned()
            ),)
            .unwrap_err()
            .to_string(),
            "duration cannot be zero"
        );

        assert_eq!(
            serde_json::from_str::<String>(
                &serde_json::to_string(&NonZeroDurationString::try_from(duration).unwrap())
                    .unwrap()
            )
            .unwrap(),
            "30d"
        );
    }
}
