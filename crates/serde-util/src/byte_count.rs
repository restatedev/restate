// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display};
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;

use bytesize::ByteSize;
use serde::de::Visitor;
use serde::{Deserializer, Serialize, Serializer, de, de::Deserialize};
use serde_with::{DeserializeAs, SerializeAs};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Copy, Hash)]
pub struct ByteCount<const CAN_BE_ZERO: bool = true>(u64);
pub type NonZeroByteCount = ByteCount<false>;

#[cfg(feature = "schema")]
impl schemars::JsonSchema for ByteCount<true> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "HumanBytes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(std::concat!(std::module_path!(), "::", "HumanBytes").to_owned())
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "human-bytes",
            "pattern": r"^\d+(\.\d+)? ?[KMG]B$",
            "minLength": 1,
            "title": "Human-readable bytes",
            "description": "Human-readable bytes",
        })
    }
}
#[cfg(feature = "schema")]
impl schemars::JsonSchema for ByteCount<false> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NonZeroHumanBytes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "NonZeroHumanBytes").to_owned(),
        )
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "non-zero human-bytes",
            "pattern": r"^\d+(\.\d+)? ?[KMG]B$",
            "minLength": 1,
            "title": "Non-zero human-readable bytes",
            "description": "Non-zero human-readable bytes",
        })
    }
}

impl ByteCount<true> {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl ByteCount<false> {
    pub const fn new(value: NonZeroUsize) -> Self {
        Self(value.get() as u64)
    }

    pub const fn as_non_zero_usize(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0 as usize).expect("ByteCount is not zero")
    }
}

impl<const CAN_BE_ZERO: bool> Display for ByteCount<CAN_BE_ZERO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ByteSize(self.0).fmt(f)
    }
}

impl<const CAN_BE_ZERO: bool> ByteCount<CAN_BE_ZERO> {
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl FromStr for ByteCount<true> {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s: ByteSize = s.parse()?;
        Ok(ByteCount(s.0))
    }
}

impl From<NonZeroUsize> for ByteCount<false> {
    fn from(value: NonZeroUsize) -> Self {
        let v: usize = value.into();
        ByteCount(v as u64)
    }
}

impl From<NonZeroU64> for ByteCount<false> {
    fn from(value: NonZeroU64) -> Self {
        ByteCount(value.into())
    }
}

impl From<u64> for ByteCount<true> {
    fn from(value: u64) -> Self {
        ByteCount(value)
    }
}

impl From<usize> for ByteCount<true> {
    fn from(value: usize) -> Self {
        ByteCount(value as u64)
    }
}

impl<const CAN_BE_ZERO: bool> From<ByteCount<CAN_BE_ZERO>> for u64 {
    fn from(value: ByteCount<CAN_BE_ZERO>) -> Self {
        value.0
    }
}

impl<const CAN_BE_ZERO: bool> Serialize for ByteCount<CAN_BE_ZERO> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&ByteSize(self.0).to_string())
        } else {
            // raw u64 num of bytes
            self.0.serialize(serializer)
        }
    }
}

impl<'de, const CAN_BE_ZERO: bool> Deserialize<'de> for ByteCount<CAN_BE_ZERO> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_any::<ByteCountVisitor<CAN_BE_ZERO>>(ByteCountVisitor)
        } else {
            deserializer.deserialize_u64::<ByteCountVisitor<CAN_BE_ZERO>>(ByteCountVisitor)
        }
    }
}

struct ByteCountVisitor<const CAN_BE_ZERO: bool>;

// Because we do what appears to be runtime-check of the const CAN_BE_ZERO but
// it's actually compile-time, compiler will inline the comparison but we need
// to make clippy happy.
#[allow(clippy::absurd_extreme_comparisons)]
impl<const CAN_BE_ZERO: bool> Visitor<'_> for ByteCountVisitor<CAN_BE_ZERO> {
    type Value = ByteCount<CAN_BE_ZERO>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an integer or string")
    }

    // TOML Integer(i64) - toml doesn't support u64
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        if let Ok(val) = u64::try_from(value) {
            if val <= 0 && !CAN_BE_ZERO {
                return Err(E::invalid_value(
                    de::Unexpected::Signed(value),
                    &"non-zero value",
                ));
            }
            Ok(ByteCount(val))
        } else {
            Err(E::invalid_value(
                de::Unexpected::Signed(value),
                &"i64 overflow",
            ))
        }
    }

    // JSON Integer(u64)
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        if value <= 0 && !CAN_BE_ZERO {
            return Err(E::invalid_value(
                de::Unexpected::Unsigned(value),
                &"non-zero value",
            ));
        }
        Ok(ByteCount(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let val: Result<ByteSize, _> = value.parse();

        if let Ok(val) = val {
            if val.0 <= 0 && !CAN_BE_ZERO {
                return Err(E::invalid_value(
                    de::Unexpected::Str(value),
                    &"non-zero value",
                ));
            }
            Ok(ByteCount(val.0))
        } else {
            Err(E::invalid_value(
                de::Unexpected::Str(value),
                &"a human-readable byte count string",
            ))
        }
    }
}

impl SerializeAs<u64> for ByteCount<true> {
    fn serialize_as<S>(source: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Self(*source).serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, u64> for ByteCount<true> {
    fn deserialize_as<D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::deserialize(deserializer)?.0)
    }
}

impl SerializeAs<usize> for ByteCount<true> {
    fn serialize_as<S>(source: &usize, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Self(*source as u64).serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, usize> for ByteCount<true> {
    fn deserialize_as<D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::deserialize(deserializer)?.0 as usize)
    }
}

impl SerializeAs<NonZeroUsize> for ByteCount<false> {
    fn serialize_as<S>(source: &NonZeroUsize, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Self::from(*source).serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, NonZeroUsize> for ByteCount<false> {
    fn deserialize_as<D>(deserializer: D) -> Result<NonZeroUsize, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::deserialize(deserializer)?.as_non_zero_usize())
    }
}

impl SerializeAs<NonZeroU64> for ByteCount<false> {
    fn serialize_as<S>(source: &NonZeroU64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Self::from(*source).serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, NonZeroU64> for ByteCount<false> {
    fn deserialize_as<D>(deserializer: D) -> Result<NonZeroU64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw_value = Self::deserialize(deserializer)?.0;
        Ok(NonZeroU64::new(raw_value).expect("ByteCount is not zero"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::Deserialize;

    #[derive(Serialize, Deserialize)]
    struct Config(ByteCount<true>);

    #[derive(Serialize, Debug, Deserialize)]
    struct Config2(Option<NonZeroByteCount>);

    #[test]
    fn deserialize_byte_count() {
        #[track_caller]
        fn check_str(s: &str) {
            assert_eq!(
                serde_json::from_str::<Config>(&format!("{s:?}")).unwrap().0,
                s.parse().unwrap()
            );
        }

        #[track_caller]
        fn check(s: &str) {
            assert_eq!(
                serde_json::from_str::<Config>(s).unwrap().0,
                s.parse().unwrap()
            );
        }

        check_str("5 MB");
        check_str("5MB");
        check_str("5mb");
        check_str("5m");
        check_str("12.34 KB");
        check_str("123");
        check("123");
        // zero is allowed
        check("0");
    }

    #[test]
    fn serialize_byte_count() {
        #[track_caller]
        fn check_ser(v: &Config) {
            assert_eq!(
                serde_json::to_string(v).unwrap(),
                format!("{:?}", v.0.to_string())
            );
        }

        check_ser(&Config(ByteCount(5_000_000)));
    }

    #[test]
    fn deserialize_non_zero_opt_byte_count() {
        assert_eq!(
            serde_json::from_str::<Config2>("\"5 MB\"").unwrap().0,
            Some(ByteCount(5_000_000)),
        );
        assert_eq!(
            serde_json::from_str::<Config2>("\"5\"").unwrap().0,
            Some(ByteCount(5)),
        );

        assert_eq!(serde_json::from_str::<Config2>("null").unwrap().0, None,);

        assert_eq!(
            serde_json::from_str::<Config2>("\"0\"")
                .unwrap_err()
                .to_string(),
            "invalid value: string \"0\", expected non-zero value at line 1 column 3"
        );
    }
}
