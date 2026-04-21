// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::ops::{Bound, RangeBounds};

/// An inclusive range of partition keys `[start, end]`.
///
/// This is a compact, `Copy` representation of an inclusive key range backed by
/// [`std::range::RangeInclusive<u64>`]. Compared to [`std::ops::RangeInclusive<u64>`]:
///
/// - 16 bytes instead of 24 (no internal `exhausted` flag)
/// - Implements `Copy` (no `.clone()` needed)
///
/// Wire-format compatible with `std::ops::RangeInclusive<u64>` for both serde and bilrost.
///
/// Since v1.7.0
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyRange(std::range::RangeInclusive<u64>);

impl KeyRange {
    /// The full key space `[0, u64::MAX]`.
    pub const FULL: Self = Self::new(u64::MIN, u64::MAX);

    /// Creates a new inclusive key range `[start, end]`.
    pub const fn new(start: u64, end: u64) -> Self {
        Self(std::range::RangeInclusive { start, last: end })
    }

    /// The inclusive lower bound.
    pub const fn start(&self) -> u64 {
        self.0.start
    }

    /// The inclusive upper bound.
    pub const fn end(&self) -> u64 {
        self.0.last
    }
}

impl RangeBounds<u64> for KeyRange {
    fn start_bound(&self) -> Bound<&u64> {
        self.0.start_bound()
    }

    fn end_bound(&self) -> Bound<&u64> {
        self.0.end_bound()
    }
}

impl From<std::ops::RangeInclusive<u64>> for KeyRange {
    fn from(range: std::ops::RangeInclusive<u64>) -> Self {
        Self::new(*range.start(), *range.end())
    }
}

impl From<KeyRange> for std::ops::RangeInclusive<u64> {
    fn from(range: KeyRange) -> Self {
        range.start()..=range.end()
    }
}

impl From<std::range::RangeInclusive<u64>> for KeyRange {
    fn from(range: std::range::RangeInclusive<u64>) -> Self {
        Self(range)
    }
}

impl From<KeyRange> for std::range::RangeInclusive<u64> {
    fn from(range: KeyRange) -> Self {
        range.0
    }
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.start(), self.end())
    }
}

// -- serde support (feature-gated) --

#[cfg(feature = "serde")]
mod serde_impl {
    use super::KeyRange;

    use serde::de::{self, MapAccess, Visitor};
    use serde::ser::SerializeStruct;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes as `{"start": <u64>, "end": <u64>}` to match `std::ops::RangeInclusive`'s
    /// serde format.
    impl Serialize for KeyRange {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut state = serializer.serialize_struct("KeyRange", 2)?;
            state.serialize_field("start", &self.start())?;
            state.serialize_field("end", &self.end())?;
            state.end()
        }
    }

    /// Deserializes from `{"start": <u64>, "end": <u64>}` (current format) or
    /// `{"from": <u64>, "to": <u64>}` (legacy format).
    impl<'de> Deserialize<'de> for KeyRange {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_struct("KeyRange", &["start", "end"], KeyRangeVisitor)
        }
    }

    struct KeyRangeVisitor;

    impl<'de> Visitor<'de> for KeyRangeVisitor {
        type Value = KeyRange;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(
                "a map with 'start'/'end' or 'from'/'to' fields representing an inclusive range",
            )
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<KeyRange, M::Error> {
            let mut start: Option<u64> = None;
            let mut end: Option<u64> = None;

            while let Some(key) = map.next_key::<&str>()? {
                match key {
                    "start" | "from" => {
                        if start.is_some() {
                            return Err(de::Error::duplicate_field("start"));
                        }
                        start = Some(map.next_value()?);
                    }
                    "end" | "to" => {
                        if end.is_some() {
                            return Err(de::Error::duplicate_field("end"));
                        }
                        end = Some(map.next_value()?);
                    }
                    other => {
                        return Err(de::Error::unknown_field(other, &["start", "end"]));
                        // Note: we could also just skip unknown fields for forward compat,
                        // but strict checking is safer for catching bugs.
                    }
                }
            }

            let start = start.ok_or_else(|| de::Error::missing_field("start"))?;
            let end = end.ok_or_else(|| de::Error::missing_field("end"))?;
            Ok(KeyRange::new(start, end))
        }
    }
}

// -- bilrost support (feature-gated) --

#[cfg(feature = "bilrost")]
mod bilrost_impl {
    use super::KeyRange;

    use bilrost::DecodeErrorKind;
    use bilrost::encoding::{EmptyState, ForOverwrite, Proxiable};

    impl Proxiable for KeyRange {
        type Proxy = (u64, u64);

        fn encode_proxy(&self) -> Self::Proxy {
            (self.start(), self.end())
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = KeyRange::new(proxy.0, proxy.1);
            Ok(())
        }
    }

    impl ForOverwrite<(), KeyRange> for () {
        fn for_overwrite() -> KeyRange {
            KeyRange::new(0, 0)
        }
    }

    impl EmptyState<(), KeyRange> for () {
        fn empty() -> KeyRange {
            KeyRange::new(0, 0)
        }

        fn is_empty(val: &KeyRange) -> bool {
            val.start() == 0 && val.end() == 0
        }

        fn clear(val: &mut KeyRange) {
            *val = <() as EmptyState<(), KeyRange>>::empty();
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::General)
        to encode proxied type (KeyRange)
        with general encodings
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let r = KeyRange::new(10, 20);
        assert_eq!(r.start(), 10);
        assert_eq!(r.end(), 20);
        assert!(r.contains(&10));
        assert!(r.contains(&15));
        assert!(r.contains(&20));
        assert!(!r.contains(&9));
        assert!(!r.contains(&21));

        // Copy
        let r2 = r;
        assert_eq!(r, r2);

        // Display
        assert_eq!(format!("{r}"), "[10, 20]");
    }

    #[test]
    fn full_range() {
        assert_eq!(KeyRange::FULL.start(), u64::MIN);
        assert_eq!(KeyRange::FULL.end(), u64::MAX);
        assert!(KeyRange::FULL.contains(&0));
        assert!(KeyRange::FULL.contains(&u64::MAX));
    }

    #[test]
    fn from_ops_range_inclusive() {
        let ops_range: std::ops::RangeInclusive<u64> = 5..=10;
        let kr = KeyRange::from(ops_range.clone());
        assert_eq!(kr.start(), 5);
        assert_eq!(kr.end(), 10);

        let back: std::ops::RangeInclusive<u64> = kr.into();
        assert_eq!(back, ops_range);
    }

    #[test]
    fn from_range_inclusive() {
        let range = std::range::RangeInclusive { start: 3, last: 7 };
        let kr = KeyRange::from(range);
        assert_eq!(kr.start(), 3);
        assert_eq!(kr.end(), 7);

        let back: std::range::RangeInclusive<u64> = kr.into();
        assert_eq!(back, range);
    }

    #[test]
    fn size_and_copy() {
        assert_eq!(size_of::<KeyRange>(), 16);
        assert_eq!(
            size_of::<KeyRange>(),
            size_of::<std::range::RangeInclusive<u64>>()
        );
        // Old type is larger due to exhausted flag + padding
        assert!(size_of::<std::ops::RangeInclusive<u64>>() > size_of::<KeyRange>());
    }

    #[test]
    fn serde_roundtrip_json() {
        let kr = KeyRange::new(100, 200);
        let json = serde_json::to_string(&kr).unwrap();
        assert_eq!(json, r#"{"start":100,"end":200}"#);

        let back: KeyRange = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kr);
    }

    #[test]
    fn serde_compat_with_ops_range() {
        // Serialize old type, deserialize as KeyRange
        let ops_range: std::ops::RangeInclusive<u64> = 42..=99;
        let json = serde_json::to_string(&ops_range).unwrap();
        let kr: KeyRange = serde_json::from_str(&json).unwrap();
        assert_eq!(kr.start(), 42);
        assert_eq!(kr.end(), 99);

        // Serialize KeyRange, deserialize as old type
        let kr = KeyRange::new(42, 99);
        let json = serde_json::to_string(&kr).unwrap();
        let back: std::ops::RangeInclusive<u64> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, 42..=99);
    }

    #[test]
    fn serde_legacy_from_to_format() {
        let json = r#"{"from":10,"to":20}"#;
        let kr: KeyRange = serde_json::from_str(json).unwrap();
        assert_eq!(kr.start(), 10);
        assert_eq!(kr.end(), 20);
    }

    #[test]
    fn bilrost_roundtrip() {
        use bilrost::{Message, OwnedMessage};

        #[derive(Debug, PartialEq, bilrost::Message)]
        struct TestMsg {
            #[bilrost(1)]
            range: KeyRange,
        }

        let msg = TestMsg {
            range: KeyRange::new(100, 200),
        };
        let encoded = msg.encode_to_vec();
        let decoded = TestMsg::decode(&*encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn bilrost_compat_with_ops_range() {
        use bilrost::{Message, OwnedMessage};

        // The bilrost crate's existing encoding for RangeInclusive<u64> uses RestateEncoding
        // which is defined in restate-encoding. We can't test cross-type bilrost compat here
        // since RestateEncoding is not available in this crate. That test belongs in
        // restate-encoding's test suite.
        //
        // Here we verify that KeyRange's bilrost encoding is self-consistent.
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct Msg {
            #[bilrost(1)]
            range: KeyRange,
        }

        let msg = Msg {
            range: KeyRange::new(u64::MIN, u64::MAX),
        };
        let encoded = msg.encode_to_vec();
        let decoded = Msg::decode(&*encoded).unwrap();
        assert_eq!(msg, decoded);
    }
}
