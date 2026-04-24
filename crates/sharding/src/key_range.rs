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
use std::range::RangeInclusiveIter;

use restate_platform::network::NetSerde;

use crate::PartitionKey;

/// An inclusive range of partition keys `[start, end]`.
///
/// This is a compact, `Copy` representation of an inclusive key range backed by
/// [`std::range::RangeInclusive<u64>`]. Compared to [`std::ops::RangeInclusive<u64>`]:
///
/// Wire-format compatible with `std::ops::RangeInclusive<u64>` for both serde and bilrost.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct KeyRange(std::range::RangeInclusive<PartitionKey>);

impl PartialOrd for KeyRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.start(), self.end()).cmp(&(other.start(), other.end()))
    }
}

impl NetSerde for KeyRange {}

impl KeyRange {
    /// The full key space `[0, u64::MAX]`.
    pub const FULL: Self = Self::new(u64::MIN, u64::MAX);

    /// Creates a new inclusive key range `[start, end]`.
    pub const fn new(start: u64, end: u64) -> Self {
        Self(std::range::RangeInclusive { start, last: end })
    }

    /// The inclusive lower bound.
    #[inline]
    pub const fn start(&self) -> u64 {
        self.0.start
    }

    /// The inclusive upper bound.
    #[inline]
    pub const fn end(&self) -> u64 {
        self.0.last
    }

    /// Returns the ceil midpoint key of this inclusive range.
    ///
    /// For odd key counts, the right side gets one extra key.
    #[inline]
    pub const fn midpoint(&self) -> PartitionKey {
        let midpoint = (self.0.start as u128 + self.0.last as u128).div_ceil(2);
        debug_assert!(midpoint <= u64::MAX as u128);
        midpoint as PartitionKey
    }

    #[inline]
    pub const fn num_keys(&self) -> u128 {
        (self.end() as u128) - (self.start() as u128) + 1
    }

    #[inline]
    pub fn intersect(&self, other: &Self) -> Option<KeyRange> {
        let start = self.0.start.max(other.0.start);
        let end = self.0.last.min(other.0.last);
        (start <= end).then_some(KeyRange::new(start, end))
    }

    /// Returns an iterator over all keys in this range.
    #[inline]
    pub fn iter(&self) -> RangeInclusiveIter<u64> {
        self.0.iter()
    }

    /// Returns `true` if `self` and `other` share at least one key.
    #[inline]
    pub const fn is_overlapping(&self, other: &KeyRange) -> bool {
        self.start() <= other.end() && other.start() <= self.end()
    }
}

impl RangeBounds<u64> for KeyRange {
    #[inline]
    fn start_bound(&self) -> Bound<&u64> {
        self.0.start_bound()
    }

    #[inline]
    fn end_bound(&self) -> Bound<&u64> {
        self.0.end_bound()
    }
}

impl From<std::ops::RangeInclusive<u64>> for KeyRange {
    #[inline]
    fn from(range: std::ops::RangeInclusive<u64>) -> Self {
        Self::new(*range.start(), *range.end())
    }
}

impl From<KeyRange> for std::ops::RangeInclusive<u64> {
    #[inline]
    fn from(range: KeyRange) -> Self {
        range.start()..=range.end()
    }
}

impl From<std::range::RangeInclusive<u64>> for KeyRange {
    #[inline]
    fn from(range: std::range::RangeInclusive<u64>) -> Self {
        Self(range)
    }
}

impl From<KeyRange> for std::range::RangeInclusive<u64> {
    #[inline]
    fn from(range: KeyRange) -> Self {
        range.0
    }
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {}]", self.start(), self.end())
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use super::KeyRange;

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

    /// Deserializes from `{"start": <u64>, "end": <u64>}` to match `std::ops::RangeInclusive`'s
    /// serde format.
    impl<'de> Deserialize<'de> for KeyRange {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Raw {
                start: u64,
                end: u64,
            }
            let raw = Raw::deserialize(deserializer)?;
            Ok(KeyRange::new(raw.start, raw.end))
        }
    }
}

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
    fn iter() {
        let r = KeyRange::new(5, 9);
        let keys: Vec<u64> = r.iter().collect();
        assert_eq!(keys, vec![5, 6, 7, 8, 9]);

        let single = KeyRange::new(42, 42);
        let keys: Vec<u64> = single.iter().collect();
        assert_eq!(keys, vec![42]);
    }

    #[test]
    fn is_overlapping() {
        let a = KeyRange::new(0, 10);
        let b = KeyRange::new(5, 15);
        assert!(a.is_overlapping(&b));
        assert!(b.is_overlapping(&a));

        // Adjacent ranges don't overlap
        let c = KeyRange::new(11, 20);
        assert!(!a.is_overlapping(&c));

        // Touching at boundary
        let d = KeyRange::new(10, 20);
        assert!(a.is_overlapping(&d));

        // Contained
        let e = KeyRange::new(2, 8);
        assert!(a.is_overlapping(&e));

        // Same range
        assert!(a.is_overlapping(&a));

        // Disjoint
        let f = KeyRange::new(100, 200);
        assert!(!a.is_overlapping(&f));
    }

    #[test]
    fn intersect() {
        let a = KeyRange::new(0, 10);

        // Overlapping in the middle.
        assert_eq!(
            a.intersect(&KeyRange::new(5, 15)),
            Some(KeyRange::new(5, 10))
        );
        // Containment: smaller range wins.
        assert_eq!(a.intersect(&KeyRange::new(2, 8)), Some(KeyRange::new(2, 8)));
        // Single-key overlap at the boundary.
        assert_eq!(
            a.intersect(&KeyRange::new(10, 20)),
            Some(KeyRange::new(10, 10))
        );
        // Same range.
        assert_eq!(a.intersect(&a), Some(a));
        // Disjoint: adjacent (no shared key).
        assert_eq!(a.intersect(&KeyRange::new(11, 20)), None);
        // Disjoint: far apart.
        assert_eq!(a.intersect(&KeyRange::new(100, 200)), None);
        // Commutativity on a representative pair.
        let b = KeyRange::new(5, 15);
        assert_eq!(a.intersect(&b), b.intersect(&a));
        // Boundary values.
        assert_eq!(
            KeyRange::FULL.intersect(&KeyRange::new(u64::MAX, u64::MAX)),
            Some(KeyRange::new(u64::MAX, u64::MAX))
        );
    }

    #[test]
    fn json_wire_compat_with_ops_range_inclusive() {
        // KeyRange must be wire-compatible with std::ops::RangeInclusive<u64> in JSON.
        let kr = KeyRange::new(100, 200);
        let ops = std::ops::RangeInclusive::new(100u64, 200u64);

        let kr_json = serde_json::to_string(&kr).unwrap();
        let ops_json = serde_json::to_string(&ops).unwrap();
        assert_eq!(kr_json, ops_json, "serialized forms must be identical");

        // Cross-deserialize in both directions
        let kr_from_ops: KeyRange = serde_json::from_str(&ops_json).unwrap();
        assert_eq!(kr_from_ops, kr);

        let ops_from_kr: std::ops::RangeInclusive<u64> = serde_json::from_str(&kr_json).unwrap();
        assert_eq!(ops_from_kr, ops);
    }

    #[test]
    fn json_wire_compat_boundary_values() {
        // Verify wire compat at boundary values (0, u64::MAX)
        for (start, end) in [(0u64, 0u64), (0, u64::MAX), (u64::MAX, u64::MAX)] {
            let kr = KeyRange::new(start, end);
            let ops = std::ops::RangeInclusive::new(start, end);

            let kr_json = serde_json::to_string(&kr).unwrap();
            let ops_json = serde_json::to_string(&ops).unwrap();
            assert_eq!(kr_json, ops_json, "mismatch at ({start}, {end})");

            let roundtrip: KeyRange = serde_json::from_str(&ops_json).unwrap();
            assert_eq!(roundtrip, kr);
        }
    }

    #[test]
    fn flexbuffers_wire_compat_with_ops_range_inclusive() {
        // KeyRange must be wire-compatible with std::ops::RangeInclusive<u64> in flexbuffers.
        let kr = KeyRange::new(100, 200);
        let ops = std::ops::RangeInclusive::new(100u64, 200u64);

        let kr_buf = flexbuffers::to_vec(kr).unwrap();
        let ops_buf = flexbuffers::to_vec(&ops).unwrap();
        assert_eq!(kr_buf, ops_buf, "serialized forms must be identical");

        // Cross-deserialize in both directions
        let kr_from_ops: KeyRange = flexbuffers::from_slice(&ops_buf).unwrap();
        assert_eq!(kr_from_ops, kr);

        let ops_from_kr: std::ops::RangeInclusive<u64> = flexbuffers::from_slice(&kr_buf).unwrap();
        assert_eq!(ops_from_kr, ops);
    }

    #[test]
    fn flexbuffers_wire_compat_boundary_values() {
        for (start, end) in [(0u64, 0u64), (0, u64::MAX), (u64::MAX, u64::MAX)] {
            let kr = KeyRange::new(start, end);
            let ops = std::ops::RangeInclusive::new(start, end);

            let kr_buf = flexbuffers::to_vec(kr).unwrap();
            let ops_buf = flexbuffers::to_vec(&ops).unwrap();
            assert_eq!(kr_buf, ops_buf, "mismatch at ({start}, {end})");

            let roundtrip: KeyRange = flexbuffers::from_slice(&ops_buf).unwrap();
            assert_eq!(roundtrip, kr);
        }
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
