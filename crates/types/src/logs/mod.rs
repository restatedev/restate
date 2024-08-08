// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crate::identifiers::PartitionId;
use crate::storage::{StorageCodecKind, StorageEncode};

pub mod builder;
pub mod metadata;

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    Serialize,
    Deserialize,
)]
pub struct LogId(u64);

impl LogId {
    // This is allows the usage of the first 62 bits for log ids while keeping a space for
    // internal logs as needed. Partitions cannot be larger than 2^62.
    pub const MAX_PARTITION_LOG: LogId = LogId((1 << 62) - 1);
    pub const MIN: LogId = LogId(0);
}

impl LogId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl From<PartitionId> for LogId {
    fn from(value: PartitionId) -> Self {
        LogId(*value)
    }
}

/// The log sequence number.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    derive_more::Into,
    derive_more::From,
    derive_more::Add,
    derive_more::Display,
    Serialize,
    Deserialize,
)]
pub struct Lsn(u64);

impl Lsn {
    pub const fn new(lsn: u64) -> Self {
        Lsn(lsn)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<crate::protobuf::common::Lsn> for Lsn {
    fn from(lsn: crate::protobuf::common::Lsn) -> Self {
        Self::from(lsn.value)
    }
}

impl From<Lsn> for crate::protobuf::common::Lsn {
    fn from(lsn: Lsn) -> Self {
        let value: u64 = lsn.into();
        Self { value }
    }
}

impl SequenceNumber for Lsn {
    /// The maximum possible sequence number, this is useful when creating a read stream
    /// with an open ended tail.
    const MAX: Self = Lsn(u64::MAX);
    /// 0 is not a valid sequence number. This sequence number represents invalid position
    /// in the log, or that the log has been that has been trimmed.
    const INVALID: Self = Lsn(0);
    /// Guaranteed to be less than or equal to the oldest possible sequence
    /// number in a log. This is useful when seeking to the head of a log.
    const OLDEST: Self = Lsn(1);

    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

pub trait SequenceNumber
where
    Self:
        Copy + std::fmt::Debug + Sized + Into<u64> + From<u64> + Eq + PartialEq + Ord + PartialOrd,
{
    /// The maximum possible sequence number, this is useful when creating a read stream
    const MAX: Self;
    /// Not a valid sequence number. This sequence number represents invalid position
    /// in the log, or that the log has been that has been trimmed.
    const INVALID: Self;

    /// Guaranteed to be less than or equal to the oldest possible sequence
    /// number in a log. This is useful when seeking to the head of a log.
    const OLDEST: Self;

    fn next(self) -> Self;
    fn prev(self) -> Self;
}

#[derive(Debug, Clone, Default)]
/// The keys that are associated with a record. This is used to filter the log when reading.
pub enum Keys {
    /// No keys are associated with the record. This record will appear to *all* readers regardless
    /// of the KeyFilter they use.
    #[default]
    None,
    /// A single key is associated with the record
    Single(u64),
    /// A pair of keys are associated with the record
    Pair(u64, u64),
    /// The record is associated with all keys within this range (inclusive)
    RangeInclusive(std::ops::RangeInclusive<u64>),
}

impl Keys {
    /// Returns true if the key matches the supplied `query`
    pub fn matches_filter(&self, query: &KeyFilter) -> bool {
        match (self, query) {
            // regardless of the matcher.
            (Keys::None, _) => true,
            (_, KeyFilter::Any) => true,
            (Keys::Single(key1), KeyFilter::Include(key2)) => key1 == key2,
            (Keys::Single(key), KeyFilter::Within(range)) => range.contains(key),
            (Keys::Pair(first, second), KeyFilter::Include(key)) => key == first || key == second,
            (Keys::Pair(first, second), KeyFilter::Within(range)) => {
                range.contains(first) || range.contains(second)
            }
            (Keys::RangeInclusive(range), KeyFilter::Include(key)) => range.contains(key),
            (Keys::RangeInclusive(range1), KeyFilter::Within(range2)) => {
                // A record matches if ranges intersect
                range1.start() <= range2.end() && range1.end() >= range2.start()
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u64> + 'static> {
        match self {
            Keys::None => Box::new(std::iter::empty()),
            Keys::Single(key) => Box::new(std::iter::once(*key)),
            Keys::Pair(first, second) => Box::new([*first, *second].into_iter()),
            Keys::RangeInclusive(range) => Box::new(range.clone()),
        }
    }
}

impl IntoIterator for Keys {
    type Item = u64;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'static>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A type that describes which records a reader should pick
#[derive(Debug, Clone, Default)]
pub enum KeyFilter {
    #[default]
    // Matches any record
    Any,
    // Match records that have a specific key, or no keys at all.
    Include(u64),
    // Match records that have _any_ keys falling within this inclusive range,
    // in addition to records with no keys.
    Within(std::ops::RangeInclusive<u64>),
}

impl From<u64> for KeyFilter {
    fn from(key: u64) -> Self {
        KeyFilter::Include(key)
    }
}

impl From<RangeInclusive<u64>> for KeyFilter {
    fn from(range: RangeInclusive<u64>) -> Self {
        KeyFilter::Within(range)
    }
}

pub trait HasRecordKeys: Send + Sync {
    /// Keys of the record. Keys are used to filter the log when reading.
    fn record_keys(&self) -> Keys;

    /// Returns true if this record matches the supplied `filter`
    fn matches_filter(&self, filter: &KeyFilter) -> bool {
        self.record_keys().matches_filter(filter)
    }
}

impl<T: HasRecordKeys> HasRecordKeys for &T {
    fn record_keys(&self) -> Keys {
        HasRecordKeys::record_keys(*self)
    }
}

pub trait WithKeys: Sized {
    fn with_keys(self, keys: Keys) -> BodyWithKeys<Self>;
}

impl<T> WithKeys for T
where
    T: StorageEncode + Sync + Send + 'static,
{
    fn with_keys(self, keys: Keys) -> BodyWithKeys<Self> {
        BodyWithKeys::new(self, keys)
    }
}

/// A transparent wrapper that combines augments a type with some keys. The type has a blanket
/// implementation of StorageEncode that's a passthrough to the inner type `T`. This means
/// that you can use this to pass payloads to Bifrost without the need to store Keys inside the
/// body `T`.
///
/// Then reading records that were appended with [`WithKeys`], you directly deserialize the inner
/// type T without having to worry about the keys.
pub struct BodyWithKeys<T> {
    inner: T,
    keys: Keys,
}

impl<T: StorageEncode> BodyWithKeys<T> {
    pub fn new(inner: T, keys: Keys) -> Self {
        Self { inner, keys }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> HasRecordKeys for BodyWithKeys<T>
where
    T: Send + Sync + 'static,
{
    fn record_keys(&self) -> Keys {
        self.keys.clone()
    }
}

// passthrough to storage-encode of the inner type
impl<T> StorageEncode for BodyWithKeys<T>
where
    T: StorageEncode,
{
    fn default_codec(&self) -> StorageCodecKind {
        StorageEncode::default_codec(&self.inner)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), crate::storage::StorageEncodeError> {
        T::encode(&self.inner, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Data {
        src_key: u64,
        dst_key: u64,
    }

    impl HasRecordKeys for Data {
        fn record_keys(&self) -> Keys {
            Keys::Pair(self.src_key, self.dst_key)
        }
    }

    #[test]
    fn has_record_keys() {
        let data = Data {
            src_key: 1,
            dst_key: 10,
        };

        assert!(!data.matches_filter(&KeyFilter::Include(5)));
        assert!(data.matches_filter(&KeyFilter::Include(1)));
        assert!(data.matches_filter(&KeyFilter::Include(10)));

        assert!(data.matches_filter(&KeyFilter::Any));
        assert!(data.matches_filter(&KeyFilter::Within(1..=200)));
        assert!(data.matches_filter(&KeyFilter::Within(10..=200)));
        assert!(!data.matches_filter(&KeyFilter::Within(11..=200)));
        assert!(!data.matches_filter(&KeyFilter::Within(100..=200)));

        let keys: Vec<_> = data.record_keys().iter().collect();
        assert_eq!(vec![1, 10], keys);
    }

    #[test]
    fn key_matches() {
        let keys = Keys::None;
        // A record with no keys matches all filters.
        assert!(keys.matches_filter(&KeyFilter::Any));
        assert!(keys.matches_filter(&KeyFilter::Include(u64::MIN)));
        assert!(keys.matches_filter(&KeyFilter::Include(100)));
        assert!(keys.matches_filter(&KeyFilter::Within(100..=1000)));

        let keys = Keys::Single(10);
        assert!(keys.matches_filter(&KeyFilter::Any));
        assert!(keys.matches_filter(&KeyFilter::Include(10)));
        assert!(keys.matches_filter(&KeyFilter::Within(1..=100)));
        assert!(keys.matches_filter(&KeyFilter::Within(5..=10)));
        assert!(!keys.matches_filter(&KeyFilter::Include(100)));
        assert!(!keys.matches_filter(&KeyFilter::Within(1..=9)));
        assert!(!keys.matches_filter(&KeyFilter::Within(20..=900)));

        let keys = Keys::Pair(1, 10);
        assert!(keys.matches_filter(&KeyFilter::Any));
        assert!(keys.matches_filter(&KeyFilter::Include(10)));
        assert!(keys.matches_filter(&KeyFilter::Include(1)));
        assert!(!keys.matches_filter(&KeyFilter::Include(0)));
        assert!(!keys.matches_filter(&KeyFilter::Include(100)));
        assert!(keys.matches_filter(&KeyFilter::Within(1..=3)));
        assert!(keys.matches_filter(&KeyFilter::Within(3..=10)));

        assert!(!keys.matches_filter(&KeyFilter::Within(2..=7)));
        assert!(!keys.matches_filter(&KeyFilter::Within(11..=100)));

        let keys = Keys::RangeInclusive(5..=100);
        assert!(keys.matches_filter(&KeyFilter::Any));
        assert!(keys.matches_filter(&KeyFilter::Include(5)));
        assert!(keys.matches_filter(&KeyFilter::Include(10)));
        assert!(keys.matches_filter(&KeyFilter::Include(100)));
        assert!(!keys.matches_filter(&KeyFilter::Include(4)));
        assert!(!keys.matches_filter(&KeyFilter::Include(101)));

        assert!(keys.matches_filter(&KeyFilter::Within(5..=100)));
        assert!(keys.matches_filter(&KeyFilter::Within(1..=100)));
        assert!(keys.matches_filter(&KeyFilter::Within(2..=105)));
        assert!(keys.matches_filter(&KeyFilter::Within(10..=88)));
        assert!(!keys.matches_filter(&KeyFilter::Within(1..=4)));
        assert!(!keys.matches_filter(&KeyFilter::Within(101..=1000)));
    }
}
