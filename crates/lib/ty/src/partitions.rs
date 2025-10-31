// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::Hash;

use restate_encoding::{BilrostNewType, NetSerde};

/// Identifying the leader epoch of a partition processor
#[derive(
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    derive_more::Debug,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[display("e{}", _0)]
#[debug("e{}", _0)]
pub struct LeaderEpoch(u64);
impl LeaderEpoch {
    pub const INVALID: Self = Self(0);
    pub const INITIAL: Self = Self(1);

    pub fn next(self) -> Self {
        LeaderEpoch(self.0 + 1)
    }
}

impl Default for LeaderEpoch {
    fn default() -> Self {
        Self::INITIAL
    }
}

impl From<crate::protobuf::LeaderEpoch> for LeaderEpoch {
    fn from(epoch: crate::protobuf::LeaderEpoch) -> Self {
        Self::from(epoch.value)
    }
}

impl From<LeaderEpoch> for crate::protobuf::LeaderEpoch {
    fn from(epoch: LeaderEpoch) -> Self {
        let value: u64 = epoch.into();
        Self { value }
    }
}

/// Identifying the partition
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::Debug,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[repr(transparent)]
#[serde(transparent)]
#[debug("{}", _0)]
pub struct PartitionId(u16);

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        u32::from(value.0)
    }
}

impl From<PartitionId> for u64 {
    fn from(value: PartitionId) -> Self {
        u64::from(value.0)
    }
}

impl PartitionId {
    /// It's your responsibility to ensure the value is within the valid range.
    pub const fn new_unchecked(v: u16) -> Self {
        Self(v)
    }

    pub const MIN: Self = Self(u16::MIN);
    // 65535 partitions.
    pub const MAX: Self = Self(u16::MAX);

    #[inline]
    pub fn next(self) -> Self {
        Self(std::cmp::min(*Self::MAX, self.0.saturating_add(1)))
    }
}

/// The leader epoch of a given partition
pub type PartitionLeaderEpoch = (PartitionId, LeaderEpoch);

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u64;

/// Returns the partition key computed from either the service_key, or idempotency_key, if possible
pub(crate) fn deterministic_partition_key(
    service_key: Option<&str>,
    idempotency_key: Option<&str>,
) -> Option<PartitionKey> {
    service_key
        .map(partitioner::HashPartitioner::compute_partition_key)
        .or_else(|| idempotency_key.map(partitioner::HashPartitioner::compute_partition_key))
}

/// Trait for data structures that have a partition key
pub trait WithPartitionKey {
    /// Returns the partition key
    fn partition_key(&self) -> PartitionKey;
}

pub mod partitioner {
    use super::PartitionKey;

    use std::hash::{Hash, Hasher};

    /// Computes the [`PartitionKey`] based on xxh3 hashing.
    pub struct HashPartitioner;

    impl HashPartitioner {
        pub fn compute_partition_key(value: impl Hash) -> PartitionKey {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            value.hash(&mut hasher);
            hasher.finish()
        }
    }
}
