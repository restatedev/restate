// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Partition-scoped statistics stored under [`KeyKind::Stats`].
//!
//! A key consists of a ten-byte [`StatKeyPrefix`] followed by the statistic's
//! payload. The prefix identifies the physical partition, statistic, and
//! aggregation kind; it does not contain a service's partition key. Ordered
//! payloads use the same schema-selected field codecs as other ordered keys.
//!
//! Stored values and pending merge operands have separate codecs. Full merges
//! apply signed deltas to unsigned counters; partial merges combine deltas
//! without needing a stored value. The persisted [`StatKind`] selects those
//! codecs even when the statistic's raw identifier is unknown to this binary.
//!
//! Zero-valued aggregates are equivalent to absent keys. Readers must omit them
//! using [`aggregated::Aggregate::should_filter`]; compaction can reclaim their
//! stored representations later. Neither path may treat a pending operand as a
//! stored value.

pub mod aggregated;
mod codec;
mod macros;

// Re-exports
pub use codec::{
    DecodeStatKey, EncodeStatKey, StatKeyHeader, StatKeyPrefix, StatMergeValueCodec, StatValueCodec,
};

use bytes::BufMut;
use rocksdb::MergeOperands;
use tracing::error;
use zerocopy::IntoBytes;

use restate_types::sharding::PartitionId;

use crate::keys::KeyKind;

use self::aggregated::Aggregate;

const KEY_KIND: KeyKind = KeyKind::Stats;

/// Globally unique identifiers for statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[allow(clippy::enum_variant_names)]
pub enum StatId {}

impl StatId {
    /// Returns the identifier's persisted representation.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// The fixed identity, key payload, and stored value of a statistic.
///
/// `define_aggregated_stat!` derives the kind and value from one
/// aggregation implementation so its writes and merges agree on the format.
pub trait Stat {
    const STAT_ID: StatId;
    const STAT_KIND: StatKind;

    type OwnedKey: DecodeStatKey<Self>;
    type Value: StatValueCodec;

    /// Appends a complete key, including its fixed prefix, without clearing `scratch`.
    fn encode_key<K, B: BufMut>(partition_id: PartitionId, key: K, scratch: &mut B)
    where
        K: EncodeStatKey<Self>,
    {
        scratch.put_slice(StatKeyPrefix::of::<Self>(partition_id).as_bytes());

        // write the key components
        key.encode(scratch);
    }

    /// The complete encoded length, including the fixed prefix.
    fn encoded_key_len<K>(key: &K) -> usize
    where
        K: EncodeStatKey<Self>,
    {
        size_of::<StatKeyPrefix>() + key.serialized_length()
    }
}

/// The aggregation algorithm and value encoding used by a statistic.
/// These discriminants are persisted in keys and must remain stable.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::FromRepr,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::Unaligned,
)]
#[repr(u8)]
#[allow(clippy::enum_variant_names)]
pub enum StatKind {
    /// An unsigned value updated with signed deltas.
    AggregatedGauge = 1,
    /// Sparse unsigned counters keyed by a compact stage representation.
    StageBucketedGauge = 2,
    /// Sparse unsigned counters keyed by compact stage and status representations.
    StageStatusBucketedGauge = 3,
}

impl StatKind {
    pub(crate) fn should_filter_value(self, value_payload: &[u8]) -> bool {
        match self {
            StatKind::AggregatedGauge => aggregated::Gauge::should_filter_value(value_payload),
            StatKind::StageBucketedGauge => {
                aggregated::StageGauge::should_filter_value(value_payload)
            }
            StatKind::StageStatusBucketedGauge => {
                aggregated::StageStatusGauge::should_filter_value(value_payload)
            }
        }
    }
}

pub fn full_merge(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    full_merge_slices(key, existing_val, operands)
}

pub fn partial_merge(key: &[u8], operands: &MergeOperands) -> Option<Vec<u8>> {
    partial_merge_slices(key, operands)
}

fn full_merge_slices<'a>(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let prefix = match StatKeyPrefix::decode_prefix(key) {
        Ok((prefix, _)) => prefix,
        Err(err) => {
            error!(?err, ?key, "failed to decode aggregated statistic key");
            return None;
        }
    };
    aggregated::full_merge(prefix, existing_val, operands)
}

fn partial_merge_slices<'a>(
    key: &[u8],
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let prefix = match StatKeyPrefix::decode_prefix(key) {
        Ok((prefix, _)) => prefix,
        Err(err) => {
            error!(?err, ?key, "failed to decode aggregated statistic key");
            return None;
        }
    };

    aggregated::partial_merge(prefix, operands)
}
