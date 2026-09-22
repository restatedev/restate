// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod aggregated;
mod codec;
mod macros;

// Re-exports
pub use codec::{DecodeStatKey, EncodeStatKey, StatKeyPrefix, StatMergeValueCodec, StatValueCodec};

use bytes::BufMut;
use rocksdb::MergeOperands;
use tracing::error;
use zerocopy::IntoBytes;

use restate_types::sharding::PartitionId;

use crate::keys::KeyKind;

use self::aggregated::Aggregate;

const KEY_KIND: KeyKind = KeyKind::Stats;

/// Globally unique identifiers for statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::FromRepr)]
#[repr(u16)]
#[allow(clippy::enum_variant_names)]
pub enum StatId {
    ServiceLoad = 1,
    DeploymentLoad = 2,
    VirtualObjectLoad = 3,
}

impl StatId {
    /// Returns the identifier's persisted representation.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

pub trait Stat {
    const STAT_ID: StatId;
    const STAT_KIND: StatKind;

    type OwnedKey: DecodeStatKey<Self>;
    type Value: StatValueCodec;

    fn encode_key<K, B: BufMut>(partition_id: PartitionId, key: K, scratch: &mut B)
    where
        K: EncodeStatKey<Self>,
    {
        scratch.put_slice(StatKeyPrefix::of::<Self>(partition_id).as_bytes());

        // write the key components
        key.encode(scratch);
    }

    fn encoded_key_len<K>(key: &K) -> usize
    where
        K: EncodeStatKey<Self>,
    {
        size_of::<StatKeyPrefix>() + key.encoded_len()
    }
}

/// The aggregation algorithm and value encoding used by a statistic.
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
    match prefix.stat_kind() {
        StatKind::AggregatedGauge => aggregated::full_merge(prefix, existing_val, operands),
        StatKind::StageBucketedGauge => aggregated::full_merge(prefix, existing_val, operands),
        StatKind::StageStatusBucketedGauge => {
            aggregated::full_merge(prefix, existing_val, operands)
        }
    }
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

    match prefix.stat_kind() {
        StatKind::AggregatedGauge => aggregated::partial_merge(prefix, operands),
        StatKind::StageBucketedGauge => aggregated::partial_merge(prefix, operands),
        StatKind::StageStatusBucketedGauge => aggregated::partial_merge(prefix, operands),
    }
}
