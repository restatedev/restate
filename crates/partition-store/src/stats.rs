// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Partition-scoped statistics, kept as registered indexes under
//! [`KeyKind::Stats`](crate::keys::KeyKind::Stats).
//!
//! A key consists of an eleven-byte header, the index's
//! [`AggregateKeyPrefix`] and the statistic's [`StatKind`], followed by the
//! statistic's payload. The header identifies the physical partition and the
//! registry's index ID of the index; it does not contain a service's
//! partition key. Ordered payloads use the same schema-selected field codecs as
//! other ordered keys. Reads resolve the Ready index of the statistic's
//! definition, and fail when there is none.
//!
//! Stored values and pending merge operands have separate codecs. Full merges
//! apply signed deltas to unsigned counters; partial merges combine deltas
//! without needing a stored value. The persisted [`StatKind`] selects those
//! codecs without consulting the registry.
//!
//! Zero-valued aggregates are equivalent to absent keys. Readers must omit them
//! using [`aggregated::Aggregate::should_filter`]; compaction can reclaim their
//! stored representations later. Neither path may treat a pending operand as a
//! stored value.
//!
//! `KeyKind::LegacyStats` is the retired layout a pre-release build used. It is
//! never decoded here; a later commit deletes such keys when a store opens.

pub mod aggregated;
mod codec;
mod macros;

// Re-exports
pub use codec::{
    AggregateKeyHeader, AggregateKeyPrefix, DecodeStatKey, EncodeStatKey, HEADER_LENGTH,
    StatMergeValueCodec, StatValueCodec,
};

use bytes::BufMut;
use rocksdb::{CompactionDecision, MergeOperands};
use tracing::{error, warn};
use zerocopy::IntoBytes;

use crate::keys::header::IndexScope;

use self::aggregated::Aggregate;

/// The registered definition, key payload, and stored value of a statistic.
///
/// `define_aggregated_stat!` derives the kind and value from one
/// aggregation implementation so its writes and merges agree on the format.
pub trait Stat {
    /// The name of the registry definition whose indexes hold this statistic.
    const DEFINITION: &'static str;
    const STAT_KIND: StatKind;

    type OwnedKey: DecodeStatKey<Self>;
    type Value: StatValueCodec;

    /// Appends a complete key of index `scope`, including its header, without
    /// clearing `scratch`.
    fn encode_key<K, B: BufMut>(scope: IndexScope, key: K, scratch: &mut B)
    where
        K: EncodeStatKey<Self>,
    {
        encode_header::<Self, _>(scope, scratch);
        key.encode(scratch);
    }

    /// The complete encoded length, including the header.
    fn encoded_key_len<K>(key: &K) -> usize
    where
        K: EncodeStatKey<Self>,
    {
        HEADER_LENGTH + key.serialized_length()
    }
}

/// Appends the header of statistic `S`'s keys in index `scope`.
pub(crate) fn encode_header<S: Stat + ?Sized, B: BufMut>(scope: IndexScope, buffer: &mut B) {
    buffer.put_slice(AggregateKeyPrefix::new(scope.partition, scope.index).as_bytes());
    buffer.put_u8(S::STAT_KIND as u8);
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

/// RocksDB's full merge for a statistic key: applies `operands` to the stored
/// value, or to zero when the key is absent.
pub fn full_merge(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    aggregated::full_merge(stat_kind(key)?, existing_val, operands)
}

/// RocksDB's partial merge for a statistic key: combines `operands` without the
/// stored value.
pub fn partial_merge(key: &[u8], operands: &MergeOperands) -> Option<Vec<u8>> {
    aggregated::partial_merge(stat_kind(key)?, operands)
}

/// RocksDB's compaction filter for a statistic key: drops a stored value that
/// stands for all-zero counts, which readers already treat as absent. RocksDB
/// keeps unresolved merge operands separately, so this only sees stored values.
pub fn compaction_filter(key: &[u8], value: &[u8]) -> CompactionDecision {
    let Ok((_, kind, _)) = AggregateKeyPrefix::decode(key) else {
        warn!(
            key = %format_args!("{key:02x?}"),
            "Failed to decode statistic key during compaction"
        );
        return CompactionDecision::Keep;
    };
    if kind.should_filter_value(value) {
        CompactionDecision::Remove
    } else {
        CompactionDecision::Keep
    }
}

fn stat_kind(key: &[u8]) -> Option<StatKind> {
    match AggregateKeyPrefix::decode(key) {
        Ok((_, kind, _)) => Some(kind),
        Err(err) => {
            error!(?err, ?key, "failed to decode statistic key");
            None
        }
    }
}
