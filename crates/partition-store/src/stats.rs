// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical encodings and merge operations for partition-level statistics.
//!
//! A statistic is an aggregate index: counters derived from a table, kept up to
//! date by RocksDB merge operands rather than by read-modify-write. Its keys are
//! `key kind (2) | partition padding (2) | partition id (2) | index id (4) |
//! aggregation kind (1) | payload`: the ten-byte [`AggregateKeyPrefix`], which
//! mirrors the secondary-index prefix with the registry's `u32` index ID, then
//! the [`StatKind`] and a payload that the owning statistic definition
//! interprets. The prefix never contains a service's partition key.
//!
//! Stored values and pending merge operands have separate codecs. A full merge
//! applies signed deltas to unsigned counters; a partial merge combines deltas
//! without needing the stored value. The persisted [`StatKind`] selects the
//! codecs, so the merge operator and the compaction filter never consult the
//! registry.
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

// Re-exports
pub use codec::{AggregateKeyPrefix, StatMergeValueCodec, StatValueCodec};

use rocksdb::{CompactionDecision, MergeOperands};
use tracing::{error, warn};

use self::aggregated::Aggregate;

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
