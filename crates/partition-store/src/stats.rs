// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical encodings and merge operations for partition-scoped statistics.
//!
//! A key consists of a ten-byte [`StatKeyPrefix`] followed by the statistic's
//! payload. The prefix identifies the physical partition, statistic, and
//! aggregation kind; it does not contain a service's partition key. Registered
//! statistics use [`AggregateKeyPrefix`] followed by the aggregation kind.
//! Key payloads are interpreted by their owning statistic definitions.
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

// Re-exports
pub use codec::{AggregateKeyPrefix, StatKeyPrefix, StatMergeValueCodec, StatValueCodec};

use rocksdb::MergeOperands;
use tracing::error;

use crate::keys::KeyKind;

use self::aggregated::Aggregate;

const KEY_KIND: KeyKind = KeyKind::Stats;

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

/// Merges registered-statistic keys ([`AggregateKeyPrefix`]).
pub fn full_merge_registered(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    aggregated::full_merge(registered_stat_kind(key)?, existing_val, operands)
}

/// Partially merges registered-statistic keys ([`AggregateKeyPrefix`]).
pub fn partial_merge_registered(key: &[u8], operands: &MergeOperands) -> Option<Vec<u8>> {
    aggregated::partial_merge(registered_stat_kind(key)?, operands)
}

fn registered_stat_kind(key: &[u8]) -> Option<StatKind> {
    match AggregateKeyPrefix::decode(key) {
        Ok((_, kind, _)) => Some(kind),
        Err(err) => {
            error!(?err, ?key, "failed to decode registered statistic key");
            None
        }
    }
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
    aggregated::full_merge(prefix.stat_kind(), existing_val, operands)
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

    aggregated::partial_merge(prefix.stat_kind(), operands)
}
