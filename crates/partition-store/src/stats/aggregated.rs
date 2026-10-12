// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod bucketed_gauge;
mod deployment_load;
mod gauge;
mod scan;
mod service_load;
mod virtual_object_load;

// Re-exports
pub use bucketed_gauge::{
    BucketedGauge, StageCounts, StageGauge, StageStatus, StageStatusCounts, StageStatusGauge,
};
pub use deployment_load::{DeploymentLoadKey, DeploymentLoadKeyRef};
pub use gauge::{Gauge, GaugeOp};
pub use service_load::{ServiceLoadKey, ServiceLoadKeyRef};
pub use virtual_object_load::VirtualObjectLoadKey;

use tracing::error;

use restate_storage_api::vqueue_table::Stage;

use crate::{PartitionStoreTransaction, StorageAccess};

use super::{
    EncodeStatKey, KEY_KIND, Stat, StatKeyPrefix, StatKind, StatMergeValueCodec, StatValueCodec,
};

/// A statistic updated through an aggregation implementation.
pub trait AggregatedStat: Stat {
    type Aggregation: Aggregate<Output = Self::Value>;

    // fn should_filter(value: &Self::Value) -> bool;
}

/// The persisted kind, value types, and merge behavior of an aggregation implementation.
pub trait Aggregate {
    const STAT_KIND: StatKind;
    type Output: Default + StatValueCodec;
    type MergeOperand: StatMergeValueCodec;

    fn aggregate_full(
        existing_val: Self::Output,
        operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Self::Output>;

    fn aggregate_partial(
        _operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Option<Self::MergeOperand>>;

    /// Return true if the key-value pair should be filtered out during compaction.
    fn should_filter_value(value_payload: &[u8]) -> bool;

    /// Return true if the key-value pair should be filtered out when returning a result.
    fn should_filter(value: &Self::Output) -> bool;
}

pub(crate) struct AggregatedStatsMut<'a, 'b> {
    storage: &'a mut PartitionStoreTransaction<'b>,
}

impl<'a, 'b> AggregatedStatsMut<'a, 'b> {
    pub fn new(storage: &'a mut PartitionStoreTransaction<'b>) -> Self {
        Self { storage }
    }

    pub fn increment_stage<S, K>(&mut self, key: K, stage: Stage)
    where
        S: AggregatedStat<Aggregation = StageGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageGauge::increment(stage));
    }

    pub fn decrement_stage<S, K>(&mut self, key: K, stage: Stage)
    where
        S: AggregatedStat<Aggregation = StageGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageGauge::decrement(stage));
    }

    pub fn transition_stage<S, K>(&mut self, key: K, old_stage: Stage, new_stage: Stage)
    where
        S: AggregatedStat<Aggregation = StageGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageGauge::transition(old_stage, new_stage));
    }

    pub fn increment_stage_status<S, K>(&mut self, key: K, bucket: StageStatus)
    where
        S: AggregatedStat<Aggregation = StageStatusGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageStatusGauge::increment(bucket));
    }

    pub fn decrement_stage_status<S, K>(&mut self, key: K, bucket: StageStatus)
    where
        S: AggregatedStat<Aggregation = StageStatusGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageStatusGauge::decrement(bucket));
    }

    pub fn transition_stage_status<S, K>(
        &mut self,
        key: K,
        old_bucket: StageStatus,
        new_bucket: StageStatus,
    ) where
        S: AggregatedStat<Aggregation = StageStatusGauge>,
        K: EncodeStatKey<S>,
    {
        self.merge::<S, _>(key, StageStatusGauge::transition(old_bucket, new_bucket));
    }

    fn merge<S: AggregatedStat, K: EncodeStatKey<S>>(&mut self, key: K, operand: impl AsRef<[u8]>) {
        let key = {
            let partition_id = self.storage.partition_id();
            let key_buf = self
                .storage
                .cleared_key_buffer_mut(S::encoded_key_len(&key));
            S::encode_key(partition_id, key, key_buf);
            key_buf.split()
        };

        self.storage.raw_merge_cf(KEY_KIND, key, operand);
    }
}

pub(super) fn partial_merge<'a>(
    prefix: &StatKeyPrefix,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let result = match prefix.stat_kind() {
        StatKind::AggregatedGauge => partial_merge_aggregator::<Gauge>(operands),
        StatKind::StageBucketedGauge => partial_merge_aggregator::<StageGauge>(operands),
        StatKind::StageStatusBucketedGauge => {
            partial_merge_aggregator::<StageStatusGauge>(operands)
        }
    };

    match result {
        Ok(value) => value,
        Err(err) => {
            error!(
                ?err,
                stat_kind = ?prefix.stat_kind(),
                "failed to partial merge aggregated statistic"
            );
            None
        }
    }
}

pub(super) fn full_merge<'a>(
    prefix: &StatKeyPrefix,
    existing_val: Option<&[u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let result = match prefix.stat_kind() {
        StatKind::AggregatedGauge => full_merge_aggregator::<Gauge>(existing_val, operands),
        StatKind::StageBucketedGauge => full_merge_aggregator::<StageGauge>(existing_val, operands),
        StatKind::StageStatusBucketedGauge => {
            full_merge_aggregator::<StageStatusGauge>(existing_val, operands)
        }
    };

    match result {
        Ok(value) => Some(value),
        Err(err) => {
            error!(
                ?err,
                stat_kind = ?prefix.stat_kind(),
                "failed to full merge aggregated statistic"
            );
            None
        }
    }
}

fn full_merge_aggregator<'a, A: Aggregate>(
    existing_val: Option<&[u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> anyhow::Result<Vec<u8>> {
    let existing_val = existing_val
        .map(A::Output::deserialize_from)
        .transpose()?
        .unwrap_or_default();
    let operands = operands.into_iter().map(A::MergeOperand::deserialize_from);
    let value = A::aggregate_full(existing_val, operands)?;

    let mut result = Vec::with_capacity(value.serialized_len());
    value.serialize_to(&mut result);
    Ok(result)
}

fn partial_merge_aggregator<'a, A: Aggregate>(
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> anyhow::Result<Option<Vec<u8>>> {
    let operands = operands.into_iter().map(A::MergeOperand::deserialize_from);
    let Some(operand) = A::aggregate_partial(operands)? else {
        return Ok(None);
    };

    let mut result = Vec::with_capacity(operand.serialized_len());
    operand.serialize_to(&mut result);
    Ok(Some(result))
}
