// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::marker::PhantomData;

use bytes::{Buf, BufMut};
use smallvec::SmallVec;
use strum::{EnumCount, VariantArray};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};

use crate::stats::{StatKind, StatMergeValueCodec, StatValueCodec};

use super::Aggregate;

/// A stage/status bucket in a service-load gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageStatus {
    pub stage: Stage,
    pub status: Status,
}

impl StageStatus {
    pub const fn new(stage: Stage, status: Status) -> Self {
        Self { stage, status }
    }
}

/// The compact bucket representation used by a bucketed gauge.
pub trait GaugeBucket: Copy + Debug + Eq {
    type Totals: AsRef<[i128]> + AsMut<[i128]>;

    const COUNT: usize;
    const ENCODED_LEN: usize;
    const STAT_KIND: StatKind;

    fn encode(self, target: &mut impl BufMut);
    fn decode(source: &mut &[u8]) -> crate::Result<Self>;
    fn index(self) -> usize;
    fn from_index(index: usize) -> Self;
    fn totals() -> Self::Totals;
}

impl GaugeBucket for Stage {
    type Totals = [i128; <Self as GaugeBucket>::COUNT];

    const COUNT: usize = <Stage as EnumCount>::COUNT;
    const ENCODED_LEN: usize = size_of::<u8>();
    const STAT_KIND: StatKind = StatKind::StageBucketedGauge;

    fn encode(self, target: &mut impl BufMut) {
        target.put_u8(self as u8);
    }

    fn decode(source: &mut &[u8]) -> crate::Result<Self> {
        if source.remaining() < Self::ENCODED_LEN {
            return Err(StorageError::DataIntegrityError);
        }
        Stage::from_repr(source.get_u8()).ok_or(StorageError::DataIntegrityError)
    }

    fn index(self) -> usize {
        match self {
            Stage::Unknown => 0,
            Stage::Inbox => 1,
            Stage::Running => 2,
            Stage::Suspended => 3,
            Stage::Paused => 4,
            Stage::Finished => 5,
        }
    }

    fn from_index(index: usize) -> Self {
        Stage::VARIANTS[index]
    }

    fn totals() -> Self::Totals {
        [0; <Self as GaugeBucket>::COUNT]
    }
}

impl GaugeBucket for StageStatus {
    type Totals = [i128; <Self as GaugeBucket>::COUNT];

    const COUNT: usize = <Stage as EnumCount>::COUNT * Status::COUNT;
    const ENCODED_LEN: usize = 2 * size_of::<u8>();
    const STAT_KIND: StatKind = StatKind::StageStatusBucketedGauge;

    fn encode(self, target: &mut impl BufMut) {
        target.put_u8(self.stage as u8);
        target.put_u8(self.status as u8);
    }

    fn decode(source: &mut &[u8]) -> crate::Result<Self> {
        if source.remaining() < Self::ENCODED_LEN {
            return Err(StorageError::DataIntegrityError);
        }
        let stage = Stage::from_repr(source.get_u8()).ok_or(StorageError::DataIntegrityError)?;
        let status = Status::from_repr(source.get_u8()).ok_or(StorageError::DataIntegrityError)?;
        Ok(Self { stage, status })
    }

    fn index(self) -> usize {
        self.stage.index() * Status::COUNT + self.status as usize
    }

    fn from_index(index: usize) -> Self {
        Self {
            stage: Stage::from_index(index / Status::COUNT),
            status: Status::VARIANTS[index % Status::COUNT],
        }
    }

    fn totals() -> Self::Totals {
        [0; <Self as GaugeBucket>::COUNT]
    }
}

type BucketValues<B> = SmallVec<[(B, u64); 6]>;
type BucketDeltas<B> = SmallVec<[(B, i64); 2]>;

/// Non-zero counters for the buckets of one stable statistic key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketedGauge<B> {
    buckets: BucketValues<B>,
}

impl<B> Default for BucketedGauge<B> {
    fn default() -> Self {
        Self {
            buckets: SmallVec::new(),
        }
    }
}

impl<B: Copy> BucketedGauge<B> {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (B, u64)> + '_ {
        self.buckets.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

impl<B: GaugeBucket> StatValueCodec for BucketedGauge<B> {
    fn serialized_len(&self) -> usize {
        self.buckets.len() * (B::ENCODED_LEN + size_of::<u64>())
    }

    fn serialize_to<T: BufMut>(&self, target: &mut T) {
        for (bucket, value) in &self.buckets {
            bucket.encode(target);
            target.put_u64(*value);
        }
    }

    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self> {
        let record_len = B::ENCODED_LEN + size_of::<u64>();
        if !bytes.len().is_multiple_of(record_len) {
            return Err(StorageError::DataIntegrityError);
        }

        let mut source = bytes;
        let mut buckets = SmallVec::with_capacity(bytes.len() / record_len);
        let mut previous_index = None;
        while !source.is_empty() {
            let bucket = B::decode(&mut source)?;
            let value = source.get_u64();
            let index = bucket.index();
            if value == 0 || previous_index.is_some_and(|previous| previous >= index) {
                return Err(StorageError::DataIntegrityError);
            }
            previous_index = Some(index);
            buckets.push((bucket, value));
        }
        Ok(Self { buckets })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BucketedGaugeOp<B> {
    buckets: BucketDeltas<B>,
}

impl<B: GaugeBucket> StatMergeValueCodec for BucketedGaugeOp<B> {
    fn serialized_len(&self) -> usize {
        self.buckets.len() * (B::ENCODED_LEN + size_of::<i64>())
    }

    fn serialize_to<T: BufMut>(&self, target: &mut T) {
        for (bucket, delta) in &self.buckets {
            bucket.encode(target);
            target.put_i64(*delta);
        }
    }

    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self> {
        let record_len = B::ENCODED_LEN + size_of::<i64>();
        if !bytes.len().is_multiple_of(record_len) {
            return Err(StorageError::DataIntegrityError);
        }

        let mut source = bytes;
        let mut buckets = SmallVec::with_capacity(bytes.len() / record_len);
        while !source.is_empty() {
            let bucket = B::decode(&mut source)?;
            let delta = source.get_i64();
            buckets.push((bucket, delta));
        }
        Ok(Self { buckets })
    }
}

/// Aggregates signed updates into independent unsigned bucket counters.
pub struct BucketedGaugeAggregation<B>(PhantomData<B>);

impl<B: GaugeBucket> Aggregate for BucketedGaugeAggregation<B> {
    const STAT_KIND: StatKind = B::STAT_KIND;
    type Output = BucketedGauge<B>;
    type MergeOperand = BucketedGaugeOp<B>;

    fn aggregate_full(
        existing_val: Self::Output,
        operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Self::Output> {
        let mut totals = B::totals();
        for (bucket, value) in existing_val.buckets {
            totals.as_mut()[bucket.index()] = i128::from(value);
        }
        for operand in operands {
            for (bucket, delta) in operand?.buckets {
                let total = &mut totals.as_mut()[bucket.index()];
                *total = total
                    .checked_add(i128::from(delta))
                    .ok_or_else(|| anyhow::anyhow!("bucketed gauge intermediate overflow"))?;
            }
        }

        let mut buckets = SmallVec::new();
        for (index, &total) in totals.as_ref().iter().enumerate() {
            if total == 0 {
                continue;
            }
            let value = u64::try_from(total).map_err(|_| {
                anyhow::anyhow!("bucketed gauge value is outside the u64 range: {total}")
            })?;
            buckets.push((B::from_index(index), value));
        }
        Ok(BucketedGauge { buckets })
    }

    fn aggregate_partial(
        operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Option<Self::MergeOperand>> {
        let mut totals = B::totals();
        let mut has_operand = false;
        for operand in operands {
            has_operand = true;
            for (bucket, delta) in operand?.buckets {
                let total = &mut totals.as_mut()[bucket.index()];
                *total = total
                    .checked_add(i128::from(delta))
                    .ok_or_else(|| anyhow::anyhow!("bucketed gauge intermediate overflow"))?;
            }
        }
        if !has_operand {
            return Ok(None);
        }

        let mut buckets = SmallVec::new();
        for (index, &total) in totals.as_ref().iter().enumerate() {
            if total == 0 {
                continue;
            }
            let delta = i64::try_from(total).map_err(|_| {
                anyhow::anyhow!("bucketed gauge delta is outside the i64 range: {total}")
            })?;
            buckets.push((B::from_index(index), delta));
        }
        Ok(Some(BucketedGaugeOp { buckets }))
    }

    fn should_filter_value(value_payload: &[u8]) -> bool {
        Self::Output::deserialize_from(value_payload).is_ok_and(|value| value.is_empty())
    }

    fn should_filter(value: &Self::Output) -> bool {
        value.is_empty()
    }
}

pub type StageCounts = BucketedGauge<Stage>;
pub type StageStatusCounts = BucketedGauge<StageStatus>;
pub type StageGauge = BucketedGaugeAggregation<Stage>;
pub type StageStatusGauge = BucketedGaugeAggregation<StageStatus>;

pub(crate) struct BucketedGaugeUpdate {
    bytes: [u8; 2 * (StageStatus::ENCODED_LEN + size_of::<i64>())],
    len: usize,
}

impl BucketedGaugeUpdate {
    fn single<B: GaugeBucket>(bucket: B, delta: i64) -> Self {
        let mut update = Self {
            bytes: [0; 2 * (StageStatus::ENCODED_LEN + size_of::<i64>())],
            len: 0,
        };
        update.push(bucket, delta);
        update
    }

    fn transition<B: GaugeBucket>(old_bucket: B, new_bucket: B) -> Self {
        debug_assert_ne!(old_bucket, new_bucket);
        let mut update = Self::single(old_bucket, -1);
        update.push(new_bucket, 1);
        update
    }

    fn push<B: GaugeBucket>(&mut self, bucket: B, delta: i64) {
        let mut target = &mut self.bytes[self.len..];
        bucket.encode(&mut target);
        target.put_i64(delta);
        self.len += B::ENCODED_LEN + size_of::<i64>();
    }
}

impl AsRef<[u8]> for BucketedGaugeUpdate {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl<B: GaugeBucket> BucketedGaugeAggregation<B> {
    pub(crate) fn increment(bucket: B) -> BucketedGaugeUpdate {
        BucketedGaugeUpdate::single(bucket, 1)
    }

    pub(crate) fn decrement(bucket: B) -> BucketedGaugeUpdate {
        BucketedGaugeUpdate::single(bucket, -1)
    }

    pub(crate) fn transition(old_bucket: B, new_bucket: B) -> BucketedGaugeUpdate {
        BucketedGaugeUpdate::transition(old_bucket, new_bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operands<B: GaugeBucket, const N: usize>(
        operands: [BucketedGaugeOp<B>; N],
    ) -> impl Iterator<Item = crate::Result<BucketedGaugeOp<B>>> {
        operands.into_iter().map(Ok)
    }

    #[test]
    fn codecs_are_sparse_compact_and_canonical() {
        let value = StageStatusCounts {
            buckets: SmallVec::from_slice(&[
                (StageStatus::new(Stage::Inbox, Status::New), 3),
                (StageStatus::new(Stage::Running, Status::Started), 2),
            ]),
        };
        let mut bytes = Vec::new();
        value.serialize_to(&mut bytes);

        assert_eq!(bytes.len(), 2 * (2 + size_of::<u64>()));
        assert_eq!(bytes[0], Stage::Inbox as u8);
        assert_eq!(bytes[1], Status::New as u8);
        assert_eq!(StageStatusCounts::deserialize_from(&bytes).unwrap(), value);

        let mut non_canonical = Vec::new();
        StageStatus::new(Stage::Running, Status::Started).encode(&mut non_canonical);
        non_canonical.put_u64(1);
        StageStatus::new(Stage::Inbox, Status::New).encode(&mut non_canonical);
        non_canonical.put_u64(1);
        assert!(StageStatusCounts::deserialize_from(&non_canonical).is_err());
        assert!(StageStatusCounts::deserialize_from(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn full_merge_moves_between_buckets_and_checks_each_counter() {
        let inbox = StageStatus::new(Stage::Inbox, Status::New);
        let running = StageStatus::new(Stage::Running, Status::Started);
        let existing = StageStatusCounts {
            buckets: SmallVec::from_slice(&[(inbox, 2)]),
        };
        let moved = BucketedGaugeOp {
            buckets: SmallVec::from_slice(&[(inbox, -1), (running, 1)]),
        };

        let result = StageStatusGauge::aggregate_full(existing, operands([moved])).unwrap();
        assert_eq!(result.buckets.as_slice(), &[(inbox, 1), (running, 1)]);

        let underflow = BucketedGaugeOp {
            buckets: SmallVec::from_slice(&[(inbox, -2)]),
        };
        assert!(
            StageStatusGauge::aggregate_full(result, operands([underflow])).is_err(),
            "one bucket cannot borrow from another"
        );
    }

    #[test]
    fn partial_merge_combines_and_cancels_independent_buckets() {
        let first = BucketedGaugeOp {
            buckets: SmallVec::from_slice(&[(Stage::Inbox, -1), (Stage::Running, 1)]),
        };
        let second = BucketedGaugeOp {
            buckets: SmallVec::from_slice(&[(Stage::Running, -1), (Stage::Finished, 1)]),
        };

        let result = StageGauge::aggregate_partial(operands([first, second]))
            .unwrap()
            .unwrap();
        assert_eq!(
            result.buckets.as_slice(),
            &[(Stage::Inbox, -1), (Stage::Finished, 1)]
        );
        assert!(
            StageGauge::aggregate_partial(
                std::iter::empty::<crate::Result<BucketedGaugeOp<Stage>>>()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn update_encoding_carries_both_sides_of_a_transition() {
        let update = StageStatusGauge::transition(
            StageStatus::new(Stage::Inbox, Status::New),
            StageStatus::new(Stage::Running, Status::Started),
        );
        let decoded = BucketedGaugeOp::<StageStatus>::deserialize_from(update.as_ref()).unwrap();
        assert_eq!(
            decoded.buckets.as_slice(),
            &[
                (StageStatus::new(Stage::Inbox, Status::New), -1),
                (StageStatus::new(Stage::Running, Status::Started), 1),
            ]
        );
    }
}
