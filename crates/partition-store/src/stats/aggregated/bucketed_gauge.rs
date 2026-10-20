// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sparse bucket counters and signed merge operands.
//!
//! Stored values are `(bucket, big-endian u64)` records with nonzero counts in
//! strictly increasing bucket-index order. Empty bytes represent all-zero counts.
//! Operands are `(bucket, big-endian i64)` records; they may repeat buckets, appear
//! in any order, and cancel. An empty operand is a valid no-op.
//!
//! Bucket tags use the raw stage/status discriminants, not the name-ordered rank
//! codecs used in ordered keys. Their bytes and canonical bucket order must stay
//! stable independently of key ordering.

use std::fmt::Debug;
use std::marker::PhantomData;

use bytes::{Buf, BufMut};
use smallvec::SmallVec;
use strum::{EnumCount, VariantArray};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};

use crate::stats::{StatKind, StatMergeValueCodec, StatValueCodec};

use super::Aggregate;

/// A stage/status bucket in a partition-scoped gauge.
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

/// Identifies one counter in a finite set of gauge buckets.
///
/// A value of this type names a bucket, not its count. For example, `Stage::Inbox`
/// identifies the inbox counter; `StageStatus` identifies a counter for one
/// stage/status pair. Counts and signed deltas are encoded separately by
/// [`BucketedGauge`] and [`BucketedGaugeOp`].
///
/// A bucket has two representations:
/// - **Encoded tag:** the fixed-width bytes identifying it in stored values and
///   merge operands. These bytes contain neither a count nor a delta.
/// - **Dense index:** a slot in [`Self::Totals`] used to accumulate signed updates.
///   Full merges emit nonzero counters in increasing index order, giving sparse
///   stored values a canonical record order. The index itself is not serialized.
///
/// # Example
///
/// `Stage` has six buckets, including `Unknown`. Some of their mappings are:
///
/// | Bucket | Dense index | Encoded tag |
/// | --- | --- | --- |
/// | `Stage::Inbox` | `1` | `b'i'` (`0x69`) |
/// | `Stage::Running` | `2` | `b'r'` (`0x72`) |
/// | `Stage::Finished` | `5` | `b'f'` (`0x66`) |
///
/// Three inbox entries and two running entries produce totals
/// `[0, 3, 2, 0, 0, 0]`. The stored value contains only `(b'i', 3_u64)` and
/// `(b'r', 2_u64)`, with each count encoded in big-endian order. A transition
/// applies `-1` to slot `1` and `+1` to slot `2` before emitting the new counters.
/// Notice that index order need not match the bytewise order of encoded tags:
/// `Finished` follows `Running` by index even though `b'f' < b'r'`.
///
/// # Implementation contract
///
/// - `index` and `from_index` form a bijection between valid buckets and `0..COUNT`.
/// - `totals` returns exactly `COUNT` zeroed slots, exposed by both slice traits.
/// - `encode` and successful `decode` round-trip a bucket using exactly
///   `ENCODED_LEN` bytes. Decoding rejects unknown tags and truncated input.
/// - Encoded tags and the relative index order of existing buckets must remain
///   stable: both affect the persisted format, even though indices are not stored.
///
/// # Adding `Stage` or `Status` variants
///
/// Preserve existing raw discriminants, bilrost numbers, and declaration order.
/// For the current implementations:
///
/// - **New stage:** append the variant with an unused byte tag and bilrost number,
///   then add its next dense index to `Stage`'s `GaugeBucket::index` match.
///   `from_index` uses `Stage::VARIANTS`, so the index must match declaration order.
/// - **New status:** append the variant with the next consecutive discriminant
///   and an unused bilrost number. `StageStatus` uses `status as usize` as the
///   within-stage slot and `Status::VARIANTS` as its inverse: discriminants must
///   remain `0..Status::COUNT` in declaration order. Supporting gaps would require
///   an explicit status-to-slot mapping instead.
/// - `COUNT`, accumulator array sizes, and derived tag decoding grow automatically.
///   Keep the bucket mapping and persisted-byte tests passing, and update the
///   examples here when the number of buckets changes.
///
/// Also update the enum's `as_str` match and its separate ordered-key codec
/// (`StageRankCodec` or `StatusCodec`), including decoding and rank tests. Assign
/// a free rank in name order without changing existing ranks; these ranks are
/// independent of the raw tags and dense slots used here.
///
/// ## Compatibility when `COUNT` grows
///
/// Adding variants under the rules above is safe for existing values and merge
/// operands, even though `COUNT` and the accumulator arrays grow immediately.
/// Neither `COUNT` nor dense indices are serialized: records store bucket tags
/// followed by counts or deltas. Each merge starts with a zeroed accumulator and
/// maps decoded buckets to their current indices. Slots for unused new variants
/// stay zero; both full and partial merges omit zero totals, so they cannot emit
/// a new bucket tag unless that bucket was present in their inputs.
///
/// Adding a status increases the stride in
/// `stage.index() * Status::COUNT + status as usize`, shifting existing slots.
/// For example, growing from ten to eleven statuses moves `(Running, Started)`
/// from slot `2 * 10 + 3 = 23` to `2 * 11 + 3 = 25`. Its encoded tag remains
/// `[b'r', 3]`, and `from_index` uses the new stride to recover the same bucket.
/// Existing buckets retain their relative order, so old stored values still
/// satisfy the decoder's canonical-order check.
///
/// With preserved tags, relative order, and inverse mappings, identical inputs
/// containing only existing variants produce identical successful full-merge
/// value bytes and partial-merge operand bytes in the old and new implementations.
/// No data rewrite is needed just because `COUNT` grew, and the resulting bytes
/// remain readable by the older implementation.
///
/// Actually writing a new variant is a separate compatibility step. Older binaries
/// reject new tags in both stored values and merge operands; enable those writes
/// only when all readers and mergers support them, and account for the persisted
/// tags when planning a downgrade.
pub trait GaugeBucket: Copy + Debug + Eq {
    /// Dense merge scratch space, typically `[i128; COUNT]`. Wide signed totals
    /// preserve intermediate updates until the final count or delta is checked.
    type Totals: AsRef<[i128]> + AsMut<[i128]>;

    /// Number of possible buckets, including buckets whose current count is zero.
    /// This count is not serialized.
    const COUNT: usize;
    /// Width of one encoded bucket tag, excluding the count or delta that follows it.
    const ENCODED_LEN: usize;
    /// Persisted aggregation kind associated with this bucket format.
    const STAT_KIND: StatKind;

    /// Appends only this bucket's tag, writing exactly `ENCODED_LEN` bytes.
    fn encode(self, target: &mut impl BufMut);
    /// Reads one bucket tag. On success, consumes exactly `ENCODED_LEN` bytes and
    /// leaves the following count or delta in `source`.
    fn decode(source: &mut &[u8]) -> crate::Result<Self>;
    /// Returns this bucket's accumulator slot in `0..COUNT`.
    fn index(self) -> usize;
    /// Inverse of `index`. Callers must supply an index in `0..COUNT`;
    /// implementations may panic for an out-of-range index.
    fn from_index(index: usize) -> Self;
    /// Creates the zeroed accumulator, with exactly one slot per possible bucket.
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
        if !bytes.len().is_multiple_of(record_len) || bytes.len() / record_len > B::COUNT {
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
            let Ok(delta) = i64::try_from(total) else {
                return Ok(None);
            };
            buckets.push((B::from_index(index), delta));
        }
        Ok(Some(BucketedGaugeOp { buckets }))
    }

    fn should_filter_value(value_payload: &[u8]) -> bool {
        // Only the empty representation is a valid zero. Keep every nonempty
        // value, including malformed data, without decoding or allocating.
        value_payload.is_empty()
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
        for &stage in Stage::VARIANTS {
            assert!(stage.index() < <Stage as GaugeBucket>::COUNT);
            assert_eq!(Stage::from_index(stage.index()), stage);
            for &status in Status::VARIANTS {
                let bucket = StageStatus::new(stage, status);
                assert!(bucket.index() < StageStatus::COUNT);
                assert_eq!(StageStatus::from_index(bucket.index()), bucket);
            }
        }

        let value = StageStatusCounts {
            buckets: SmallVec::from_slice(&[
                (StageStatus::new(Stage::Inbox, Status::New), 3),
                (StageStatus::new(Stage::Running, Status::Started), 2),
            ]),
        };
        let mut bytes = Vec::new();
        value.serialize_to(&mut bytes);

        assert_eq!(
            bytes,
            b"i\x01\x00\x00\x00\x00\x00\x00\x00\x03r\x03\x00\x00\x00\x00\x00\x00\x00\x02"
        );
        assert_eq!(StageStatusCounts::deserialize_from(&bytes).unwrap(), value);

        let mut non_canonical = Vec::new();
        StageStatus::new(Stage::Running, Status::Started).encode(&mut non_canonical);
        non_canonical.put_u64(1);
        StageStatus::new(Stage::Inbox, Status::New).encode(&mut non_canonical);
        non_canonical.put_u64(1);
        assert!(StageStatusCounts::deserialize_from(&non_canonical).is_err());
        assert!(StageStatusCounts::deserialize_from(&bytes[..bytes.len() - 1]).is_err());

        bytes[2..10].fill(0);
        assert!(StageStatusCounts::deserialize_from(&bytes).is_err());
        assert!(!StageStatusGauge::should_filter_value(&bytes));
        assert!(StageStatusGauge::should_filter_value(&[]));
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

        let delta = |amount| BucketedGaugeOp {
            buckets: SmallVec::from_slice(&[(Stage::Inbox, amount)]),
        };
        let cancelled = StageGauge::aggregate_partial(operands([delta(1), delta(-1)]))
            .unwrap()
            .unwrap();
        assert!(cancelled.buckets.is_empty());

        // Declining an unrepresentable partial merge must leave a valid full
        // merge possible, including when later deltas bring it back into range.
        for (base, amount, extra, expected) in [
            (0, i64::MAX, 1, 1_u64 << 63),
            (u64::MAX, i64::MIN, -1, (1_u64 << 63) - 2),
        ] {
            assert!(
                StageGauge::aggregate_partial(operands([delta(amount), delta(extra)]))
                    .unwrap()
                    .is_none()
            );
            let existing = || BucketedGauge {
                buckets: if base == 0 {
                    SmallVec::new()
                } else {
                    SmallVec::from_slice(&[(Stage::Inbox, base)])
                },
            };
            let full =
                StageGauge::aggregate_full(existing(), operands([delta(amount), delta(extra)]))
                    .unwrap();
            assert_eq!(full.buckets.as_slice(), &[(Stage::Inbox, expected)]);
            let partial = StageGauge::aggregate_partial(operands([
                delta(amount),
                delta(extra),
                delta(-extra),
            ]))
            .unwrap()
            .unwrap();
            assert_eq!(partial.buckets.as_slice(), &[(Stage::Inbox, amount)]);
            assert_eq!(
                StageGauge::aggregate_full(existing(), operands([partial])).unwrap(),
                StageGauge::aggregate_full(
                    existing(),
                    operands([delta(amount), delta(extra), delta(-extra)]),
                )
                .unwrap(),
            );
        }
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
