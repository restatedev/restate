// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BufMut;
use itertools::Itertools;
use zerocopy::big_endian::I64;
use zerocopy::{FromBytes, Immutable, IntoBytes};

use restate_storage_api::StorageError;

use crate::stats::{StatKind, StatMergeValueCodec, StatValueCodec};

use super::Aggregate;

/// A u64 statistic kind that can be incremented, decremented, or set.
pub enum Gauge {}

impl Gauge {
    pub fn add(n: u32) -> GaugeOp {
        GaugeOp::new(i64::from(n))
    }

    pub fn sub(n: u32) -> GaugeOp {
        GaugeOp::new(-i64::from(n))
    }
}

/// A big-endian signed delta applied to a gauge value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, Immutable)]
#[repr(transparent)]
pub struct GaugeOp(I64);

impl GaugeOp {
    fn new(delta: i64) -> Self {
        Self(I64::new(delta))
    }

    fn delta(self) -> i64 {
        self.0.get()
    }
}

impl AsRef<[u8]> for GaugeOp {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

static_assertions::assert_eq_size!(GaugeOp, [u8; size_of::<i64>()]);
static_assertions::assert_eq_align!(GaugeOp, u8);

impl Aggregate for Gauge {
    const STAT_KIND: StatKind = StatKind::AggregatedGauge;
    type Output = u64;
    type MergeOperand = GaugeOp;

    fn aggregate_full(
        existing_val: Self::Output,
        mut operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Self::Output> {
        let output = operands.fold_ok(i128::from(existing_val), |acc, op| {
            acc + i128::from(op.delta())
        })?;

        u64::try_from(output)
            .map_err(|_| anyhow::anyhow!("gauge value is outside the u64 range: {output}"))
    }

    fn aggregate_partial(
        mut operands: impl Iterator<Item = crate::Result<Self::MergeOperand>>,
    ) -> anyhow::Result<Option<Self::MergeOperand>> {
        let Some(first) = operands.next().transpose()? else {
            return Ok(None);
        };
        let delta = operands.fold_ok(i128::from(first.delta()), |acc, op| {
            acc + i128::from(op.delta())
        })?;
        Ok(i64::try_from(delta).ok().map(GaugeOp::new))
    }

    /// Return true if the key-value pair should be filtered out during compaction.
    fn should_filter_value(value_payload: &[u8]) -> bool {
        <u64 as StatValueCodec>::deserialize_from(value_payload)
            .is_ok_and(|value| Self::should_filter(&value))
    }

    #[inline(always)]
    fn should_filter(value: &Self::Output) -> bool {
        *value == 0
    }
}

impl StatMergeValueCodec for GaugeOp {
    fn serialized_len(&self) -> usize {
        size_of::<Self>()
    }

    fn serialize_to<B: BufMut>(&self, out: &mut B) {
        out.put_slice(self.as_bytes());
    }

    fn deserialize_from(bytes: &[u8]) -> crate::Result<Self> {
        Self::read_from_bytes(bytes).map_err(|_| StorageError::DataIntegrityError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::StatValueCodec;

    fn operands<const N: usize>(
        operands: [GaugeOp; N],
    ) -> impl Iterator<Item = crate::Result<GaugeOp>> {
        operands.into_iter().map(Ok)
    }

    #[test]
    fn codecs_use_fixed_width_big_endian_values() {
        let add = Gauge::add(42);
        let sub = Gauge::sub(42);

        assert_eq!(add.as_ref(), 42_i64.to_be_bytes());
        assert_eq!(sub.as_ref(), (-42_i64).to_be_bytes());

        let mut encoded = Vec::new();
        StatMergeValueCodec::serialize_to(&sub, &mut encoded);
        assert_eq!(
            <GaugeOp as StatMergeValueCodec>::deserialize_from(&encoded).unwrap(),
            sub
        );
        assert!(<GaugeOp as StatMergeValueCodec>::deserialize_from(&encoded[..7]).is_err());

        encoded.clear();
        StatValueCodec::serialize_to(&u64::MAX, &mut encoded);
        assert_eq!(
            <u64 as StatValueCodec>::deserialize_from(&encoded).unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn partial_merge_checks_the_final_i64_delta() {
        assert_eq!(
            Gauge::aggregate_partial(operands([Gauge::add(5), Gauge::sub(3)])).unwrap(),
            Some(GaugeOp::new(2))
        );
        assert_eq!(
            Gauge::aggregate_partial(operands([
                GaugeOp::new(i64::MAX),
                GaugeOp::new(1),
                GaugeOp::new(-1),
            ]))
            .unwrap(),
            Some(GaugeOp::new(i64::MAX))
        );
        assert_eq!(
            Gauge::aggregate_partial(operands([
                GaugeOp::new(i64::MIN),
                GaugeOp::new(-1),
                GaugeOp::new(1),
            ]))
            .unwrap(),
            Some(GaugeOp::new(i64::MIN))
        );
        assert!(
            Gauge::aggregate_partial(operands([GaugeOp::new(i64::MAX), GaugeOp::new(1)]))
                .unwrap()
                .is_none()
        );
        assert!(
            Gauge::aggregate_partial(operands([GaugeOp::new(i64::MIN), GaugeOp::new(-1)]))
                .unwrap()
                .is_none()
        );
        assert!(
            Gauge::aggregate_partial(std::iter::empty::<crate::Result<GaugeOp>>())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn full_merge_widens_and_checks_the_final_value() {
        assert_eq!(
            Gauge::aggregate_full(u64::MAX, operands([GaugeOp::new(1), GaugeOp::new(-1)])).unwrap(),
            u64::MAX
        );
        assert_eq!(
            Gauge::aggregate_full(0, operands([GaugeOp::new(-1), GaugeOp::new(1)])).unwrap(),
            0
        );
        assert!(Gauge::aggregate_full(u64::MAX, operands([GaugeOp::new(1)])).is_err());
        assert!(Gauge::aggregate_full(0, operands([GaugeOp::new(-1)])).is_err());
    }

    #[test]
    fn merge_propagates_operand_decode_errors() {
        assert!(
            Gauge::aggregate_full(0, [Err(StorageError::DataIntegrityError)].into_iter()).is_err()
        );
        assert!(
            Gauge::aggregate_partial([Err(StorageError::DataIntegrityError)].into_iter()).is_err()
        );
    }
}
