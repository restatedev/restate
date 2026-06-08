// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::ops::RangeBounds as _;

use crate::{KeyRange, PartitionKey};

pub type ShardIdx = u16;

/// A sub-range of a [`ShardPlan`]'s key range, identified by a [`ShardIdx`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    idx: ShardIdx,
    range: KeyRange,
}

impl Shard {
    #[inline]
    pub const fn idx(&self) -> ShardIdx {
        self.idx
    }

    #[inline]
    pub const fn key_range(&self) -> &KeyRange {
        &self.range
    }
}

/// A deterministic sharding plan for an inclusive partition-key range.
///
/// A plan splits a [`KeyRange`] into contiguous, non-overlapping [`Shard`]s indexed
/// from `0` to `shard_count() - 1`.
#[derive(Debug)]
pub struct ShardPlan {
    range: KeyRange,
    num_shards: u16,
}

impl ShardPlan {
    /// Build a plan for sharding `range` into at most `max_shards` (e.g. 1024).
    /// The actual shard count is clamped to the number of distinct keys.
    pub const fn new(range: KeyRange, max_shards: NonZeroU16) -> Self {
        let range_start = range.start();
        let range_end = range.end();
        assert!(range_start <= range_end, "empty or inverted range");

        // Use u128 to avoid overflow when span == 2^64.
        let span: u128 = (range_end as u128) - (range_start as u128) + 1;
        let max_shards = max_shards.get();

        let num_shards = if (max_shards as u128) < span {
            max_shards
        } else {
            // span <= max_shards here, so cast is safe
            span as u16
        };
        Self { range, num_shards }
    }

    /// Find a shard by its index.
    ///
    /// # Panics
    /// Panics if `idx` is outside the range of shards.
    pub const fn find_shard_unchecked(&self, idx: ShardIdx) -> Shard {
        assert!(
            idx < self.num_shards,
            "There cannot be a shard_idx which is larger than the number of keys in the partition-key space"
        );

        // scale up from u16 to u128 is safe.
        let idx = idx as u128;

        let num_keys = self.range.num_keys();
        let num_shards = self.num_shards as u128;

        // Ceil boundaries so any remainder is distributed across the lower-index shards.
        let start_offset = (idx * num_keys).div_ceil(num_shards);
        let end_offset = ((idx + 1) * num_keys).div_ceil(num_shards) - 1;

        let start = (self.range.start() as u128) + start_offset;
        let end = (self.range.start() as u128) + end_offset;

        debug_assert!(start <= u64::MAX as u128);
        debug_assert!(end <= u64::MAX as u128);

        Shard {
            idx: idx as u16,
            range: KeyRange::new(start as u64, end as u64),
        }
    }

    /// Find a shard by its index.
    pub const fn find_shard(&self, idx: ShardIdx) -> Option<Shard> {
        if idx >= self.num_shards {
            return None;
        }
        Some(self.find_shard_unchecked(idx))
    }

    /// Number of shards actually used.
    pub const fn shard_count(&self) -> u16 {
        self.num_shards
    }

    /// Iterate shard ranges along with their shard_idx (0..shard_count-1).
    /// Each shard i has size: base + (i < rem ? 1 : 0).
    pub fn shards(&self) -> impl ExactSizeIterator<Item = Shard> + '_ {
        (0..self.num_shards).map(|idx| self.find_shard_unchecked(idx))
    }

    /// O(1) mapping from key -> shard_idx.
    /// Returns None if key is outside the plan's range.
    pub fn key_to_idx(&self, key: PartitionKey) -> Option<ShardIdx> {
        if !self.range.contains(&key) {
            return None;
        }
        let offset = (key as u128) - (self.range.start() as u128);
        let num_shards = self.num_shards as u128;
        let idx = (offset * num_shards) / self.range.num_keys(); // floor
        Some(idx as u16)
    }

    /// Split the original plan at midpoint of the range, forming:
    ///   Left:  [start ..= split_at-1] if non-empty
    ///   Right: [split_at ..= end]     if non-empty
    /// Then shard each with `new_max_shards`, and produce a piecewise remap to map the old index
    /// to the new index on both sides.
    ///
    /// Useful for sub-sharding and repartitioning workflows.
    pub fn split(&self, new_max_shards: NonZeroU16) -> Split<'_> {
        // Split at the ceil-midpoint of the range. This aligns with old shard boundaries only
        // when `num_shards` divides the range evenly; otherwise an old shard may straddle the
        // split and `cut_by_new_shards` will emit multiple remap pieces for it.
        let split_at = self.range.midpoint();

        // Build left/right plans if non-empty.
        let left_range = if self.range.start() < split_at {
            Some(KeyRange::new(
                self.range.start(),
                split_at.saturating_sub(1),
            ))
        } else {
            None
        };

        let right_range = if split_at <= self.range.end() {
            Some(KeyRange::new(split_at, self.range.end()))
        } else {
            None
        };

        let left = left_range.map(|r| ShardPlan::new(r, new_max_shards));
        let right = right_range.map(|r| ShardPlan::new(r, new_max_shards));

        let mut pieces = Vec::new();

        for old_shard in self.shards() {
            if let (Some(ref lr), Some(lp)) = (left_range, &left)
                && let Some(seg) = old_shard.key_range().intersect(lr)
            {
                cut_by_new_shards(old_shard.idx(), Side::Left, lp, seg, &mut pieces);
            }
            if let (Some(ref rr), Some(rp)) = (right_range, &right)
                && let Some(seg) = old_shard.key_range().intersect(rr)
            {
                cut_by_new_shards(old_shard.idx(), Side::Right, rp, seg, &mut pieces);
            }
        }

        Split {
            original: self,
            left,
            right,
            remap: pieces,
        }
    }
}

/// Result of splitting one [`ShardPlan`] into two plans at the range midpoint.
///
/// The `left` and `right` plans represent the post-split key space, and `remap`
/// contains piecewise mappings from old shard indices to new shard indices.
pub struct Split<'a> {
    original: &'a ShardPlan,
    left: Option<ShardPlan>,
    right: Option<ShardPlan>,
    remap: Vec<RemapPiece>,
}

impl<'a> Split<'a> {
    /// Returns the pre-split plan.
    pub fn original(&self) -> &'a ShardPlan {
        self.original
    }

    /// Returns the left post-split plan, if non-empty.
    pub fn left(&self) -> Option<&ShardPlan> {
        self.left.as_ref()
    }

    /// Returns the right post-split plan, if non-empty.
    pub fn right(&self) -> Option<&ShardPlan> {
        self.right.as_ref()
    }

    /// Returns the piecewise remap from original shards to post-split shards.
    pub fn remap(&self) -> &[RemapPiece] {
        &self.remap
    }
}

/// Side of the midpoint split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Piece belongs to the left half: `[start, midpoint - 1]`.
    Left,
    /// Piece belongs to the right half: `[midpoint, end]`.
    Right,
}

/// One contiguous remapping segment from an original shard into a split-side shard.
#[derive(Debug, Clone)]
pub struct RemapPiece {
    /// Original shard index before split.
    pub old_idx: ShardIdx,
    /// Which side of the split this piece belongs to.
    pub side: Side,
    /// Target shard index inside the selected split side.
    pub new_idx: ShardIdx,
    /// Inclusive key subrange to remap.
    pub range: KeyRange,
}

/// Cut `subrange` (which is inside `plan`) into pieces aligned to new shard boundaries.
/// Produces RemapPiece entries.
fn cut_by_new_shards(
    old_idx: ShardIdx,
    side: Side,
    plan: &ShardPlan,
    subrange: KeyRange,
    out: &mut Vec<RemapPiece>,
) {
    let mut current = subrange.start();
    let end = subrange.end();

    while current <= end {
        let new_idx = plan.key_to_idx(current).unwrap();
        let shard = plan.find_shard_unchecked(new_idx);
        let piece_end = end.min(shard.key_range().end());
        out.push(RemapPiece {
            old_idx,
            side,
            new_idx,
            range: KeyRange::new(current, piece_end),
        });
        if piece_end == u64::MAX {
            break;
        }
        current = piece_end.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u16) -> NonZeroU16 {
        NonZeroU16::new(n).expect("test shard count must be non-zero")
    }

    #[test]
    fn key_range_midpoint() {
        assert_eq!(KeyRange::new(0, 10).midpoint(), 5);
        assert_eq!(KeyRange::new(100, 199).midpoint(), 150);
        assert_eq!(KeyRange::new(0, 0).midpoint(), 0);
        assert_eq!(KeyRange::new(100, 100).midpoint(), 100);
        assert_eq!(KeyRange::new(u64::MAX, u64::MAX).midpoint(), u64::MAX);
        assert_eq!(KeyRange::new(0, u64::MAX).midpoint(), 9223372036854775808);
    }

    #[test]
    fn shards_iterator_is_exact_size() {
        fn assert_exact_size<I: ExactSizeIterator<Item = Shard>>(_: I) {}

        let plan = ShardPlan::new(KeyRange::from(100..=104), nz(4));
        assert_exact_size(plan.shards());

        let iter = plan.shards();
        assert_eq!(iter.len(), plan.shard_count() as usize);
        assert_eq!(
            iter.size_hint(),
            (
                plan.shard_count() as usize,
                Some(plan.shard_count() as usize)
            )
        );
    }

    #[test]
    fn small_range() {
        let plan = ShardPlan::new(KeyRange::from(0..=9), nz(1024));
        assert_eq!(plan.shard_count(), 10);
        for k in 0..=9 {
            assert_eq!(plan.key_to_idx(k), Some(k as u16));
        }
    }

    #[test]
    fn single_key() {
        let plan = ShardPlan::new(KeyRange::from(55..=55), nz(1024));
        assert_eq!(plan.shard_count(), 1);
        assert_eq!(plan.key_to_idx(55), Some(0));
        assert_eq!(
            *plan.find_shard(0).unwrap().key_range(),
            KeyRange::from(55..=55)
        );
    }

    #[test]
    fn larger_equal_splits() {
        let plan = ShardPlan::new(KeyRange::from(100..=199), nz(4)); // span=100 => base=25, rem=0
        let v: Vec<_> = plan.shards().collect();
        assert_eq!(*v[0].key_range(), KeyRange::from(100..=124));
        assert_eq!(*v[1].key_range(), KeyRange::from(125..=149));
        assert_eq!(*v[2].key_range(), KeyRange::from(150..=174));
        assert_eq!(*v[3].key_range(), KeyRange::from(175..=199));

        assert_eq!(plan.key_to_idx(124), Some(0));
        assert_eq!(plan.key_to_idx(125), Some(1));
        assert_eq!(plan.key_to_idx(174), Some(2));
        assert_eq!(plan.key_to_idx(175), Some(3));

        // find by idx
        assert_eq!(
            *plan.find_shard(0).unwrap().key_range(),
            KeyRange::from(100..=124)
        );
        assert_eq!(
            *plan.find_shard(1).unwrap().key_range(),
            KeyRange::from(125..=149)
        );
        assert_eq!(
            *plan.find_shard(2).unwrap().key_range(),
            KeyRange::from(150..=174)
        );
        assert_eq!(
            *plan.find_shard(3).unwrap().key_range(),
            KeyRange::from(175..=199)
        );
        assert_eq!(plan.find_shard(4), None);
    }

    #[test]
    fn clamps_to_1024() {
        let plan = ShardPlan::new(KeyRange::from(0..=1_000_000), nz(1024));
        assert_eq!(plan.shard_count(), 1024);
        // A couple of spot checks are consistent with O(1) formula
        let first = plan.key_to_idx(0).unwrap();
        let last = plan.key_to_idx(1_000_000).unwrap();
        assert!(first <= last);
    }

    #[test]
    fn full_partition_key_range() {
        let plan = ShardPlan::new(KeyRange::FULL, nz(1024));
        assert_eq!(plan.shard_count(), 1024);
        // Edge keys map
        assert_eq!(plan.key_to_idx(PartitionKey::MIN).unwrap(), 0);
        assert!(plan.key_to_idx(PartitionKey::MAX).unwrap() < 1024);
    }
    #[test]
    fn example_100_199_split() {
        // Old: [100..=199] into 4 shards -> sizes 25 each:
        // 0:[100..124], 1:[125..149], 2:[150..174], 3:[175..199]
        let old = ShardPlan::new(KeyRange::from(100..=199), nz(4));
        assert_eq!(
            *old.find_shard(0).unwrap().key_range(),
            KeyRange::from(100..=124)
        );
        assert_eq!(
            *old.find_shard(1).unwrap().key_range(),
            KeyRange::from(125..=149)
        );
        assert_eq!(
            *old.find_shard(2).unwrap().key_range(),
            KeyRange::from(150..=174)
        );
        assert_eq!(
            *old.find_shard(3).unwrap().key_range(),
            KeyRange::from(175..=199)
        );

        let split = old.split(nz(4));

        let left = split.left().unwrap();
        let right = split.right().unwrap();

        // Left [100..=149] into 4 (ceil-based):
        // 0:[100..112], 1:[113..124], 2:[125..137], 3:[138..149]
        assert_eq!(
            *left.find_shard(0).unwrap().key_range(),
            KeyRange::from(100..=112)
        );
        assert_eq!(
            *left.find_shard(1).unwrap().key_range(),
            KeyRange::from(113..=124)
        );
        assert_eq!(
            *left.find_shard(2).unwrap().key_range(),
            KeyRange::from(125..=137)
        );
        assert_eq!(
            *left.find_shard(3).unwrap().key_range(),
            KeyRange::from(138..=149)
        );

        // Right [150..=199] into 4:
        // 0:[150..162], 1:[163..174], 2:[175..187], 3:[188..199]
        assert_eq!(
            *right.find_shard(0).unwrap().key_range(),
            KeyRange::from(150..=162)
        );
        assert_eq!(
            *right.find_shard(1).unwrap().key_range(),
            KeyRange::from(163..=174)
        );
        assert_eq!(
            *right.find_shard(2).unwrap().key_range(),
            KeyRange::from(175..=187)
        );
        assert_eq!(
            *right.find_shard(3).unwrap().key_range(),
            KeyRange::from(188..=199)
        );

        // Now the remap is exactly 8 pieces (2 per old shard):
        // Remap should align perfectly to new shard boundaries:
        // Old0 [100..124] -> Left0 [100..112], Left1 [113..124]
        // Old1 [125..149] -> Left2 [125..137], Left3 [138..149]
        // Old2 [150..174] -> Right0 [150..162], Right1 [163..174]
        // Old3 [175..199] -> Right2 [175..187], Right3 [188..199]
        let want = [
            (0, Side::Left, 0, KeyRange::from(100..=112)),
            (0, Side::Left, 1, KeyRange::from(113..=124)),
            (1, Side::Left, 2, KeyRange::from(125..=137)),
            (1, Side::Left, 3, KeyRange::from(138..=149)),
            (2, Side::Right, 0, KeyRange::from(150..=162)),
            (2, Side::Right, 1, KeyRange::from(163..=174)),
            (3, Side::Right, 2, KeyRange::from(175..=187)),
            (3, Side::Right, 3, KeyRange::from(188..=199)),
        ];

        assert_eq!(split.remap().len(), want.len());
        for (i, piece) in split.remap().iter().enumerate() {
            let (oi, side, ni, r) = &want[i];
            assert_eq!(piece.old_idx, *oi);
            assert_eq!(&piece.side, side);
            assert_eq!(piece.new_idx, *ni);
            assert_eq!(piece.range, *r);
        }
    }

    #[test]
    fn split_tight_range() {
        let old = ShardPlan::new(KeyRange::from(100..=104), nz(4));
        let v: Vec<_> = old.shards().collect();
        assert_eq!(*v[0].key_range(), KeyRange::from(100..=101));
        assert_eq!(*v[1].key_range(), KeyRange::from(102..=102));
        assert_eq!(*v[2].key_range(), KeyRange::from(103..=103));
        assert_eq!(*v[3].key_range(), KeyRange::from(104..=104));

        let split = old.split(nz(4));
        let left = split.left().unwrap();
        let right = split.right().unwrap();

        assert_eq!(left.shard_count(), 2);
        assert_eq!(
            *left.find_shard(0).unwrap().key_range(),
            KeyRange::from(100..=100)
        );
        assert_eq!(
            *left.find_shard(1).unwrap().key_range(),
            KeyRange::from(101..=101)
        );

        assert_eq!(right.shard_count(), 3);
        assert_eq!(
            *right.find_shard(0).unwrap().key_range(),
            KeyRange::from(102..=102)
        );
        assert_eq!(
            *right.find_shard(1).unwrap().key_range(),
            KeyRange::from(103..=103)
        );
        assert_eq!(
            *right.find_shard(2).unwrap().key_range(),
            KeyRange::from(104..=104)
        );

        let want = [
            (0, Side::Left, 0, KeyRange::from(100..=100)),
            (0, Side::Left, 1, KeyRange::from(101..=101)),
            (1, Side::Right, 0, KeyRange::from(102..=102)),
            (2, Side::Right, 1, KeyRange::from(103..=103)),
            (3, Side::Right, 2, KeyRange::from(104..=104)),
        ];

        assert_eq!(split.remap().len(), want.len());
        for (i, piece) in split.remap().iter().enumerate() {
            let (oi, side, ni, r) = &want[i];
            assert_eq!(piece.old_idx, *oi);
            assert_eq!(&piece.side, side);
            assert_eq!(piece.new_idx, *ni);
            assert_eq!(piece.range, *r);
        }
    }

    #[test]
    fn split_single_key() {
        // Single-key range cannot actually be split: only the right side is populated,
        // with a single remap piece echoing the original key.
        let old = ShardPlan::new(KeyRange::from(42..=42), nz(4));
        assert_eq!(old.shard_count(), 1);

        let split = old.split(nz(4));
        assert!(split.left().is_none());
        let right = split.right().unwrap();
        assert_eq!(right.shard_count(), 1);
        assert_eq!(
            *right.find_shard(0).unwrap().key_range(),
            KeyRange::from(42..=42)
        );

        assert_eq!(split.remap().len(), 1);
        let piece = &split.remap()[0];
        assert_eq!(piece.old_idx, 0);
        assert_eq!(piece.side, Side::Right);
        assert_eq!(piece.new_idx, 0);
        assert_eq!(piece.range, KeyRange::from(42..=42));
    }

    #[test]
    fn split_shrinking_to_fewer_shards() {
        // Old: [0..=99] into 8 shards (sizes 13,13,12,13,12,13,12,12).
        // Split with new_max_shards=2 => each side gets at most 2 shards.
        let old = ShardPlan::new(KeyRange::from(0..=99), nz(8));
        let split = old.split(nz(2));

        let left = split.left().unwrap();
        let right = split.right().unwrap();
        assert_eq!(left.shard_count(), 2);
        assert_eq!(right.shard_count(), 2);

        // Every old shard's keys must be remapped exactly once, with pieces fully
        // covering the old shard's range.
        let mut covered_by_old: std::collections::BTreeMap<ShardIdx, Vec<KeyRange>> =
            Default::default();
        for piece in split.remap() {
            covered_by_old
                .entry(piece.old_idx)
                .or_default()
                .push(piece.range);

            // new_idx must be in-bounds on the side it claims.
            let side_plan = match piece.side {
                Side::Left => left,
                Side::Right => right,
            };
            assert!(piece.new_idx < side_plan.shard_count());
            // Piece must lie entirely within the claimed new shard.
            let new_shard = side_plan.find_shard(piece.new_idx).unwrap();
            assert!(piece.range.start() >= new_shard.key_range().start());
            assert!(piece.range.end() <= new_shard.key_range().end());
        }

        assert_eq!(covered_by_old.len(), old.shard_count() as usize);
        for (old_idx, pieces) in covered_by_old {
            let old_shard = old.find_shard(old_idx).unwrap();
            // Pieces (already in remap order) must be contiguous and cover exactly the
            // old shard's range.
            assert_eq!(
                pieces.first().unwrap().start(),
                old_shard.key_range().start()
            );
            assert_eq!(pieces.last().unwrap().end(), old_shard.key_range().end());
            for w in pieces.windows(2) {
                assert_eq!(w[0].end() + 1, w[1].start());
            }
        }
    }
}
