// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::subsharding::ShardPlan;
use crate::{KeyRange, PartitionId};

#[derive(Debug)]
pub struct EqualSizedPartitionPartitioner {
    plan: ShardPlan,
}

impl EqualSizedPartitionPartitioner {
    pub fn new(num_partitions: u16) -> Self {
        Self {
            plan: ShardPlan::new(KeyRange::FULL, num_partitions),
        }
    }
}

impl IntoIterator for EqualSizedPartitionPartitioner {
    type Item = (PartitionId, KeyRange);
    type IntoIter = EqualSizedPartitions;

    fn into_iter(self) -> Self::IntoIter {
        EqualSizedPartitions {
            plan: self.plan,
            next: 0,
        }
    }
}

/// Iterator of `(PartitionId, KeyRange)` pairs produced by
/// [`EqualSizedPartitionPartitioner::into_iter`].
#[derive(Debug)]
pub struct EqualSizedPartitions {
    plan: ShardPlan,
    next: u16,
}

impl Iterator for EqualSizedPartitions {
    type Item = (PartitionId, KeyRange);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next < self.plan.shard_count() {
            let idx = self.next;
            self.next += 1;
            let range = *self.plan.find_shard_unchecked(idx).key_range();
            Some((PartitionId::new_unchecked(idx), range))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.plan.shard_count() - self.next) as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for EqualSizedPartitions {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionKey;

    #[test]
    fn partitioner_produces_consecutive_ranges() {
        let partitioner = EqualSizedPartitionPartitioner::new(10);
        let mut previous_end = None;
        let mut previous_length = None::<PartitionKey>;

        for (_id, range) in partitioner {
            let current_length = range.end() - range.start();

            if let Some(previous_length) = previous_length {
                let length_diff = previous_length.abs_diff(current_length);
                assert!(length_diff <= 1);
            } else {
                assert_eq!(range.start(), 0);
            }

            if let Some(previous_end) = previous_end {
                assert_eq!(previous_end + 1, range.start());
            }

            previous_end = Some(range.end());
            previous_length = Some(current_length);
        }

        assert_eq!(previous_end, Some(PartitionKey::MAX));
    }
}
