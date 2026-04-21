// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{KeyRange, PartitionId};

#[derive(Debug)]
pub struct EqualSizedPartitionPartitioner {
    num_partitions: u16,
    next_partition_id: PartitionId,
}

impl EqualSizedPartitionPartitioner {
    const PARTITION_KEY_RANGE_END: u128 = 1 << 64;

    pub fn new(num_partitions: u16) -> Self {
        Self {
            num_partitions,
            next_partition_id: PartitionId::MIN,
        }
    }

    pub fn partition_id_to_partition_range(
        num_partitions: u16,
        partition_id: PartitionId,
    ) -> KeyRange {
        let num_partitions = u128::from(num_partitions);
        let partition_id = u128::from(*partition_id);

        assert!(
            partition_id < num_partitions,
            "There cannot be a partition id which is larger than the number of partitions \
                '{num_partitions}', when using the fixed consecutive partitioning scheme."
        );

        // adding num_partitions - 1 to dividend is equivalent to applying ceil function to result
        let start = (partition_id * Self::PARTITION_KEY_RANGE_END).div_ceil(num_partitions);
        let end = ((partition_id + 1) * Self::PARTITION_KEY_RANGE_END).div_ceil(num_partitions) - 1;

        let start = u64::try_from(start)
            .expect("Resulting partition start '{start}' should be <= u64::MAX.");
        let end =
            u64::try_from(end).expect("Resulting partition end '{end}' should be <= u64::MAX.");

        KeyRange::new(start, end)
    }
}

impl Iterator for EqualSizedPartitionPartitioner {
    type Item = (PartitionId, KeyRange);

    fn next(&mut self) -> Option<Self::Item> {
        if *self.next_partition_id < self.num_partitions {
            let partition_id = self.next_partition_id;
            self.next_partition_id = self.next_partition_id.next();

            let partition_range =
                Self::partition_id_to_partition_range(self.num_partitions, partition_id);

            Some((partition_id, partition_range))
        } else {
            None
        }
    }
}

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
