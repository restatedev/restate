use restate_common::types::{PartitionKey, PeerId};
use std::ops::RangeInclusive;

pub(crate) struct RangePartitioner {
    num_partitions: u32,
    partition_length: PartitionKey,
    partition_remainder: PartitionKey,

    current_remainder: PartitionKey,
    next_partition: u32,
    next_start_partition_key: PartitionKey,
}

impl RangePartitioner {
    pub(crate) fn new(num_partitions: u32) -> Self {
        Self {
            num_partitions,
            partition_length: PartitionKey::MAX / num_partitions,
            partition_remainder: PartitionKey::MAX % num_partitions,
            current_remainder: num_partitions - 1,
            next_partition: 0,
            next_start_partition_key: 0,
        }
    }
}

impl Iterator for RangePartitioner {
    type Item = (PeerId, RangeInclusive<PartitionKey>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_partition < self.num_partitions {
            let mut end_partition_key = self.next_start_partition_key + self.partition_length;
            self.current_remainder += self.partition_remainder;

            while self.current_remainder >= self.num_partitions {
                end_partition_key += 1;
                self.current_remainder -= self.num_partitions;
            }

            let partition_key_range = if self.next_partition == self.num_partitions - 1 {
                debug_assert_eq!(end_partition_key, PartitionKey::MAX);
                self.next_start_partition_key..=PartitionKey::MAX
            } else {
                self.next_start_partition_key..=(end_partition_key - 1)
            };

            let result = Some((u64::from(self.next_partition), partition_key_range));

            self.next_partition += 1;
            self.next_start_partition_key = end_partition_key;

            result
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::range_partitioner::RangePartitioner;
    use restate_common::types::PartitionKey;
    use restate_test_util::test;

    #[test]
    fn range_partitioner_produces_consecutive_ranges() {
        let range_partitioner = RangePartitioner::new(10);
        let mut previous_end = None;
        let mut previous_length = None::<PartitionKey>;

        for (_id, range) in range_partitioner {
            let current_length = *range.end() - *range.start();

            if let Some(previous_length) = previous_length {
                let length_diff = previous_length.abs_diff(current_length);
                assert!(length_diff <= 1);
            } else {
                assert_eq!(*range.start(), 0);
            }

            if let Some(previous_end) = previous_end {
                assert_eq!(previous_end + 1, *range.start());
            }

            previous_end = Some(*range.end());
            previous_length = Some(current_length);
        }

        assert_eq!(previous_end, Some(PartitionKey::MAX));
    }
}
