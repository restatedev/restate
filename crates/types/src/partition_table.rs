// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::{PartitionId, PartitionKey};
use crate::{Version, Versioned};
use std::borrow::Borrow;
use std::ops::RangeInclusive;

#[derive(Debug, thiserror::Error)]
#[error("Cannot find target peer for partition key {0}")]
pub struct PartitionTableError(PartitionKey);

pub trait FindPartition {
    fn find_partition_id(
        &self,
        partition_key: PartitionKey,
    ) -> Result<PartitionId, PartitionTableError>;
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FixedPartitionTable {
    version: Version,
    num_partitions: u64,
}

impl FixedPartitionTable {
    const PARTITION_KEY_RANGE_END: u128 = 1 << 64;

    pub fn new(version: Version, num_partitions: u64) -> Self {
        Self {
            version,
            num_partitions,
        }
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn increment_version(&mut self) {
        self.version = self.version.next();
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
    }

    pub fn partitioner(&self) -> Partitioner {
        Partitioner::new(self.num_partitions)
    }

    fn partition_key_to_partition_id(
        num_partitions: u64,
        partition_key: PartitionKey,
    ) -> PartitionId {
        let num_partitions = u128::from(num_partitions);
        let partition_key = u128::from(partition_key);

        u64::try_from(partition_key * num_partitions / Self::PARTITION_KEY_RANGE_END)
            .expect("u64::MAX * u64::MAX / 1^64 should be <= u64::MAX.")
    }

    fn partition_id_to_partition_range(
        num_partitions: u64,
        partition_id: PartitionId,
    ) -> RangeInclusive<PartitionKey> {
        let num_partitions = u128::from(num_partitions);
        let partition_id = u128::from(partition_id);

        assert!(
            partition_id < num_partitions,
            "There cannot be a partition id which is larger than the number of partitions \
                '{num_partitions}', when using the fixed consecutive partitioning scheme."
        );

        // adding num_partitions - 1 to dividend is equivalent to applying ceil function to result
        let start =
            (partition_id * Self::PARTITION_KEY_RANGE_END + (num_partitions - 1)) / num_partitions;
        let end = ((partition_id + 1) * Self::PARTITION_KEY_RANGE_END + (num_partitions - 1))
            / num_partitions
            - 1;

        let start = u64::try_from(start)
            .expect("Resulting partition start '{start}' should be <= u64::MAX.");
        let end =
            u64::try_from(end).expect("Resulting partition end '{end}' should be <= u64::MAX.");

        start..=end
    }
}

impl Versioned for FixedPartitionTable {
    fn version(&self) -> Version {
        self.version()
    }
}

impl<T> FindPartition for T
where
    T: Borrow<FixedPartitionTable>,
{
    fn find_partition_id(
        &self,
        partition_key: PartitionKey,
    ) -> Result<PartitionId, PartitionTableError> {
        let partition_id = FixedPartitionTable::partition_key_to_partition_id(
            self.borrow().num_partitions,
            partition_key,
        );

        Ok(partition_id)
    }
}

#[derive(Debug)]
pub struct Partitioner {
    num_partitions: u64,
    next_partition_id: u64,
}

impl Partitioner {
    fn new(num_partitions: u64) -> Self {
        Self {
            num_partitions,
            next_partition_id: 0,
        }
    }
}

impl Iterator for Partitioner {
    type Item = (PartitionId, RangeInclusive<PartitionKey>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_partition_id < self.num_partitions {
            let partition_id = self.next_partition_id;
            self.next_partition_id += 1;

            let partition_range = FixedPartitionTable::partition_id_to_partition_range(
                self.num_partitions,
                partition_id,
            );

            Some((partition_id, partition_range))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use crate::identifiers::{PartitionId, PartitionKey};
    use crate::partition_table::{FindPartition, FixedPartitionTable, Partitioner};
    use crate::Version;

    #[test]
    fn partitioner_produces_consecutive_ranges() {
        let partitioner = Partitioner::new(10);
        let mut previous_end = None;
        let mut previous_length = None::<PartitionKey>;

        for (_id, range) in partitioner {
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

    impl FixedPartitionTable {
        fn unchecked_partition_key_to_target_peer(
            &self,
            partition_key: PartitionKey,
        ) -> PartitionId {
            self.find_partition_id(partition_key).unwrap()
        }
    }

    #[test(tokio::test)]
    async fn partition_table_resolves_partition_keys() {
        let num_partitions = 10;
        let partition_table = FixedPartitionTable::new(Version::MIN, num_partitions);
        let partitioner = partition_table.partitioner();

        for (partition_id, partition_range) in partitioner {
            assert_eq!(
                partition_table.unchecked_partition_key_to_target_peer(*partition_range.start()),
                partition_id
            );
            assert_eq!(
                partition_table.unchecked_partition_key_to_target_peer(*partition_range.end()),
                partition_id
            );
        }
    }
}
