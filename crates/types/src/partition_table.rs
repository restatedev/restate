// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use crate::identifiers::{PartitionId, PartitionKey};
use crate::{flexbuffers_storage_encode_decode, Version, Versioned};

#[derive(Debug, thiserror::Error)]
#[error("Cannot find partition for partition key '{0}'")]
pub struct PartitionTableError(PartitionKey);

pub trait FindPartition {
    fn find_partition_id(
        &self,
        partition_key: PartitionKey,
    ) -> Result<PartitionId, PartitionTableError>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    pub from: PartitionKey,
    pub to: PartitionKey,
}

impl From<KeyRange> for RangeInclusive<PartitionKey> {
    fn from(val: KeyRange) -> Self {
        RangeInclusive::new(val.from, val.to)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "PartitionTableShadow", into = "PartitionTableShadow")]
pub struct PartitionTable {
    version: Version,
    partitions: BTreeMap<PartitionId, Partition>,
    // Interval-map like structure which maps the inclusive end partition key of a partition to its
    // [`PartitionId`]. To validate that a partition key falls into a partition one also needs to
    // verify that the start partition key is smaller or equal than the given key, because holes
    // are not visible from this index structure.
    partition_key_index: BTreeMap<PartitionKey, PartitionId>,
}

impl Default for PartitionTable {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            partitions: BTreeMap::default(),
            partition_key_index: BTreeMap::default(),
        }
    }
}

impl PartitionTable {
    pub fn with_equally_sized_partitions(version: Version, number_partitions: u64) -> Self {
        let partitioner = EqualSizedPartitionPartitioner::new(number_partitions);
        let mut builder = PartitionTableBuilder::new(version);

        for (partition_id, partition_key_range) in partitioner {
            builder
                .add_partition(partition_id, Partition::new(partition_key_range))
                .expect("partitions should not overlap");
        }

        builder.build_with_same_version()
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

    pub fn partitions(&self) -> impl Iterator<Item = (&PartitionId, &Partition)> {
        self.partitions.iter()
    }

    pub fn partitions_mut(&mut self) -> impl Iterator<Item = (&PartitionId, &mut Partition)> {
        self.partitions.iter_mut()
    }

    pub fn num_partitions(&self) -> u64 {
        u64::try_from(self.partitions.len()).expect("number of partitions should fit into u64")
    }

    pub fn get_partition(&self, partition_id: &PartitionId) -> Option<&Partition> {
        self.partitions.get(partition_id)
    }
}

impl Versioned for PartitionTable {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(PartitionTable);

impl FindPartition for PartitionTable {
    fn find_partition_id(
        &self,
        partition_key: PartitionKey,
    ) -> Result<PartitionId, PartitionTableError> {
        // partition key ranges are inclusive, so let's look for the next partition key boundary >=
        // partition_key to find the owning partition candidate
        let candidate = self
            .partition_key_index
            .range(partition_key..)
            .next()
            .map(|(_, partition_id)| *partition_id)
            .ok_or(PartitionTableError(partition_key))?;

        // next we validate that the partition key is actually contained within the partition
        self.partitions
            .get(&candidate)
            .and_then(|partition| {
                if partition.key_range.start() <= &partition_key {
                    Some(candidate)
                } else {
                    None
                }
            })
            .ok_or(PartitionTableError(partition_key))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    pub key_range: RangeInclusive<PartitionKey>,
}

impl Partition {
    pub fn new(key_range: RangeInclusive<PartitionKey>) -> Self {
        Self { key_range }
    }
}

/// Errors when building a [`PartitionTable`] via the [`PartitionTableBuilder`].
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("the new partition overlaps with partition '{0}'")]
    Overlap(PartitionId),
    #[error("partition '{0}' already exists")]
    Duplicate(PartitionId),
}

#[derive(Debug, Default)]
pub struct PartitionTableBuilder {
    inner: PartitionTable,
}

impl PartitionTableBuilder {
    fn new(version: Version) -> Self {
        let inner = PartitionTable {
            version,
            ..Default::default()
        };
        Self { inner }
    }

    /// Adds a new partition to the partition table. The newly added partition must exist and must
    /// not intersect with any other partition. Otherwise, this operation fails.
    pub fn add_partition(
        &mut self,
        partition_id: PartitionId,
        partition: Partition,
    ) -> Result<(), BuilderError> {
        if self.inner.partitions.contains_key(&partition_id) {
            return Err(BuilderError::Duplicate(partition_id));
        }

        let start = *partition.key_range.start();
        let end = *partition.key_range.end();

        if let Some((_, partition_id)) = self.inner.partition_key_index.range(end..).next() {
            let partition = self
                .inner
                .partitions
                .get(partition_id)
                .expect("partition should be present");
            if *partition.key_range.start() <= end {
                return Err(BuilderError::Overlap(*partition_id));
            }
        }

        if let Some((_, partition_id)) = self.inner.partition_key_index.range(start..end).next() {
            return Err(BuilderError::Overlap(*partition_id));
        }

        self.inner.partitions.insert(partition_id, partition);
        self.inner.partition_key_index.insert(end, partition_id);

        Ok(())
    }

    pub fn remove_partition(&mut self, partition_id: &PartitionId) {
        if let Some(partition) = self.inner.partitions.remove(partition_id) {
            self.inner
                .partition_key_index
                .remove(partition.key_range.end());
        }
    }

    /// Builds the new [`PartitionTable`] with an incremented version.
    pub fn build(mut self) -> PartitionTable {
        self.inner.version = Version::MIN.max(self.inner.version.next());
        self.inner
    }

    /// Builds the new [`PartitionTable`] with the same version.
    fn build_with_same_version(self) -> PartitionTable {
        self.inner
    }
}

impl From<PartitionTable> for PartitionTableBuilder {
    fn from(value: PartitionTable) -> Self {
        Self { inner: value }
    }
}

/// Serialization helper which handles the deserialization of the current and older
/// [`PartitionTable`] versions.
#[serde_with::serde_as]
#[derive(serde::Serialize, serde::Deserialize)]
struct PartitionTableShadow {
    version: Version,
    // only needed for deserializing the FixedPartitionTable created in v1 of Restate. Can be
    // removed once we no longer support reading FixedPartitionTable data.
    num_partitions: u64,
    // partitions field is used by the PartitionTable introduced in v1.1 of Restate.
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    partitions: Option<BTreeMap<PartitionId, Partition>>,
}

impl From<PartitionTable> for PartitionTableShadow {
    fn from(value: PartitionTable) -> Self {
        let num_partitions = value.num_partitions();
        Self {
            version: value.version,
            num_partitions,
            partitions: Some(value.partitions),
        }
    }
}

impl TryFrom<PartitionTableShadow> for PartitionTable {
    type Error = anyhow::Error;

    fn try_from(value: PartitionTableShadow) -> Result<Self, Self::Error> {
        match value.partitions {
            // the partitions field is set if the data has been written with >= v1.1
            Some(partitions) => {
                let mut builder = PartitionTableBuilder::new(value.version);

                for (partition_id, partition) in partitions {
                    builder.add_partition(partition_id, partition)?;
                }

                Ok(builder.build_with_same_version())
            }
            // the partitions field is unset if the data has been written with v1
            None => Ok(PartitionTable::with_equally_sized_partitions(
                value.version,
                value.num_partitions,
            )),
        }
    }
}

#[derive(Debug)]
pub struct EqualSizedPartitionPartitioner {
    num_partitions: u64,
    next_partition_id: PartitionId,
}

impl EqualSizedPartitionPartitioner {
    const PARTITION_KEY_RANGE_END: u128 = 1 << 64;

    fn new(num_partitions: u64) -> Self {
        Self {
            num_partitions,
            next_partition_id: PartitionId::MIN,
        }
    }

    fn partition_id_to_partition_range(
        num_partitions: u64,
        partition_id: PartitionId,
    ) -> RangeInclusive<PartitionKey> {
        let num_partitions = u128::from(num_partitions);
        let partition_id = u128::from(*partition_id);

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

impl Iterator for EqualSizedPartitionPartitioner {
    type Item = (PartitionId, RangeInclusive<PartitionKey>);

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
    use bytes::BytesMut;
    use test_log::test;

    use crate::identifiers::{PartitionId, PartitionKey};
    use crate::partition_table::{
        EqualSizedPartitionPartitioner, FindPartition, Partition, PartitionTable,
        PartitionTableBuilder,
    };
    use crate::storage::StorageCodec;
    use crate::{flexbuffers_storage_encode_decode, Version};

    #[test]
    fn partitioner_produces_consecutive_ranges() {
        let partitioner = EqualSizedPartitionPartitioner::new(10);
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

    #[test(tokio::test)]
    async fn partition_table_resolves_partition_keys() {
        let num_partitions = 10;
        let partition_table =
            PartitionTable::with_equally_sized_partitions(Version::MIN, num_partitions);
        let partitioner = partition_table.partitions();

        for (partition_id, partition) in partitioner {
            assert_eq!(
                partition_table
                    .find_partition_id(*partition.key_range.start())
                    .expect("partition should exist"),
                *partition_id
            );
            assert_eq!(
                partition_table
                    .find_partition_id(*partition.key_range.end())
                    .expect("partition should exist"),
                *partition_id
            );
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct FixedPartitionTable {
        version: Version,
        num_partitions: u64,
    }

    impl FixedPartitionTable {
        pub fn new(version: Version, num_partitions: u64) -> Self {
            Self {
                version,
                num_partitions,
            }
        }
    }

    flexbuffers_storage_encode_decode!(FixedPartitionTable);

    #[test]
    fn ensure_compatibility() -> anyhow::Result<()> {
        let version = Version::from(42);
        let num_partitions = 1337;
        let expected_fixed_partition_table = FixedPartitionTable::new(version, num_partitions);
        let mut buf = BytesMut::default();

        StorageCodec::encode(&expected_fixed_partition_table, &mut buf)?;
        let partition_table = StorageCodec::decode::<PartitionTable, _>(&mut buf)?;

        assert_eq!(partition_table.version, version);
        assert_eq!(partition_table.num_partitions(), num_partitions);

        buf.clear();
        StorageCodec::encode(&partition_table, &mut buf)?;
        let fixed_partition_table = StorageCodec::decode::<FixedPartitionTable, _>(&mut buf)?;

        assert_eq!(fixed_partition_table.version, version);
        assert_eq!(fixed_partition_table.num_partitions, num_partitions);

        Ok(())
    }

    #[test]
    fn detect_holes_in_partition_table() -> googletest::Result<()> {
        let mut builder = PartitionTableBuilder::new(Version::INVALID);
        builder.add_partition(PartitionId::from(0), Partition::new(0..=1024))?;
        builder.add_partition(PartitionId::from(1), Partition::new(2048..=4096))?;

        let partition_table = builder.build();

        assert!(partition_table.find_partition_id(1024).is_ok());
        assert!(partition_table.find_partition_id(1025).is_err());

        Ok(())
    }
}
