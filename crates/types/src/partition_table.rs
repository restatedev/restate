// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::num::{NonZero, NonZeroU32};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Context;
use regex::Regex;

use crate::identifiers::{PartitionId, PartitionKey};
use crate::protobuf::cluster::{
    replication_strategy, PartitionProcessorReplicationReplicationStrategyFactor,
    PartitionProcessorReplicationReplicationStrategyOnAllNodes,
    ReplicationStrategy as ProtoReplicationStrategy,
};
use crate::{flexbuffers_storage_encode_decode, Version, Versioned};

static REPLICATION_STRATEGY_FACTOR_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?i)factor\(\s*(?<factor>\d+)\s*\)$").expect("is valid pattern")
});

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

    replication_strategy: ReplicationStrategy,
}

impl Default for PartitionTable {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            partitions: BTreeMap::default(),
            partition_key_index: BTreeMap::default(),
            replication_strategy: ReplicationStrategy::default(),
        }
    }
}

impl PartitionTable {
    pub fn with_equally_sized_partitions(version: Version, number_partitions: u16) -> Self {
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

    pub fn partition_ids(&self) -> impl Iterator<Item = &PartitionId> {
        self.partitions.keys()
    }

    pub fn num_partitions(&self) -> u16 {
        u16::try_from(self.partitions.len()).expect("number of partitions should fit into u16")
    }

    pub fn get_partition(&self, partition_id: &PartitionId) -> Option<&Partition> {
        self.partitions.get(partition_id)
    }

    pub fn contains_partition(&self, partition_id: &PartitionId) -> bool {
        self.partitions.contains_key(partition_id)
    }

    pub fn replication_strategy(&self) -> ReplicationStrategy {
        self.replication_strategy
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
    #[error("partition table has reached its limits")]
    LimitReached,
}

#[derive(Debug, Default)]
pub struct PartitionTableBuilder {
    inner: PartitionTable,
    modified: bool,
}

impl PartitionTableBuilder {
    fn new(version: Version) -> Self {
        let inner = PartitionTable {
            version,
            ..Default::default()
        };
        Self {
            inner,
            modified: false,
        }
    }

    pub fn with_equally_sized_partitions(
        &mut self,
        number_partitions: u16,
    ) -> Result<(), BuilderError> {
        let partitioner = EqualSizedPartitionPartitioner::new(number_partitions);

        for (partition_id, partition_key_range) in partitioner {
            self.add_partition(partition_id, Partition::new(partition_key_range))?
        }

        Ok(())
    }

    pub fn num_partitions(&self) -> u16 {
        self.inner.num_partitions()
    }

    pub fn set_replication_strategy(&mut self, replication_strategy: ReplicationStrategy) {
        if self.inner.replication_strategy != replication_strategy {
            self.inner.replication_strategy = replication_strategy;
            self.modified = true;
        }
    }

    pub fn replication_strategy(&self) -> ReplicationStrategy {
        self.inner.replication_strategy
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

        if self.inner.partitions.len() > usize::from(*PartitionId::MAX) {
            return Err(BuilderError::LimitReached);
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
        self.modified = true;

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

    pub fn build_if_modified(self) -> Option<PartitionTable> {
        if self.modified {
            return Some(self.build());
        }

        None
    }
    /// Builds the new [`PartitionTable`] with the same version.
    fn build_with_same_version(self) -> PartitionTable {
        self.inner
    }
}

impl From<PartitionTable> for PartitionTableBuilder {
    fn from(value: PartitionTable) -> Self {
        Self {
            inner: value,
            modified: false,
        }
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
    num_partitions: u16,
    // partitions field is used by the PartitionTable introduced in v1.1 of Restate.
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    partitions: Option<BTreeMap<PartitionId, Partition>>,
    // replication strategy
    replication_strategy: Option<ReplicationStrategy>,
}

impl From<PartitionTable> for PartitionTableShadow {
    fn from(value: PartitionTable) -> Self {
        let num_partitions = value.num_partitions();
        Self {
            version: value.version,
            num_partitions,
            partitions: Some(value.partitions),
            replication_strategy: Some(value.replication_strategy),
        }
    }
}

impl TryFrom<PartitionTableShadow> for PartitionTable {
    type Error = anyhow::Error;

    fn try_from(value: PartitionTableShadow) -> Result<Self, Self::Error> {
        let mut builder = PartitionTableBuilder::new(value.version);
        // replication strategy is unset of data has been written with version <= v1.1.3
        builder.set_replication_strategy(value.replication_strategy.unwrap_or_default());

        match value.partitions {
            Some(partitions) => {
                for (partition_id, partition) in partitions {
                    builder.add_partition(partition_id, partition)?;
                }
            }
            None => {
                builder.with_equally_sized_partitions(value.num_partitions)?;
            }
        }

        Ok(builder.build_with_same_version())
    }
}

#[derive(Debug)]
pub struct EqualSizedPartitionPartitioner {
    num_partitions: u16,
    next_partition_id: PartitionId,
}

impl EqualSizedPartitionPartitioner {
    const PARTITION_KEY_RANGE_END: u128 = 1 << 64;

    fn new(num_partitions: u16) -> Self {
        Self {
            num_partitions,
            next_partition_id: PartitionId::MIN,
        }
    }

    fn partition_id_to_partition_range(
        num_partitions: u16,
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

/// Replication strategy for partition processors.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ReplicationStrategy {
    /// Schedule partition processor replicas on all available nodes
    #[default]
    OnAllNodes,
    /// Schedule this number of partition processor replicas
    Factor(NonZero<u32>),
}

impl TryFrom<ProtoReplicationStrategy> for ReplicationStrategy {
    type Error = anyhow::Error;

    fn try_from(value: ProtoReplicationStrategy) -> Result<Self, Self::Error> {
        let strategy = value.strategy.context("Replication strategy is required")?;

        let value = match strategy {
            replication_strategy::Strategy::OnAllNodes(_) => Self::OnAllNodes,
            replication_strategy::Strategy::Factor(factor) => Self::Factor(
                NonZeroU32::new(factor.factor)
                    .context("Replication strategy factor must be non zero")?,
            ),
        };

        Ok(value)
    }
}

impl Display for ReplicationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnAllNodes => {
                write!(f, "on-all-nodes")
            }
            Self::Factor(factor) => {
                write!(f, "factor({})", factor)
            }
        }
    }
}

impl FromStr for ReplicationStrategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "on-all-nodes" => Ok(Self::OnAllNodes),
            "factor" => anyhow::bail!("Missing replication factor value. Should be 'factor(<f>)'."),
            s => {
                let Some(m) = REPLICATION_STRATEGY_FACTOR_PATTERN.captures(s) else {
                    anyhow::bail!("Unknown replication strategy '{}'", s);
                };

                let factor: NonZeroU32 = m["factor"]
                    .parse()
                    .context("Invalid replication strategy factor")?;

                Ok(Self::Factor(factor))
            }
        }
    }
}

impl From<ReplicationStrategy> for ProtoReplicationStrategy {
    fn from(value: ReplicationStrategy) -> Self {
        match value {
            ReplicationStrategy::OnAllNodes => Self {
                strategy: Some(replication_strategy::Strategy::OnAllNodes(
                    PartitionProcessorReplicationReplicationStrategyOnAllNodes {},
                )),
            },
            ReplicationStrategy::Factor(factor) => Self {
                strategy: Some(replication_strategy::Strategy::Factor(
                    PartitionProcessorReplicationReplicationStrategyFactor {
                        factor: factor.get(),
                    },
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use bytes::BytesMut;
    use test_log::test;

    use super::ReplicationStrategy;
    use crate::identifiers::{PartitionId, PartitionKey};
    use crate::partition_table::{
        EqualSizedPartitionPartitioner, FindPartition, Partition, PartitionTable,
        PartitionTableBuilder,
    };
    use crate::storage::StorageCodec;
    use crate::{flexbuffers_storage_encode_decode, Version};

    #[test]
    fn test_replication_strategy_parse() {
        let strategy: ReplicationStrategy = "on-all-nodes".parse().unwrap();
        assert_eq!(ReplicationStrategy::OnAllNodes, strategy);

        let strategy: ReplicationStrategy = "factor(10)".parse().unwrap();
        assert_eq!(
            ReplicationStrategy::Factor(NonZeroU32::new(10).expect("is non zero")),
            strategy
        );

        let strategy: anyhow::Result<ReplicationStrategy> = "factor(0)".parse();
        assert!(strategy.is_err());
    }
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
        num_partitions: u16,
    }

    impl FixedPartitionTable {
        pub fn new(version: Version, num_partitions: u16) -> Self {
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
