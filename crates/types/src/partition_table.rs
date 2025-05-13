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
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;

use restate_encoding::{BilrostAs, BilrostDisplayFromStr, BilrostNewType, NetSerde};
use serde_with::serde_as;

use crate::identifiers::{PartitionId, PartitionKey};
use crate::logs::LogId;
use crate::metadata::GlobalMetadata;
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::replication::{NodeSet, ReplicationProperty};
use crate::{
    NetRangeInclusive, PlainNodeId, Version, Versioned, flexbuffers_storage_encode_decode,
};

const DB_NAME: &str = "db";
const PARTITION_CF_PREFIX: &str = "data-";

type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

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

/// Specified how partitions are replicated across the cluster.
#[serde_as]
#[derive(
    Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, BilrostAs, NetSerde,
)]
#[serde(rename_all = "kebab-case")]
#[bilrost_as(dto::PartitionReplication)]
pub enum PartitionReplication {
    /// All partitions are replicated on all nodes in the cluster.
    ///
    /// #[deprecated]
    ///
    /// Note: It's only kept for backward compatibility with older metadata
    /// but users can't select this value directly.
    Everywhere,
    /// Replication of partitions is limited to the specified replication property.
    /// for example a replication property of `{node: 2}` will run
    /// each partition on maximum of two nodes (one leader, and one follower)
    Limit(#[serde_as(as = "crate::replication::ReplicationPropertyFromTo")] ReplicationProperty),
}

impl Default for PartitionReplication {
    fn default() -> Self {
        Self::Limit(ReplicationProperty::new_unchecked(1))
    }
}

impl From<ReplicationProperty> for PartitionReplication {
    fn from(value: ReplicationProperty) -> Self {
        Self::Limit(value)
    }
}

#[derive(
    Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, bilrost::Message, NetSerde,
)]
#[serde(try_from = "PartitionTableShadow", into = "PartitionTableShadow")]
pub struct PartitionTable {
    #[bilrost(1)]
    version: Version,
    #[bilrost(2)]
    partitions: BTreeMap<PartitionId, Partition>,
    // Interval-map like structure which maps the inclusive end partition key of a partition to its
    // [`PartitionId`]. To validate that a partition key falls into a partition one also needs to
    // verify that the start partition key is smaller or equal than the given key, because holes
    // are not visible from this index structure.
    #[bilrost(3)]
    partition_key_index: BTreeMap<PartitionKey, PartitionId>,

    replication: PartitionReplication,
}

impl Default for PartitionTable {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            partitions: BTreeMap::default(),
            partition_key_index: BTreeMap::default(),
            replication: PartitionReplication::Limit(ReplicationProperty::new_unchecked(1)),
        }
    }
}

impl GlobalMetadata for PartitionTable {
    const KEY: &'static str = "partition_table";

    const KIND: MetadataKind = MetadataKind::PartitionTable;

    fn into_container(self: Arc<Self>) -> MetadataContainer {
        MetadataContainer::PartitionTable(self)
    }
}

impl PartitionTable {
    pub fn with_equally_sized_partitions(version: Version, number_partitions: u16) -> Self {
        let partitioner = EqualSizedPartitionPartitioner::new(number_partitions);
        let mut builder = PartitionTableBuilder::new(version);

        for (partition_id, partition_key_range) in partitioner {
            builder
                .add_partition(Partition::new(partition_id, partition_key_range))
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

    pub fn partition_replication(&self) -> &PartitionReplication {
        &self.replication
    }

    pub fn into_builder(self) -> PartitionTableBuilder {
        self.into()
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

#[derive(
    Debug,
    Clone,
    Default,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Deref,
    derive_more::DerefMut,
    BilrostNewType,
    NetSerde,
)]
#[serde(transparent)]
pub struct PartitionPlacement(NodeSet);

impl PartitionPlacement {
    pub fn leader(&self) -> Option<PlainNodeId> {
        self.0.first()
    }

    pub fn set_leader(&mut self, node_id: PlainNodeId) {
        self.0.insert_at_first(node_id);
    }

    fn hashed(&self) -> u64 {
        let mut state = ahash::AHasher::default();
        self.hash(&mut state);
        state.finish()
    }
}

impl FromIterator<PlainNodeId> for PartitionPlacement {
    fn from_iter<T: IntoIterator<Item = PlainNodeId>>(iter: T) -> Self {
        let inner = NodeSet::from_iter(iter);
        Self(inner)
    }
}

impl PartialEq for PartitionPlacement {
    fn eq(&self, other: &Self) -> bool {
        // eq implementation should respect the order of the items in the two sets
        // not only that it has the 2 values.
        self.0.len() == other.0.len() && self.0.iter().zip(other.0.iter()).all(|(l, r)| *l == *r)
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    derive_more::FromStr,
    BilrostAs,
    NetSerde,
)]
#[bilrost_as(BilrostDisplayFromStr)]
pub struct DbName(#[net_serde(skip)] SmartString);

impl Default for DbName {
    fn default() -> Self {
        DbName(DB_NAME.into())
    }
}

#[derive(
    Clone,
    Default,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    derive_more::FromStr,
    BilrostAs,
    NetSerde,
)]
#[bilrost_as(BilrostDisplayFromStr)]
pub struct CfName(#[net_serde(skip)] SmartString);

impl CfName {
    pub fn for_partition(partition_id: PartitionId) -> Self {
        Self(format!("{PARTITION_CF_PREFIX}{partition_id}").into())
    }
}

#[derive(
    Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, bilrost::Message, NetSerde,
)]
pub struct Partition {
    pub partition_id: PartitionId,
    pub key_range: NetRangeInclusive<PartitionKey>,
    #[serde(default)]
    pub placement: PartitionPlacement,
    log_id: Option<LogId>,
    db_name: Option<DbName>,
    cf_name: Option<CfName>,
}

impl Partition {
    pub fn new(partition_id: PartitionId, key_range: RangeInclusive<PartitionKey>) -> Self {
        Self {
            partition_id,
            key_range: key_range.into(),
            placement: PartitionPlacement::default(),
            log_id: None,
            db_name: None,
            cf_name: None,
        }
    }

    pub fn log_id(&self) -> LogId {
        self.log_id
            .unwrap_or_else(|| LogId::from(self.partition_id))
    }

    pub fn db_name(&self) -> DbName {
        self.db_name.clone().unwrap_or_default()
    }

    pub fn cf_name(&self) -> CfName {
        self.cf_name
            .clone()
            .unwrap_or_else(|| CfName::for_partition(self.partition_id))
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
            self.add_partition(Partition::new(partition_id, partition_key_range))?
        }

        Ok(())
    }

    pub fn num_partitions(&self) -> u16 {
        self.inner.num_partitions()
    }

    pub fn set_partition_replication(&mut self, partition_replication: PartitionReplication) {
        if self.inner.replication != partition_replication {
            self.inner.replication = partition_replication;
            self.modified = true;
        }
    }

    pub fn partition_replication(&self) -> &PartitionReplication {
        &self.inner.replication
    }

    /// Adds a new partition to the partition table. The newly added partition must exist and must
    /// not intersect with any other partition. Otherwise, this operation fails.
    pub fn add_partition(&mut self, partition: Partition) -> Result<(), BuilderError> {
        if self.inner.partitions.contains_key(&partition.partition_id) {
            return Err(BuilderError::Duplicate(partition.partition_id));
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

        let partition_id = partition.partition_id;
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

    pub fn for_each<F>(&mut self, mut modify: F)
    where
        F: FnMut(&PartitionId, &mut PartitionPlacement),
    {
        for (partition_id, partition) in self.inner.partitions.iter_mut() {
            let hash = partition.placement.hashed();
            modify(partition_id, &mut partition.placement);

            if hash != partition.placement.hashed() {
                self.modified = true;
            }
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PartitionShadow {
    pub log_id: Option<LogId>,
    pub key_range: RangeInclusive<PartitionKey>,
    #[serde(default)]
    pub db_name: Option<DbName>,
    pub cf_name: Option<CfName>,
    #[serde(default)]
    pub placement: PartitionPlacement,
}

/// Serialization helper which handles the deserialization of the current and older
/// [`PartitionTable`] versions.
#[serde_as]
#[derive(serde::Serialize, serde::Deserialize)]
struct PartitionTableShadow {
    version: Version,
    // only needed for deserializing the FixedPartitionTable created in v1 of Restate. Can be
    // removed once we no longer support reading FixedPartitionTable data.
    num_partitions: u16,
    // partitions field is used by the PartitionTable introduced in v1.1 of Restate.
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    partitions: Option<BTreeMap<PartitionId, PartitionShadow>>,

    replication: Option<PartitionReplication>,
}

impl From<PartitionTable> for PartitionTableShadow {
    fn from(value: PartitionTable) -> Self {
        let num_partitions = value.num_partitions();
        Self {
            version: value.version,
            num_partitions,
            partitions: Some(
                value
                    .partitions
                    .into_iter()
                    .map(|(partition_id, partition)| {
                        let partition_shadow = PartitionShadow {
                            log_id: partition.log_id,
                            key_range: partition.key_range.into(),
                            cf_name: partition.cf_name,
                            db_name: partition.db_name,
                            placement: partition.placement,
                        };

                        (partition_id, partition_shadow)
                    })
                    .collect(),
            ),
            replication: Some(value.replication),
        }
    }
}

impl TryFrom<PartitionTableShadow> for PartitionTable {
    type Error = anyhow::Error;

    fn try_from(value: PartitionTableShadow) -> Result<Self, Self::Error> {
        let mut builder = PartitionTableBuilder::new(value.version);
        // replication strategy is unset if data has been written with version <= v1.1.3
        builder.set_partition_replication(value.replication.unwrap_or(
            PartitionReplication::Limit(ReplicationProperty::new_unchecked(1)),
        ));

        match value.partitions {
            Some(partitions) => {
                for (partition_id, partition_shadow) in partitions {
                    let partition = Partition {
                        partition_id,
                        log_id: partition_shadow.log_id,
                        key_range: partition_shadow.key_range.into(),
                        placement: partition_shadow.placement,
                        db_name: partition_shadow.db_name,
                        cf_name: partition_shadow.cf_name,
                    };

                    builder.add_partition(partition)?;
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
        let start = (partition_id * Self::PARTITION_KEY_RANGE_END).div_ceil(num_partitions);
        let end = ((partition_id + 1) * Self::PARTITION_KEY_RANGE_END).div_ceil(num_partitions) - 1;

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

mod dto {
    use crate::replication::ReplicationProperty;

    #[derive(Clone, Debug, bilrost::Message)]
    pub(super) struct PartitionReplication {
        replication_property: Option<ReplicationProperty>,
    }

    impl From<&super::PartitionReplication> for PartitionReplication {
        fn from(value: &super::PartitionReplication) -> Self {
            match value {
                super::PartitionReplication::Everywhere => Self {
                    replication_property: None,
                },
                super::PartitionReplication::Limit(replication_property) => Self {
                    replication_property: Some(replication_property.clone()),
                },
            }
        }
    }

    impl From<PartitionReplication> for super::PartitionReplication {
        fn from(value: PartitionReplication) -> Self {
            match value.replication_property {
                None => Self::Everywhere,
                Some(replication_property) => Self::Limit(replication_property),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use test_log::test;

    use super::PartitionPlacement;
    use crate::identifiers::{PartitionId, PartitionKey};
    use crate::partition_table::{
        EqualSizedPartitionPartitioner, FindPartition, Partition, PartitionTable,
        PartitionTableBuilder,
    };
    use crate::storage::StorageCodec;
    use crate::{PlainNodeId, Version, flexbuffers_storage_encode_decode};

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
        builder.add_partition(Partition::new(PartitionId::from(0), 0..=1024))?;
        builder.add_partition(Partition::new(PartitionId::from(1), 2048..=4096))?;

        let partition_table = builder.build();

        assert!(partition_table.find_partition_id(1024).is_ok());
        assert!(partition_table.find_partition_id(1025).is_err());

        Ok(())
    }

    #[test]
    fn test_placement_equal() {
        let placement_1 = PartitionPlacement::from_iter([
            PlainNodeId::from(1),
            PlainNodeId::from(2),
            PlainNodeId::from(3),
        ]);

        let placement_2 = PartitionPlacement::from_iter([
            PlainNodeId::from(1),
            PlainNodeId::from(2),
            PlainNodeId::from(3),
        ]);

        assert_eq!(placement_1, placement_2);

        let placement_2 = PartitionPlacement::from_iter([
            PlainNodeId::from(2),
            PlainNodeId::from(1),
            PlainNodeId::from(3),
        ]);

        assert_ne!(placement_1, placement_2);

        let placement_2 = PartitionPlacement::from_iter([
            PlainNodeId::from(1),
            PlainNodeId::from(2),
            PlainNodeId::from(3),
            PlainNodeId::from(4),
        ]);

        assert_ne!(placement_1, placement_2);
    }

    #[test]
    fn test_placement_hash() {
        let placement_1 = PartitionPlacement::from_iter([PlainNodeId::from(0)]);

        let mut placement_2 =
            PartitionPlacement::from_iter([PlainNodeId::from(0), PlainNodeId::from(0)]);

        assert_eq!(placement_1, placement_2);
        assert_eq!(placement_1.hashed(), placement_2.hashed());

        placement_2.set_leader(PlainNodeId::new(1));
        assert_ne!(placement_1, placement_2);
        assert_ne!(placement_1.hashed(), placement_2.hashed());
    }
}
