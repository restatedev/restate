// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::num::{NonZero, NonZeroU16, NonZeroU32, NonZeroU8};
use std::ops::RangeInclusive;
use std::str::FromStr;

use anyhow::Context;
use serde_with::serde_as;
use xxhash_rust::xxh3::Xxh3Builder;

use crate::cluster::cluster_state::RunMode;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::logs::metadata::ProviderKind;
use crate::partition_table::PartitionTable;
use crate::protobuf::cluster::{
    node_set_selection_strategy, replication_strategy,
    ClusterConfiguration as ProtoClusterConfiguration,
    NodeSetSelectionStrategy as ProtoNodeSetSelectionStrategy,
    NodeSetSelectionStrategyStrictFaultTolerantGreedy,
    PartitionProcessorReplicationReplicationStrategyFactor,
    PartitionProcessorReplicationReplicationStrategyOnAllNodes,
    ReplicationProperty as ProtoReplicationProperty,
    ReplicationStrategy as ProtoReplicationStrategy,
};
use crate::replicated_loglet::{LocationScope, ReplicationProperty};
use crate::{flexbuffers_storage_encode_decode, PlainNodeId, Version, Versioned};

/// Replication strategy for partition processors.
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ReplicationStrategy {
    /// Schedule partition processor replicas on all available nodes
    OnAllNodes,
    /// Schedule this number of partition processor replicas
    Factor(NonZero<u32>),
    /// A specially strategy that is forced with
    /// Local and InMemory loglet provider.
    ///
    /// The user should never select this himself
    OnOneNodeNoFollowers,
}

/// The scheduling plan represents the target state of the cluster. The cluster controller will
/// try to drive the observed cluster state to match the target state.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchedulingPlan {
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    partitions: BTreeMap<PartitionId, TargetPartitionState>,
}

flexbuffers_storage_encode_decode!(SchedulingPlan);

impl SchedulingPlan {
    pub fn from(
        partition_table: &PartitionTable,
        replication_strategy: ReplicationStrategy,
    ) -> Self {
        let mut scheduling_plan_builder = SchedulingPlanBuilder::default();

        for (partition_id, partition) in partition_table.partitions() {
            scheduling_plan_builder.insert_partition(
                *partition_id,
                TargetPartitionState::new(partition.key_range.clone(), replication_strategy),
            );
        }

        scheduling_plan_builder.build()
    }

    pub fn into_builder(self) -> SchedulingPlanBuilder {
        SchedulingPlanBuilder::from(self)
    }

    pub fn partition_ids(&self) -> impl Iterator<Item = &PartitionId> {
        self.partitions.keys()
    }

    pub fn get(&self, partition_id: &PartitionId) -> Option<&TargetPartitionState> {
        self.partitions.get(partition_id)
    }

    pub fn get_mut(&mut self, partition_id: &PartitionId) -> Option<&mut TargetPartitionState> {
        self.partitions.get_mut(partition_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PartitionId, &TargetPartitionState)> {
        self.partitions.iter()
    }

    #[cfg(feature = "test-util")]
    pub fn partitions(&self) -> &BTreeMap<PartitionId, TargetPartitionState> {
        &self.partitions
    }
}

impl Versioned for SchedulingPlan {
    fn version(&self) -> Version {
        self.version
    }
}

impl Default for SchedulingPlan {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            partitions: BTreeMap::default(),
        }
    }
}

/// The target state of a partition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TargetPartitionState {
    pub partition_key_range: RangeInclusive<PartitionKey>,
    /// Node which is the designated leader
    pub leader: Option<PlainNodeId>,
    /// Set of nodes that should run a partition processor for this partition
    pub node_set: HashSet<PlainNodeId, Xxh3Builder>,
    pub replication_strategy: ReplicationStrategy,
}

impl TargetPartitionState {
    pub fn new(
        partition_key_range: RangeInclusive<PartitionKey>,
        replication_strategy: ReplicationStrategy,
    ) -> Self {
        Self {
            partition_key_range,
            replication_strategy,
            leader: None,
            node_set: HashSet::default(),
        }
    }

    pub fn add_node(&mut self, node_id: PlainNodeId, is_leader: bool) {
        self.node_set.insert(node_id);

        if is_leader {
            self.leader = Some(node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: PlainNodeId) {
        self.node_set.remove(&node_id);

        if self.leader == Some(node_id) {
            self.leader = None;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (PlainNodeId, RunMode)> + '_ {
        self.node_set.iter().map(|node_id| {
            if self.leader.as_ref() == Some(node_id) {
                (*node_id, RunMode::Leader)
            } else {
                (*node_id, RunMode::Follower)
            }
        })
    }
}

#[derive(Default)]
pub struct SchedulingPlanBuilder {
    modified: bool,
    inner: SchedulingPlan,
}

impl SchedulingPlanBuilder {
    pub fn modify_partition<F>(&mut self, partition_id: &PartitionId, mut modify: F)
    where
        F: FnMut(&mut TargetPartitionState) -> bool,
    {
        if let Some(partition) = self.inner.partitions.get_mut(partition_id) {
            if modify(partition) {
                self.modified = true;
            }
        }
    }

    pub fn insert_partition(&mut self, partition_id: PartitionId, partition: TargetPartitionState) {
        self.inner.partitions.insert(partition_id, partition);
        self.modified = true;
    }

    pub fn build_if_modified(mut self) -> Option<SchedulingPlan> {
        if self.modified {
            self.inner.version = self.inner.version.next();
            Some(self.inner)
        } else {
            None
        }
    }

    pub fn build(mut self) -> SchedulingPlan {
        self.inner.version = self.inner.version.next();
        self.inner
    }

    pub fn partition_ids(&self) -> impl Iterator<Item = &PartitionId> {
        self.inner.partition_ids()
    }

    pub fn contains_partition(&self, partition_id: &PartitionId) -> bool {
        self.inner.partitions.contains_key(partition_id)
    }
}

impl From<SchedulingPlan> for SchedulingPlanBuilder {
    fn from(value: SchedulingPlan) -> Self {
        Self {
            inner: value,
            modified: false,
        }
    }
}

/// Node set selection strategy for picking cluster members to host replicated logs. Note that this
/// concerns loglet replication configuration across storage servers during log bootstrap or cluster
/// reconfiguration, for example when expanding capacity.
///
/// It is expected that the Bifrost data plane will deal with short-term server unavailability.
/// Therefore, we can afford to aim high with our nodeset selections and optimise for maximum
/// possible fault tolerance. It is the data plane's responsibility to achieve availability within
/// this nodeset during periods of individual node downtime.
///
/// Finally, nodeset selection is orthogonal to log sequencer placement.
#[derive(Debug, Clone, PartialEq, Eq, Copy, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum NodeSetSelectionStrategy {
    /// Selects an optimal nodeset size based on the replication factor. The nodeset size is at
    /// least 2f+1, calculated by working backwards from a replication factor of f+1. If there are
    /// more nodes available in the cluster, the strategy will use them.
    ///
    /// This strategy will never suggest a nodeset smaller than 2f+1, thus ensuring that there is
    /// always plenty of fault tolerance built into the loglet. This is a safe default choice.
    #[default]
    StrictFaultTolerantGreedy,
}

/// # Cluster Configuration
#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "ClusterConfiguration", default)
)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterConfigurationSeed {
    /// # Partitions
    ///
    /// Note that this value is only used during cluster bootstrap.
    /// Repartitioning is not yet supported.
    ///
    /// Valid range: `1` - `65535`
    ///
    /// For most uses the recommended range is 1-2x the number of machine vCPUs.
    pub num_partitions: NonZeroU16,

    /// # Default Provider
    ///
    /// The default bifrost provider
    ///
    /// Default: [`ProviderKind::Replicated`]
    pub default_provider: ProviderKind,

    /// This implies a _minimum_ nodeset size `N` of `2f+1`. The actual size of the node set is determined by the
    /// specific strategy in use.
    ///
    /// Example:
    /// A replication factor of F=2 (configured as `{node: 2}`)
    ///
    /// F = f+1 = 2
    /// then f = 1
    /// hence N = 2f+1 => 3
    ///
    /// A replication factor of 1 means we cannot afford losing any nodes and hence the
    /// _min_ nodeset size is 1.
    ///
    /// A replication factor of 3, means f = 3, and hence _min_ nodset size is 5
    ///
    /// Default: {node: 1}
    #[cfg_attr(feature = "schemars", schemars(with = "schemars::Map<String, u8>"))]
    pub replication_property: ReplicationProperty,

    /// # Nodeset Selection Strategy
    ///
    /// Defines the policy for nodes selection to satisfy the configured
    /// replication_property.
    ///
    /// Currently only `strict-fault-tolerant-greedy` strategy is supported
    ///
    /// Default: "strict-fault-tolerant-greedy"
    pub nodeset_selection_strategy: NodeSetSelectionStrategy,

    /// # Replication Strategy
    ///
    /// Defines the replication strategy for partition processors
    ///
    /// Default: "on-all-nodes"
    pub partition_processor_replication_strategy: ReplicationStrategy,
}

impl Default for ClusterConfigurationSeed {
    fn default() -> Self {
        Self {
            num_partitions: NonZeroU16::new(24).expect("is valid"),
            default_provider: ProviderKind::Replicated,
            replication_property: ReplicationProperty::new(NonZeroU8::new(1).expect("is valid")),
            nodeset_selection_strategy: NodeSetSelectionStrategy::default(),
            partition_processor_replication_strategy: ReplicationStrategy::OnAllNodes,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_more::Deref)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterConfiguration {
    pub version: Version,
    #[serde(flatten)]
    #[deref]
    pub configuration: ClusterConfigurationSeed,
}

flexbuffers_storage_encode_decode!(ClusterConfiguration);

impl Versioned for ClusterConfiguration {
    fn version(&self) -> Version {
        self.version
    }
}

impl TryFrom<ProtoClusterConfiguration> for ClusterConfigurationSeed {
    type Error = anyhow::Error;

    fn try_from(value: ProtoClusterConfiguration) -> Result<Self, Self::Error> {
        let replication_property = value
            .replication_property
            .context("Missing `replication_property")?;

        let result = Self {
            default_provider: ProviderKind::from_str(&value.default_provider)
                .map_err(|_| anyhow::anyhow!("Unknown provider kind"))?,
            num_partitions: NonZeroU16::new(
                u16::try_from(value.num_partitions).context("Number of partitions is too high")?,
            )
            .context("Number of partitions cannot be zero")?,
            replication_property: replication_property.try_into()?,
            nodeset_selection_strategy: NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            partition_processor_replication_strategy: value
                .partition_processor_replication_strategy
                .context("Partition processor replication strategy is required")?
                .try_into()?,
        };

        Ok(result)
    }
}

impl TryFrom<ProtoReplicationProperty> for ReplicationProperty {
    type Error = anyhow::Error;

    fn try_from(value: ProtoReplicationProperty) -> Result<Self, Self::Error> {
        let mut replication_property = ReplicationProperty::new(
            NonZeroU8::new(u8::try_from(value.node).context("Node replication factor is too big")?)
                .context("Node replication factor cannot be zero")?,
        );

        if let Some(zone) = value.zone {
            replication_property.set_scope(
                LocationScope::Zone,
                NonZeroU8::new(u8::try_from(zone).context("Zone replication factor is too big")?)
                    .context("Zone replication factor cannot be zero")?,
            )?;
        }

        if let Some(region) = value.region {
            replication_property.set_scope(
                LocationScope::Region,
                NonZeroU8::new(
                    u8::try_from(region).context("Region replication factor is too big")?,
                )
                .context("Region replication factor cannot be zero")?,
            )?;
        }

        Ok(replication_property)
    }
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
            ReplicationStrategy::OnOneNodeNoFollowers => Self { strategy: None },
        }
    }
}
impl From<NodeSetSelectionStrategy> for ProtoNodeSetSelectionStrategy {
    fn from(value: NodeSetSelectionStrategy) -> Self {
        match value {
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy => Self {
                strategy: Some(
                    node_set_selection_strategy::Strategy::StrictFaultTolerantGreedy(
                        NodeSetSelectionStrategyStrictFaultTolerantGreedy {},
                    ),
                ),
            },
        }
    }
}
impl From<ClusterConfigurationSeed> for ProtoClusterConfiguration {
    fn from(value: ClusterConfigurationSeed) -> Self {
        Self {
            default_provider: value.default_provider.to_string(),
            num_partitions: value.num_partitions.get().into(),
            replication_property: Some(value.replication_property.into()),
            nodeset_selection_strategy: Some(value.nodeset_selection_strategy.into()),
            partition_processor_replication_strategy: Some(
                value.partition_processor_replication_strategy.into(),
            ),
        }
    }
}

impl From<ReplicationProperty> for ProtoReplicationProperty {
    fn from(value: ReplicationProperty) -> Self {
        let mut result = Self {
            node: value.num_copies().into(),
            ..Default::default()
        };
        for (scope, factor) in value.iter() {
            match scope {
                LocationScope::Region => result.region = Some((*factor).into()),
                LocationScope::Zone => result.zone = Some((*factor).into()),
                _ => {}
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use googletest::prelude::*;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU8};

    use crate::{
        logs::metadata::ProviderKind,
        protobuf::cluster::{
            node_set_selection_strategy, replication_strategy,
            NodeSetSelectionStrategyStrictFaultTolerantGreedy,
            PartitionProcessorReplicationReplicationStrategyFactor,
        },
        replicated_loglet::{LocationScope, ReplicationProperty},
    };

    use super::{
        ClusterConfigurationSeed, NodeSetSelectionStrategy, ProtoNodeSetSelectionStrategy,
        ProtoReplicationStrategy, ReplicationStrategy,
    };

    #[test]
    fn test_cluster_config_from() {
        let mut replication_property =
            ReplicationProperty::new(NonZeroU8::new(2).expect("is valid"));
        replication_property
            .set_scope(LocationScope::Zone, NonZeroU8::new(2).expect("is valid"))
            .expect("valid replication property");

        let config_1 = ClusterConfigurationSeed {
            default_provider: ProviderKind::Replicated,
            nodeset_selection_strategy: NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            num_partitions: NonZeroU16::new(10).expect("is valid"),
            partition_processor_replication_strategy: ReplicationStrategy::Factor(
                NonZeroU32::new(10).expect("is valid"),
            ),
            replication_property,
        };

        let proto_config: super::ProtoClusterConfiguration = config_1.clone().into();

        assert_that!(
            proto_config.nodeset_selection_strategy,
            some(eq(ProtoNodeSetSelectionStrategy {
                strategy: Some(
                    node_set_selection_strategy::Strategy::StrictFaultTolerantGreedy(
                        NodeSetSelectionStrategyStrictFaultTolerantGreedy {}
                    )
                )
            }))
        );

        assert_that!(
            proto_config.partition_processor_replication_strategy,
            some(eq(ProtoReplicationStrategy {
                strategy: Some(replication_strategy::Strategy::Factor(
                    PartitionProcessorReplicationReplicationStrategyFactor { factor: 10 }
                ))
            }))
        );

        let config_2: ClusterConfigurationSeed = proto_config.try_into().unwrap();

        assert_that!(config_2, eq(config_1));
    }
}
