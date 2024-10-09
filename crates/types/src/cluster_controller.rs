// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZero;
use std::ops::RangeInclusive;

use serde_with::serde_as;

use crate::cluster::cluster_state::RunMode;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::logs::metadata::{LogletParams, SegmentIndex};
use crate::logs::LogId;
use crate::partition_table::PartitionTable;
use crate::replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicationProperty};
use crate::{
    flexbuffers_storage_encode_decode, GenerationalNodeId, PlainNodeId, Version, Versioned,
};

/// Replication strategy for partition processors.
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum ReplicationStrategy {
    /// Schedule partition processor replicas on all available nodes
    OnAllNodes,
    /// Schedule this number of partition processor replicas
    Factor(NonZero<u32>),
}

/// The scheduling plan represents the target state of the cluster. The cluster controller will
/// try to drive the observed cluster state to match the target state.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchedulingPlan {
    pub version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub partitions: BTreeMap<PartitionId, TargetPartitionState>,
    // /// Replicated loglet target configuration.
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub logs: BTreeMap<LogId, TargetLogletState>,
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

    pub fn loglet_config(&self, log_id: &LogId) -> Option<&TargetLogletState> {
        self.logs.get(log_id)
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
            logs: BTreeMap::default(),
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
    pub node_set: BTreeSet<PlainNodeId>,
    pub replication_strategy: ReplicationStrategy,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LogletLifecycleState {
    #[default]
    Unknown,

    /// New loglets start in this state before transitioning to Available.
    New,

    /// An ongoing sealing operation is in progress. The loglet will transition back to Available once extended with
    /// a new segment.
    Sealing,

    /// The loglet is usable.
    Available,

    /// The existing loglet segment is impaired and requires reconfiguration, which is currently blocked.
    /// The control plane may be able to remediate the problem once resources become available.
    /// This can happen for example if tail segment sequencer has disappeared, and we don't have a candidate
    /// to replace it, or if an insufficient number of log servers are available to meet the replication requirement.
    ReconfigurationPendingResources,

    /// The existing loglet segment is impaired and operator intervention is required. This can happen if we don't
    /// have sufficient healthy log servers to seal the current segment. In such cases, a manual seal operation can
    /// be performed on the loglet.
    PermanentlyImpaired,

    /// This is a terminal state for a loglet. The control plane will never transition to this state.
    Terminated,
}

/// The target state of a log. We must be able to derive the LogletProvider configuration
/// from this as an input.
///
/// See: [LogletProviderFactory], [ReplicatedLogletParams]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TargetLogletState {
    pub log_id: LogId,
    pub loglet_id: ReplicatedLogletId,
    pub segment_index: SegmentIndex,
    pub loglet_state: LogletLifecycleState,
    /// Matches the sequencer in the current serialized LogletParams.
    pub sequencer: GenerationalNodeId,
    pub log_servers: NodeSet,
    pub replication: ReplicationProperty,
    pub params: LogletParams,
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
            node_set: BTreeSet::default(),
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

    pub fn loglet_config(&self, log_id: &LogId) -> Option<&TargetLogletState> {
        self.inner.logs.get(log_id)
    }

    pub fn insert_loglet(&mut self, loglet: TargetLogletState) -> &mut Self {
        self.inner.logs.insert(loglet.log_id, loglet);
        self.modified = true;
        self
    }

    pub fn build_if_modified(mut self) -> Option<SchedulingPlan> {
        if self.modified {
            self.inner.version = self.inner.version.next();
            Some(self.inner)
        } else {
            None
        }
    }

    pub fn build(&mut self) -> SchedulingPlan {
        self.inner.version = self.inner.version.next();
        self.inner.clone()
    }

    pub fn partition_ids(&self) -> impl Iterator<Item = &PartitionId> {
        self.inner.partition_ids()
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
