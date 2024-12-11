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
use std::ops::RangeInclusive;

use serde_with::serde_as;
use xxhash_rust::xxh3::Xxh3Builder;

use crate::cluster::cluster_state::RunMode;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::partition_table::PartitionTable;
use crate::{flexbuffers_storage_encode_decode, PlainNodeId, Version, Versioned};

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
    pub fn from(partition_table: &PartitionTable) -> Self {
        let mut scheduling_plan_builder = SchedulingPlanBuilder::default();

        for (partition_id, partition) in partition_table.partitions() {
            scheduling_plan_builder.insert_partition(
                *partition_id,
                TargetPartitionState::new(partition.key_range.clone()),
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
}

impl TargetPartitionState {
    pub fn new(partition_key_range: RangeInclusive<PartitionKey>) -> Self {
        Self {
            partition_key_range,
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
