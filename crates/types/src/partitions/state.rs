// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dashmap::Entry;
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, watch};

use restate_encoding::NetSerde;
use restate_ty::partitions::{LeaderEpoch, PartitionId};

use crate::cluster_state::ClusterState;
use crate::logs::{Lsn, SequenceNumber};
use crate::partitions::PartitionConfiguration;
use crate::{GenerationalNodeId, Merge, PlainNodeId, Version};

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

/// A map of max observed replica-set configurations for partitions
#[derive(Clone, Default)]
pub struct PartitionReplicaSetStates {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    partitions: DashMap<PartitionId, MembershipState>,
    global_notify: Notify,
}

impl PartitionReplicaSetStates {
    /// Update the leadership state for a partition
    ///
    /// The leadership state should only be updated after we are confident that
    /// the new leader has committed the leader epoch to the log. It's also acceptable
    /// to delay updating it until the actual leader or any of the followers have
    /// observed the leader epoch as being the winner of the elections.
    pub fn note_observed_leader(
        &self,
        partition_id: PartitionId,
        incoming_leader: LeadershipState,
    ) {
        let modified = match self.inner.partitions.entry(partition_id) {
            Entry::Occupied(mut occupied_entry) => occupied_entry
                .get_mut()
                .current_leader
                .send_if_modified(|l| l.merge(incoming_leader)),
            Entry::Vacant(entry) => {
                entry.insert(MembershipState {
                    current_leader: watch::Sender::new(incoming_leader),
                    observed_current_membership: Default::default(),
                    observed_next_membership: Default::default(),
                });
                true
            }
        };

        if modified {
            self.inner.global_notify.notify_waiters();
        }
    }

    /// Update the membership state for a partition
    ///
    /// If you don't have a new leadership state, use Default::default() instead for the parameter
    /// `leadershipstate`.
    pub fn note_observed_membership(
        &self,
        partition_id: PartitionId,
        current_leader: LeadershipState,
        current_membership: &ReplicaSetState,
        next_membership: &Option<ReplicaSetState>,
    ) {
        let modified = match self.inner.partitions.entry(partition_id) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry
                    .get_mut()
                    .merge(current_leader, current_membership, next_membership)
            }
            Entry::Vacant(entry) => {
                entry.insert(MembershipState {
                    current_leader: watch::Sender::new(current_leader),
                    observed_current_membership: current_membership.clone(),
                    observed_next_membership: next_membership.clone(),
                });
                true
            }
        };

        if modified {
            self.inner.global_notify.notify_waiters();
        }
    }

    pub fn note_durable_lsn(
        &self,
        partition_id: PartitionId,
        node_id: PlainNodeId,
        durable_lsn: Lsn,
    ) {
        let Some(mut state) = self.inner.partitions.get_mut(&partition_id) else {
            return;
        };
        let mut modified = false;

        // update durable lsn in members of current and/or next
        for member in state
            .value_mut()
            .observed_current_membership
            .members
            .iter_mut()
        {
            if member.node_id == node_id && durable_lsn > member.durable_lsn {
                member.durable_lsn = durable_lsn;
                modified |= true;
            }
        }

        if let Some(next_membership) = state.value_mut().observed_next_membership.as_mut() {
            for member in next_membership.members.iter_mut() {
                if member.node_id == node_id && durable_lsn > member.durable_lsn {
                    member.durable_lsn = durable_lsn;
                    modified |= true;
                }
            }
        }

        if modified {
            self.inner.global_notify.notify_waiters();
        }
    }

    /// Returns the minimum durable lsn of the given partition by looking at
    /// the union of the current and next membership configuration.
    ///
    /// This returns `Lsn::INVALID` if we can't establish a reasonable view of the
    /// durable lsns from replica-set members.
    pub fn get_min_durable_lsn(&self, partition_id: PartitionId) -> Lsn {
        let Some(state) = self.inner.partitions.get(&partition_id) else {
            return Lsn::INVALID;
        };
        let min_durable_in_current = state
            .observed_current_membership
            .members
            .iter()
            .map(|m| m.durable_lsn)
            .min()
            .unwrap_or(Lsn::INVALID);

        if let Some(next) = &state.observed_next_membership {
            let next_min_durable = next
                .members
                .iter()
                .map(|m| m.durable_lsn)
                .min()
                .unwrap_or(Lsn::INVALID);
            min_durable_in_current.min(next_min_durable)
        } else {
            min_durable_in_current
        }
    }

    /// Returns the maximum durable lsn of the given partition by looking at
    /// the union of the current and next membership configuration.
    ///
    /// This returns `Lsn::INVALID` if we can't establish a reasonable view of the
    /// durable lsns from replica-set members.
    pub fn get_max_durable_lsn(&self, partition_id: PartitionId) -> Lsn {
        let Some(state) = self.inner.partitions.get(&partition_id) else {
            return Lsn::INVALID;
        };
        let max_durable_in_current = state
            .observed_current_membership
            .members
            .iter()
            .map(|m| m.durable_lsn)
            .max()
            .unwrap_or(Lsn::INVALID);

        if let Some(next) = &state.observed_next_membership {
            let next_max_durable = next
                .members
                .iter()
                .map(|m| m.durable_lsn)
                .max()
                .unwrap_or(Lsn::INVALID);
            max_durable_in_current.max(next_max_durable)
        } else {
            max_durable_in_current
        }
    }

    pub fn watch_leadership_state(
        &self,
        partition_id: PartitionId,
    ) -> watch::Receiver<LeadershipState> {
        match self.inner.partitions.entry(partition_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.get().watch_current_leader(),
            Entry::Vacant(entry) => {
                let value = entry.insert(MembershipState::default());
                self.inner.global_notify.notify_waiters();
                value.value().watch_current_leader()
            }
        }
    }

    pub fn membership_state(&self, partition_id: PartitionId) -> MembershipState {
        match self.inner.partitions.entry(partition_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            Entry::Vacant(entry) => {
                let value = entry.insert(MembershipState::default());
                self.inner.global_notify.notify_waiters();
                value.value().clone()
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (PartitionId, MembershipState)> {
        self.inner
            .partitions
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
    }

    /// Future to monitor changes to the partition replica set states.
    ///
    /// If you don't want to miss any changes, it's advised to create this future first, read the
    /// partition replica set states, then await this future for updates.
    pub fn changed(&self) -> Notified<'_> {
        self.inner.global_notify.notified()
    }
}

#[derive(Debug, Clone, Copy, bilrost::Message, NetSerde)]
pub struct LeadershipState {
    pub current_leader_epoch: LeaderEpoch,
    pub current_leader: GenerationalNodeId,
}

impl Default for LeadershipState {
    fn default() -> Self {
        Self {
            current_leader_epoch: LeaderEpoch::INVALID,
            current_leader: GenerationalNodeId::INVALID,
        }
    }
}

impl Merge for LeadershipState {
    fn merge(&mut self, other: Self) -> bool {
        match self.current_leader_epoch.cmp(&other.current_leader_epoch) {
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Less => {
                self.current_leader_epoch = other.current_leader_epoch;
                self.current_leader = other.current_leader;
                true
            }
            std::cmp::Ordering::Equal
                if !self.current_leader.is_valid() && other.current_leader.is_valid() =>
            {
                // update our current leader if the incoming knows about it and we don't.
                self.current_leader = other.current_leader;
                true
            }
            std::cmp::Ordering::Equal => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MembershipState {
    current_leader: watch::Sender<LeadershipState>,
    pub observed_current_membership: ReplicaSetState,
    pub observed_next_membership: Option<ReplicaSetState>,
}

impl Default for MembershipState {
    fn default() -> Self {
        Self {
            current_leader: watch::Sender::new(LeadershipState::default()),
            observed_current_membership: ReplicaSetState::default(),
            observed_next_membership: None,
        }
    }
}

impl MembershipState {
    fn merge(
        &mut self,
        incoming_leadership_state: LeadershipState,
        incoming_current_membership: &ReplicaSetState,
        incoming_next_membership: &Option<ReplicaSetState>,
    ) -> bool {
        let mut modified = false;
        match incoming_current_membership
            .version
            .cmp(&self.observed_current_membership.version)
        {
            // we have a new current membership
            std::cmp::Ordering::Greater => {
                // todo: try to use previous durable lsns if the two replica-sets intersect
                self.observed_current_membership = incoming_current_membership.clone();
                modified = true;
                if self
                    .observed_next_membership
                    .as_ref()
                    .is_some_and(|my_next| {
                        my_next.version <= self.observed_current_membership.version
                    })
                {
                    // unset our next, our current has caught up
                    self.observed_next_membership = None;
                }
            }
            std::cmp::Ordering::Equal => {
                // merge member's durable lsns
                modified = self
                    .observed_current_membership
                    .merge(incoming_current_membership.clone());
            }
            std::cmp::Ordering::Less => { /* ignore it */ }
        }

        modified |= self
            .current_leader
            .send_if_modified(|l| l.merge(incoming_leadership_state));

        // dealing with next membership configuration
        let Some(incoming_next_membership) = incoming_next_membership else {
            return modified;
        };

        // incoming has next but it older/equal to our own current
        if incoming_next_membership.version <= self.observed_current_membership.version {
            // ignore it, their next is lower that our current's
            return modified;
        }

        let Some(my_next_membership) = &mut self.observed_next_membership else {
            self.observed_next_membership = Some(incoming_next_membership.clone());
            return true;
        };

        match incoming_next_membership
            .version
            .cmp(&my_next_membership.version)
        {
            std::cmp::Ordering::Greater => {
                *my_next_membership = incoming_next_membership.clone();
                modified = true;
            }
            std::cmp::Ordering::Equal => {
                modified = my_next_membership.merge(incoming_next_membership.clone());
            }
            std::cmp::Ordering::Less => { /* ignore it */ }
        }

        modified
    }

    /// Returns true if the given node_id is part of the current or next membership.
    pub fn contains(&self, node_id: PlainNodeId) -> bool {
        self.observed_current_membership
            .members
            .iter()
            .any(|m| m.node_id == node_id)
            || self
                .observed_next_membership
                .as_ref()
                .map(|m| m.members.iter().any(|m| m.node_id == node_id))
                .unwrap_or(false)
    }

    pub fn current_leader(&self) -> LeadershipState {
        *self.current_leader.borrow()
    }

    pub fn watch_current_leader(&self) -> watch::Receiver<LeadershipState> {
        self.current_leader.subscribe()
    }

    /// Returns the first alive node from `observed_current_membership` by overlaying it with the
    /// current cluster state.
    pub fn first_alive_node(&self, cluster_state: &ClusterState) -> Option<GenerationalNodeId> {
        self.observed_current_membership
            .members
            .iter()
            .find_map(|member| {
                let (node_id, state) =
                    cluster_state.get_node_state_and_generation(member.node_id)?;
                if state.is_alive() {
                    Some(node_id)
                } else {
                    None
                }
            })
    }
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct ReplicaSetState {
    pub version: Version,
    // ordered, akin to NodeSet
    pub members: Vec<MemberState>,
}

impl ReplicaSetState {
    /// Creates the replica set state from the given partition configuration. It assumes the
    /// durable lsn to be invalid since we don't have information about it yet.
    pub fn from_partition_configuration(partition_configuration: &PartitionConfiguration) -> Self {
        let members = partition_configuration
            .replica_set()
            .iter()
            .map(|node_id| MemberState {
                node_id: *node_id,
                durable_lsn: Lsn::INVALID,
            })
            .collect();
        Self {
            version: partition_configuration.version,
            members,
        }
    }
}

impl Default for ReplicaSetState {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            members: Vec::new(),
        }
    }
}

impl Merge for ReplicaSetState {
    fn merge(&mut self, other: Self) -> bool {
        assert!(
            itertools::equal(
                self.members.iter().map(|m| m.node_id),
                other.members.iter().map(|m| m.node_id)
            ),
            "The system currently relies on a consistent view of the replica set state (e.g. for routing decisions and starting partition processors)"
        );
        let mut modified = false;
        for (member, incoming_member) in self.members.iter_mut().zip(other.members) {
            if incoming_member.durable_lsn > member.durable_lsn {
                member.durable_lsn = incoming_member.durable_lsn;
                modified = true;
            }
        }

        modified
    }
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct MemberState {
    pub node_id: PlainNodeId,
    pub durable_lsn: Lsn,
}
