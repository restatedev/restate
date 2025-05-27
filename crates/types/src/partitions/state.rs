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
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use restate_encoding::NetSerde;

use crate::identifiers::PartitionId;
use crate::logs::{Lsn, SequenceNumber};
use crate::partitions::PartitionConfiguration;
use crate::{Merge, PlainNodeId, Version};

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
    /// Update the membership state for a partition
    pub fn note_observed_membership(
        &self,
        partition_id: PartitionId,
        current_membership: &ReplicaSetState,
        next_membership: &Option<ReplicaSetState>,
    ) {
        let modified = match self.inner.partitions.entry(partition_id) {
            Entry::Occupied(mut occupied_entry) => occupied_entry
                .get_mut()
                .merge(current_membership, next_membership),
            Entry::Vacant(entry) => {
                entry.insert(MembershipState {
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

    pub fn get_membership_state(&self, partition_id: PartitionId) -> Option<MembershipState> {
        self.inner
            .partitions
            .get(&partition_id)
            .map(|k| k.value().clone())
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
    pub fn changed(&self) -> Notified {
        self.inner.global_notify.notified()
    }
}

#[derive(Debug, Clone)]
pub struct MembershipState {
    pub observed_current_membership: ReplicaSetState,
    pub observed_next_membership: Option<ReplicaSetState>,
}

impl MembershipState {
    fn merge(
        &mut self,
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
        debug_assert_eq!(
            self.members.iter().map(|m| m.node_id).collect::<Vec<_>>(),
            other.members.iter().map(|m| m.node_id).collect::<Vec<_>>(),
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
