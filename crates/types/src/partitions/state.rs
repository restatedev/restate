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

use restate_encoding::NetSerde;

use crate::identifiers::PartitionId;
use crate::logs::Lsn;
use crate::{Merge, PlainNodeId, Version};

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

/// The maximum observed partition membership configuration by this node's worker
#[derive(Clone, Default)]
pub struct PartitionReplicaSetStates {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    partitions: DashMap<PartitionId, MembershipState>,
}

impl PartitionReplicaSetStates {
    pub fn note_observed_membership(
        &self,
        partition_id: PartitionId,
        current_membership: &ReplicaSetState,
        next_membership: &Option<ReplicaSetState>,
    ) {
        // we insert the partition id if unknown
        self.inner
            .partitions
            .entry(partition_id)
            .and_modify(|existing| {
                existing.merge(current_membership, next_membership);
            })
            .or_insert_with(|| MembershipState {
                observed_current_membership: current_membership.clone(),
                observed_next_membership: next_membership.clone(),
            });
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
            .map(|entry| (entry.key().clone(), entry.value().clone()))
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
        if incoming_current_membership.version > self.observed_current_membership.version {
            self.observed_current_membership = incoming_current_membership.clone();
            if self
                .observed_next_membership
                .as_ref()
                .is_some_and(|my_next| my_next.version <= self.observed_current_membership.version)
            {
                // unset our next, our current has caught up
                self.observed_next_membership = None;
            }
            modified = true;
        } else if incoming_current_membership.version == self.observed_current_membership.version {
            // merge member's durable lsns
            modified = self
                .observed_current_membership
                .merge(incoming_current_membership.clone());
        }

        // dealing with next membership configuration
        let Some(incoming_next_membership) = incoming_next_membership else {
            return modified;
        };

        if incoming_next_membership.version <= self.observed_current_membership.version {
            // ignore it, their next is lower that our current's
            return modified;
        }

        let Some(my_next_membership) = &mut self.observed_next_membership else {
            self.observed_next_membership = Some(incoming_next_membership.clone());
            return true;
        };

        if incoming_next_membership.version > my_next_membership.version {
            *my_next_membership = incoming_next_membership.clone();
            modified = true;
        } else if incoming_next_membership.version == my_next_membership.version {
            // merge
            modified = my_next_membership.merge(incoming_next_membership.clone());
        }

        modified
    }
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct ReplicaSetState {
    pub version: Version,
    // ordered, akin to NodeSet
    pub members: Vec<MemberState>,
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
