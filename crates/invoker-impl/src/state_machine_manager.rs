// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use std::ops::RangeInclusive;

use restate_invoker_api::Effect;
use restate_invoker_api::invocation_reader::InvocationReader;
use restate_types::identifiers::PartitionKey;

/// Tree of [InvocationStateMachine] held by the [Service].
#[derive(Debug)]
pub(super) struct InvocationStateMachineManager<SR> {
    partitions: HashMap<PartitionLeaderEpoch, PartitionInvocationStateMachineCoordinator<SR>>,
}

impl<SR> Default for InvocationStateMachineManager<SR> {
    fn default() -> Self {
        InvocationStateMachineManager {
            partitions: Default::default(),
        }
    }
}

#[derive(Debug)]
struct PartitionInvocationStateMachineCoordinator<IR> {
    output_tx: mpsc::Sender<Box<Effect>>,
    invocation_state_machines: HashMap<InvocationId, InvocationStateMachine>,
    partition_key_range: RangeInclusive<PartitionKey>,
    storage_reader: IR,
}

impl<IR> InvocationStateMachineManager<IR>
where
    IR: InvocationReader + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(super) fn has_partition(&self, partition: PartitionLeaderEpoch) -> bool {
        self.partitions.contains_key(&partition)
    }

    #[inline]
    pub(super) fn partition_storage_reader(&self, partition: PartitionLeaderEpoch) -> Option<&IR> {
        self.partitions.get(&partition).map(|p| &p.storage_reader)
    }

    #[inline]
    pub(super) fn resolve_partition_sender(
        &self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mpsc::Sender<Box<Effect>>> {
        self.partitions.get(&partition).map(|p| &p.output_tx)
    }

    // todo: only used in tests of invocation_epoch.Should be removed along with invocation_epoch.
    #[allow(dead_code)]
    pub(super) fn resolve_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<Box<Effect>>, &mut InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .get_mut(invocation_id)
                .map(|ism| (&p.output_tx, ism))
        })
    }

    #[inline]
    pub(super) fn handle_for_invocation<R>(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
        invocation_epoch: InvocationEpoch,
        f: impl FnOnce(&mpsc::Sender<Box<Effect>>, &mut InvocationStateMachine) -> R,
    ) -> Option<R> {
        if let Some((tx, ism)) =
            self.resolve_invocation_with_epoch(partition, invocation_id, invocation_epoch)
        {
            Some(f(tx, ism))
        } else {
            // If no state machine
            trace!("No state machine found for selected server header");
            None
        }
    }

    #[inline]
    pub(super) fn resolve_invocation_with_epoch(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
        invocation_epoch: InvocationEpoch,
    ) -> Option<(&mpsc::Sender<Box<Effect>>, &mut InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .get_mut(invocation_id)
                .filter(|f| f.invocation_epoch == invocation_epoch)
                .map(|ism| (&p.output_tx, ism))
        })
    }

    /// Removes invocation with invocation_id and invocation_epoch.
    /// When `invocation_epoch` is `None`, removes any epoch for that invocation.
    #[inline]
    pub(super) fn remove_invocation_with_epoch(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
        invocation_epoch: Option<InvocationEpoch>,
    ) -> Option<(&mpsc::Sender<Box<Effect>>, &IR, InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            if let Some(ism) = p.invocation_state_machines.get(invocation_id)
                && invocation_epoch
                    .is_none_or(|invocation_epoch| invocation_epoch == ism.invocation_epoch)
            {
                return Some((
                    &p.output_tx,
                    &p.storage_reader,
                    p.invocation_state_machines.remove(invocation_id).unwrap(),
                ));
            }
            None
        })
    }

    #[inline]
    pub(super) fn remove_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<HashMap<InvocationId, InvocationStateMachine>> {
        self.partitions
            .remove(&partition)
            .map(|p| p.invocation_state_machines)
    }

    #[inline]
    pub(super) fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: IR,
        sender: mpsc::Sender<Box<Effect>>,
    ) {
        self.partitions.insert(
            partition,
            PartitionInvocationStateMachineCoordinator {
                output_tx: sender,
                invocation_state_machines: Default::default(),
                partition_key_range,
                storage_reader,
            },
        );
    }

    #[inline]
    pub(super) fn register_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        id: InvocationId,
        ism: InvocationStateMachine,
    ) {
        self.resolve_partition(partition)
            .expect("Cannot register an invocation on an unknown partition")
            .invocation_state_machines
            .insert(id, ism);
    }

    #[inline]
    pub(super) fn registered_partitions(&self) -> Vec<PartitionLeaderEpoch> {
        self.partitions.keys().cloned().collect()
    }

    pub(super) fn registered_partitions_with_keys(
        &self,
        keys: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = PartitionLeaderEpoch> + '_ {
        self.partitions
            .iter()
            .filter_map(move |(partition_leader_epoch, coordinator)| {
                // check that there is some intersection
                if coordinator.partition_key_range.start() <= keys.end()
                    && keys.start() <= coordinator.partition_key_range.end()
                {
                    Some(*partition_leader_epoch)
                } else {
                    None
                }
            })
    }

    #[inline]
    fn resolve_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mut PartitionInvocationStateMachineCoordinator<IR>> {
        self.partitions.get_mut(&partition)
    }
}
