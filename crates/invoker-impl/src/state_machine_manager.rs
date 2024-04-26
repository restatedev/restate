// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
struct PartitionInvocationStateMachineCoordinator<SR> {
    output_tx: mpsc::Sender<Effect>,
    invocation_state_machines: HashMap<InvocationId, InvocationStateMachine>,
    partition_key_range: RangeInclusive<PartitionKey>,
    storage_reader: SR,
}

impl<SR> InvocationStateMachineManager<SR>
where
    SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
    <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
    <SR as StateReader>::StateIter: Send,
{
    #[inline]
    pub(super) fn has_partition(&self, partition: PartitionLeaderEpoch) -> bool {
        self.partitions.contains_key(&partition)
    }

    #[inline]
    pub(super) fn partition_storage_reader(&self, partition: PartitionLeaderEpoch) -> Option<&SR> {
        self.partitions.get(&partition).map(|p| &p.storage_reader)
    }

    #[inline]
    pub(super) fn resolve_partition_sender(
        &self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mpsc::Sender<Effect>> {
        self.partitions.get(&partition).map(|p| &p.output_tx)
    }

    #[inline]
    pub(super) fn resolve_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<Effect>, &mut InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .get_mut(invocation_id)
                .map(|ism| (&p.output_tx, ism))
        })
    }

    #[inline]
    pub(super) fn remove_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<Effect>, &SR, InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .remove(invocation_id)
                .map(|ism| (&p.output_tx, &p.storage_reader, ism))
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
        storage_reader: SR,
        sender: mpsc::Sender<Effect>,
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
    ) -> Option<&mut PartitionInvocationStateMachineCoordinator<SR>> {
        self.partitions.get_mut(&partition)
    }
}
