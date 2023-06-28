use crate::service::invocation_state_machine::InvocationStateMachine;
use crate::service::*;

/// Tree of [InvocationStateMachine] held by the [Service].
#[derive(Debug, Default)]
pub(in crate::service) struct InvocationStateMachineTree {
    partitions: HashMap<PartitionLeaderEpoch, PartitionInvocationStateMachineCoordinator>,
}

#[derive(Debug)]
struct PartitionInvocationStateMachineCoordinator {
    output_tx: mpsc::Sender<Effect>,
    invocation_state_machines: HashMap<ServiceInvocationId, InvocationStateMachine>,
}

impl InvocationStateMachineTree {
    #[inline]
    pub(in crate::service) fn has_partition(&self, partition: PartitionLeaderEpoch) -> bool {
        self.partitions.contains_key(&partition)
    }

    #[inline]
    pub(in crate::service) fn resolve_partition_sender(
        &self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mpsc::Sender<Effect>> {
        self.partitions.get(&partition).map(|p| &p.output_tx)
    }

    #[inline]
    pub(in crate::service) fn resolve_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: &ServiceInvocationId,
    ) -> Option<(&mpsc::Sender<Effect>, &mut InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .get_mut(service_invocation_id)
                .map(|ism| (&p.output_tx, ism))
        })
    }

    #[inline]
    pub(in crate::service) fn remove_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: &ServiceInvocationId,
    ) -> Option<(&mpsc::Sender<Effect>, InvocationStateMachine)> {
        self.resolve_partition(partition).and_then(|p| {
            p.invocation_state_machines
                .remove(service_invocation_id)
                .map(|ism| (&p.output_tx, ism))
        })
    }

    #[inline]
    pub(in crate::service) fn remove_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<HashMap<ServiceInvocationId, InvocationStateMachine>> {
        self.partitions
            .remove(&partition)
            .map(|p| p.invocation_state_machines)
    }

    #[inline]
    pub(in crate::service) fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) {
        self.partitions.insert(
            partition,
            PartitionInvocationStateMachineCoordinator {
                output_tx: sender,
                invocation_state_machines: Default::default(),
            },
        );
    }

    #[inline]
    pub(in crate::service) fn register_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        sid: ServiceInvocationId,
        ism: InvocationStateMachine,
    ) {
        self.resolve_partition(partition)
            .expect("Cannot register an invocation on an unknown partition")
            .invocation_state_machines
            .insert(sid, ism);
    }

    #[inline]
    pub(in crate::service) fn registered_partitions(&self) -> Vec<PartitionLeaderEpoch> {
        self.partitions.keys().cloned().collect()
    }

    #[inline]
    fn resolve_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mut PartitionInvocationStateMachineCoordinator> {
        self.partitions.get_mut(&partition)
    }
}
