use crate::partition::state_machine::{JournalStatus, StateReader};
use crate::partition::InvocationStatus;
use common::types::{PartitionId, ServiceId, ServiceInvocation};

pub(super) struct PartitionStorage<'a, Storage> {
    partition_id: PartitionId,
    storage: &'a Storage,
}

impl<'a, Storage> PartitionStorage<'a, Storage> {
    pub(super) fn new(partition_id: PartitionId, storage: &'a Storage) -> Self {
        Self {
            partition_id,
            storage,
        }
    }
}

impl<'a, Storage> StateReader for PartitionStorage<'a, Storage> {
    type Error = ();

    fn get_invocation_status(
        &self,
        service_id: &ServiceId,
    ) -> Result<InvocationStatus, Self::Error> {
        todo!()
    }

    fn peek_inbox(
        &self,
        service_id: &ServiceId,
    ) -> Result<Option<(u64, ServiceInvocation)>, Self::Error> {
        todo!()
    }

    fn get_journal_status(&self, service_id: &ServiceId) -> Result<JournalStatus, Self::Error> {
        todo!()
    }
}
