use crate::partition::effects::{Committable, OutboxMessage, StateStorage};
use crate::partition::state_machine::{JournalStatus, StateReader};
use crate::partition::InvocationStatus;
use common::types::{EntryIndex, PartitionId, ServiceId, ServiceInvocation, ServiceInvocationId};
use futures::stream;
use journal::raw::RawEntry;
use journal::{CompletionResult, JournalRevision};
use tokio_stream::Stream;

pub(super) struct PartitionStorage<Storage> {
    partition_id: PartitionId,
    storage: Storage,
}

impl<Storage> PartitionStorage<Storage> {
    pub(super) fn new(partition_id: PartitionId, storage: Storage) -> Self {
        Self {
            partition_id,
            storage,
        }
    }

    pub(super) fn create_transaction(&mut self) -> Transaction<'_, Storage> {
        Transaction::new(self)
    }

    pub(super) fn scan_invoked_invocations(&self) -> impl Stream<Item = ServiceInvocationId> {
        stream::empty()
    }
}

impl<Storage> StateReader for PartitionStorage<Storage> {
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

pub(super) struct Transaction<'a, Storage> {
    inner: &'a mut PartitionStorage<Storage>,
}

impl<'a, Storage> Transaction<'a, Storage> {
    pub(super) fn new(inner: &'a mut PartitionStorage<Storage>) -> Self {
        Self { inner }
    }

    pub(super) fn commit(self) {}
}

impl<'a, Storage> StateStorage for Transaction<'a, Storage> {
    fn write_invocation_status(&self, service_id: &ServiceId, status: &InvocationStatus) {
        todo!()
    }

    fn create_journal(&self, service_id: &ServiceId, method_name: impl AsRef<str>) {
        todo!()
    }

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) -> JournalRevision {
        todo!()
    }

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) {
        todo!()
    }

    fn enqueue_into_inbox(&self, seq_number: u64, service_invocation: ServiceInvocation) {
        todo!()
    }

    fn enqueue_into_outbox(&self, seq_number: u64, message: OutboxMessage) {
        todo!()
    }

    fn store_inbox_seq_number(&self, seq_number: u64) {
        todo!()
    }

    fn store_outbox_seq_number(&self, seq_number: u64) {
        todo!()
    }

    fn write_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        todo!()
    }

    fn clear_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>) {
        todo!()
    }

    fn store_timer(
        &self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) {
        todo!()
    }

    fn delete_timer(&self, service_id: &ServiceId, wake_up_time: u64, entry_index: EntryIndex) {
        todo!()
    }

    fn read_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<CompletionResult> {
        todo!()
    }

    fn read_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<RawEntry> {
        todo!()
    }

    fn truncate_outbox(&self, outbox_sequence_number: u64) {
        todo!()
    }

    fn truncate_inbox(&self, service_id: &ServiceId, inbox_sequence_number: u64) {
        todo!()
    }

    fn drop_journal(&self, service_id: &ServiceId) {
        todo!()
    }
}

impl<'a, Storage> Committable for Transaction<'a, Storage> {
    fn commit(self) {
        todo!()
    }
}
