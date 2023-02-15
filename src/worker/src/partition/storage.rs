use crate::partition::effects::{Committable, OutboxMessage, StateStorage};
use crate::partition::leadership::InvocationReader;
use crate::partition::state_machine::{JournalStatus, StateReader};
use crate::partition::InvocationStatus;
use bytes::Bytes;
use common::types::{EntryIndex, PartitionId, ServiceId, ServiceInvocation, ServiceInvocationId};
use futures::future::BoxFuture;
use futures::{future, stream, FutureExt};
use journal::raw::RawEntry;
use journal::{CompletionResult, JournalRevision};

pub(super) struct PartitionStorage<Storage> {
    _partition_id: PartitionId,
    _storage: Storage,
}

impl<Storage> PartitionStorage<Storage> {
    pub(super) fn new(partition_id: PartitionId, storage: Storage) -> Self {
        Self {
            _partition_id: partition_id,
            _storage: storage,
        }
    }

    pub(super) fn create_transaction(&mut self) -> Transaction<'_, Storage> {
        Transaction::new(self)
    }
}

impl<Storage> StateReader for PartitionStorage<Storage> {
    type Error = ();

    fn get_invocation_status(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, Self::Error>> {
        todo!()
    }

    fn peek_inbox(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<(u64, ServiceInvocation)>, Self::Error>> {
        todo!()
    }

    fn get_journal_status(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, Self::Error>> {
        todo!()
    }
}

impl<Storage> InvocationReader for PartitionStorage<Storage> {
    type InvokedInvocationStream = stream::Empty<ServiceInvocationId>;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream {
        stream::empty()
    }
}

pub(super) struct Transaction<'a, Storage> {
    _inner: &'a mut PartitionStorage<Storage>,
}

impl<'a, Storage> Transaction<'a, Storage> {
    pub(super) fn new(inner: &'a mut PartitionStorage<Storage>) -> Self {
        Self { _inner: inner }
    }

    pub(super) fn commit(self) {}
}

impl<'a, Storage> StateStorage for Transaction<'a, Storage> {
    type Error = ();

    fn store_invocation_status(
        &self,
        _service_id: &ServiceId,
        _status: &InvocationStatus,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn create_journal(
        &self,
        _service_id: &ServiceId,
        _method_name: impl AsRef<str>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn drop_journal(&self, _service_id: &ServiceId) -> Result<(), Self::Error> {
        todo!()
    }

    fn store_journal_entry(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
        _raw_entry: &RawEntry,
    ) -> Result<JournalRevision, Self::Error> {
        todo!()
    }

    fn store_completion_result(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
        _completion_result: &CompletionResult,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn load_completion_result(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, Self::Error>> {
        todo!()
    }

    fn load_journal_entry(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<RawEntry>, Self::Error>> {
        todo!()
    }

    fn enqueue_into_inbox(
        &self,
        _seq_number: u64,
        _service_invocation: &ServiceInvocation,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn enqueue_into_outbox(
        &self,
        _seq_number: u64,
        _message: &OutboxMessage,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn store_inbox_seq_number(&self, _seq_number: u64) -> Result<(), Self::Error> {
        todo!()
    }

    fn store_outbox_seq_number(&self, _seq_number: u64) -> Result<(), Self::Error> {
        todo!()
    }

    fn truncate_outbox(&self, _outbox_sequence_number: u64) -> Result<(), Self::Error> {
        todo!()
    }

    fn truncate_inbox(
        &self,
        _service_id: &ServiceId,
        _inbox_sequence_number: u64,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn store_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
        _value: impl AsRef<[u8]>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn load_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Bytes, Self::Error>> {
        todo!()
    }

    fn clear_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn store_timer(
        &self,
        _service_invocation_id: &ServiceInvocationId,
        _wake_up_time: u64,
        _entry_index: EntryIndex,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn delete_timer(
        &self,
        _service_id: &ServiceId,
        _wake_up_time: u64,
        _entry_index: EntryIndex,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<'a, Storage> Committable for Transaction<'a, Storage> {
    type Error = ();

    fn commit(self) -> BoxFuture<'static, Result<(), Self::Error>> {
        self.commit();
        future::ready(Ok(())).boxed()
    }
}
