use crate::partition::effects::{
    CommitError, Committable, OutboxMessage, StateStorage, StateStorageError,
};
use crate::partition::leadership::InvocationReader;
use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use crate::partition::state_machine::{JournalStatus, StateReader, StateReaderError};
use crate::partition::types::EnrichedRawEntry;
use crate::partition::InvocationStatus;
use bytes::Bytes;
use common::types::{
    EntryIndex, MessageIndex, PartitionId, ServiceId, ServiceInvocation, ServiceInvocationId,
    ServiceInvocationResponseSink,
};
use futures::future::BoxFuture;
use futures::{future, stream, FutureExt};
use journal::CompletionResult;

mod memory;

pub use memory::InMemoryPartitionStorage;

#[derive(Debug, Clone)]
pub(super) struct PartitionStorage<Storage> {
    _partition_id: PartitionId,
    _storage: Storage,
}

impl<Storage> PartitionStorage<Storage> {
    #[allow(dead_code)]
    pub(super) fn new(partition_id: PartitionId, storage: Storage) -> Self {
        Self {
            _partition_id: partition_id,
            _storage: storage,
        }
    }

    #[allow(dead_code)]
    pub(super) fn create_transaction(&mut self) -> Transaction<'_, Storage> {
        Transaction::new(self)
    }
}

impl<Storage> StateReader for PartitionStorage<Storage> {
    fn get_invocation_status(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StateReaderError>> {
        todo!()
    }

    fn peek_inbox(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<(MessageIndex, ServiceInvocation)>, StateReaderError>> {
        todo!()
    }

    fn get_journal_status(
        &self,
        _service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, StateReaderError>> {
        todo!()
    }

    fn is_entry_completed(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>> {
        todo!()
    }

    fn get_response_sink(
        &self,
        _service_invocation_id: &ServiceInvocationId,
    ) -> BoxFuture<Result<Option<crate::partition::state_machine::ResponseSink>, StateReaderError>>
    {
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
    #[allow(dead_code)]
    pub(super) fn new(inner: &'a mut PartitionStorage<Storage>) -> Self {
        Self { _inner: inner }
    }

    pub(super) fn commit(self) {}
}

impl<'a, Storage> StateStorage for Transaction<'a, Storage> {
    fn store_invocation_status(
        &self,
        _service_id: &ServiceId,
        _status: &InvocationStatus,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn create_journal(
        &self,
        _service_invocation_id: &ServiceInvocationId,
        _method_name: impl AsRef<str>,
        _response_sink: &ServiceInvocationResponseSink,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn drop_journal(&self, _service_id: &ServiceId) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_journal_entry(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
        _journal_entry: &EnrichedRawEntry,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_completion_result(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
        _completion_result: &CompletionResult,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn load_completion_result(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>> {
        todo!()
    }

    fn load_journal_entry(
        &self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<EnrichedRawEntry>, StateStorageError>> {
        todo!()
    }

    fn enqueue_into_inbox(
        &self,
        _seq_number: MessageIndex,
        _service_invocation: &ServiceInvocation,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn enqueue_into_outbox(
        &self,
        _seq_number: MessageIndex,
        _message: &OutboxMessage,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_inbox_seq_number(&self, _seq_number: MessageIndex) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_outbox_seq_number(&self, _seq_number: MessageIndex) -> Result<(), StateStorageError> {
        todo!()
    }

    fn truncate_outbox(
        &self,
        _outbox_sequence_number: MessageIndex,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn truncate_inbox(
        &self,
        _service_id: &ServiceId,
        _inbox_sequence_number: MessageIndex,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
        _value: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn load_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Option<Bytes>, StateStorageError>> {
        todo!()
    }

    fn clear_state(
        &self,
        _service_id: &ServiceId,
        _key: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn store_timer(
        &self,
        _service_invocation_id: &ServiceInvocationId,
        _wake_up_time: u64,
        _entry_index: EntryIndex,
    ) -> Result<(), StateStorageError> {
        todo!()
    }

    fn delete_timer(
        &self,
        _service_id: &ServiceId,
        _wake_up_time: u64,
        _entry_index: EntryIndex,
    ) -> Result<(), StateStorageError> {
        todo!()
    }
}

impl<Storage> OutboxReader for PartitionStorage<Storage> {
    fn get_next_message(
        &self,
        _next_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>> {
        future::ok(None).boxed()
    }
}

impl<'a, Storage> Committable for Transaction<'a, Storage> {
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>> {
        self.commit();
        future::ready(Ok(())).boxed()
    }
}
