use crate::partition::{InvocationStatus, OutboxMessage};
use crate::{InboxEntry, JournalStatus};
use bytes::Bytes;
use common::types::{EntryIndex, ServiceId, ServiceInvocation, ServiceInvocationId};
use common::utils::GenericError;
use futures::future::BoxFuture;
use futures::Stream;
use journal::raw::RawEntry;
use journal::CompletionResult;

#[derive(Debug, thiserror::Error)]
#[error("failed committing results: {source:?}")]
pub(crate) struct CommitError {
    source: Option<GenericError>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateStorageError {
    #[error("write failed: {source:?}")]
    #[allow(dead_code)]
    WriteFailed { source: Option<GenericError> },
    #[error("read failed: {source:?}")]
    #[allow(dead_code)]
    ReadFailed { source: Option<GenericError> },
}

#[derive(Debug, thiserror::Error)]
#[error("failed reading state: {source:?}")]
pub(crate) struct StateReaderError {
    source: Option<GenericError>,
}

pub(crate) trait Committable {
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>>;
}

pub(crate) trait StateReader {
    fn get_invocation_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StateReaderError>>;

    fn peek_inbox(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, StateReaderError>>;

    fn get_journal_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, StateReaderError>>;

    fn is_entry_completed(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>>;
}

pub(crate) trait StateStorage {
    // Invocation status
    fn store_invocation_status(
        &self,
        service_id: &ServiceId,
        status: &InvocationStatus,
    ) -> Result<(), StateStorageError>;

    // Journal operations
    fn create_journal(
        &self,
        service_id: &ServiceId,
        method_name: impl AsRef<str>,
    ) -> Result<(), StateStorageError>;

    fn drop_journal(&self, service_id: &ServiceId) -> Result<(), StateStorageError>;

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        raw_entry: &RawEntry,
    ) -> Result<(), StateStorageError>;

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) -> Result<(), StateStorageError>;

    fn load_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>>;

    fn load_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<RawEntry>, StateStorageError>>;

    // In-/outbox
    fn enqueue_into_inbox(
        &self,
        seq_number: u64,
        service_invocation: &ServiceInvocation,
    ) -> Result<(), StateStorageError>;

    fn enqueue_into_outbox(
        &self,
        seq_number: u64,
        message: &OutboxMessage,
    ) -> Result<(), StateStorageError>;

    fn store_inbox_seq_number(&self, seq_number: u64) -> Result<(), StateStorageError>;

    fn store_outbox_seq_number(&self, seq_number: u64) -> Result<(), StateStorageError>;

    fn truncate_outbox(&self, outbox_sequence_number: u64) -> Result<(), StateStorageError>;

    fn truncate_inbox(
        &self,
        service_id: &ServiceId,
        inbox_sequence_number: u64,
    ) -> Result<(), StateStorageError>;

    fn store_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError>;

    fn load_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Bytes, StateStorageError>>;

    fn clear_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError>;

    fn store_timer(
        &self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError>;

    fn delete_timer(
        &self,
        service_id: &ServiceId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError>;
}

pub(super) trait InvocationReader {
    type InvokedInvocationStream: Stream<Item = ServiceInvocationId> + Unpin;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream;
}
