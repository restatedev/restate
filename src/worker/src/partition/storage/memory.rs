use crate::partition::effects::{
    CommitError, Committable, OutboxMessage, StateStorage, StateStorageError,
};
use crate::partition::leadership::InvocationReader;
use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use crate::partition::state_machine::{
    InboxEntry, JournalStatus, ResponseSink, StateReader, StateReaderError,
};
use crate::partition::types::EnrichedRawEntry;
use crate::partition::InvocationStatus;
use bytes::Bytes;
use common::types::{
    EntryIndex, MessageIndex, ServiceId, ServiceInvocation, ServiceInvocationId,
    ServiceInvocationResponseSink,
};
use futures::future::{ok, BoxFuture};
use futures::{stream, FutureExt};
use journal::raw::Header;
use journal::CompletionResult;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct InMemoryPartitionStorage {
    inner: Arc<Mutex<Storage>>,
}

impl InMemoryPartitionStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Storage::new())),
        }
    }

    pub fn create_transaction(&mut self) -> Transaction<'_> {
        Transaction { inner: &self.inner }
    }
}

#[derive(Debug)]
enum EntryType {
    Entry(EnrichedRawEntry),
    CompletionResult(CompletionResult),
}

#[derive(Debug, Default)]
struct Journal {
    entries: Vec<EntryType>,
    #[allow(dead_code)]
    method_name: String,
    length: usize,
    response_sink: Option<ResponseSink>,
}

impl Journal {
    fn len(&self) -> usize {
        self.length
    }

    fn get_entry(&self, index: usize) -> Option<EnrichedRawEntry> {
        self.entries
            .get(index)
            .and_then(|entry| {
                if let EntryType::Entry(raw_entry) = entry {
                    Some(raw_entry)
                } else {
                    None
                }
            })
            .cloned()
    }

    fn store_entry(&mut self, index: usize, journal_entry: EnrichedRawEntry) {
        debug_assert!(index <= self.length);
        let entry_type = EntryType::Entry(journal_entry);

        if index == self.length {
            self.entries.push(entry_type);
            self.length += 1;
        } else {
            self.entries[index] = entry_type;
        }
    }

    fn store_completion_result(&mut self, index: usize, completion_result: CompletionResult) {
        debug_assert!(index >= self.length);
        self.entries
            .insert(index, EntryType::CompletionResult(completion_result));
    }

    fn get_completion_result(&self, index: usize) -> Option<CompletionResult> {
        self.entries
            .get(index)
            .and_then(|entry| {
                if let EntryType::CompletionResult(completion_result) = entry {
                    Some(completion_result)
                } else {
                    None
                }
            })
            .cloned()
    }

    fn new(method_name: String, response_sink: Option<ResponseSink>) -> Self {
        Self {
            method_name,
            entries: Vec::new(),
            length: 0,
            response_sink,
        }
    }
}

#[derive(Debug)]
struct Storage {
    invocation_status: HashMap<ServiceId, InvocationStatus>,
    inboxes: HashMap<ServiceId, VecDeque<InboxEntry>>,
    journals: HashMap<ServiceId, Journal>,
    outbox: VecDeque<(MessageIndex, OutboxMessage)>,
    state: HashMap<ServiceId, HashMap<Bytes, Bytes>>,
}

impl Storage {
    fn new() -> Self {
        Self {
            invocation_status: HashMap::new(),
            inboxes: HashMap::new(),
            journals: HashMap::new(),
            outbox: VecDeque::new(),
            state: HashMap::new(),
        }
    }

    fn get_invocation_status(&self, service_id: &ServiceId) -> InvocationStatus {
        self.invocation_status
            .get(service_id)
            .cloned()
            .unwrap_or(InvocationStatus::Free)
    }

    fn peek_inbox(&self, service_id: &ServiceId) -> Option<InboxEntry> {
        self.inboxes
            .get(service_id)
            .and_then(|inbox| inbox.front().cloned())
    }

    fn get_journal_status(&self, service_id: &ServiceId) -> JournalStatus {
        self.journals
            .get(service_id)
            .map(|journal| JournalStatus {
                length: journal.len() as EntryIndex,
            })
            .unwrap_or(JournalStatus::default())
    }

    fn is_entry_completed(&self, service_id: &ServiceId, entry_index: EntryIndex) -> bool {
        self.journals
            .get(service_id)
            .and_then(|journal| journal.get_entry(entry_index as usize))
            .and_then(|entry| entry.header.is_completed())
            .unwrap_or(false)
    }

    fn get_next_outbox_message(&self, next_sequence_number: u64) -> Option<(u64, OutboxMessage)> {
        self.outbox
            .iter()
            .find(|(sequence_number, _)| *sequence_number >= next_sequence_number)
            .cloned()
    }

    fn store_invocation_status(
        &mut self,
        service_id: &ServiceId,
        invocation_status: &InvocationStatus,
    ) {
        self.invocation_status
            .insert(service_id.clone(), invocation_status.clone());
    }

    fn create_journal(
        &mut self,
        service_invocation_id: &ServiceInvocationId,
        method_name: impl AsRef<str>,
        response_sink: &ServiceInvocationResponseSink,
    ) {
        self.journals.insert(
            service_invocation_id.service_id.clone(),
            Journal::new(
                method_name.as_ref().to_string(),
                ResponseSink::from_service_invocation_response_sink(
                    service_invocation_id,
                    response_sink,
                ),
            ),
        );
    }

    fn drop_journal(&mut self, service_id: &ServiceId) {
        self.journals.remove(service_id);
    }

    fn store_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        journal_entry: &EnrichedRawEntry,
    ) {
        let journal = self.journals.get_mut(service_id).unwrap();
        journal.store_entry(entry_index as usize, journal_entry.clone());
    }

    fn store_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) {
        let journal = self.journals.get_mut(service_id).unwrap();
        journal.store_completion_result(entry_index as usize, completion_result.clone());
    }

    fn load_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<CompletionResult> {
        let journal = self.journals.get(service_id).unwrap();
        journal.get_completion_result(entry_index as usize)
    }

    fn load_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<EnrichedRawEntry> {
        let journal = self.journals.get(service_id).unwrap();
        journal.get_entry(entry_index as usize)
    }

    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: &ServiceInvocation,
    ) {
        self.inboxes
            .get_mut(&service_invocation.id.service_id)
            .unwrap()
            .push_back((seq_number, service_invocation.clone()))
    }

    fn enqueue_into_outbox(&mut self, seq_number: MessageIndex, message: &OutboxMessage) {
        self.outbox.push_back((seq_number, message.clone()))
    }

    fn truncate_outbox(&mut self, seq_number_to_truncate: MessageIndex) {
        let partition_point = self
            .outbox
            .partition_point(|(seq_number, _)| *seq_number <= seq_number_to_truncate);

        drop(self.outbox.drain(..partition_point));
    }

    fn truncate_inbox(&mut self, service_id: &ServiceId, seq_number_to_truncate: MessageIndex) {
        let inbox = self.inboxes.get_mut(service_id).unwrap();

        let partition_point =
            inbox.partition_point(|(seq_number, _)| *seq_number <= seq_number_to_truncate);
        drop(inbox.drain(..=partition_point));
    }

    fn store_state(
        &mut self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        let state_entries = self
            .state
            .entry(service_id.clone())
            .or_insert(HashMap::new());

        state_entries.insert(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        );
    }

    fn load_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>) -> Option<Bytes> {
        self.state
            .get(service_id)
            .and_then(|state_entries| state_entries.get(key.as_ref()).cloned())
    }

    fn clear_state(&mut self, service_id: &ServiceId, key: impl AsRef<[u8]>) {
        if let Some(state_entries) = self.state.get_mut(service_id) {
            state_entries.remove(key.as_ref());

            if state_entries.is_empty() {
                self.state.remove(service_id);
            }
        }
    }

    fn get_response_sink(
        &self,
        service_invocation_id: &ServiceInvocationId,
    ) -> Option<ResponseSink> {
        self.journals
            .get(&service_invocation_id.service_id)
            .and_then(|journal| journal.response_sink.clone())
    }
}

impl StateReader for InMemoryPartitionStorage {
    fn get_invocation_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StateReaderError>> {
        ok(self.inner.lock().unwrap().get_invocation_status(service_id)).boxed()
    }

    fn peek_inbox(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, StateReaderError>> {
        ok(self.inner.lock().unwrap().peek_inbox(service_id)).boxed()
    }

    fn get_journal_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, StateReaderError>> {
        ok(self.inner.lock().unwrap().get_journal_status(service_id)).boxed()
    }

    fn is_entry_completed(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>> {
        ok(self
            .inner
            .lock()
            .unwrap()
            .is_entry_completed(service_id, entry_index))
        .boxed()
    }

    fn get_response_sink(
        &self,
        service_invocation_id: &ServiceInvocationId,
    ) -> BoxFuture<Result<Option<ResponseSink>, StateReaderError>> {
        ok(self
            .inner
            .lock()
            .unwrap()
            .get_response_sink(service_invocation_id))
        .boxed()
    }
}

pub struct Transaction<'a> {
    inner: &'a Arc<Mutex<Storage>>,
}

impl<'a> StateStorage for Transaction<'a> {
    fn store_invocation_status(
        &self,
        service_id: &ServiceId,
        status: &InvocationStatus,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .store_invocation_status(service_id, status);
        Ok(())
    }

    fn create_journal(
        &self,
        service_invocation_id: &ServiceInvocationId,
        method_name: impl AsRef<str>,
        response_sink: &ServiceInvocationResponseSink,
    ) -> Result<(), StateStorageError> {
        self.inner.lock().unwrap().create_journal(
            service_invocation_id,
            method_name,
            response_sink,
        );
        Ok(())
    }

    fn drop_journal(&self, service_id: &ServiceId) -> Result<(), StateStorageError> {
        self.inner.lock().unwrap().drop_journal(service_id);
        Ok(())
    }

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        journal_entry: &EnrichedRawEntry,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .store_journal_entry(service_id, entry_index, journal_entry);
        Ok(())
    }

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) -> Result<(), StateStorageError> {
        self.inner.lock().unwrap().store_completion_result(
            service_id,
            entry_index,
            completion_result,
        );
        Ok(())
    }

    fn load_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>> {
        ok(self
            .inner
            .lock()
            .unwrap()
            .load_completion_result(service_id, entry_index))
        .boxed()
    }

    fn load_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<EnrichedRawEntry>, StateStorageError>> {
        ok(self
            .inner
            .lock()
            .unwrap()
            .load_journal_entry(service_id, entry_index))
        .boxed()
    }

    fn enqueue_into_inbox(
        &self,
        seq_number: MessageIndex,
        service_invocation: &ServiceInvocation,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .enqueue_into_inbox(seq_number, service_invocation);
        Ok(())
    }

    fn enqueue_into_outbox(
        &self,
        seq_number: MessageIndex,
        message: &OutboxMessage,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .enqueue_into_outbox(seq_number, message);
        Ok(())
    }

    fn store_inbox_seq_number(&self, _seq_number: MessageIndex) -> Result<(), StateStorageError> {
        Ok(())
    }

    fn store_outbox_seq_number(&self, _seq_number: MessageIndex) -> Result<(), StateStorageError> {
        Ok(())
    }

    fn truncate_outbox(
        &self,
        outbox_sequence_number: MessageIndex,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .truncate_outbox(outbox_sequence_number);
        Ok(())
    }

    fn truncate_inbox(
        &self,
        service_id: &ServiceId,
        inbox_sequence_number: MessageIndex,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .truncate_inbox(service_id, inbox_sequence_number);
        Ok(())
    }

    fn store_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .store_state(service_id, key, value);
        Ok(())
    }

    fn load_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Option<Bytes>, StateStorageError>> {
        ok(self.inner.lock().unwrap().load_state(service_id, key)).boxed()
    }

    fn clear_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError> {
        self.inner.lock().unwrap().clear_state(service_id, key);
        Ok(())
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

impl<'a> Committable for Transaction<'a> {
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>> {
        ok(()).boxed()
    }
}

impl InvocationReader for InMemoryPartitionStorage {
    type InvokedInvocationStream = stream::Empty<ServiceInvocationId>;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream {
        stream::empty()
    }
}

impl OutboxReader for InMemoryPartitionStorage {
    fn get_next_message(
        &self,
        next_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>> {
        ok(self
            .inner
            .lock()
            .unwrap()
            .get_next_outbox_message(next_sequence_number))
        .boxed()
    }
}
