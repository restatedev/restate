use crate::partition::effects::{CommitError, Committable, StateStorage, StateStorageError};
use crate::partition::leadership::InvocationReader;
use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use crate::partition::state_machine::{StateReader, StateReaderError};
use crate::partition::storage::memory::timer_key::{TimerKey, TimerKeyRef};
use crate::partition::types::TimerValue;
use assert2::let_assert;
use bytes::Bytes;
use common::types::{
    CompletionResult, EnrichedRawEntry, EntryIndex, InboxEntry, InvocationId, InvocationStatus,
    InvokedStatus, JournalEntry, JournalMetadata, MessageIndex, MillisSinceEpoch, OutboxMessage,
    ServiceId, ServiceInvocation, ServiceInvocationId,
};
use futures::future::{err, ok, BoxFuture};
use futures::{stream, FutureExt};
use invoker::JournalReader;
use journal::raw::{Header, PlainRawEntry};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;
use timer::TimerReader;

mod timer_key;

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
struct Journal {
    entries: Vec<JournalEntry>,
    length: usize,
}

impl Journal {
    fn get_entry(&self, index: usize) -> Option<EnrichedRawEntry> {
        self.entries
            .get(index)
            .and_then(|entry| {
                if let JournalEntry::Entry(raw_entry) = entry {
                    Some(raw_entry)
                } else {
                    None
                }
            })
            .cloned()
    }

    fn store_entry(&mut self, index: usize, journal_entry: EnrichedRawEntry) {
        debug_assert!(index <= self.length);
        let entry_type = JournalEntry::Entry(journal_entry);

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
            .insert(index, JournalEntry::Completion(completion_result));
    }

    fn get_completion_result(&self, index: usize) -> Option<CompletionResult> {
        self.entries
            .get(index)
            .and_then(|entry| {
                if let JournalEntry::Completion(completion_result) = entry {
                    Some(completion_result)
                } else {
                    None
                }
            })
            .cloned()
    }

    fn new() -> Self {
        Self {
            entries: Vec::new(),
            length: 0,
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
    timers: BTreeMap<TimerKey, InvocationId>,
}

impl Storage {
    fn new() -> Self {
        Self {
            invocation_status: HashMap::new(),
            inboxes: HashMap::new(),
            journals: HashMap::new(),
            outbox: VecDeque::new(),
            state: HashMap::new(),
            timers: BTreeMap::new(),
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

    fn drop_journal(&mut self, service_id: &ServiceId) {
        self.journals.remove(service_id);
    }

    fn store_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        journal_entry: &EnrichedRawEntry,
    ) {
        let_assert!(
            InvocationStatus::Invoked(ref mut invoked) = self
                .invocation_status
                .get_mut(service_id)
                .expect("service must be invoked"),
            "invocation must be invoked",
        );
        let journal = self
            .journals
            .entry(service_id.clone())
            .or_insert(Journal::new());
        journal.store_entry(entry_index as usize, journal_entry.clone());

        invoked.journal_metadata.length =
            EntryIndex::try_from(journal.length).expect("journal length should fit in EntryIndex");
    }

    fn store_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) {
        let journal = self
            .journals
            .entry(service_id.clone())
            .or_insert(Journal::new());
        journal.store_completion_result(entry_index as usize, completion_result.clone());
    }

    fn load_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<CompletionResult> {
        let journal = self
            .journals
            .entry(service_id.clone())
            .or_insert(Journal::new());
        journal.get_completion_result(entry_index as usize)
    }

    fn load_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<EnrichedRawEntry> {
        let journal = self
            .journals
            .entry(service_id.clone())
            .or_insert(Journal::new());
        journal.get_entry(entry_index as usize)
    }

    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: &ServiceInvocation,
    ) {
        self.inboxes
            .entry(service_invocation.id.service_id.clone())
            .or_insert(VecDeque::new())
            .push_back(InboxEntry::new(seq_number, service_invocation.clone()))
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
        if let Some(inbox) = self.inboxes.get_mut(service_id) {
            let partition_point = inbox.partition_point(
                |InboxEntry {
                     inbox_sequence_number,
                     ..
                 }| *inbox_sequence_number <= seq_number_to_truncate,
            );
            drop(inbox.drain(..partition_point));

            if inbox.is_empty() {
                self.inboxes.remove(service_id);
            }
        }
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

    fn store_timer(
        &mut self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) {
        self.timers.insert(
            TimerKey::new(
                wake_up_time,
                service_invocation_id.service_id.clone(),
                entry_index,
            ),
            service_invocation_id.invocation_id,
        );
    }

    fn delete_timer(
        &mut self,
        service_id: &ServiceId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) {
        self.timers
            .remove(&(service_id, wake_up_time, entry_index) as &dyn TimerKeyRef);
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
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .store_timer(service_invocation_id, wake_up_time, entry_index);
        Ok(())
    }

    fn delete_timer(
        &self,
        service_id: &ServiceId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError> {
        self.inner
            .lock()
            .unwrap()
            .delete_timer(service_id, wake_up_time, entry_index);
        Ok(())
    }
}

impl<'a> Committable for Transaction<'a> {
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>> {
        ok(()).boxed()
    }
}

impl InvocationReader for InMemoryPartitionStorage {
    type InvokedInvocationStream = stream::Iter<IntoIter<ServiceInvocationId>>;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream {
        let invoked_invocations: Vec<ServiceInvocationId> = self
            .inner
            .lock()
            .unwrap()
            .invocation_status
            .iter()
            .filter_map(|(service_id, status)| match status {
                InvocationStatus::Invoked(invoked_status) => Some(ServiceInvocationId {
                    service_id: service_id.clone(),
                    invocation_id: invoked_status.invocation_id,
                }),
                _ => None,
            })
            .collect();

        stream::iter(invoked_invocations)
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

impl TimerReader<TimerValue> for InMemoryPartitionStorage {
    type TimerStream = stream::Iter<IntoIter<TimerValue>>;

    fn scan_timers(
        &self,
        num_timers: usize,
        previous_timer_key: Option<TimerValue>,
    ) -> Self::TimerStream {
        let next_timer_key = previous_timer_key
            .map(|timer_value| {
                TimerKey::new(
                    timer_value.wake_up_time,
                    timer_value.service_invocation_id.service_id,
                    timer_value.entry_index + 1,
                )
            })
            .unwrap_or(TimerKey::from_wake_up_time(MillisSinceEpoch::UNIX_EPOCH));

        let timers: Vec<TimerValue> = self
            .inner
            .lock()
            .unwrap()
            .timers
            .range(next_timer_key..)
            .map(|(timer_key, invocation_id)| {
                let (wake_up_time, service_id, entry_index) = timer_key.clone().into_inner();
                TimerValue::new(
                    ServiceInvocationId {
                        service_id: service_id.expect("Must be known."),
                        invocation_id: *invocation_id,
                    },
                    entry_index.expect("Must be known."),
                    wake_up_time,
                )
            })
            .take(num_timers)
            .collect();

        stream::iter(timers)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryJournalReader {
    storages: Arc<Mutex<Vec<InMemoryPartitionStorage>>>,
}

impl InMemoryJournalReader {
    pub fn new() -> Self {
        Self {
            storages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn register(&self, storage: InMemoryPartitionStorage) {
        self.storages.lock().unwrap().push(storage);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("could not find journal {0}")]
pub struct InMemoryJournalReaderError(ServiceInvocationId);

impl JournalReader for InMemoryJournalReader {
    type JournalStream = stream::Iter<IntoIter<PlainRawEntry>>;
    type Error = InMemoryJournalReaderError;
    type Future = BoxFuture<'static, Result<(JournalMetadata, Self::JournalStream), Self::Error>>;

    fn read_journal(&self, sid: &ServiceInvocationId) -> Self::Future {
        let storages = self.storages.lock().unwrap();

        for storage in storages.iter() {
            let storage = storage.inner.lock().unwrap();

            let meta = if let Some(InvocationStatus::Invoked(InvokedStatus {
                journal_metadata,
                ..
            })) = storage.invocation_status.get(&sid.service_id)
            {
                journal_metadata.clone()
            } else {
                continue;
            };

            if let Some(journal) = storage.journals.get(&sid.service_id) {
                let journal: Vec<PlainRawEntry> = journal.entries[0..journal.length]
                    .iter()
                    .map(|entry| match entry {
                        JournalEntry::Entry(EnrichedRawEntry { header, entry }) => {
                            PlainRawEntry::new(header.clone().into(), entry.clone())
                        }
                        JournalEntry::Completion(_) => panic!("Should not happen."),
                    })
                    .collect();

                return ok((meta, stream::iter(journal))).boxed();
            }
        }

        err(InMemoryJournalReaderError(sid.clone())).boxed()
    }
}
