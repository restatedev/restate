// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::{PARTITION_STORAGE_TX_COMMITTED, PARTITION_STORAGE_TX_CREATED};
use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, StreamExt, TryStreamExt};
use metrics::counter;
use restate_storage_api::deduplication_table::{
    DedupSequenceNumber, ProducerId, ReadOnlyDeduplicationTable,
};
use restate_storage_api::fsm_table::{ReadOnlyFsmTable, SequenceNumber};
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::promise_table::{OwnedPromiseRow, Promise};
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus,
};
use restate_storage_api::state_table::ReadOnlyStateTable;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerTable};
use restate_storage_api::Result as StorageResult;
use restate_storage_api::StorageError;
use restate_timer::TimerReader;
use restate_types::identifiers::{
    EntryIndex, IdempotencyId, InvocationId, PartitionId, PartitionKey, ServiceId, WithPartitionKey,
};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::CompletionResult;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_wal_protocol::timer::TimerKeyValue;
use std::future::Future;
use std::ops::RangeInclusive;

pub mod invoker;

// todo(asoli): merge into PartitionStore
#[derive(Debug, Clone)]
pub(crate) struct PartitionStorage<Storage> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    storage: Storage,
}

impl<Storage> PartitionStorage<Storage> {
    pub(super) fn new(
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage: Storage,
    ) -> Self {
        Self {
            partition_id,
            partition_key_range,
            storage,
        }
    }
}

impl<Storage> PartitionStorage<Storage>
where
    Storage: restate_storage_api::Storage,
{
    pub(super) fn create_transaction(&mut self) -> Transaction<Storage::TransactionType<'_>> {
        counter!(PARTITION_STORAGE_TX_CREATED).increment(1);
        Transaction::new(
            self.partition_id,
            self.partition_key_range.clone(),
            self.storage.transaction(),
        )
    }
}

async fn load_seq_number<F: ReadOnlyFsmTable + Send>(
    storage: &mut F,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<MessageIndex, StorageError> {
    let seq_number = storage
        .get::<SequenceNumber>(partition_id, state_id)
        .await?;
    Ok(seq_number
        .map(Into::into)
        .unwrap_or(MessageIndex::default()))
}

impl<Storage> PartitionStorage<Storage>
where
    Storage: ReadOnlyFsmTable
        + ReadOnlyInvocationStatusTable
        + ReadOnlyVirtualObjectStatusTable
        + ReadOnlyJournalTable
        + ReadOnlyStateTable
        + Clone
        + Send,
{
    pub fn clone_storage(&self) -> Storage {
        self.storage.clone()
    }

    pub fn load_inbox_seq_number(
        &mut self,
    ) -> impl Future<Output = Result<MessageIndex, StorageError>> + Send + '_ {
        load_seq_number(
            &mut self.storage,
            self.partition_id,
            fsm_variable::INBOX_SEQ_NUMBER,
        )
    }

    pub fn load_outbox_seq_number(
        &mut self,
    ) -> impl Future<Output = Result<MessageIndex, StorageError>> + Send + '_ {
        load_seq_number(
            &mut self.storage,
            self.partition_id,
            fsm_variable::OUTBOX_SEQ_NUMBER,
        )
    }

    pub async fn load_applied_lsn(&mut self) -> StorageResult<Option<Lsn>> {
        let seq_number = self
            .storage
            .get::<SequenceNumber>(self.partition_id, fsm_variable::APPLIED_LSN)
            .await?;

        Ok(seq_number.map(|seq_number| Lsn::from(u64::from(seq_number))))
    }

    pub fn scan_invoked_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget), StorageError>> + Send + '_
    {
        self.storage
            .invoked_invocations(self.partition_key_range.clone())
    }
}

#[inline]
fn assert_partition_key(
    partition_key_range: &RangeInclusive<PartitionKey>,
    partition_key: &impl WithPartitionKey,
) {
    let partition_key = partition_key.partition_key();
    assert!(partition_key_range.contains(&partition_key),
            "Partition key '{}' is not part of PartitionStorage's partition '{:?}'. This indicates a bug.",
            partition_key,
            partition_key_range);
}

pub struct Transaction<TransactionType> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    inner: TransactionType,
}

impl<TransactionType> Transaction<TransactionType> {
    #[inline]
    fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) {
        assert_partition_key(&self.partition_key_range, partition_key);
    }
}

impl<TransactionType> Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction,
{
    pub(super) fn new(
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        inner: TransactionType,
    ) -> Self {
        Self {
            partition_id,
            partition_key_range,
            inner,
        }
    }

    pub(super) async fn commit(self) -> Result<(), StorageError> {
        let res = self.inner.commit().await;
        counter!(PARTITION_STORAGE_TX_COMMITTED).increment(1);
        res
    }

    async fn store_seq_number(
        &mut self,
        seq_number: MessageIndex,
        state_id: u64,
    ) -> Result<(), StorageError> {
        self.inner
            .put(
                self.partition_id,
                state_id,
                SequenceNumber::from(seq_number),
            )
            .await;

        Ok(())
    }

    pub async fn store_dedup_sequence_number(
        &mut self,
        producer_id: ProducerId,
        dedup_seq_number: DedupSequenceNumber,
    ) {
        self.inner
            .put_dedup_seq_number(self.partition_id, producer_id, dedup_seq_number)
            .await
    }

    pub async fn store_applied_lsn(&mut self, lsn: Lsn) -> StorageResult<()> {
        self.inner
            .put(
                self.partition_id,
                fsm_variable::APPLIED_LSN,
                SequenceNumber::from(u64::from(lsn)),
            )
            .await;

        Ok(())
    }
}

// Avoid adding methods here, but rather use directly the storage_api traits!!!
// See https://github.com/restatedev/restate/issues/276
impl<TransactionType> super::state_machine::StateReader for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> StorageResult<VirtualObjectStatus> {
        self.assert_partition_key(service_id);
        self.inner.get_virtual_object_status(service_id).await
    }

    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> StorageResult<InvocationStatus> {
        self.assert_partition_key(invocation_id);
        self.inner.get_invocation_status(invocation_id).await
    }

    // Returns true if the entry is a completable journal entry and is completed,
    // or if the entry is a non-completable journal entry. In the latter case,
    // the entry must be a Custom entry with requires_ack flag.
    async fn is_entry_resumable(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> StorageResult<bool> {
        self.assert_partition_key(invocation_id);
        Ok(self
            .inner
            .get_journal_entry(invocation_id, entry_index)
            .await?
            .map(|journal_entry| match journal_entry {
                JournalEntry::Entry(entry) => entry.header().is_completed().unwrap_or(true),
                JournalEntry::Completion(_) => false,
            })
            .unwrap_or(false))
    }

    async fn load_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> StorageResult<Option<Bytes>> {
        super::state_machine::StateStorage::load_state(self, service_id, key).await
    }

    async fn load_state_keys(&mut self, service_id: &ServiceId) -> StorageResult<Vec<Bytes>> {
        super::state_machine::StateStorage::get_all_user_states(self, service_id)
            .map(|res| res.map(|v| v.0))
            .try_collect()
            .await
    }

    async fn load_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        super::state_machine::StateStorage::load_completion_result(self, invocation_id, entry_index)
            .await
    }

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        length: EntryIndex,
    ) -> impl Stream<Item = StorageResult<(EntryIndex, JournalEntry)>> + Send {
        self.inner.get_journal(invocation_id, length)
    }
}

// Avoid adding methods here, but rather use directly the storage_api traits!!!
// See https://github.com/restatedev/restate/issues/276
impl<TransactionType> super::state_machine::StateStorage for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    async fn store_service_status(
        &mut self,
        service_id: &ServiceId,
        status: VirtualObjectStatus,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner
            .put_virtual_object_status(service_id, status)
            .await;
        Ok(())
    }

    async fn store_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
    ) -> StorageResult<()> {
        self.assert_partition_key(invocation_id);
        self.inner
            .put_invocation_status(invocation_id, status)
            .await;
        Ok(())
    }

    async fn drop_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> StorageResult<()> {
        self.assert_partition_key(invocation_id);
        self.inner
            .delete_journal(invocation_id, journal_length)
            .await;
        Ok(())
    }

    async fn store_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> StorageResult<()> {
        self.assert_partition_key(invocation_id);
        self.inner
            .put_journal_entry(
                invocation_id,
                entry_index,
                JournalEntry::Entry(journal_entry),
            )
            .await;

        Ok(())
    }

    async fn store_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> StorageResult<()> {
        self.assert_partition_key(invocation_id);
        self.inner
            .put_journal_entry(
                invocation_id,
                entry_index,
                JournalEntry::Completion(completion_result),
            )
            .await;
        Ok(())
    }

    async fn load_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        self.assert_partition_key(invocation_id);
        let result = self
            .inner
            .get_journal_entry(invocation_id, entry_index)
            .await?;

        Ok(result.and_then(|journal_entry| match journal_entry {
            JournalEntry::Entry(_) => None,
            JournalEntry::Completion(completion_result) => Some(completion_result),
        }))
    }

    async fn load_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<EnrichedRawEntry>> {
        self.assert_partition_key(invocation_id);
        let result = self
            .inner
            .get_journal_entry(invocation_id, entry_index)
            .await?;

        Ok(result.and_then(|journal_entry| match journal_entry {
            JournalEntry::Entry(entry) => Some(entry),
            JournalEntry::Completion(_) => None,
        }))
    }

    async fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        inbox_entry: InboxEntry,
    ) -> StorageResult<()> {
        self.assert_partition_key(inbox_entry.service_id());

        // TODO: Avoid cloning when moving this logic into the RocksDB storage impl
        let service_id = inbox_entry.service_id().clone();

        self.inner
            .put_inbox_entry(
                &service_id,
                SequenceNumberInboxEntry::new(seq_number, inbox_entry),
            )
            .await;

        Ok(())
    }

    async fn enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> StorageResult<()> {
        self.inner
            .add_message(self.partition_id, seq_number, message)
            .await;

        Ok(())
    }

    async fn store_inbox_seq_number(&mut self, seq_number: MessageIndex) -> StorageResult<()> {
        self.store_seq_number(seq_number, fsm_variable::INBOX_SEQ_NUMBER)
            .await
    }

    async fn store_outbox_seq_number(&mut self, seq_number: MessageIndex) -> StorageResult<()> {
        self.store_seq_number(seq_number, fsm_variable::OUTBOX_SEQ_NUMBER)
            .await
    }

    async fn truncate_outbox(&mut self, outbox_sequence_number: MessageIndex) -> StorageResult<()> {
        self.inner
            .truncate_outbox(
                self.partition_id,
                outbox_sequence_number..outbox_sequence_number + 1,
            )
            .await;

        Ok(())
    }

    async fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> StorageResult<Option<SequenceNumberInboxEntry>> {
        self.inner.pop_inbox(service_id).await
    }

    async fn delete_inbox_entry(&mut self, service_id: &ServiceId, sequence_number: MessageIndex) {
        self.inner
            .delete_inbox_entry(service_id, sequence_number)
            .await;
    }

    fn get_all_user_states(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = StorageResult<(Bytes, Bytes)>> + Send {
        self.inner.get_all_user_states(service_id)
    }

    async fn store_state(
        &mut self,
        service_id: &ServiceId,
        key: Bytes,
        value: Bytes,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner.put_user_state(service_id, &key, &value).await;

        Ok(())
    }

    async fn load_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> StorageResult<Option<Bytes>> {
        self.assert_partition_key(service_id);
        self.inner.get_user_state(service_id, key).await
    }

    async fn clear_state(&mut self, service_id: &ServiceId, key: &Bytes) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner.delete_user_state(service_id, key).await;
        Ok(())
    }

    async fn clear_all_state(&mut self, service_id: &ServiceId) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner.delete_all_user_state(service_id).await?;
        Ok(())
    }

    async fn store_timer(&mut self, timer_key: TimerKey, timer: Timer) -> StorageResult<()> {
        self.inner
            .add_timer(self.partition_id, &timer_key, timer)
            .await;
        Ok(())
    }

    async fn delete_timer(&mut self, timer_key: &TimerKey) -> StorageResult<()> {
        self.inner.delete_timer(self.partition_id, timer_key).await;
        Ok(())
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> ReadOnlyJournalTable for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = StorageResult<Option<JournalEntry>>> + Send {
        self.inner.get_journal_entry(invocation_id, journal_index)
    }

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = StorageResult<(EntryIndex, JournalEntry)>> + Send {
        self.inner.get_journal(invocation_id, journal_length)
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> ReadOnlyInvocationStatusTable for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = StorageResult<InvocationStatus>> + Send {
        self.inner.get_invocation_status(invocation_id)
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = StorageResult<(InvocationId, InvocationTarget)>> + Send {
        self.inner.invoked_invocations(partition_key_range)
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> restate_storage_api::idempotency_table::ReadOnlyIdempotencyTable
    for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = StorageResult<Option<IdempotencyMetadata>>> + Send {
        self.inner.get_idempotency_metadata(idempotency_id)
    }

    fn all_idempotency_metadata(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = StorageResult<(IdempotencyId, IdempotencyMetadata)>> + Send {
        self.inner.all_idempotency_metadata(range)
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> restate_storage_api::idempotency_table::IdempotencyTable
    for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: IdempotencyMetadata,
    ) -> impl Future<Output = ()> + Send {
        self.inner
            .put_idempotency_metadata(idempotency_id, metadata)
    }

    fn delete_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> impl Future<Output = ()> + Send {
        self.inner.delete_idempotency_metadata(idempotency_id)
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> restate_storage_api::promise_table::ReadOnlyPromiseTable
    for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> impl Future<Output = StorageResult<Option<Promise>>> + Send {
        self.inner.get_promise(service_id, key)
    }

    fn all_promises(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = StorageResult<OwnedPromiseRow>> + Send {
        self.inner.all_promises(range)
    }
}

// Workaround until https://github.com/restatedev/restate/issues/276 is sorted out
impl<TransactionType> restate_storage_api::promise_table::PromiseTable
    for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        metadata: Promise,
    ) -> impl Future<Output = ()> + Send {
        self.inner.put_promise(service_id, key, metadata)
    }

    fn delete_all_promises(&mut self, service_id: &ServiceId) -> impl Future<Output = ()> + Send {
        self.inner.delete_all_promises(service_id)
    }
}

mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;

    pub(crate) const APPLIED_LSN: u64 = 2;
}

impl<Storage> OutboxReader for PartitionStorage<Storage>
where
    for<'a> Storage: OutboxTable + Send + 'a,
{
    async fn get_next_message(
        &mut self,
        next_sequence_number: MessageIndex,
    ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
        let partition_id = self.partition_id;

        let result = if let Some((message_index, outbox_message)) = self
            .storage
            .get_next_outbox_message(partition_id, next_sequence_number)
            .await?
        {
            Some((message_index, outbox_message))
        } else {
            None
        };

        Ok(result)
    }

    async fn get_message(
        &mut self,
        sequence_number: MessageIndex,
    ) -> Result<Option<OutboxMessage>, OutboxReaderError> {
        let partition_id = self.partition_id;

        self.storage
            .get_outbox_message(partition_id, sequence_number)
            .await
            .map_err(OutboxReaderError::Storage)
    }
}

impl<Storage> TimerReader<TimerKeyValue> for PartitionStorage<Storage>
where
    for<'a> Storage: TimerTable + Send + Sync + 'a,
{
    async fn get_timers(
        &mut self,
        num_timers: usize,
        previous_timer_key: Option<TimerKey>,
    ) -> Vec<TimerKeyValue> {
        self.storage
            .next_timers_greater_than(self.partition_id, previous_timer_key.as_ref(), num_timers)
            .map(|result| result.map(|(timer_key, timer)| TimerKeyValue::new(timer_key, timer)))
            // TODO: Update timer service to maintain transaction while reading the timer stream: See https://github.com/restatedev/restate/issues/273
            // have to collect the stream because it depends on the local transaction
            .try_collect::<Vec<_>>()
            .await
            // TODO: Extend TimerReader to return errors: See https://github.com/restatedev/restate/issues/274
            .expect("timer deserialization should not fail")
    }
}

pub trait DedupSequenceNumberResolver {
    fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> impl Future<Output = StorageResult<Option<DedupSequenceNumber>>> + Send;
}

impl<Storage> DedupSequenceNumberResolver for PartitionStorage<Storage>
where
    Storage: ReadOnlyDeduplicationTable + Send + 'static,
{
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> StorageResult<Option<DedupSequenceNumber>> {
        self.storage
            .get_dedup_sequence_number(self.partition_id, producer_id)
            .await
    }
}

impl<TransactionType> DedupSequenceNumberResolver for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction,
{
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> StorageResult<Option<DedupSequenceNumber>> {
        self.inner
            .get_dedup_sequence_number(self.partition_id, producer_id)
            .await
    }
}
