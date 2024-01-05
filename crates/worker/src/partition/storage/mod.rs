// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use crate::partition::{CommitError, Committable, TimerValue};
use bytes::{Buf, Bytes};
use futures::stream::BoxStream;
use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use restate_storage_api::deduplication_table::SequenceNumberSource;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::status_table::InvocationStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::Result as StorageResult;
use restate_storage_api::{StorageError, Transaction as OtherTransaction};
use restate_timer::TimerReader;
use restate_types::identifiers::{
    EntryIndex, FullInvocationId, InvocationId, InvocationUuid, PartitionId, PartitionKey,
    ServiceId, WithPartitionKey,
};
use restate_types::invocation::{MaybeFullInvocationId, ServiceInvocation};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::CompletionResult;
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;
use std::future::Future;
use std::ops::RangeInclusive;

pub mod invoker;

#[derive(Debug, Clone)]
pub(crate) struct PartitionStorage<Storage> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    storage: Storage,
}

impl<Storage> PartitionStorage<Storage>
where
    Storage: restate_storage_api::Storage,
{
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

    pub(super) fn create_transaction(&self) -> Transaction<Storage::TransactionType<'_>> {
        Transaction::new(
            self.partition_id,
            self.partition_key_range.clone(),
            self.storage.transaction(),
        )
    }
}

pub struct Transaction<TransactionType> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    inner: TransactionType,
}

impl<TransactionType> Transaction<TransactionType> {
    #[inline]
    fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) {
        let partition_key = partition_key.partition_key();
        assert!(self.partition_key_range.contains(&partition_key),
                "Partition key '{}' is not part of PartitionStorage's partition '{:?}'. This indicates a bug.",
                partition_key,
                self.partition_key_range);
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

    pub(super) fn commit(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.commit()
    }

    pub(super) fn scan_invoked_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<FullInvocationId, StorageError>> + Send + '_ {
        self.inner
            .invoked_invocations(self.partition_key_range.clone())
    }

    pub(super) fn next_timers_greater_than<'a>(
        &'a mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&'a TimerKey>,
        limit: usize,
    ) -> impl Stream<Item = Result<(TimerKey, Timer), StorageError>> + Send + '_ {
        self.inner
            .next_timers_greater_than(partition_id, exclusive_start, limit)
    }

    pub(super) async fn load_outbox_seq_number(&mut self) -> Result<MessageIndex, StorageError> {
        self.load_seq_number(fsm_variable::OUTBOX_SEQ_NUMBER).await
    }

    pub(super) async fn load_inbox_seq_number(&mut self) -> Result<MessageIndex, StorageError> {
        self.load_seq_number(fsm_variable::INBOX_SEQ_NUMBER).await
    }

    async fn store_seq_number(
        &mut self,
        seq_number: MessageIndex,
        state_id: u64,
    ) -> Result<(), StorageError> {
        let bytes = Bytes::copy_from_slice(&seq_number.to_be_bytes());
        self.inner.put(self.partition_id, state_id, &bytes).await;

        Ok(())
    }

    async fn load_seq_number(&mut self, state_id: u64) -> Result<MessageIndex, StorageError> {
        let seq_number = self.inner.get(self.partition_id, state_id).await?;

        if let Some(mut seq_number) = seq_number {
            let mut buffer = [0; 8];
            seq_number.copy_to_slice(&mut buffer);
            Ok(MessageIndex::from_be_bytes(buffer))
        } else {
            Ok(0)
        }
    }

    pub(super) async fn load_dedup_seq_number(
        &mut self,
        source: SequenceNumberSource,
    ) -> Result<Option<MessageIndex>, StorageError> {
        self.inner
            .get_sequence_number(self.partition_id, source)
            .await
    }

    pub(super) async fn store_dedup_seq_number(
        &mut self,
        source: SequenceNumberSource,
        dedup_seq_number: MessageIndex,
    ) {
        self.inner
            .put_sequence_number(self.partition_id, source, dedup_seq_number)
            .await
    }
}

impl<TransactionType> super::state_machine::StateReader for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    async fn get_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> StorageResult<InvocationStatus> {
        self.assert_partition_key(service_id);
        Ok(self
            .inner
            .get_invocation_status(service_id)
            .await?
            .unwrap_or_default())
    }

    async fn resolve_invocation_status_from_invocation_id(
        &mut self,
        invocation_id: &InvocationId,
    ) -> StorageResult<(FullInvocationId, InvocationStatus)> {
        self.assert_partition_key(invocation_id);
        let (service_id, status) = match self
            .inner
            .get_invocation_status_from(
                invocation_id.partition_key(),
                invocation_id.invocation_uuid(),
            )
            .await?
        {
            None => {
                // We can just default these here for the time being.
                // This is similar to the behavior of get_invocation_status.
                (ServiceId::new("", ""), InvocationStatus::default())
            }
            Some(t) => t,
        };

        Ok((
            FullInvocationId::with_service_id(service_id, invocation_id.invocation_uuid()),
            status,
        ))
    }

    async fn peek_inbox(&mut self, service_id: &ServiceId) -> StorageResult<Option<InboxEntry>> {
        self.assert_partition_key(service_id);
        self.inner.peek_inbox(service_id).await
    }

    fn get_inbox_entry(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> impl Future<Output = StorageResult<Option<InboxEntry>>> + Send {
        let maybe_fid = maybe_fid.into();
        self.assert_partition_key(&maybe_fid);
        self.inner.get_inbox_entry(maybe_fid)
    }

    // Returns true if the entry is a completable journal entry and is completed,
    // or if the entry is a non-completable journal entry. In the latter case,
    // the entry must be a Custom entry with requires_ack flag.
    async fn is_entry_resumable(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> StorageResult<bool> {
        self.assert_partition_key(service_id);
        Ok(self
            .inner
            .get_journal_entry(service_id, entry_index)
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

    async fn load_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        super::state_machine::StateStorage::load_completion_result(self, service_id, entry_index)
            .await
    }

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        length: EntryIndex,
    ) -> impl Stream<Item = StorageResult<(EntryIndex, JournalEntry)>> + Send {
        self.inner.get_journal(service_id, length)
    }
}

impl<TransactionType> super::state_machine::StateStorage for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    async fn store_invocation_status(
        &mut self,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner.put_invocation_status(service_id, status).await;
        Ok(())
    }

    async fn drop_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner.delete_journal(service_id, journal_length).await;
        Ok(())
    }

    async fn store_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner
            .put_journal_entry(service_id, entry_index, JournalEntry::Entry(journal_entry))
            .await;

        Ok(())
    }

    async fn store_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> StorageResult<()> {
        self.assert_partition_key(service_id);
        self.inner
            .put_journal_entry(
                service_id,
                entry_index,
                JournalEntry::Completion(completion_result),
            )
            .await;
        Ok(())
    }

    async fn load_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        self.assert_partition_key(service_id);
        let result = self
            .inner
            .get_journal_entry(service_id, entry_index)
            .await?;

        Ok(result.and_then(|journal_entry| match journal_entry {
            JournalEntry::Entry(_) => None,
            JournalEntry::Completion(completion_result) => Some(completion_result),
        }))
    }

    async fn load_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> StorageResult<Option<EnrichedRawEntry>> {
        self.assert_partition_key(service_id);
        let result = self
            .inner
            .get_journal_entry(service_id, entry_index)
            .await?;

        Ok(result.and_then(|journal_entry| match journal_entry {
            JournalEntry::Entry(entry) => Some(entry),
            JournalEntry::Completion(_) => None,
        }))
    }

    async fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    ) -> StorageResult<()> {
        self.assert_partition_key(&service_invocation.fid.service_id);

        // TODO: Avoid cloning when moving this logic into the RocksDB storage impl
        let service_id = service_invocation.fid.service_id.clone();

        self.inner
            .put_invocation(&service_id, InboxEntry::new(seq_number, service_invocation))
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

    async fn truncate_inbox(
        &mut self,
        service_id: &ServiceId,
        inbox_sequence_number: MessageIndex,
    ) -> StorageResult<()> {
        self.inner
            .delete_invocation(service_id, inbox_sequence_number)
            .await;
        Ok(())
    }

    async fn delete_inbox_entry(&mut self, service_id: &ServiceId, sequence_number: MessageIndex) {
        self.inner
            .delete_invocation(service_id, sequence_number)
            .await;
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

    async fn store_timer(
        &mut self,
        invocation_uuid: InvocationUuid,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        timer: Timer,
    ) -> StorageResult<()> {
        self.assert_partition_key(timer.service_id());
        let timer_key = TimerKey {
            invocation_uuid,
            timestamp: wake_up_time.as_u64(),
            journal_index: entry_index,
        };

        self.inner
            .add_timer(self.partition_id, &timer_key, timer)
            .await;
        Ok(())
    }

    async fn delete_timer(
        &mut self,
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> StorageResult<()> {
        self.assert_partition_key(&full_invocation_id.service_id);
        let timer_key = TimerKey {
            invocation_uuid: full_invocation_id.invocation_uuid,
            timestamp: wake_up_time.as_u64(),
            journal_index: entry_index,
        };

        self.inner.delete_timer(self.partition_id, &timer_key).await;
        Ok(())
    }
}

mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;
}

impl<TransactionType> Committable for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction,
{
    async fn commit(self) -> Result<(), CommitError> {
        self.inner.commit().await.map_err(CommitError::with_source)
    }
}

impl<Storage> OutboxReader for PartitionStorage<Storage>
where
    for<'a> Storage: restate_storage_api::Storage + Sync + 'a,
{
    async fn get_next_message(
        &self,
        next_sequence_number: MessageIndex,
    ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
        let mut transaction = self.storage.transaction();
        let partition_id = self.partition_id;

        let result = if let Some((message_index, outbox_message)) = transaction
            .get_next_outbox_message(partition_id, next_sequence_number)
            .await?
        {
            Some((message_index, outbox_message))
        } else {
            None
        };

        // we have to close the transaction here
        transaction.commit().await?;

        Ok(result)
    }
}

impl<Storage> TimerReader<TimerValue> for PartitionStorage<Storage>
where
    for<'a> Storage: restate_storage_api::Storage + Send + Sync + 'a,
{
    type TimerStream<'a> = BoxStream<'a, TimerValue> where Self: 'a;

    fn scan_timers(
        &self,
        num_timers: usize,
        previous_timer_key: Option<TimerValue>,
    ) -> Self::TimerStream<'_> {
        let mut transaction = self.create_transaction();

        async move {
            let exclusive_start = previous_timer_key.map(|timer_value| TimerKey {
                invocation_uuid: timer_value.invocation_uuid,
                journal_index: timer_value.entry_index,
                timestamp: timer_value.wake_up_time.as_u64(),
            });

            let timer_stream = transaction
                .next_timers_greater_than(self.partition_id, exclusive_start.as_ref(), num_timers)
                .map(|result| {
                    result.map(|(timer_key, timer)| TimerValue {
                        invocation_uuid: timer_key.invocation_uuid,
                        wake_up_time: MillisSinceEpoch::new(timer_key.timestamp),
                        entry_index: timer_key.journal_index,
                        value: timer,
                    })
                })
                // TODO: Update timer service to maintain transaction while reading the timer stream: See https://github.com/restatedev/restate/issues/273
                // have to collect the stream because it depends on the local transaction
                .try_collect::<Vec<_>>()
                .await
                // TODO: Extend TimerReader to return errors: See https://github.com/restatedev/restate/issues/274
                .expect("timer deserialization should not fail");

            // we didn't do any writes so committing should not fail
            let _ = transaction.commit().await;

            stream::iter(timer_stream)
        }
        .flatten_stream()
        .boxed()
    }
}
