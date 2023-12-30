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
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use restate_storage_api::deduplication_table::SequenceNumberSource;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::status_table::InvocationStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::{StorageError, Transaction as OtherTransaction};
use restate_timer::TimerReader;
use restate_types::identifiers::{
    EntryIndex, FullInvocationId, InvocationId, PartitionId, PartitionKey, ServiceId,
    WithPartitionKey,
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

    pub(super) fn load_outbox_seq_number(
        &mut self,
    ) -> BoxFuture<'_, Result<MessageIndex, StorageError>> {
        self.load_seq_number(fsm_variable::OUTBOX_SEQ_NUMBER)
    }

    pub(super) fn load_inbox_seq_number(
        &mut self,
    ) -> BoxFuture<'_, Result<MessageIndex, StorageError>> {
        self.load_seq_number(fsm_variable::INBOX_SEQ_NUMBER)
    }

    fn store_seq_number(
        &mut self,
        seq_number: MessageIndex,
        state_id: u64,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        async move {
            let bytes = Bytes::copy_from_slice(&seq_number.to_be_bytes());
            self.inner.put(self.partition_id, state_id, &bytes).await;

            Ok(())
        }
        .boxed()
    }

    fn load_seq_number(
        &mut self,
        state_id: u64,
    ) -> BoxFuture<'_, Result<MessageIndex, StorageError>> {
        async move {
            let seq_number = self.inner.get(self.partition_id, state_id).await?;

            if let Some(mut seq_number) = seq_number {
                let mut buffer = [0; 8];
                seq_number.copy_to_slice(&mut buffer);
                Ok(MessageIndex::from_be_bytes(buffer))
            } else {
                Ok(0)
            }
        }
        .boxed()
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
    fn get_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StorageError>> {
        self.assert_partition_key(service_id);
        async {
            Ok(self
                .inner
                .get_invocation_status(service_id)
                .await?
                .unwrap_or_default())
        }
        .boxed()
    }

    fn resolve_invocation_status_from_invocation_id<'a>(
        &'a mut self,
        invocation_id: &'a InvocationId,
    ) -> BoxFuture<'a, Result<(FullInvocationId, InvocationStatus), StorageError>> {
        self.assert_partition_key(invocation_id);
        async {
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
        .boxed()
    }

    fn peek_inbox<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, StorageError>> {
        self.assert_partition_key(service_id);
        async { self.inner.peek_inbox(service_id).await }.boxed()
    }

    fn get_inbox_entry(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> BoxFuture<Result<Option<InboxEntry>, StorageError>> {
        let maybe_fid = maybe_fid.into();
        self.assert_partition_key(&maybe_fid);
        async { self.inner.get_inbox_entry(maybe_fid).await }.boxed()
    }

    // Returns true if the entry is a completable journal entry and is completed,
    // or if the entry is a non-completable journal entry. In the latter case,
    // the entry must be a Custom entry with requires_ack flag.
    fn is_entry_resumable<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StorageError>> {
        self.assert_partition_key(service_id);
        async move {
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
        .boxed()
    }

    fn load_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<Option<Bytes>, StorageError>> {
        super::state_machine::StateStorage::load_state(self, service_id, key)
    }

    fn load_completion_result<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StorageError>> {
        super::state_machine::StateStorage::load_completion_result(self, service_id, entry_index)
    }

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry), StorageError>> + Send {
        self.inner.get_journal(service_id, length)
    }
}

impl<TransactionType> super::state_machine::StateStorage for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn store_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        status: InvocationStatus,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async {
            self.inner.put_invocation_status(service_id, status).await;
            Ok(())
        }
        .boxed()
    }

    fn drop_journal<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        journal_length: EntryIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            self.inner.delete_journal(service_id, journal_length).await;
            Ok(())
        }
        .boxed()
    }

    fn store_journal_entry<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            self.inner
                .put_journal_entry(service_id, entry_index, JournalEntry::Entry(journal_entry))
                .await;

            Ok(())
        }
        .boxed()
    }

    fn store_completion_result<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            self.inner
                .put_journal_entry(
                    service_id,
                    entry_index,
                    JournalEntry::Completion(completion_result),
                )
                .await;
            Ok(())
        }
        .boxed()
    }

    fn load_completion_result<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            let result = self
                .inner
                .get_journal_entry(service_id, entry_index)
                .await?;

            Ok(result.and_then(|journal_entry| match journal_entry {
                JournalEntry::Entry(_) => None,
                JournalEntry::Completion(completion_result) => Some(completion_result),
            }))
        }
        .boxed()
    }

    fn load_journal_entry<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<EnrichedRawEntry>, StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            let result = self
                .inner
                .get_journal_entry(service_id, entry_index)
                .await?;

            Ok(result.and_then(|journal_entry| match journal_entry {
                JournalEntry::Entry(entry) => Some(entry),
                JournalEntry::Completion(_) => None,
            }))
        }
        .boxed()
    }

    async fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    ) -> Result<(), StorageError> {
        self.assert_partition_key(&service_invocation.fid.service_id);

        // TODO: Avoid cloning when moving this logic into the RocksDB storage impl
        let service_id = service_invocation.fid.service_id.clone();

        self.inner
            .put_invocation(&service_id, InboxEntry::new(seq_number, service_invocation))
            .await;

        Ok(())
    }

    fn enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> BoxFuture<Result<(), StorageError>> {
        async move {
            self.inner
                .add_message(self.partition_id, seq_number, message)
                .await;

            Ok(())
        }
        .boxed()
    }

    fn store_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.store_seq_number(seq_number, fsm_variable::INBOX_SEQ_NUMBER)
    }

    fn store_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.store_seq_number(seq_number, fsm_variable::OUTBOX_SEQ_NUMBER)
    }

    fn truncate_outbox(
        &mut self,
        outbox_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        async move {
            self.inner
                .truncate_outbox(
                    self.partition_id,
                    outbox_sequence_number..outbox_sequence_number + 1,
                )
                .await;

            Ok(())
        }
        .boxed()
    }

    fn truncate_inbox<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        inbox_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        async move {
            self.inner
                .delete_invocation(service_id, inbox_sequence_number)
                .await;
            Ok(())
        }
        .boxed()
    }

    fn delete_inbox_entry<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        sequence_number: MessageIndex,
    ) -> BoxFuture<()> {
        self.inner
            .delete_invocation(service_id, sequence_number)
            .boxed()
    }

    fn store_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: Bytes,
        value: Bytes,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            self.inner.put_user_state(service_id, &key, &value).await;

            Ok(())
        }
        .boxed()
    }

    fn load_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<Option<Bytes>, StorageError>> {
        self.assert_partition_key(service_id);
        async move { self.inner.get_user_state(service_id, key).await }.boxed()
    }

    fn clear_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(service_id);
        async move {
            self.inner.delete_user_state(service_id, key).await;
            Ok(())
        }
        .boxed()
    }

    fn store_timer(
        &mut self,
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        timer: Timer,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(&full_invocation_id.service_id);
        async move {
            let timer_key = TimerKey {
                full_invocation_id,
                timestamp: wake_up_time.as_u64(),
                journal_index: entry_index,
            };

            self.inner
                .add_timer(self.partition_id, &timer_key, timer)
                .await;
            Ok(())
        }
        .boxed()
    }

    fn delete_timer(
        &mut self,
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<(), StorageError>> {
        self.assert_partition_key(&full_invocation_id.service_id);
        async move {
            let timer_key = TimerKey {
                full_invocation_id,
                timestamp: wake_up_time.as_u64(),
                journal_index: entry_index,
            };

            self.inner.delete_timer(self.partition_id, &timer_key).await;
            Ok(())
        }
        .boxed()
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
    for<'a> Storage: restate_storage_api::Storage + 'a,
{
    fn get_next_message(
        &self,
        next_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>> {
        let mut transaction = self.storage.transaction();
        let partition_id = self.partition_id;

        async move {
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
        .boxed()
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
                full_invocation_id: timer_value.full_invocation_id,
                journal_index: timer_value.entry_index,
                timestamp: timer_value.wake_up_time.as_u64(),
            });

            let timer_stream = transaction
                .next_timers_greater_than(self.partition_id, exclusive_start.as_ref(), num_timers)
                .map(|result| {
                    result.map(|(timer_key, timer)| TimerValue {
                        full_invocation_id: timer_key.full_invocation_id,
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
