use crate::partition::effects::{CommitError, Committable, StateStorage, StateStorageError};
use crate::partition::shuffle::{OutboxReader, OutboxReaderError};
use crate::partition::state_machine::{StateReader, StateReaderError};
use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use restate_common::types::{
    CompletionResult, EnrichedRawEntry, EntryIndex, InboxEntry, InvocationStatus, JournalEntry,
    MessageIndex, MillisSinceEpoch, OutboxMessage, PartitionId, PartitionKey, ServiceId,
    ServiceInvocation, ServiceInvocationId, Timer, TimerKey,
};
use restate_journal::raw::Header;
use std::ops::RangeInclusive;

pub mod journal_reader;

use crate::partition::TimerValue;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::{PutFuture, Transaction as OtherTransaction};
use restate_timer::TimerReader;

#[derive(Debug, Clone)]
pub(super) struct PartitionStorage<Storage> {
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

    pub(super) fn create_transaction(&self) -> Transaction<Storage::TransactionType> {
        Transaction::new(
            self.partition_id,
            self.partition_key_range.clone(),
            self.storage.transaction(),
        )
    }
}

pub(crate) struct Transaction<TransactionType> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    inner: TransactionType,
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

    pub(super) fn commit(
        self,
    ) -> BoxFuture<'static, Result<(), restate_storage_api::StorageError>> {
        self.inner.commit()
    }

    pub(super) fn scan_invoked_invocations(
        &mut self,
    ) -> BoxStream<'_, Result<ServiceInvocationId, restate_storage_api::StorageError>> {
        self.inner
            .invoked_invocations(self.partition_key_range.clone())
    }

    pub(super) fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> BoxStream<'_, Result<(TimerKey, Timer), restate_storage_api::StorageError>> {
        self.inner
            .next_timers_greater_than(partition_id, exclusive_start, limit)
    }

    pub(super) fn load_outbox_seq_number(
        &mut self,
    ) -> BoxFuture<'_, Result<MessageIndex, restate_storage_api::StorageError>> {
        self.load_seq_number(fsm_variable::OUTBOX_SEQ_NUMBER)
    }

    pub(super) fn load_inbox_seq_number(
        &mut self,
    ) -> BoxFuture<'_, Result<MessageIndex, restate_storage_api::StorageError>> {
        self.load_seq_number(fsm_variable::INBOX_SEQ_NUMBER)
    }

    fn store_seq_number(
        &mut self,
        seq_number: MessageIndex,
        state_id: u64,
    ) -> BoxFuture<'_, Result<(), StateStorageError>> {
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
    ) -> BoxFuture<'_, Result<MessageIndex, restate_storage_api::StorageError>> {
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

    pub(super) fn load_dedup_seq_number(
        &mut self,
        producer_id: PartitionId,
    ) -> BoxFuture<'_, Result<Option<MessageIndex>, restate_storage_api::StorageError>> {
        self.inner
            .get_sequence_number(self.partition_id, producer_id)
    }

    pub(super) fn store_dedup_seq_number(
        &mut self,
        producer_id: PartitionId,
        dedup_seq_number: MessageIndex,
    ) -> PutFuture {
        self.inner
            .put_sequence_number(self.partition_id, producer_id, dedup_seq_number)
    }
}

impl<TransactionType> StateReader for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn get_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StateReaderError>> {
        async {
            Ok(self
                .inner
                .get_invocation_status(service_id)
                .await?
                .unwrap_or_default())
        }
        .boxed()
    }

    fn peek_inbox<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, StateReaderError>> {
        async {
            self.inner
                .peek_inbox(service_id)
                .await
                .map_err(StateReaderError::Storage)
        }
        .boxed()
    }

    // Returns true if the entry is a completable journal entry and is completed,
    // or if the entry is a non-completable journal entry. In the latter case,
    // the entry must be a Custom entry with requires_ack flag.
    fn is_entry_resumable<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>> {
        async move {
            Ok(self
                .inner
                .get_journal_entry(service_id, entry_index)
                .await?
                .map(|journal_entry| match journal_entry {
                    JournalEntry::Entry(EnrichedRawEntry { header, .. }) => {
                        header.is_completed().unwrap_or(true)
                    }
                    JournalEntry::Completion(_) => false,
                })
                .unwrap_or(false))
        }
        .boxed()
    }
}

impl<TransactionType> StateStorage for Transaction<TransactionType>
where
    TransactionType: restate_storage_api::Transaction + Send,
{
    fn store_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        status: InvocationStatus,
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>> {
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
    ) -> BoxFuture<Result<Option<EnrichedRawEntry>, StateStorageError>> {
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

    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    ) -> BoxFuture<Result<(), StateStorageError>> {
        async move {
            // TODO: Avoid cloning when moving this logic into the RocksDB storage impl
            let service_id = service_invocation.id.service_id.clone();

            self.inner
                .put_invocation(&service_id, InboxEntry::new(seq_number, service_invocation))
                .await;

            Ok(())
        }
        .boxed()
    }

    fn enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<(), StateStorageError>> {
        self.store_seq_number(seq_number, fsm_variable::INBOX_SEQ_NUMBER)
    }

    fn store_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>> {
        self.store_seq_number(seq_number, fsm_variable::OUTBOX_SEQ_NUMBER)
    }

    fn truncate_outbox(
        &mut self,
        outbox_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<(), StateStorageError>> {
        async move {
            self.inner
                .delete_invocation(service_id, inbox_sequence_number)
                .await;
            Ok(())
        }
        .boxed()
    }

    fn store_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: Bytes,
        value: Bytes,
    ) -> BoxFuture<Result<(), StateStorageError>> {
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
    ) -> BoxFuture<Result<Option<Bytes>, StateStorageError>> {
        async move { Ok(self.inner.get_user_state(service_id, key).await?) }.boxed()
    }

    fn clear_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<(), StateStorageError>> {
        async move {
            self.inner.delete_user_state(service_id, key).await;
            Ok(())
        }
        .boxed()
    }

    fn store_timer(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        timer: Timer,
    ) -> BoxFuture<Result<(), StateStorageError>> {
        async move {
            let timer_key = TimerKey {
                service_invocation_id,
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
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<(), StateStorageError>> {
        async move {
            let timer_key = TimerKey {
                service_invocation_id,
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
    TransactionType: restate_storage_api::Transaction + 'static,
{
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>> {
        async { self.inner.commit().await.map_err(CommitError::with_source) }.boxed()
    }
}

impl<Storage> OutboxReader for PartitionStorage<Storage>
where
    Storage: restate_storage_api::Storage + 'static,
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
    Storage: restate_storage_api::Storage + Send + Sync,
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
                service_invocation_id: timer_value.service_invocation_id,
                journal_index: timer_value.entry_index,
                timestamp: timer_value.wake_up_time.as_u64(),
            });

            let timer_stream = transaction
                .next_timers_greater_than(self.partition_id, exclusive_start.as_ref(), num_timers)
                .map(|result| {
                    result.map(|(timer_key, value)| TimerValue {
                        service_invocation_id: timer_key.service_invocation_id,
                        wake_up_time: MillisSinceEpoch::new(timer_key.timestamp),
                        entry_index: timer_key.journal_index,
                        value,
                    })
                })
                // TODO: Update timer service to maintain transaction while reading the timer stream: See https://github.com/restatedev/restate/issues/273
                // have to collect the stream because it depends on the local transaction
                .try_collect::<Vec<_>>()
                .await
                // TODO: Extend TimerReader to return errors: See https://github.com/restatedev/restate/issues/274
                .expect("timer deserialization should not fail");

            transaction.commit();

            stream::iter(timer_stream)
        }
        .flatten_stream()
        .boxed()
    }
}
