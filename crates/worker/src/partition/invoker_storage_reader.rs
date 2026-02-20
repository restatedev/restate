// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};

use restate_invoker_api::JournalMetadata;
use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderError, InvocationReaderTransaction, JournalEntry,
    JournalKind,
};
use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_storage_api::invocation_status_table::{InvocationStatus, ReadInvocationStatusTable};
use restate_storage_api::state_table::ReadStateTable;
use restate_storage_api::{IsolationLevel, journal_table as journal_table_v1, journal_table_v2};
use restate_types::identifiers::{InvocationId, ServiceId};

#[derive(Debug, thiserror::Error)]
pub enum InvokerStorageReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error("outbound memory budget exhausted (needed {needed} bytes)")]
    OutOfMemory { needed: usize },
}

impl InvocationReaderError for InvokerStorageReaderError {
    fn budget_exhaustion(&self) -> Option<usize> {
        match self {
            Self::OutOfMemory { needed } => Some(*needed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InvokerStorageReader<Storage>(Storage);

impl<Storage> InvokerStorageReader<Storage> {
    pub(crate) fn new(storage: Storage) -> Self {
        InvokerStorageReader(storage)
    }
}

impl<Storage> InvocationReader for InvokerStorageReader<Storage>
where
    Storage: restate_storage_api::Storage
        + journal_table_v1::ReadJournalTable
        + journal_table_v2::ReadJournalTable
        + Send
        + 'static,
{
    type Transaction<'a> = InvokerStorageReaderTransaction<'a, Storage>;
    type Error = InvokerStorageReaderError;

    fn transaction(&mut self) -> Self::Transaction<'_> {
        InvokerStorageReaderTransaction {
            txn: self
                .0
                // we must use repeatable reads to avoid reading inconsistent values in the presence
                // of concurrent writes
                .transaction_with_isolation(IsolationLevel::RepeatableReads),
        }
    }

    async fn read_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
    ) -> Result<Option<JournalEntry>, InvokerStorageReaderError> {
        if journal_kind == JournalKind::V2 {
            let entry = journal_table_v2::ReadJournalTable::get_journal_entry(
                &mut self.0,
                *invocation_id,
                entry_index,
            )
            .await?;
            Ok(entry.map(JournalEntry::JournalV2))
        } else {
            let entry = journal_table_v1::ReadJournalTable::get_journal_entry(
                &mut self.0,
                invocation_id,
                entry_index,
            )
            .await?;
            Ok(entry.map(|je| match je {
                journal_table_v1::JournalEntry::Entry(entry) => {
                    JournalEntry::JournalV1(entry.erase_enrichment())
                }
                journal_table_v1::JournalEntry::Completion(result) => {
                    JournalEntry::JournalV1Completion(result)
                }
            }))
        }
    }

    // Note: budget is currently acquired after decode. A follow-up PR will push
    // the budget into the storage layer (two-phase peek/decode) so that leases
    // are reserved before entries are decoded into memory.
    async fn read_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
        budget: &mut LocalMemoryPool,
    ) -> Result<Option<(JournalEntry, LocalMemoryLease)>, InvokerStorageReaderError> {
        let entry = self
            .read_journal_entry(invocation_id, entry_index, journal_kind)
            .await?;
        match entry {
            Some(je) => {
                let size = estimate_journal_entry_size(&je);
                let lease = budget
                    .reserve(size)
                    .await
                    .map_err(|e| InvokerStorageReaderError::OutOfMemory { needed: e.needed })?;
                Ok(Some((je, lease)))
            }
            None => Ok(None),
        }
    }
}

pub(crate) struct InvokerStorageReaderTransaction<'a, Storage>
where
    Storage: restate_storage_api::Storage + 'static,
{
    txn: Storage::TransactionType<'a>,
}

impl<Storage> InvocationReaderTransaction for InvokerStorageReaderTransaction<'_, Storage>
where
    Storage: restate_storage_api::Storage + 'static,
{
    type JournalStream<'a>
        = Pin<Box<dyn Stream<Item = Result<JournalEntry, Self::Error>> + Send + 'a>>
    where
        Self: 'a;
    type StateStream<'a>
        = Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes), Self::Error>> + Send + 'a>>
    where
        Self: 'a;
    type LocalMemoryPooledJournalStream<'a>
        = Pin<
        Box<dyn Stream<Item = Result<(JournalEntry, LocalMemoryLease), Self::Error>> + Send + 'a>,
    >
    where
        Self: 'a;
    type LocalMemoryPooledStateStream<'a>
        = Pin<
        Box<dyn Stream<Item = Result<((Bytes, Bytes), LocalMemoryLease), Self::Error>> + Send + 'a>,
    >
    where
        Self: 'a;
    type Error = InvokerStorageReaderError;

    async fn read_journal_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<JournalMetadata>, Self::Error> {
        let invocation_status = self.txn.get_invocation_status(invocation_id).await?;

        let random_seed = invocation_status
            .get_random_seed()
            .unwrap_or_else(|| invocation_id.to_random_seed());

        if let InvocationStatus::Invoked(invoked_status) = invocation_status {
            // Check if using journal v2 by seeing if v2 has any entries
            let mut journal_v2_stream =
                std::pin::pin!(journal_table_v2::ReadJournalTable::get_journal(
                    &self.txn,
                    *invocation_id,
                    1, // Just check first entry to determine version
                )?);
            let journal_kind = if journal_v2_stream.next().await.transpose()?.is_some() {
                JournalKind::V2
            } else {
                JournalKind::V1
            };

            Ok(Some(JournalMetadata::new(
                invoked_status.journal_metadata.length,
                invoked_status.journal_metadata.span_context,
                invoked_status.pinned_deployment,
                invoked_status.timestamps.modification_time(),
                random_seed,
                journal_kind,
            )))
        } else {
            Ok(None)
        }
    }

    fn read_journal(
        &self,
        invocation_id: &InvocationId,
        length: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
    ) -> Result<Self::JournalStream<'_>, Self::Error> {
        if journal_kind == JournalKind::V2 {
            let journal_entries =
                journal_table_v2::ReadJournalTable::get_journal(&self.txn, *invocation_id, length)?;
            Ok(Box::pin(journal_entries.map(|result| {
                result
                    .map(|(_, entry)| JournalEntry::JournalV2(entry))
                    .map_err(InvokerStorageReaderError::Storage)
            })))
        } else {
            // todo remove once we no longer support journal v1: https://github.com/restatedev/restate/issues/3184
            let journal_entries =
                journal_table_v1::ReadJournalTable::get_journal(&self.txn, invocation_id, length)?;
            Ok(Box::pin(journal_entries.map(|result| {
                result
                    .map(|(_, journal_entry)| match journal_entry {
                        journal_table_v1::JournalEntry::Entry(entry) => {
                            JournalEntry::JournalV1(entry.erase_enrichment())
                        }
                        journal_table_v1::JournalEntry::Completion(_) => {
                            panic!("should only read entries when reading the journal")
                        }
                    })
                    .map_err(InvokerStorageReaderError::Storage)
            })))
        }
    }

    fn read_state(
        &self,
        service_id: &ServiceId,
    ) -> Result<EagerState<Self::StateStream<'_>>, Self::Error> {
        let stream = self
            .txn
            .get_all_user_states_for_service(service_id)?
            .map_err(InvokerStorageReaderError::Storage);
        Ok(EagerState::new_complete(Box::pin(stream)))
    }

    fn read_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        length: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<Self::LocalMemoryPooledJournalStream<'a>, Self::Error> {
        let inner = self.read_journal(invocation_id, length, journal_kind)?;
        Ok(Box::pin(futures::stream::unfold(
            (inner, budget),
            |(mut stream, budget)| async move {
                let item = stream.next().await?;
                match item {
                    Ok(je) => {
                        let size = estimate_journal_entry_size(&je);
                        match budget.reserve(size).await {
                            Ok(lease) => Some((Ok((je, lease)), (stream, budget))),
                            Err(e) => Some((
                                Err(InvokerStorageReaderError::OutOfMemory { needed: e.needed }),
                                (stream, budget),
                            )),
                        }
                    }
                    Err(e) => Some((Err(e), (stream, budget))),
                }
            },
        )))
    }

    fn read_state_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<EagerState<Self::LocalMemoryPooledStateStream<'a>>, Self::Error> {
        let inner_stream = self
            .txn
            .get_all_user_states_for_service(service_id)?
            .map_err(InvokerStorageReaderError::Storage);
        let budgeted = futures::stream::unfold(
            (Box::pin(inner_stream), budget),
            |(mut stream, budget)| async move {
                let item = stream.next().await?;
                match item {
                    Ok(kv) => {
                        let size = kv.0.len() + kv.1.len();
                        match budget.reserve(size).await {
                            Ok(lease) => Some((Ok((kv, lease)), (stream, budget))),
                            Err(e) => Some((
                                Err(InvokerStorageReaderError::OutOfMemory { needed: e.needed }),
                                (stream, budget),
                            )),
                        }
                    }
                    Err(e) => Some((Err(e), (stream, budget))),
                }
            },
        );
        Ok(EagerState::new_complete(Box::pin(budgeted)))
    }
}

/// Estimates the in-memory byte size of a journal entry for budget accounting.
///
/// This is an approximation of the serialized content that will be sent over the
/// wire to the service deployment. The exact on-wire encoding may differ slightly,
/// but this is sufficient for backpressure purposes.
fn estimate_journal_entry_size(entry: &JournalEntry) -> usize {
    match entry {
        JournalEntry::JournalV1(raw) => raw.serialized_entry().len(),
        JournalEntry::JournalV1Completion(result) => match result {
            restate_types::journal::CompletionResult::Empty => 0,
            restate_types::journal::CompletionResult::Success(b) => b.len(),
            restate_types::journal::CompletionResult::Failure(_, msg) => msg.len(),
        },
        JournalEntry::JournalV2(stored) => match &stored.inner {
            restate_types::journal_v2::raw::RawEntry::Command(cmd) => cmd.serialized_content.len(),
            restate_types::journal_v2::raw::RawEntry::Notification(notif) => {
                notif.serialized_content().len()
            }
        },
    }
}
