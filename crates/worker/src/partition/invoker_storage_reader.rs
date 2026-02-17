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
    EagerState, InvocationReader, InvocationReaderTransaction, JournalEntry,
};
use restate_storage_api::invocation_status_table::{InvocationStatus, ReadInvocationStatusTable};
use restate_storage_api::state_table::ReadStateTable;
use restate_storage_api::{IsolationLevel, journal_table as journal_table_v1, journal_table_v2};
use restate_types::identifiers::{InvocationId, ServiceId};

#[derive(Debug, thiserror::Error)]
pub enum InvokerStorageReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
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
        using_journal_table_v2: bool,
    ) -> Result<Option<JournalEntry>, InvokerStorageReaderError> {
        if using_journal_table_v2 {
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
            let using_v2 = journal_v2_stream.next().await.transpose()?.is_some();

            Ok(Some(JournalMetadata::new(
                invoked_status.journal_metadata.length,
                invoked_status.journal_metadata.span_context,
                invoked_status.pinned_deployment,
                invoked_status.timestamps.modification_time(),
                random_seed,
                using_v2,
            )))
        } else {
            Ok(None)
        }
    }

    fn read_journal(
        &self,
        invocation_id: &InvocationId,
        length: restate_types::identifiers::EntryIndex,
        using_journal_table_v2: bool,
    ) -> Result<Self::JournalStream<'_>, Self::Error> {
        if using_journal_table_v2 {
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
}
