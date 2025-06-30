// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use restate_invoker_api::JournalMetadata;
use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction,
};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::state_table::ReadOnlyStateTable;
use restate_storage_api::{IsolationLevel, journal_table as journal_table_v1, journal_table_v2};
use restate_types::identifiers::InvocationId;
use restate_types::identifiers::ServiceId;
use restate_types::journal_v2::{EntryIndex, EntryMetadata, EntryType};
use restate_types::service_protocol::ServiceProtocolVersion;
use std::vec::IntoIter;

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
    Storage: restate_storage_api::Storage + 'static,
{
    type Transaction<'a> = InvokerStorageReaderTransaction<'a, Storage>;

    fn transaction(&mut self) -> Self::Transaction<'_> {
        InvokerStorageReaderTransaction {
            txn: self
                .0
                // we must use repeatable reads to avoid reading inconsistent values in the presence
                // of concurrent writes
                .transaction_with_isolation(IsolationLevel::RepeatableReads),
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
    type JournalStream =
        stream::Iter<IntoIter<restate_invoker_api::invocation_reader::JournalEntry>>;
    type StateIter = IntoIter<(Bytes, Bytes)>;
    type Error = InvokerStorageReaderError;

    async fn read_journal(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<(JournalMetadata, Self::JournalStream)>, Self::Error> {
        let invocation_status = self.txn.get_invocation_status(invocation_id).await?;

        if let InvocationStatus::Invoked(invoked_status) = invocation_status {
            let (journal_metadata, journal_stream) = if invoked_status
                .pinned_deployment
                .as_ref()
                .is_some_and(|p| p.service_protocol_version >= ServiceProtocolVersion::V4)
            {
                // If pinned service protocol version exists and >= V4, we need to read from Journal Table V2!
                let entries = journal_table_v2::ReadOnlyJournalTable::get_journal(
                    &mut self.txn,
                    *invocation_id,
                    invoked_status.journal_metadata.length,
                )?
                .filter_map(|entry| {
                    std::future::ready(
                        entry
                            .map_err(InvokerStorageReaderError::Storage)
                            .map(|(_, entry)| {
                                if entry.ty() == EntryType::Event {
                                    // Filter out events!
                                    None
                                } else {
                                    Some(
                                    restate_invoker_api::invocation_reader::JournalEntry::JournalV2(
                                        entry,
                                    ),
                                )
                                }
                            })
                            .transpose(),
                    )
                })
                // TODO: Update invoker to maintain transaction while reading the journal stream: See https://github.com/restatedev/restate/issues/275
                // collecting the stream because we cannot keep the transaction open
                .try_collect::<Vec<_>>()
                .await?;

                let journal_metadata = JournalMetadata::new(
                    // Use entries len here, because we might be filtering out events
                    entries.len() as EntryIndex,
                    invoked_status.journal_metadata.span_context,
                    invoked_status.pinned_deployment,
                    invoked_status.current_invocation_epoch,
                    invoked_status.timestamps.modification_time(),
                );

                (journal_metadata, entries)
            } else {
                (
                    JournalMetadata::new(
                        // Use entries len here, because we might be filtering out events
                        invoked_status.journal_metadata.length,
                        invoked_status.journal_metadata.span_context,
                        invoked_status.pinned_deployment,
                        invoked_status.current_invocation_epoch,
                        invoked_status.timestamps.modification_time(),
                    ),
                    journal_table_v1::ReadOnlyJournalTable::get_journal(
                        &mut self.txn,
                        invocation_id,
                        invoked_status.journal_metadata.length,
                    )?
                    .map(|entry| {
                        entry.map_err(InvokerStorageReaderError::Storage).map(
                            |(_, journal_entry)| match journal_entry {
                                journal_table_v1::JournalEntry::Entry(entry) => {
                                    restate_invoker_api::invocation_reader::JournalEntry::JournalV1(
                                        entry.erase_enrichment(),
                                    )
                                }
                                journal_table_v1::JournalEntry::Completion(_) => {
                                    panic!("should only read entries when reading the journal")
                                }
                            },
                        )
                    })
                    // TODO: Update invoker to maintain transaction while reading the journal stream: See https://github.com/restatedev/restate/issues/275
                    // collecting the stream because we cannot keep the transaction open
                    .try_collect::<Vec<_>>()
                    .await?,
                )
            };

            Ok(Some((journal_metadata, stream::iter(journal_stream))))
        } else {
            Ok(None)
        }
    }

    async fn read_state(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<EagerState<Self::StateIter>, Self::Error> {
        let user_states = self
            .txn
            .get_all_user_states_for_service(service_id)?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(EagerState::new_complete(user_states.into_iter()))
    }
}
