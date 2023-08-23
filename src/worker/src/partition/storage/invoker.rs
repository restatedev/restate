// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use restate_invoker_api::EagerState;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_storage_api::state_table::StateTable;
use restate_storage_api::status_table::{InvocationStatus, StatusTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::ServiceId;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::JournalMetadata;
use std::vec::IntoIter;

#[derive(Debug, thiserror::Error)]
pub enum InvokerStorageReaderError {
    #[error("not invoked")]
    NotInvoked,
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

impl<Storage> restate_invoker_api::JournalReader for InvokerStorageReader<Storage>
where
    for<'a> Storage: restate_storage_api::Storage + 'a,
{
    type JournalStream = stream::Iter<IntoIter<PlainRawEntry>>;
    type Error = InvokerStorageReaderError;
    type Future<'a> = BoxFuture<'a, Result<(JournalMetadata, Self::JournalStream), Self::Error>> where Self: 'a;

    fn read_journal<'a>(&'a self, fid: &'a FullInvocationId) -> Self::Future<'_> {
        let mut transaction = self.0.transaction();

        async move {
            let invocation_status = transaction.get_invocation_status(&fid.service_id).await?;

            if let Some(InvocationStatus::Invoked(invoked_status)) = invocation_status {
                let journal_metadata = invoked_status.journal_metadata;
                let journal_stream = transaction
                    .get_journal(&fid.service_id, journal_metadata.length)
                    .map(|entry| {
                        entry
                            .map_err(InvokerStorageReaderError::Storage)
                            .map(|journal_entry| match journal_entry {
                                JournalEntry::Entry(EnrichedRawEntry { header, entry }) => {
                                    PlainRawEntry::new(header.into(), entry)
                                }
                                JournalEntry::Completion(_) => {
                                    panic!("should only read entries when reading the journal")
                                }
                            })
                    })
                    // TODO: Update invoker to maintain transaction while reading the journal stream: See https://github.com/restatedev/restate/issues/275
                    // collecting the stream because we cannot keep the transaction open
                    .try_collect::<Vec<_>>()
                    .await?;

                transaction.commit().await?;

                Ok((journal_metadata, stream::iter(journal_stream)))
            } else {
                Err(InvokerStorageReaderError::NotInvoked)
            }
        }
        .boxed()
    }
}

impl<Storage> restate_invoker_api::StateReader for InvokerStorageReader<Storage>
where
    for<'a> Storage: restate_storage_api::Storage + 'a,
{
    type StateIter = IntoIter<(Bytes, Bytes)>;
    type Error = InvokerStorageReaderError;
    type Future<'a> = BoxFuture<'a, Result<EagerState<Self::StateIter>, Self::Error>> where Self: 'a;

    fn read_state<'a>(&'a self, service_id: &'a ServiceId) -> Self::Future<'_> {
        let mut transaction = self.0.transaction();

        async move {
            let user_states = transaction
                .get_all_user_states(service_id)
                // TODO: Update invoker to maintain transaction while reading the state stream: See https://github.com/restatedev/restate/issues/275
                // collecting the stream because we cannot keep the transaction open
                .try_collect::<Vec<_>>()
                .await?;

            Ok(EagerState::new_complete(user_states.into_iter()))
        }
        .boxed()
    }
}
