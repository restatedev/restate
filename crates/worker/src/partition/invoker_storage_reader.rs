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
use bytestring::ByteString;
use futures::future::Either;
use futures::{Stream, StreamExt, TryStreamExt};

use restate_memory::{
    ClaimedMemoryPool, LocalMemoryLease, LocalMemoryPool, NonZeroByteCount, OutOfMemory,
    OutOfMemoryKind, PinnableMapErr, PinnableMemoryStream,
};
use restate_partition_store::{PartitionStore, PartitionStoreSnapshot};
use restate_rocksdb::SinglePoolExecutor;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::explicit_recovery_table::ReadExplicitRecoveryTable;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::state_table::ReadStateTable;
use restate_storage_api::{
    BudgetedReadError, StorageError, journal_table as journal_table_v1, journal_table_v2,
};
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::Header;
use restate_types::journal_v2::{InputCommand, SignalResult};
use restate_types::schema::invocation_target::StatePreloadPolicy;
use restate_worker_api::invoker::JournalMetadata;
use restate_worker_api::invoker::explicit_recovery::{
    ErAttemptMetadata, ExplicitRecoveryReader, ExplicitRecoveryReaderTransaction,
};
use restate_worker_api::invoker::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderError, InvocationReaderTransaction, JournalEntry,
    JournalKind,
};

#[derive(Debug, thiserror::Error)]
pub enum InvokerStorageReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error("outbound memory budget exhausted ({kind}): needed {needed}")]
    OutOfMemory {
        needed: NonZeroByteCount,
        kind: OutOfMemoryKind,
    },
}

impl InvocationReaderError for InvokerStorageReaderError {
    fn budget_exhaustion(&self) -> Option<OutOfMemory> {
        match self {
            Self::OutOfMemory { needed, kind } => Some(OutOfMemory {
                needed: *needed,
                kind: *kind,
            }),
            _ => None,
        }
    }
}

impl From<BudgetedReadError> for InvokerStorageReaderError {
    fn from(e: BudgetedReadError) -> Self {
        match e {
            BudgetedReadError::Storage(e) => Self::Storage(e),
            BudgetedReadError::OutOfMemory { needed, kind } => Self::OutOfMemory { needed, kind },
        }
    }
}

#[derive(Clone)]
pub(crate) struct InvokerStorageReader {
    storage: PartitionStore,
    // Reads what isn't in memory, so that the invoker's tasks don't wait for
    // the disk.
    executor: SinglePoolExecutor,
}

impl InvokerStorageReader {
    /// Reads from `storage`. Its scans and point reads read what isn't in
    /// memory on the workers of `executor`.
    pub(crate) fn new(storage: PartitionStore, executor: SinglePoolExecutor) -> Self {
        InvokerStorageReader { storage, executor }
    }
}

impl InvocationReader for InvokerStorageReader {
    type Transaction<'a> = InvokerStorageReaderTransaction<'a>;
    type Error = InvokerStorageReaderError;

    fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
        // A snapshot gives consistent reads in the presence of concurrent writes.
        let snapshot = self
            .storage
            .snapshot()
            .map_err(|_| InvokerStorageReaderError::Storage(StorageError::OperationalError))?;
        Ok(InvokerStorageReaderTransaction {
            snapshot,
            executor: &self.executor,
        })
    }

    async fn read_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
        budget: &mut LocalMemoryPool,
    ) -> Result<Option<(JournalEntry, LocalMemoryLease)>, InvokerStorageReaderError> {
        if journal_kind == JournalKind::V2 {
            let result = self
                .storage
                .get_journal_v2_entry_budgeted(*invocation_id, entry_index, budget, &self.executor)
                .await?;
            Ok(result.map(|(entry, lease)| (JournalEntry::JournalV2(entry), lease)))
        } else {
            let result = self
                .storage
                .get_journal_v1_entry_budgeted(invocation_id, entry_index, budget, &self.executor)
                .await?;
            Ok(result.map(|(je, lease)| {
                let entry = match je {
                    journal_table_v1::JournalEntry::Entry(entry) => {
                        JournalEntry::JournalV1(entry.erase_enrichment())
                    }
                    journal_table_v1::JournalEntry::Completion(result) => {
                        JournalEntry::JournalV1Completion(result)
                    }
                };
                (entry, lease)
            }))
        }
    }

    async fn read_state_entry_budgeted(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        budget: &mut LocalMemoryPool,
    ) -> Result<Option<(Bytes, LocalMemoryLease)>, InvokerStorageReaderError> {
        Ok(self
            .storage
            .get_user_state_budgeted(service_id, key, budget, &self.executor)
            .await?)
    }
}

pub(crate) struct InvokerStorageReaderTransaction<'a> {
    snapshot: PartitionStoreSnapshot<'a>,
    executor: &'a SinglePoolExecutor,
}

impl InvocationReaderTransaction for InvokerStorageReaderTransaction<'_> {
    type LocalMemoryPooledJournalStream<'a>
        = Pin<
        Box<dyn Stream<Item = Result<(JournalEntry, LocalMemoryLease), Self::Error>> + Send + 'a>,
    >
    where
        Self: 'a;
    type LocalMemoryPooledStateStream<'a>
        = Pin<
        Box<
            dyn PinnableMemoryStream<Item = Result<(Bytes, Bytes, LocalMemoryLease), Self::Error>>
                + Send
                + 'a,
        >,
    >
    where
        Self: 'a;
    type Error = InvokerStorageReaderError;

    async fn read_journal_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<JournalMetadata>, Self::Error> {
        // These reads leave their disk reads to a storage worker.
        let invocation_status = self
            .snapshot
            .get_invocation_status_cached_or_offload(invocation_id, self.executor)
            .await?;

        let random_seed = invocation_status
            .get_random_seed()
            .unwrap_or_else(|| invocation_id.to_random_seed());

        if let InvocationStatus::Invoked(invoked_status) = invocation_status {
            // Check if using journal v2 by seeing if v2 has any entries
            let has_journal_v2 = self
                .snapshot
                // Just check first entry to determine version
                .has_journal_v2_entry_cached_or_offload(*invocation_id, 0, self.executor)
                .await?;
            let journal_kind = if has_journal_v2 {
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

    fn read_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        length: restate_types::identifiers::EntryIndex,
        journal_kind: JournalKind,
        budget: ClaimedMemoryPool<'a>,
    ) -> Result<Self::LocalMemoryPooledJournalStream<'a>, Self::Error> {
        if journal_kind == JournalKind::V2 {
            let stream = self.snapshot.get_journal_v2_budgeted(
                *invocation_id,
                length,
                budget,
                self.executor,
            )?;
            Ok(Box::pin(stream.map(|result| {
                let (_, entry, lease) = result?;
                Ok((JournalEntry::JournalV2(entry), lease))
            })))
        } else {
            let stream = self.snapshot.get_journal_v1_budgeted(
                invocation_id,
                length,
                budget,
                self.executor,
            )?;
            Ok(Box::pin(stream.map(|result| {
                result
                    .map(|(_, journal_entry, lease)| {
                        let je = match journal_entry {
                            journal_table_v1::JournalEntry::Entry(entry) => {
                                JournalEntry::JournalV1(entry.erase_enrichment())
                            }
                            journal_table_v1::JournalEntry::Completion(_) => {
                                panic!("should only read entries when reading the journal")
                            }
                        };
                        (je, lease)
                    })
                    .map_err(InvokerStorageReaderError::from)
            })))
        }
    }

    fn read_state_budgeted<'a>(
        &'a mut self,
        service_id: &ServiceId,
        eager_state_config: &StatePreloadPolicy,
        budget: ClaimedMemoryPool<'a>,
    ) -> Result<EagerState<Self::LocalMemoryPooledStateStream<'a>>, Self::Error> {
        let (stream, partial) = match eager_state_config {
            // Preload the full state.
            StatePreloadPolicy::All => {
                let stream = self.snapshot.get_all_user_states_budgeted(
                    service_id,
                    budget,
                    self.executor,
                )?;
                let stream = PinnableMapErr::new(stream, InvokerStorageReaderError::from);
                (Either::Left(stream), false)
            }
            // Lazy default: preload only the whitelisted keys via exact point reads and serve
            // everything else on demand, so the result is partial.
            StatePreloadPolicy::Partial(eager_keys) => {
                let stream = self.snapshot.get_user_states_budgeted(
                    service_id,
                    eager_keys.as_ref(),
                    budget,
                    self.executor,
                )?;
                let stream = PinnableMapErr::new(stream, InvokerStorageReaderError::from);
                (Either::Right(stream), true)
            }
        };

        let stream: Self::LocalMemoryPooledStateStream<'a> = Box::pin(stream);
        Ok(if partial {
            EagerState::new_partial(stream)
        } else {
            EagerState::new_complete(stream)
        })
    }
}

impl ExplicitRecoveryReaderTransaction for InvokerStorageReaderTransaction<'_> {
    type Error = InvokerStorageReaderError;

    async fn read_er_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<ErAttemptMetadata>, Self::Error> {
        let status = self
            .snapshot
            .get_invocation_status_cached_or_offload(invocation_id, self.executor)
            .await?;
        let InvocationStatus::Invoked(metadata) = status else {
            return Ok(None);
        };
        Ok(metadata.explicit_recovery.map(|status| ErAttemptMetadata {
            status,
            span_context: metadata.journal_metadata.span_context,
            journal_length: metadata.journal_metadata.length,
        }))
    }

    async fn read_input(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<(Vec<Header>, Bytes)>, Self::Error> {
        let Some(entry) = journal_table_v2::ReadJournalTable::get_journal_entry(
            &mut self.snapshot,
            *invocation_id,
            0,
        )
        .await?
        else {
            return Ok(None);
        };
        let entry =
            journal_table_v2::ReadJournalTable::hydrate_journal_entry(&self.snapshot, entry)?;
        let input = entry
            .decode::<ServiceProtocolV4Codec, InputCommand>()
            .map_err(|e| StorageError::Conversion(e.into()))?;
        Ok(Some((input.headers, input.payload)))
    }

    async fn read_object_state_preload(
        &mut self,
        service_id: &ServiceId,
        policy: &StatePreloadPolicy,
        size_limit: usize,
    ) -> Result<(Vec<(Bytes, Bytes)>, bool), Self::Error> {
        let entries = match policy {
            StatePreloadPolicy::All => {
                ReadStateTable::get_all_user_states_for_service(&self.snapshot, service_id)?
                    .try_collect()
                    .await?
            }
            StatePreloadPolicy::Partial(keys) => {
                let mut entries = Vec::with_capacity(keys.len());
                for key in keys {
                    let key = key.as_bytes().clone();
                    if let Some(value) =
                        ReadStateTable::get_user_state(&mut self.snapshot, service_id, &key).await?
                    {
                        entries.push((key, value));
                    }
                }
                entries
            }
        };
        Ok(preload_within(entries, policy, size_limit))
    }

    async fn read_invocation_state_preload(
        &mut self,
        invocation_id: &InvocationId,
        policy: &StatePreloadPolicy,
        size_limit: usize,
    ) -> Result<(Vec<(Bytes, Bytes)>, bool), Self::Error> {
        let entries = match policy {
            StatePreloadPolicy::All => {
                ReadExplicitRecoveryTable::get_all_invocation_state(
                    &mut self.snapshot,
                    invocation_id,
                )
                .await?
            }
            StatePreloadPolicy::Partial(keys) => {
                let mut entries = Vec::with_capacity(keys.len());
                for key in keys {
                    let key = key.as_bytes().clone();
                    if let Some(value) = ReadExplicitRecoveryTable::get_invocation_state(
                        &mut self.snapshot,
                        invocation_id,
                        &key,
                    )
                    .await?
                    {
                        entries.push((key, value));
                    }
                }
                entries
            }
        };
        Ok(preload_within(entries, policy, size_limit))
    }
}

/// The entries, in order, as long as they fit `size_limit` bytes. Complete only when the policy
/// preloads everything and nothing was left out.
fn preload_within(
    entries: Vec<(Bytes, Bytes)>,
    policy: &StatePreloadPolicy,
    size_limit: usize,
) -> (Vec<(Bytes, Bytes)>, bool) {
    let mut size = 0usize;
    let total = entries.len();
    let kept: Vec<_> = entries
        .into_iter()
        .take_while(|(k, v)| {
            size += k.len() + v.len();
            size <= size_limit
        })
        .collect();
    let partial = kept.len() < total || !matches!(policy, StatePreloadPolicy::All);
    (kept, partial)
}

impl ExplicitRecoveryReader for InvokerStorageReader {
    type ErTransaction<'a> = InvokerStorageReaderTransaction<'a>;

    fn er_transaction(&mut self) -> Result<Self::ErTransaction<'_>, Self::Error> {
        self.transaction()
    }

    async fn read_object_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> Result<Option<Bytes>, Self::Error> {
        Ok(ReadStateTable::get_user_state(&mut self.storage, service_id, key).await?)
    }

    async fn read_object_state_keys(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Vec<Bytes>, Self::Error> {
        Ok(
            ReadStateTable::get_all_user_states_for_service(&self.storage, service_id)?
                .map(|entry| entry.map(|(key, _)| key))
                .try_collect()
                .await?,
        )
    }

    async fn read_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
        key: &Bytes,
    ) -> Result<Option<Bytes>, Self::Error> {
        Ok(
            ReadExplicitRecoveryTable::get_invocation_state(&mut self.storage, invocation_id, key)
                .await?,
        )
    }

    async fn read_invocation_state_keys(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Vec<Bytes>, Self::Error> {
        Ok(
            ReadExplicitRecoveryTable::get_all_invocation_state(&mut self.storage, invocation_id)
                .await?
                .into_iter()
                .map(|(key, _)| key)
                .collect(),
        )
    }

    async fn read_signal(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
        index: u32,
    ) -> Result<Option<SignalResult>, Self::Error> {
        Ok(
            ReadExplicitRecoveryTable::get_signal(&mut self.storage, invocation_id, name, index)
                .await?,
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytestring::ByteString;

    use super::InvokerStorageReader;
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::explicit_recovery_table::WriteExplicitRecoveryTable;
    use restate_storage_api::invocation_status_table::{
        ExplicitRecoveryStatus, InFlightInvocationMetadata, InvocationStatus,
        WriteInvocationStatusTable,
    };
    use restate_storage_api::state_table::WriteStateTable;
    use restate_types::identifiers::{InvocationId, PartitionId};
    use restate_types::invocation::InvocationTarget;
    use restate_types::journal_v2::SignalResult;
    use restate_types::partitions::Partition;
    use restate_types::schema::invocation_target::StatePreloadPolicy;
    use restate_types::sharding::KeyRange;
    use restate_worker_api::invoker::explicit_recovery::{
        ExplicitRecoveryReader, ExplicitRecoveryReaderTransaction,
    };

    #[restate_core::test]
    async fn explicit_recovery_reads() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();

        let target = InvocationTarget::mock_virtual_object();
        let service_id = target.as_keyed_service_id().unwrap();
        let invocation_id = InvocationId::mock_generate(&target);
        let er = ExplicitRecoveryStatus {
            attempt: 2,
            ..ExplicitRecoveryStatus::new()
        };
        let (a, b) = (Bytes::from_static(b"a"), Bytes::from_static(b"b"));
        let mut txn = store.transaction();
        txn.put_invocation_status(
            &invocation_id,
            &InvocationStatus::Invoked(InFlightInvocationMetadata {
                invocation_target: target,
                explicit_recovery: Some(er.clone()),
                ..InFlightInvocationMetadata::mock()
            }),
        )
        .unwrap();
        txn.put_user_state(&service_id, &a, Bytes::from_static(b"object"))
            .unwrap();
        txn.put_invocation_state(&invocation_id, &a, &Bytes::from_static(b"1"))
            .unwrap();
        txn.put_invocation_state(&invocation_id, &b, &Bytes::from_static(b"2"))
            .unwrap();
        txn.put_signal(&invocation_id, "approval", 0, &SignalResult::Void)
            .unwrap();
        txn.commit().await.unwrap();

        let executor = store.partition_db().rocksdb().executor().low_pri().clone();
        let mut reader = InvokerStorageReader::new(store, executor);

        // In the snapshot an attempt starts from.
        {
            let mut txn = reader.er_transaction().unwrap();
            let metadata = txn.read_er_metadata(&invocation_id).await.unwrap().unwrap();
            assert_eq!(metadata.status, er);
            assert_eq!(
                txn.read_invocation_state_preload(
                    &invocation_id,
                    &StatePreloadPolicy::All,
                    usize::MAX
                )
                .await
                .unwrap(),
                (
                    vec![
                        (a.clone(), Bytes::from_static(b"1")),
                        (b.clone(), Bytes::from_static(b"2"))
                    ],
                    false
                )
            );
            // Cut by the size limit: partial.
            assert_eq!(
                txn.read_invocation_state_preload(&invocation_id, &StatePreloadPolicy::All, 2)
                    .await
                    .unwrap(),
                (vec![(a.clone(), Bytes::from_static(b"1"))], true)
            );
            // A whitelist: only the keys with a value, and partial.
            let whitelist = StatePreloadPolicy::Partial(vec![
                ByteString::from_static("b"),
                ByteString::from_static("zzz"),
            ]);
            assert_eq!(
                txn.read_invocation_state_preload(&invocation_id, &whitelist, usize::MAX)
                    .await
                    .unwrap(),
                (vec![(b.clone(), Bytes::from_static(b"2"))], true)
            );
            assert_eq!(
                txn.read_object_state_preload(&service_id, &StatePreloadPolicy::All, usize::MAX)
                    .await
                    .unwrap(),
                (vec![(a.clone(), Bytes::from_static(b"object"))], false)
            );
        }

        // While the attempt runs: the latest committed state.
        assert_eq!(
            reader.read_object_state(&service_id, &a).await.unwrap(),
            Some(Bytes::from_static(b"object"))
        );
        assert_eq!(
            reader.read_object_state_keys(&service_id).await.unwrap(),
            vec![a.clone()]
        );
        assert_eq!(
            reader
                .read_invocation_state_keys(&invocation_id)
                .await
                .unwrap(),
            vec![a, b]
        );
        assert_eq!(
            reader
                .read_signal(&invocation_id, "approval", 0)
                .await
                .unwrap(),
            Some(SignalResult::Void)
        );
        assert_eq!(
            reader
                .read_signal(&invocation_id, "approval", 1)
                .await
                .unwrap(),
            None
        );
    }
}
