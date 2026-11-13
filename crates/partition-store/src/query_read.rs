// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads of a partition through one admitted snapshot, on the caller's thread.
//!
//! # Concepts
//!
//! - **Read snapshot** ([`ReadSnapshot`]): what a read sees of a partition: a
//!   RocksDB snapshot together with the storage features and the index registry
//!   as they were when the processor admitted the read. A read therefore never
//!   sees an index in a state newer than its data. Clones share all of it.
//! - **Cancellation**: the token the processor hands in ends the read; from then
//!   on data reads and cursor steps fail. Looking an index up in the captured
//!   registry still works.
//! - **Completion**: the snapshot is released when its last holder, a clone, a
//!   cursor or a queued storage job, is dropped. The processor waits for that
//!   before tearing the partition store down. The future reporting it
//!   ([`ReadSnapshot::closed`]) holds nothing itself.
//! - **Cursor** ([`SnapshotCursor`]): a raw iterator over the snapshot that keeps
//!   it alive, can pause and move between threads, and seeks forward.
//!
//! ```text
//! processor admits a read ──▶ ReadSnapshot ──clone──▶ cursors, storage jobs
//!           │                      │                          │
//!        cancel ────────▶ further reads fail                drop
//!                                                             ▼
//!                      completion ◀── the last holder is released
//! ```
//!
//! Every read here blocks the calling thread; scheduling, budgets and output
//! capacity are the caller's, as for every in-process read.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use rocksdb::ReadOptions;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use restate_storage_api::StorageError;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::features::StorageFeatures;
use crate::fsm_table::{registry_key, walks_key};
use crate::keys::KeyKind;
use crate::registry::{Index, IndexKind, IndexNotReady, IndexRegistry, IndexStatus};
use crate::scan::PhysicalScan;
use crate::table::{IndexSource, ReadMetadata};
use crate::walk::Walks;
use crate::{PartitionDb, PartitionReadSnapshot, PartitionSnapshotScan, Result};

/// A read's view of one partition, admitted by its processor: the RocksDB
/// snapshot plus the storage features and the index registry of that moment.
/// Clones share the snapshot, its cancellation and its completion. Every read
/// through it blocks the calling thread; scheduling is the caller's.
#[derive(Clone)]
pub struct ReadSnapshot {
    inner: Arc<QuerySnapshot>,
}

struct QuerySnapshot {
    // Release the snapshot before its database/CF, then notify the owner.
    snapshot: PartitionReadSnapshot,
    features: StorageFeatures,
    /// The registry as of `snapshot`, decoded on first use.
    registry: OnceLock<std::result::Result<Option<IndexRegistry>, String>>,
    /// The walks as of `snapshot`, decoded on first use.
    walks: OnceLock<std::result::Result<Walks, String>>,
    cancel: CancellationToken,
    completion: ReadCompletion,
}

struct ReadCompletion(CancellationToken);

impl Drop for ReadCompletion {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl std::fmt::Debug for ReadSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("partition_id", &self.partition_id())
            .finish_non_exhaustive()
    }
}

impl ReadSnapshot {
    pub(crate) fn new(
        db: PartitionDb,
        features: StorageFeatures,
        cancel: CancellationToken,
        completion: CancellationToken,
    ) -> Result<Self> {
        let snapshot = db
            .read_snapshot()
            .map_err(|_| StorageError::OperationalError)?;
        Ok(Self {
            inner: Arc::new(QuerySnapshot {
                snapshot,
                features,
                registry: OnceLock::new(),
                walks: OnceLock::new(),
                cancel,
                completion: ReadCompletion(completion),
            }),
        })
    }

    pub(crate) fn storage_snapshot(&self) -> &PartitionReadSnapshot {
        &self.inner.snapshot
    }

    pub(crate) fn metadata(&self) -> ReadMetadata<'_> {
        ReadMetadata {
            partition: self.inner.snapshot.partition(),
            features: self.inner.features,
            indexes: IndexSource::Snapshot(self),
        }
    }

    pub(crate) fn check_active(&self) -> Result<()> {
        if self.inner.cancel.is_cancelled() {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "partition query read cancelled"
            )));
        }
        Ok(())
    }

    pub fn partition_id(&self) -> PartitionId {
        self.inner.snapshot.partition().partition_id
    }

    /// The index ID of the Ready index of `definition` in this snapshot.
    ///
    /// The registry is read through this snapshot, so readiness and the ID match
    /// the data the query reads. Anything but a Ready index of that kind (not
    /// registered, Backfilling, Dropped, or an unreadable registry) fails with
    /// [`IndexNotReady`]: an incomplete index is never read as complete.
    pub fn ready_index(&self, definition: &str, kind: IndexKind) -> Result<u32> {
        let not_ready = |reason: String| {
            StorageError::Generic(
                IndexNotReady {
                    definition: definition.into(),
                    partition: self.partition_id(),
                    reason,
                }
                .into(),
            )
        };
        let registry = self.inner.registry.get_or_init(|| {
            let key = registry_key(self.partition_id());
            self.inner
                .snapshot
                .reader()
                .get_pinned(KeyKind::Fsm, &key)
                .map_err(|err| err.to_string())?
                .map(|value| IndexRegistry::decode(&value).map_err(|err| err.to_string()))
                .transpose()
        });
        let registry = registry
            .as_ref()
            .map_err(|err| not_ready(format!("unreadable registry: {err}")))?
            .as_ref()
            .ok_or_else(|| not_ready("nothing is registered".to_owned()))?;
        let index = registry
            .maintained(definition)
            .filter(|index| index.kind == kind)
            .ok_or_else(|| not_ready("no maintained index".to_owned()))?;
        if index.state != IndexStatus::Ready {
            return Err(not_ready(format!(
                "index {} is still being completed: {}",
                index.id,
                self.backfill_status(index)
            )));
        }
        Ok(index.id)
    }

    /// How far the backfill of the Backfilling `index` is in this snapshot.
    fn backfill_status(&self, index: &Index) -> String {
        let Some(id) = index.walk else {
            return "its backfill has not started".to_owned();
        };
        let walks = self.inner.walks.get_or_init(|| {
            let key = walks_key(self.partition_id());
            self.inner
                .snapshot
                .reader()
                .get_pinned(KeyKind::Fsm, &key)
                .map_err(|err| err.to_string())?
                .map(|value| Walks::decode(&value).map_err(|err| err.to_string()))
                .transpose()
                .map(Option::unwrap_or_default)
        });
        let walk = match walks {
            Ok(walks) => walks.get(id),
            Err(err) => return format!("unreadable walks: {err}"),
        };
        let Some(walk) = walk else {
            return format!("its backfill walk {id} is missing");
        };
        let mut status = format!("backfill walk {id} has examined {} entries", walk.scanned);
        if let Some(error) = &walk.last_error {
            status.push_str(&format!(
                "; failed attempts: {}, the last with: {error}",
                walk.failures
            ));
        }
        status
    }

    pub fn partition_key_range(&self) -> KeyRange {
        self.inner.snapshot.partition().key_range
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// Resolves once every clone, cursor and queued or running storage job has
    /// released this snapshot. The future itself holds nothing, so waiting on it
    /// never delays that.
    pub fn closed(&self) -> WaitForCancellationFutureOwned {
        self.inner.completion.0.clone().cancelled_owned()
    }

    pub(crate) fn physical_cursor(&self, scan: PhysicalScan<Bytes>) -> Result<SnapshotCursor> {
        self.check_active()?;
        let iterator = self
            .inner
            .snapshot
            .owned_scan(scan.clone(), ReadOptions::default());
        Ok(SnapshotCursor {
            iterator,
            scan,
            snapshot: self.clone(),
        })
    }
}

/// A raw iterator over one scan of the snapshot, driven by the caller. It keeps
/// the snapshot and its column families alive across pauses and can move
/// between threads. Every step blocks the calling thread.
pub struct SnapshotCursor {
    // Drop the raw iterator/read options before releasing the snapshot.
    iterator: PartitionSnapshotScan<'static>,
    scan: PhysicalScan<Bytes>,
    snapshot: ReadSnapshot,
}

impl SnapshotCursor {
    pub fn snapshot(&self) -> &ReadSnapshot {
        &self.snapshot
    }

    /// Borrows the current bytes without advancing or copying them.
    pub fn peek(&self) -> Result<Option<(&[u8], &[u8])>> {
        self.snapshot.check_active()?;
        self.iterator
            .item()
            .map_err(|error| StorageError::Generic(error.into()))
    }

    pub fn advance(&mut self) -> Result<()> {
        self.snapshot.check_active()?;
        self.iterator.next();
        Ok(())
    }

    /// Seeks forward within the cursor's original ownership/request bounds.
    pub fn seek_forward(&mut self, target: Bytes) -> Result<()> {
        let Some((key, _)) = self.peek()? else {
            return Ok(());
        };
        if target.as_ref() <= key || !self.scan.contains_key(&target) {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "scan navigation must advance and remain within its bounds"
            )));
        }
        self.iterator.seek(&target);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::FutureExt;

    use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
    use restate_core::io::IoExecutor;
    use restate_limiter::LimitKey;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
        filters::ScanEntryIdFilter,
        metadata::{VQueueLink, VQueueMeta},
        stats::EntryStatistics,
    };
    use restate_storage_query_api::scan::{BlockingScan, CursorScan, CursorStep, ReadCursor};
    use restate_types::ServiceName;
    use restate_types::identifiers::BaseEntryId;
    use restate_types::partitions::Partition;
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, Seq, VQueueId};

    use crate::PartitionStoreManager;
    use crate::keys::selection::KeySelection;
    use crate::keys::{EncodeTableKey, KeyKind};
    use crate::state_table::{ScopedStateKey, ScopedStateTable, StateKey, StateTable};

    use super::*;

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_snapshot_unifies_layouts_primary_and_auxiliary_reads() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let qid = VQueueId::custom(9, "snapshot");
        let id = BaseEntryId::new(9, EntryId::new(EntryKind::Invocation, [1; 16]));
        let at = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
            .unwrap();
        let mut legacy = Vec::new();
        StateKey {
            partition_key: 9,
            service_name: "svc".into(),
            service_key: "object".into(),
            state_key: Bytes::from_static(b"key"),
        }
        .serialize_to(&mut legacy);
        let mut scoped = Vec::new();
        ScopedStateKey {
            partition_key: 9,
            scope: None,
            service_name: "svc".into(),
            service_key: "object".into(),
            state_key: Bytes::from_static(b"key"),
        }
        .serialize_to(&mut scoped);
        let mut snapshots = Vec::new();
        for (sequence, service) in [(1, "before"), (2, "after")] {
            let key = EntryKey::new(false, at.to_unix_millis(), sequence, *id.as_entry_id());
            let mut tx = store.transaction();
            tx.raw_put_cf(KeyKind::State, &legacy, service.as_bytes());
            tx.raw_put_cf(KeyKind::ScopedState, &scoped, service.as_bytes());
            tx.create_vqueue(
                &qid,
                &VQueueMeta::new(
                    at,
                    None,
                    LimitKey::None,
                    VQueueLink::Service(ServiceName::new(service)),
                ),
            );
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::Service {
                        scope: None,
                        service,
                        handler: "run",
                    },
                },
                EntryStateRef {
                    stage: Stage::Inbox,
                    status: Status::New,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(at, key.run_at()),
                },
            );
            tx.commit().await.unwrap();
            drop(tx);
            snapshots.push(store.query_read(CancellationToken::new()).unwrap());
        }
        for ((read, completed), (sequence, service)) in
            snapshots.into_iter().zip([(1, "before"), (2, "after")])
        {
            assert_eq!(
                read.map_vqueue_meta_blocking(&qid, |meta| meta.service_name().unwrap().to_owned())
                    .unwrap()
                    .as_deref(),
                Some(service),
                "auxiliary_lookup_uses_admitted_snapshot"
            );
            for filter in [
                ScanEntryIdFilter::PartitionKey(KeyRange::FULL),
                ScanEntryIdFilter::EntryIdSet([id].into()),
            ] {
                let caller = std::thread::current().id();
                let mut rows = Vec::new();
                read.for_each_vqueue_entry_status_blocking(filter, |id, header| {
                    assert_eq!(
                        std::thread::current().id(),
                        caller,
                        "read_runs_on_callers_thread"
                    );
                    rows.push((*id, header.seq));
                    ControlFlow::Continue(())
                })
                .unwrap();
                assert_eq!(
                    rows,
                    [(id, Seq::new(sequence))],
                    "scan_and_primary_lookup_see_same_incarnation"
                );
            }
            let (tx, rx) = std::sync::mpsc::channel();
            let scoped_tx = tx.clone();
            <ReadSnapshot as BlockingScan<StateTable>>::scan_blocking(
                &read,
                Arc::new(KeySelection::all()),
                move |(_, value)| {
                    tx.send(value.to_vec()).unwrap();
                    ControlFlow::Continue(())
                },
            )
            .unwrap();
            <ReadSnapshot as BlockingScan<ScopedStateTable>>::scan_blocking(
                &read,
                Arc::new(KeySelection::all()),
                move |(_, value)| {
                    scoped_tx.send(value.to_vec()).unwrap();
                    ControlFlow::Continue(())
                },
            )
            .unwrap();
            assert_eq!(
                rx.into_iter().collect::<Vec<_>>(),
                [service.as_bytes(), service.as_bytes()],
                "both_layouts_share_admission_time"
            );
            // The cursor is an ordinary owner: dropping the originating handle
            // does not invalidate its borrowed records or change visibility.
            let mut cursor = <ReadSnapshot as CursorScan<StateTable>>::scan_cursor(
                &read,
                Arc::new(KeySelection::all()),
            )
            .unwrap();
            let mut lookup = read
                .entry_status_cursor(ScanEntryIdFilter::EntryIdSet([id].into()))
                .unwrap();
            assert_eq!(lookup.advance().unwrap(), CursorStep::Record);
            drop(read);
            let mut completed = Box::pin(completed);
            assert!(
                completed.as_mut().now_or_never().is_none(),
                "returned_cursor_pins_its_snapshot"
            );
            // Moving an already-positioned pinned lookup and a raw iterator to
            // another thread is the same ownership handoff used between quanta.
            std::thread::spawn(move || {
                assert_eq!(cursor.advance().unwrap(), CursorStep::Record);
                assert_eq!(
                    cursor.current().unwrap().1,
                    service.as_bytes(),
                    "resumed_cursor_preserves_snapshot"
                );
                assert_eq!(
                    lookup.current().unwrap().1.seq,
                    Seq::new(sequence),
                    "pinned_primary_survives_cross_thread_pause"
                );
                assert_eq!(cursor.advance().unwrap(), CursorStep::Done);
            })
            .join()
            .unwrap();
            tokio::time::timeout(Duration::from_secs(5), completed)
                .await
                .unwrap();
        }
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_completion_waits_for_storage_after_waiter_and_capability_drop() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut key = Vec::new();
        StateKey {
            partition_key: 9,
            service_name: "svc".into(),
            service_key: "object".into(),
            state_key: Bytes::from_static(b"key"),
        }
        .serialize_to(&mut key);
        let mut tx = store.transaction();
        tx.raw_put_cf(KeyKind::State, key, b"value");
        tx.commit().await.unwrap();
        drop(tx);
        let cancel = CancellationToken::new();
        let (read, completed) = store.query_read(cancel.clone()).unwrap();
        let mut completed = Box::pin(completed);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let (release, release_rx) = std::sync::mpsc::sync_channel(0);
        let snapshot = read.clone();
        let storage_task = store
            .partition_db()
            .rocksdb()
            .executor()
            .low_pri()
            .spawn_fn("query-completion-test", move || {
                <ReadSnapshot as BlockingScan<StateTable>>::scan_blocking(
                    &snapshot,
                    Arc::new(KeySelection::all()),
                    |_| {
                        started.take().unwrap().send(()).unwrap();
                        release_rx.recv().unwrap();
                        ControlFlow::Continue(())
                    },
                )
            })
            .unwrap();
        let waiter = tokio::spawn(storage_task);
        started_rx.await.unwrap();
        cancel.cancel();
        assert!(
            read.map_vqueue_meta_blocking(&VQueueId::custom(9, "cancelled"), |_| ())
                .is_err(),
            "cancelled_capability_rejects_new_reads"
        );
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        drop(read);
        assert!(
            completed.as_mut().now_or_never().is_none(),
            "dropped_waiter_is_not_storage_completion"
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), completed)
            .await
            .unwrap();
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_teardown_waits_for_cursors_and_other_partitions_remain_readable() {
        RocksDbManager::init();
        // Exercise the same partition-owned drain with shared and exclusive DBs.
        for multi_db in [false, true] {
            let manager = PartitionStoreManager::create(multi_db).await.unwrap();
            let partition = Partition::new(
                PartitionId::from(if multi_db { 4 } else { 2 }),
                KeyRange::new(0, 9),
            );
            let other = Partition::new(
                PartitionId::from(if multi_db { 5 } else { 3 }),
                KeyRange::new(10, u64::MAX),
            );
            let store = manager.open(&partition, None).await.unwrap();
            let other_store = manager.open(&other, None).await.unwrap();
            let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
            let cursor = <ReadSnapshot as CursorScan<StateTable>>::scan_cursor(
                &read,
                Arc::new(KeySelection::all()),
            )
            .unwrap();
            let cancel = read.cancellation_token();
            drop(read);

            let mut dropping = Box::pin(manager.close(partition.partition_id));
            assert!(dropping.as_mut().now_or_never().is_none());
            assert!(
                store.query_read(CancellationToken::new()).is_err(),
                "closed_domain_rejects_new_snapshots"
            );
            assert!(!cancel.is_cancelled(), "closing_admission_preserves_grace");
            assert!(
                dropping.as_mut().now_or_never().is_none(),
                "destructive_drop_waits_for_cursor_release"
            );
            assert!(
                !cancel.is_cancelled(),
                "session_close_preserves_admitted_readers"
            );
            assert!(
                store
                    .partition_db()
                    .rocksdb()
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_some(),
                "column_family_survives_until_readers_release_it"
            );
            let (other_read, _) = other_store.query_read(CancellationToken::new()).unwrap();
            let mut other_cursor = <ReadSnapshot as CursorScan<StateTable>>::scan_cursor(
                &other_read,
                Arc::new(KeySelection::all()),
            )
            .unwrap();
            assert_eq!(
                other_cursor.advance().unwrap(),
                CursorStep::Done,
                "other_partition_keeps_making_progress"
            );
            drop(other_read);
            drop(cursor);
            tokio::time::timeout(Duration::from_secs(5), closed)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), dropping)
                .await
                .unwrap();
            assert!(
                store
                    .partition_db()
                    .rocksdb()
                    .inner()
                    .cf_handle(partition.cf_name().as_ref())
                    .is_some(),
                "session_close_preserves_physical_storage"
            );

            let reopened = manager.open(&partition, None).await.unwrap();
            let (read, _) = reopened.query_read(CancellationToken::new()).unwrap();
            assert!(
                !read.cancellation_token().is_cancelled(),
                "replacement_has_fresh_admission_domain"
            );
            assert!(
                store.query_read(CancellationToken::new()).is_err(),
                "old_store_cannot_admit_against_replacement"
            );
            drop(read);

            // Interrupting the close waiter cannot let reopen forget old readers.
            let (read, _) = reopened.query_read(CancellationToken::new()).unwrap();
            let mut close = Box::pin(manager.close(partition.partition_id));
            assert!(close.as_mut().now_or_never().is_none());
            drop(close);
            let mut reopen = Box::pin(manager.open(&partition, None));
            assert!(
                reopen.as_mut().now_or_never().is_none(),
                "reopen_waits_after_cancelled_close_waiter"
            );
            drop(read);
            let fresh = tokio::time::timeout(Duration::from_secs(5), reopen)
                .await
                .unwrap()
                .unwrap();
            let (read, _) = fresh.query_read(CancellationToken::new()).unwrap();
            drop(read);
            drop(fresh);
            drop(reopened);
            drop(store);
            drop(other_store);
            manager.drop_partition_store(&partition).await.unwrap();
            manager.drop_partition_store(&other).await.unwrap();
        }
    }
}
