// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads of a partition through one admitted snapshot.
//!
//! # Concepts
//!
//! - **Read snapshot** ([`ReadSnapshot`]): what a read sees of a partition: a
//!   RocksDB snapshot together with the storage features and the index registry
//!   as they were when the processor admitted the read. A read therefore never
//!   sees an index in a state newer than its data. Clones share all of it.
//! - **Cancellation**: the processor's token or the read's child gate ends the
//!   read; from then on data reads and cursor steps fail. Parent cancellation
//!   reaches the read, while sealing alone allows admitted work to finish.
//!   Looking an index up in the captured registry still works.
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
//! Direct reads and cursor methods block the calling thread. [`ReadSnapshot::scan`]
//! instead submits a bounded, cooperative scan to the query-owned [`QueryIoExecutor`],
//! where a typed consumer can await output capacity while borrowing a record.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use rocksdb::ReadOptions;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use restate_core::io::{Cancelled, FutureTask, Gate, OwnedLease, TrackedTask};
use restate_rocksdb::RocksDbPerfExt;
use restate_storage_api::StorageError;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_query_api::scan::{CursorScan, Scan, Table, consume_cursor};
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::features::StorageFeatures;
use crate::fsm_table::{fsm_variable, migration_jobs_key, registry_key};
use crate::job::{MigrationJobId, MigrationJobStatus, MigrationJobs};
use crate::keys::KeyKind;
use crate::registry::{Index, IndexKind, IndexNotReady, IndexRegistry, IndexStatus};
use crate::scan::PhysicalScan;
use crate::table::{IndexSource, ReadMetadata};
use crate::{PartitionDb, PartitionReadSnapshot, PartitionSnapshotScan, Result};

mod executor;

pub use executor::QueryIoExecutor;

/// A read's view of one partition, admitted by its processor: the RocksDB
/// snapshot plus the storage features and the index registry of that moment.
/// Clones share the snapshot, its admission, cancellation and completion. Direct
/// reads block the caller; [`Self::scan`] runs on the shared query IO pool.
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
    /// The unfinished jobs as of `snapshot`, decoded on first use.
    jobs: OnceLock<std::result::Result<MigrationJobs, String>>,
    /// The processor's token for ending this read.
    cancel: CancellationToken,
    /// This read's child of the partition gate; `ReadSnapshot::cancel` cancels it.
    gate: Arc<Gate>,
    /// Observes cancellation of this read's gate or its parent partition gate.
    cancelled: Cancelled,
    // Last resource guard: the native snapshot above is released before the
    // query's admission, then completion wakes the processor.
    lease: OwnedLease,
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
        let gate = db.gate().child();
        let (lease, cancelled) = gate
            .try_acquire_cancellable()
            .ok_or(StorageError::OperationalError)?;
        let lease = lease.into_owned();
        Ok(Self {
            inner: Arc::new(QuerySnapshot {
                snapshot,
                features,
                registry: OnceLock::new(),
                jobs: OnceLock::new(),
                cancel,
                gate,
                cancelled,
                lease,
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
        if self.inner.cancel.is_cancelled() || self.inner.cancelled.is_cancelled() {
            return Err(StorageError::ScanInterrupted);
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

    /// How far the backfill of the Backfilling `index` is in this snapshot, from
    /// its build job's record.
    fn backfill_status(&self, index: &Index) -> String {
        let jobs = self.inner.jobs.get_or_init(|| {
            let key = migration_jobs_key(self.partition_id(), fsm_variable::MIGRATION_JOBS);
            self.inner
                .snapshot
                .reader()
                .get_pinned(KeyKind::Fsm, &key)
                .map_err(|err| err.to_string())?
                .map(|value| MigrationJobs::decode(&value).map_err(|err| err.to_string()))
                .transpose()
                .map(Option::unwrap_or_default)
        });
        let job = jobs
            .as_ref()
            .ok()
            .and_then(|jobs| jobs.get(MigrationJobId::for_index(index.id)));
        let Some(walk) = index.walk else {
            // Between passes, or not started yet.
            return match job {
                Some(job) if job.status == MigrationJobStatus::Waiting => {
                    match job.rows_uncovered {
                        Some(rows) => format!(
                            "it waits for its next pass; the last full pass found {rows} entries it couldn't cover"
                        ),
                        None => "it waits for its next pass".to_owned(),
                    }
                }
                _ => "its backfill has not started".to_owned(),
            };
        };
        if let Err(err) = jobs {
            return format!("backfill walk {walk}; unreadable job records: {err}");
        }
        let Some(job) = job else {
            return format!("backfill walk {walk}; its job record is missing");
        };
        let mut status = format!(
            "backfill walk {walk} has examined {} entries",
            job.rows_examined
        );
        if let Some(error) = &job.last_error {
            status.push_str(&format!(
                "; failed attempts: {}, the last with: {error}",
                job.failures
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

    /// Cancels this partition's read and its scans, without cancelling another
    /// query or its parent partition. Drop all snapshot holders and await
    /// [`Self::closed`] to know that storage cleanup has finished.
    pub fn cancel(&self) {
        self.inner.gate.cancel();
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

impl Scan for ReadSnapshot {
    type Executor = QueryIoExecutor;

    /// Submits a prepared scan to the shared query IO pool.
    ///
    /// Submission is eager and shares this snapshot's existing admission. Sealing
    /// the partition refuses new snapshots but lets an admitted query submit its
    /// remaining scans. Explicit cancellation interrupts output waits and slice
    /// yields. Opening the cursor, physical navigation and consumption all run on
    /// the IO worker, using the same snapshot for every scan of this capability.
    ///
    /// Dropping the future releases this scan's snapshot holders and consumer
    /// immediately if no worker has claimed it. Otherwise the worker stops at
    /// its next wait or yield, after any blocking storage call returns.
    ///
    /// A returned consumer that owns storage resources must retain their snapshot
    /// and lease too. Release those holders and all snapshot clones, then await
    /// [`Self::closed`] to know that storage cleanup has finished.
    fn scan<T, C>(
        &self,
        executor: &QueryIoExecutor,
        name: &'static str,
        request: Arc<T::Scan>,
        consumer: C,
    ) -> Result<impl Future<Output = Result<ScanOutcome<C>>> + Send + 'static>
    where
        T: Table,
        Self: CursorScan<T>,
        C: for<'a> RowConsumer<Item<'a> = T::Record<'a>>,
    {
        self.check_active()?;
        let lease = self.inner.lease.fork();
        let snapshot = self.clone();
        let cancel = self.inner.cancel.clone();
        let cancelled = self.inner.cancelled.clone();
        let task = TrackedTask::new(
            FutureTask(move || async move {
                tokio::select! {
                    biased;
                    _ = interrupted(&snapshot.inner.cancel, &snapshot.inner.cancelled) => {
                        Err(StorageError::ScanInterrupted)
                    }
                    result = async {
                        let cursor = <Self as CursorScan<T>>::scan_cursor(&snapshot, request)?;
                        consume_cursor::<T, C>(cursor, consumer).await
                    }.with_read_perf(name) => result,
                }
            }),
            lease,
        );
        let result = executor.spawn(task)?;
        Ok(async move {
            // The waiter also observes cancellation, so an unclaimed task can
            // release its snapshot even while all IO workers are blocked.
            tokio::select! {
                biased;
                _ = interrupted(&cancel, &cancelled) => Err(StorageError::ScanInterrupted),
                result = result => result?,
            }
        })
    }
}

/// Completes when the processor's token or the read's or partition's gate is cancelled.
async fn interrupted(cancel: &CancellationToken, cancelled: &Cancelled) {
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = cancelled.cancelled() => {}
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

    use bytes::Bytes;
    use futures::FutureExt;

    use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
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
    use restate_storage_query_api::test_util::{BindingTableProvider, datafusion};
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
    async fn query_binding_sql_preserves_residual_limit_and_snapshot_cancellation() {
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::arrow::util::display::array_value_to_string;
        use datafusion::prelude::SessionContext;

        use crate::state_table::StateBinding;

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut tx = store.transaction();
        for state_key in [b"a".as_slice(), b"b", b"cc", b"dd"] {
            let mut key = Vec::new();
            StateKey {
                partition_key: 9,
                service_name: "svc".into(),
                service_key: "object".into(),
                state_key: Bytes::copy_from_slice(state_key),
            }
            .serialize_to(&mut key);
            tx.raw_put_cf(KeyKind::State, &key, b"value");
        }
        tx.commit().await.unwrap();
        drop(tx);

        let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
        let read = Arc::new(read);
        let executor = Arc::new(QueryIoExecutor::new(
            std::num::NonZeroUsize::new(1).unwrap(),
        ));
        let ctx = SessionContext::new();
        ctx.register_table(
            "actual",
            Arc::new(BindingTableProvider::<StateBinding, _>::new(
                Arc::clone(&read),
                executor,
            )),
        )
        .unwrap();
        // Equality can restrict the read, but length(key) is a residual. The
        // first physical rows fail it, so LIMIT cannot be pushed below it.
        let rows = ctx
            .sql(
                "SELECT key FROM actual
                 WHERE service_name = 'svc' AND length(key) = 2
                 ORDER BY key LIMIT 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = ctx
            .sql(
                "SELECT key FROM (VALUES ('a'), ('b'), ('cc'), ('dd')) AS reference(key)
                 WHERE length(key) = 2
                 ORDER BY key LIMIT 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values = |batches: Vec<RecordBatch>| {
            batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows())
                        .map(|row| array_value_to_string(batch.column(0), row).unwrap())
                })
                .collect::<Vec<_>>()
        };
        let rows = values(rows);
        assert_eq!(rows, values(expected));
        assert_eq!(rows, ["cc"]);
        read.cancel();
        assert!(
            ctx.sql("SELECT key FROM actual")
                .await
                .unwrap()
                .collect()
                .await
                .is_err()
        );
        drop(ctx);
        drop(read);
        closed.await;
    }

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
            completed.await;
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

        #[derive(Default)]
        struct Consumer {
            started: Option<tokio::sync::oneshot::Sender<()>>,
            blocking: Option<std::sync::mpsc::Receiver<()>>,
            suspended: Option<tokio::sync::oneshot::Receiver<()>>,
            values: Vec<Bytes>,
        }

        impl RowConsumer for Consumer {
            type Item<'a> = <StateTable as Table>::Record<'a>;

            async fn consume(&mut self, (_, value): Self::Item<'_>) -> Result<ControlFlow<()>> {
                assert_eq!(std::thread::current().name(), Some("rs:query-io"));
                if let Some(started) = self.started.take() {
                    started.send(()).unwrap();
                }
                if let Some(release) = self.blocking.take() {
                    release.recv().unwrap();
                }
                if let Some(release) = self.suspended.take() {
                    release.await.unwrap();
                }
                self.values.push(Bytes::copy_from_slice(value));
                Ok(ControlFlow::Continue(()))
            }
        }

        let executor = QueryIoExecutor::new(std::num::NonZeroUsize::new(1).unwrap());
        let request = || Arc::new(KeySelection::all());

        // An unpolled, unclaimed scan releases its admitted snapshot immediately
        // on drop, even with the only worker blocked in unrelated native work.
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let blocker = executor
            .spawn(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
            })
            .unwrap();
        start.await.unwrap();
        let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
        let queued = read
            .scan::<StateTable, _>(&executor, "queued-scan", request(), Consumer::default())
            .unwrap();
        drop(read);
        drop(queued);
        assert!(
            closed.now_or_never().is_some(),
            "queued_snapshot_released_without_a_free_worker"
        );
        release.send(()).unwrap();
        blocker.await.unwrap();

        // Once claimed, a blocking visit owns its resources until it returns.
        let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let running = read
            .scan::<StateTable, _>(
                &executor,
                "running-scan",
                request(),
                Consumer {
                    started: Some(started),
                    blocking: Some(released),
                    ..Consumer::default()
                },
            )
            .unwrap();
        start.await.unwrap();
        drop(read);
        drop(running);
        let mut closed = Box::pin(closed);
        assert!(closed.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        closed.await;

        // Admission is shared by the query's scans. Sealing preserves those
        // admitted reads; cancelling one child leaves its sibling usable.
        let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
        let (other, other_closed) = store.query_read(CancellationToken::new()).unwrap();
        store.partition_db().gate().seal();
        assert!(store.query_read(CancellationToken::new()).is_err());
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let waiting = read
            .scan::<StateTable, _>(
                &executor,
                "suspended-scan",
                request(),
                Consumer {
                    started: Some(started),
                    suspended: Some(released),
                    ..Consumer::default()
                },
            )
            .unwrap();
        start.await.unwrap();
        read.cancel();
        assert!(matches!(waiting.await, Err(StorageError::ScanInterrupted)));
        assert!(matches!(
            read.map_vqueue_meta_blocking(&VQueueId::custom(9, "cancelled"), |_| ()),
            Err(StorageError::ScanInterrupted)
        ));
        drop(read);
        closed.await;
        assert!(release.send(()).is_err(), "suspended_consumer_was_dropped");
        let outcome = other
            .scan::<StateTable, _>(&executor, "peer-scan", request(), Consumer::default())
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(outcome, ScanOutcome::Exhausted(_)));
        assert_eq!(outcome.into_inner().values, [Bytes::from_static(b"value")]);

        store.partition_db().gate().cancel();
        assert!(matches!(
            other.scan::<StateTable, _>(
                &executor,
                "cancelled-peer",
                request(),
                Consumer::default()
            ),
            Err(StorageError::ScanInterrupted)
        ));
        drop(other);
        other_closed.await;
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
            closed.await;
            dropping.await;
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
            let fresh = reopen.await.unwrap();
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
