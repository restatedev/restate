// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod admission;
mod background;
#[cfg(test)]
mod close_tests;
pub mod configuration;
mod db_env;
mod db_manager;
mod db_spec;
mod error;
mod iterator;
mod logging;
mod metric_definitions;
mod perf;
mod rock_access;
#[cfg(test)]
mod write_tests;

use std::future::Future;
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::Arc;

use metrics::counter;
use parking_lot::Mutex;
use rocksdb::ExportImportFilesMetaData;
use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use restate_core::ShutdownError;
use restate_core::io::{
    AdmissionClosed, FlightTracker, IoExecutor, Lease, OwnedLease, TrackedExecutor, TrackedTask,
};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::protobuf::common::DatabaseKind;
pub use restate_types::rocksdb::{BottommostLevelCompaction, ManualCompactionOptions};

// re-exports
use self::admission::AdmissionScopes;
pub use self::admission::{DatabaseLease, DatabaseScope};
pub use self::background::{
    GlobalExecutor, ReadyStorageTask, SinglePoolExecutor, StorageOp, StorageTaskKind,
};
pub use self::db_env::DbEnv;
pub use self::db_manager::{DbRef, RocksDbManager, WeakRocksDbManager};
pub use self::db_spec::*;
pub use self::error::*;
pub use self::iterator::IterAction;
use self::iterator::RocksIterator;
pub use self::perf::{RocksDbReadPerfGuard, RocksDbWritePerfGuard};
pub use self::rock_access::RocksAccess;

use self::background::StorageTask;
use self::metric_definitions::*;

pub type RawRocksDb = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;
type BoxedCfMatcher = Box<dyn CfNameMatch + Send + Sync>;

pub use restate_core::io::Priority;

/// Defines how to perform a potentially blocking rocksdb IO operation.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
pub enum IoMode {
    /// [Dangerous] Allow blocking IO operation to happen in the worker thread (tokio)
    AllowBlockingIO,
    /// Fail the operation if operation needs to block on IO
    OnlyIfNonBlocking,
    /// Always perform the IO operation in the background thread pool
    AlwaysBackground,
    /// Attempts to perform the operation without blocking IO in worker thread, if it's not
    /// possible, it'll spawn work in the background thread pool.
    #[default]
    Default,
}

// Field order releases operation-owned resources before completing admission, even
// when execution fails, unwinds, or an unsubmitted task is dropped. Keep the entire
// operation together when capturing it in a background closure.
struct WriteOperation<D, B, OP, L> {
    batch: B,
    options: rocksdb::WriteOptions,
    write_op: OP,
    db: D,
    // The lease from the database or one of its scopes. It moves with the write from
    // the inline attempt to a background retry, so the retry isn't rejected if the
    // database is sealed in between.
    lease: L,
}

impl<D, B, OP, L> WriteOperation<D, B, OP, L>
where
    D: AsRef<RocksDb>,
    OP: Fn(&RocksAccess, &B, &rocksdb::WriteOptions) -> Result<(), rocksdb::Error>,
{
    fn apply(&self) -> Result<(), rocksdb::Error> {
        (self.write_op)(&self.db.as_ref().db, &self.batch, &self.options)
    }

    fn run(self, name: &'static str) -> Result<B, rocksdb::Error> {
        let _perf = RocksDbWritePerfGuard::new(name);
        self.apply()?;
        Ok(self.batch)
    }
}

impl<B, OP> WriteOperation<&Arc<RocksDb>, B, OP, Lease<'_>> {
    fn into_background(self) -> WriteOperation<Arc<RocksDb>, B, OP, OwnedLease> {
        WriteOperation {
            batch: self.batch,
            options: self.options,
            write_op: self.write_op,
            db: Arc::clone(self.db),
            lease: self.lease.into_owned(),
        }
    }
}

#[derive(derive_more::Display, derive_more::Debug)]
#[display("{:?}", db)]
#[debug("{:?}", db)]
pub struct RocksDb {
    executor: GlobalExecutor,
    background_tracker: Arc<FlightTracker>,
    admission_scopes: Mutex<AdmissionScopes>,
    db: ManuallyDrop<RocksAccess>,
    closed: CancellationToken,
    registration: Option<DbRegistration>,
}

/// Links a database to its record in the manager. Set once, before the first
/// `Arc<RocksDb>` is handed out. Keeps the manager alive until the database is
/// closed, and carries the record's `closed` signal, so callers that started
/// waiting on [`RocksDbManager::closed`] while the open was still running are
/// woken too.
struct DbRegistration {
    manager: RocksDbManager,
    tracked: DbRef,
    closed: CancellationToken,
}

/// Closes the native database when dropped, whether its cleanup job runs or is
/// discarded from the queue. The `closed` signal fires only after native
/// destruction returns; if destruction panics, it never fires.
pub(crate) struct DbCleanup {
    db: Option<RocksAccess>,
    closed: CancellationToken,
    registration: Option<DbRegistration>,
    needs_shutdown: bool,
}

impl Drop for DbCleanup {
    fn drop(&mut self) {
        let db = self.db.take().expect("cleanup runs once");
        if self.needs_shutdown {
            db.shutdown();
        }
        drop(db);
        if let Some(registration) = &self.registration {
            registration
                .manager
                .finish_close(registration.tracked, registration.closed.clone());
        }
        self.closed.cancel();
    }
}

impl Drop for RocksDb {
    fn drop(&mut self) {
        // Safety: self.db is exclusives "taken" at drop time, so we know that it's
        // still valid. We don't provide &mut access to db, and it's not a public field.
        let inner_db = unsafe { ManuallyDrop::take(&mut self.db) };

        let cleanup = DbCleanup {
            db: Some(inner_db),
            closed: self.closed.clone(),
            registration: self.registration.take(),
            needs_shutdown: true,
        };
        if let Some(registration) = &cleanup.registration {
            registration.manager.clone().close_db(cleanup);
        } else {
            // No manager: close the database right here, on this thread.
            drop(cleanup);
        }
    }
}

static_assertions::assert_impl_all!(RocksDb: Send, Sync);

impl RocksDb {
    pub fn name(&self) -> &DbName {
        &self.db.spec().name
    }

    pub fn kind(&self) -> DatabaseKind {
        self.db.spec().kind
    }

    /// Wraps an open native database. To put it under a manager, pass it to
    /// [`RocksDbManager::attach_db`] before sharing it. Otherwise it stays
    /// standalone, and dropping its last `Arc` closes it on the dropping thread.
    pub fn new(db: RocksAccess, executor: GlobalExecutor) -> Self {
        Self {
            executor,
            background_tracker: Arc::default(),
            admission_scopes: Mutex::default(),
            db: ManuallyDrop::new(db),
            closed: CancellationToken::new(),
            registration: None,
        }
    }

    /// This database's [`DbRef`] in its manager, or `None` if it has no manager.
    pub fn registration(&self) -> Option<DbRef> {
        self.registration
            .as_ref()
            .map(|registration| registration.tracked)
    }

    /// Creates a new [`DatabaseScope`] for this database, e.g. one per partition.
    /// A scope created after the database is sealed starts out sealed, so it's safe
    /// to register scopes during shutdown. Registering takes a lock, but operations
    /// in a scope never do.
    pub fn register_scope(self: &Arc<Self>) -> Arc<DatabaseScope> {
        let mut scopes = self.admission_scopes.lock();
        let tracker = Arc::new(FlightTracker::default());
        if scopes.sealed {
            tracker.seal();
        } else {
            scopes
                .trackers
                .retain(|tracker| tracker.strong_count() != 0);
            scopes.trackers.push(Arc::downgrade(&tracker));
        }
        Arc::new(DatabaseScope::new(self, tracker))
    }

    /// Permanently rejects new operations on this database and in all its scopes.
    /// Operations that were already accepted keep running, and code that uses the
    /// raw RocksDB handle directly isn't affected.
    pub fn seal(&self) {
        let mut scopes = self.admission_scopes.lock();
        scopes.sealed = true;
        self.background_tracker.seal();
        for tracker in scopes.trackers.iter().filter_map(std::sync::Weak::upgrade) {
            tracker.seal();
        }
    }

    /// Seals the database and waits until every accepted operation, including work
    /// in registered scopes, has finished and released its resources. This doesn't
    /// wait for the database to close, or for `Arc<RocksDb>` handles held elsewhere
    /// to be dropped.
    pub async fn drain(&self) {
        self.seal();
        let scopes: Vec<_> = self
            .admission_scopes
            .lock()
            .trackers
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        self.background_tracker.drain().await;
        for tracker in scopes {
            tracker.drain().await;
        }
    }

    #[cfg(test)]
    fn tracked_executor(&self) -> TrackedExecutor<'_, GlobalExecutor> {
        TrackedExecutor::new(&self.executor, &self.background_tracker)
    }

    pub(crate) fn note_config_update(&self) {
        let configurator = &self.db.spec().db_configurator;
        configurator.note_config_update(&self.db);
    }

    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    ///
    /// todo: remove this once all access is migrated to this abstraction
    pub fn inner(self: &Arc<Self>) -> &RocksAccess {
        &self.db
    }

    pub fn cfs(&self) -> Vec<CfName> {
        self.db.cfs()
    }

    /// Write a [`rocksdb::WriteBatch`] to the database.
    ///
    /// On success, returns the batch so callers can reuse it (e.g. via
    /// [`rocksdb::WriteBatch::clear()`]) without re-allocating.
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatch,
    ) -> Result<rocksdb::WriteBatch, RocksError> {
        self.write_batch_internal(
            name,
            priority,
            io_mode,
            WriteOperation {
                batch: write_batch,
                options: write_options,
                write_op: RocksAccess::write_batch,
                db: self,
                lease: self
                    .background_tracker
                    .try_acquire()
                    .ok_or(AdmissionClosed)?,
            },
        )
        .await
    }

    /// Write a [`rocksdb::WriteBatch`] under a lease from one of this database's
    /// [`DatabaseScope`]s.
    ///
    /// The lease stays held until the write finishes, including when it is retried
    /// on a background thread. If the returned future is dropped before a background
    /// write starts, the write is skipped; a write that already started completes.
    ///
    /// # Panics
    /// Panics if the lease belongs to a different database.
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch_leased(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatch,
        lease: DatabaseLease<'_>,
    ) -> Result<rocksdb::WriteBatch, RocksError> {
        self.write_batch_internal(
            name,
            priority,
            io_mode,
            WriteOperation {
                batch: write_batch,
                options: write_options,
                write_op: RocksAccess::write_batch,
                db: self,
                lease: lease.into_lease(self),
            },
        )
        .await
    }

    /// Write a [`rocksdb::WriteBatchWithIndex`] to the database.
    ///
    /// On success, returns the batch so callers can reuse it. The lease stays held
    /// until the write finishes, including when it is retried on a background thread.
    /// If the returned future is dropped before a background write starts, the write
    /// is skipped; a write that already started completes.
    ///
    /// # Panics
    /// Panics if the lease belongs to a different database.
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch_with_index(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatchWithIndex,
        lease: DatabaseLease<'_>,
    ) -> Result<rocksdb::WriteBatchWithIndex, RocksError> {
        self.write_batch_internal(
            name,
            priority,
            io_mode,
            WriteOperation {
                batch: write_batch,
                options: write_options,
                write_op: RocksAccess::write_batch_with_index,
                db: self,
                lease: lease.into_lease(self),
            },
        )
        .await
    }

    async fn write_batch_internal<B, OP>(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        mut write: WriteOperation<&Arc<Self>, B, OP, Lease<'_>>,
    ) -> Result<B, RocksError>
    where
        B: Send + 'static,
        OP: Fn(&RocksAccess, &B, &rocksdb::WriteOptions) -> Result<(), rocksdb::Error>
            + Send
            + 'static,
    {
        //  depending on the IoMode, we decide how to do the write.
        match io_mode {
            IoMode::AllowBlockingIO => {
                let _x = RocksDbWritePerfGuard::new(name);
                debug!(
                    "Blocking IO is allowed for write_batch, stall detection will not be used in this operation!"
                );
                write.options.set_no_slowdown(false);
                if let Err(e) = write.apply() {
                    return Err(e.into());
                }
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_MAYBE_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(write.batch);
            }
            IoMode::AlwaysBackground => {
                // Operation will block, dispatch to background.
                // In the background thread pool we can block on IO
                write.options.set_no_slowdown(false);
                let write = write.into_background();
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || write.run(name))
                    .build()
                    .unwrap();

                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_BACKGROUND,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);

                return self
                    .executor
                    .spawn(task)?
                    .await
                    .map_err(|_| RocksError::Shutdown(ShutdownError))?
                    .map_err(|e| e.into());
            }
            IoMode::OnlyIfNonBlocking => {
                let _x = RocksDbWritePerfGuard::new(name);
                write.options.set_no_slowdown(true);
                if let Err(e) = write.apply() {
                    return Err(e.into());
                }
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_NON_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(write.batch);
            }
            _ => {}
        }

        // Auto...
        // First, attempt to write without blocking
        write.options.set_no_slowdown(true);

        let perf_guard = RocksDbWritePerfGuard::new(name);
        let result = write.apply();
        match result {
            Ok(_) => {
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_NON_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                Ok(write.batch)
            }
            Err(e) if is_retryable_error(e.kind()) => {
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_MOVED_TO_BG,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                // We chose to not measure perf for this operation since it might skew telemetry.
                // We might change this in the future, but in that case, we need to use a different
                // StorageOpKind to differentiate the two.
                perf_guard.forget();
                // Operation will block, dispatch to background.
                // In the background thread pool we can block on IO
                write.options.set_no_slowdown(false);
                let write = write.into_background();
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || write.run(name))
                    .build()
                    .unwrap();

                self.executor
                    .spawn(task)?
                    .await
                    .map_err(|_| RocksError::Shutdown(ShutdownError))?
                    .map_err(|e| e.into())
            }
            Err(e) => {
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_FAILED,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                Err(e.into())
            }
        }
    }

    /// Runs an iterator on a background thread, counted against `scope`. Whatever
    /// `on_item` captures is dropped after the iterator and before the work counts as
    /// finished, so draining the scope also waits for those captures.
    ///
    /// # Panics
    /// Panics if the scope belongs to a different database.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub fn run_background_iterator(
        self: Arc<Self>,
        scope: &DatabaseScope,
        cf: CfName,
        name: &'static str,
        priority: Priority,
        initial_action: IterAction,
        mut read_options: rocksdb::ReadOptions,
        mut on_item: impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
    ) -> Result<(), AdmissionClosed> {
        // ensure that we allow blocking IO.
        read_options.set_read_tier(rocksdb::ReadTier::All);

        let executor = self.executor.clone();
        let tracker = scope.tracker_for(&self);
        let task = StorageTask::default()
            .kind(StorageTaskKind::BackgroundIterator)
            .priority(priority)
            .op(move || {
                // note: the perf guard's lifetime encapsulates all operations in the iterator and
                // the total duration includes all the time spent blocking on the tx's capacity.
                let _x = RocksDbReadPerfGuard::new(name);
                let Some(cf) = self.db.cf_handle(cf.as_str()) else {
                    on_item(Err(RocksError::UnknownColumnFamily(cf)));
                    return;
                };
                let mut iter = RocksIterator::new(
                    self.db.as_raw_db().raw_iterator_cf_opt(&cf, read_options),
                    initial_action,
                    // Borrow `on_item` instead of moving it in, so what it captures is dropped
                    // only after the iterator and the CF handle are gone.
                    &mut on_item,
                );
                loop {
                    match iter.step() {
                        iterator::Disposition::WouldBlock => {
                            // we should not be here if the iterator is configured correctly.
                            // as we are already in the storage thread pool, we expect the
                            // iterator to be blocking.
                            //
                            // Why do we panic? because this iterator will be stopped short of all
                            // values it should return and we don't want to mislead readers that
                            // they have consumed the iterator cleanly.
                            panic!(
                                "RocksDB iterator is reporting that it WouldBlock but it's \
                                already on the storage thread pool. This is a bug!"
                            );
                        }
                        iterator::Disposition::Continue => continue,
                        iterator::Disposition::Stop => return,
                    }
                }
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, tracker).dispatch(task)
    }

    /// The shared I/O executor this database was created with. Handy for submitting
    /// operations prepared with [`Self::prepare_op`].
    pub fn executor(&self) -> &GlobalExecutor {
        &self.executor
    }

    /// Prepares an operation on this database without running it yet.
    ///
    /// Run it on the current thread with [`StorageOp::call`], or turn it into a
    /// background task with [`StorageOp::background`] and submit it to any
    /// [`IoExecutor`]. Admission is checked at that point, not now: a prepared
    /// operation doesn't delay [`Self::drain`], and if the database is sealed first,
    /// calling or submitting it fails with [`AdmissionClosed`].
    ///
    /// The closure receives an `Arc` of this database and may keep it in its
    /// result. See [`StorageOp::with_guard`] for examples.
    pub fn prepare_op<R: Send + 'static>(
        self: &Arc<Self>,
        op: impl FnOnce(Arc<Self>) -> R + Send + 'static,
    ) -> StorageOp<'_, impl FnOnce() -> R + Send + 'static> {
        let db = self.clone();
        StorageOp::new(move || op(db), &self.background_tracker)
    }

    /// Prepares deleting a column family, to submit to an executor of your choice.
    /// Stop all users of the column family first. Unlike other operations, this is
    /// still accepted after the database is sealed. If the task is dropped without
    /// running, the column family isn't deleted.
    pub fn prepare_drop_cf(
        self: Arc<Self>,
        name: CfName,
    ) -> ReadyStorageTask<'static, impl FnOnce() -> Result<(), RocksError> + Send + 'static> {
        let lease = self
            .registration
            .as_ref()
            .map(|r| r.manager.track_lifecycle());
        let task = TrackedTask::new(
            move || {
                self.db
                    .as_raw_db()
                    .drop_cf(name.as_str())
                    .map_err(RocksError::from)
            },
            lease,
        );
        StorageTask::default()
            .kind(StorageTaskKind::DropColumnFamily)
            .op(move || task.run())
            .build()
            .unwrap()
    }

    /// Runs a blocking read operation on the storage background thread-pool and
    /// awaits its result.
    ///
    /// Unlike [`Self::run_background_iterator`], this is for bounded operations
    /// that run to completion and produce a single value, rather than streaming
    /// rows. The op receives the raw RocksDB handle and must not let borrowed
    /// database values escape its own scope.
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn run_background_read_op<OP, R>(
        self: Arc<Self>,
        name: &'static str,
        kind: StorageTaskKind,
        priority: Priority,
        op: OP,
    ) -> Result<R, RocksError>
    where
        OP: FnOnce(&RawRocksDb) -> R + Send + 'static,
        R: Send + 'static,
    {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(kind)
            .priority(priority)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new(name);
                op(self.db.as_raw_db())
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError.into())
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_wal(self: Arc<Self>, sync: bool) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("flush-wal");
                self.db.flush_wal(sync)
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub fn run_bg_wal_sync(self: Arc<Self>) -> Result<(), AdmissionClosed> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("bg-wal-sync");
                if let Err(e) = self.db.flush_wal(true) {
                    error!("Failed to flush rocksdb WAL: {}", e);
                }
            })
            .build()
            .unwrap();
        TrackedExecutor::new(&executor, &tracker).dispatch(task)
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_memtables(
        self: Arc<Self>,
        cfs: &[CfName],
        wait: bool,
    ) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let mut owned_cfs = Vec::with_capacity(cfs.len());
        owned_cfs.extend_from_slice(cfs);
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushMemtables)
            .op(move || self.db.flush_memtables(&owned_cfs, wait))
            .build()
            .unwrap();
        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_all(self: Arc<Self>) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushMemtables)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("manual-flush");
                self.db.flush_all()
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn compact_all(
        self: Arc<Self>,
        options: ManualCompactionOptions,
    ) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::Compaction)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("manual-compaction");
                self.db.compact_all(options);
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?;
        Ok(())
    }

    pub fn get_histogram_data(&self, histogram: Histogram) -> HistogramData {
        self.db.db_options().get_histogram_data(histogram)
    }

    pub fn get_ticker_count(&self, ticker: Ticker) -> u64 {
        self.db.db_options().get_ticker_count(ticker)
    }

    pub fn get_statistics_str(&self) -> Option<String> {
        self.db.db_options().get_statistics()
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn open_cf(self: Arc<Self>, name: CfName) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenColumnFamily)
            .op(move || self.db.open_cf(name))
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn import_cf(
        self: Arc<Self>,
        name: CfName,
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::ImportColumnFamily)
            .priority(Priority::Low)
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("import-column-family");
                self.db.import_cf(name, metadata)
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn export_cf(
        self: Arc<Self>,
        name: CfName,
        export_dir: PathBuf,
    ) -> Result<ExportImportFilesMetaData, RocksError> {
        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::ExportColumnFamily)
            .priority(Priority::Low)
            .op(move || self.db.export_cf(name, export_dir))
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
    }

    /// Deletes a column family and returns a future that resolves once it's gone.
    ///
    /// Stop all users of the column family first (drop its handles and finish its
    /// iterators). Deletion is still allowed after the database is sealed, and
    /// dropping the returned future doesn't cancel it. With a manager, deletion runs
    /// on the manager's lifecycle pool. Without one, it runs on the calling thread
    /// before this function returns, so avoid calling it from an async task.
    pub fn drop_cf(
        self: Arc<Self>,
        name: CfName,
    ) -> impl Future<Output = Result<(), RocksError>> + Send + 'static + use<> {
        let manager = self
            .registration
            .as_ref()
            .map(|registration| registration.manager.clone());
        let (completion, result) = tokio::sync::oneshot::channel();
        let cleanup = move || {
            let result = self
                .db
                .as_raw_db()
                .drop_cf(name.as_str())
                .map_err(RocksError::from);
            drop(self);
            let _ = completion.send(result);
        };
        if let Some(manager) = manager {
            manager.dispatch_cleanup(cleanup);
        } else {
            cleanup();
        }
        async move { result.await.map_err(|_| ShutdownError)? }
    }

    /// Observes completion of native database destruction and resource release.
    ///
    /// This does not initiate closing or retain the database. Obtain the future
    /// before releasing the last database reference. Cancelling an observer does
    /// not cancel cleanup, and completion does not require executor threads to exit.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        self.closed.clone().cancelled_owned()
    }

    /// Performs a clean shutdown and waits for native database destruction.
    ///
    /// This fails if there are other live references to the database. Once closing
    /// starts, cancelling this future does not cancel cleanup. A separate
    /// [`Self::closed`] observer can continue waiting for physical completion.
    pub async fn close(self: Arc<Self>) -> Result<(), Arc<Self>> {
        let db = Arc::try_unwrap(self)?;
        let closed = db.closed().log_slow_after(
            std::time::Duration::from_secs(5),
            tracing::Level::WARN,
            format!("Closing RocksDB database '{}'", db.name()),
        );
        drop(db);
        closed.await;
        Ok(())
    }
}

fn is_retryable_error(error_kind: rocksdb::ErrorKind) -> bool {
    matches!(
        error_kind,
        rocksdb::ErrorKind::Incomplete | rocksdb::ErrorKind::TryAgain | rocksdb::ErrorKind::Busy
    )
}
