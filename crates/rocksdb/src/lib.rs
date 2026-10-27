// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use rocksdb::ExportImportFilesMetaData;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use restate_core::ShutdownError;
use restate_core::io::{FlightTracker, IoExecutor, Lease, OwnedLease, TrackedExecutor};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::protobuf::common::DatabaseKind;
pub use restate_types::rocksdb::{BottommostLevelCompaction, ManualCompactionOptions};

// re-exports
pub use self::background::{GlobalExecutor, StorageTaskKind};
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
    // Optional caller admission followed by database admission. Both follow the
    // operation across nonblocking attempts and background retries without readmission.
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

impl<B, OP> WriteOperation<&Arc<RocksDb>, B, OP, (Option<Lease<'_>>, Lease<'_>)> {
    fn into_background(
        self,
    ) -> WriteOperation<Arc<RocksDb>, B, OP, (Option<OwnedLease>, OwnedLease)> {
        WriteOperation {
            batch: self.batch,
            options: self.options,
            write_op: self.write_op,
            db: Arc::clone(self.db),
            lease: (
                self.lease.0.map(Lease::into_owned),
                self.lease.1.into_owned(),
            ),
        }
    }
}

#[derive(derive_more::Display, derive_more::Debug)]
#[display("{:?}", db)]
#[debug("{:?}", db)]
pub struct RocksDb {
    executor: GlobalExecutor,
    background_tracker: Arc<FlightTracker>,
    db: ManuallyDrop<RocksAccess>,
    closed: CancellationToken,
    registration: Option<DbRegistration>,
}

/// Installed once, before shared handles are published. Retains the manager
/// through cleanup and preserves observers created while opening was pending.
struct DbRegistration {
    manager: RocksDbManager,
    tracked: DbRef,
    closed: CancellationToken,
}

/// Owns destruction, including when a queued cleanup job is discarded. Neither
/// unwinding nor dropping an observer certifies successful physical completion.
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
            registration.manager.clone().drop_db(cleanup);
        } else {
            // Standalone destruction needs neither a runtime nor an open executor.
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

    /// Construct an independent database. It may be attached to a manager before
    /// being shared, or used standalone. Standalone final-reference cleanup is
    /// synchronous, even when the ordinary I/O executor has been closed.
    pub fn new(db: RocksAccess, executor: GlobalExecutor) -> Self {
        Self {
            executor,
            background_tracker: Arc::default(),
            db: ManuallyDrop::new(db),
            closed: CancellationToken::new(),
            registration: None,
        }
    }

    /// The bound incarnation, or None for a standalone database.
    pub fn registration(&self) -> Option<DbRef> {
        self.registration
            .as_ref()
            .map(|registration| registration.tracked)
    }

    /// Permanently reject new operations. Existing raw handles are not revoked.
    pub fn seal(&self) {
        self.background_tracker.seal();
    }

    /// Seal admission and wait for accepted operations to release their resources.
    /// This does not certify physical close or the release of external handles.
    pub async fn drain(&self) {
        self.background_tracker.seal();
        self.background_tracker.drain().await;
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
                lease: (
                    None,
                    self.background_tracker.try_acquire().ok_or(ShutdownError)?,
                ),
            },
        )
        .await
    }

    /// Write a [`rocksdb::WriteBatchWithIndex`] to the database.
    ///
    /// On success, returns the batch so callers can reuse it. The lease follows the
    /// actual write, including background retries, independently of the result waiter.
    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch_with_index(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatchWithIndex,
        lease: Lease<'_>,
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
                lease: (
                    Some(lease),
                    self.background_tracker.try_acquire().ok_or(ShutdownError)?,
                ),
            },
        )
        .await
    }

    async fn write_batch_internal<B, OP>(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        mut write: WriteOperation<&Arc<Self>, B, OP, (Option<Lease<'_>>, Lease<'_>)>,
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

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub fn run_background_iterator(
        self: Arc<Self>,
        cf: CfName,
        name: &'static str,
        priority: Priority,
        initial_action: IterAction,
        mut read_options: rocksdb::ReadOptions,
        mut on_item: impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
    ) -> Result<(), ShutdownError> {
        // ensure that we allow blocking IO.
        read_options.set_read_tier(rocksdb::ReadTier::All);

        let executor = self.executor.clone();
        let tracker = self.background_tracker.clone();
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
                    on_item,
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

        TrackedExecutor::new(&executor, &tracker).dispatch(task)
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
    ) -> Result<R, ShutdownError>
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
            .map_err(|_| ShutdownError)
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
    pub fn run_bg_wal_sync(self: Arc<Self>) -> Result<(), ShutdownError> {
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
            .op(move || {
                let _x = RocksDbReadPerfGuard::new("export-column-family");

                let checkpoint = Checkpoint::new(self.db.as_raw_db()).unwrap();

                let data_cf_handle = self
                    .db
                    .cf_handle(name.as_str())
                    .ok_or_else(|| RocksError::UnknownColumnFamily(name.clone()))?;

                let metadata = checkpoint
                    .export_column_family(&data_cf_handle, export_dir.as_path())
                    .map_err(RocksError::Other)?;

                if metadata.get_files().is_empty() {
                    error!(
                        "Refusing to create an empty snapshot! RocksDB column family export \
                        returned an empty set of files. The export is retained at: {}",
                        export_dir.display()
                    );
                    return Err(RocksError::SnapshotEmpty);
                }

                Ok(metadata)
            })
            .build()
            .unwrap();

        TrackedExecutor::new(&executor, &tracker)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)?
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
