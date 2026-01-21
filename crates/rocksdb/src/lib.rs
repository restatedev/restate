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
pub mod configuration;
mod db_manager;
mod db_spec;
mod error;
mod iterator;
mod logging;
mod metric_definitions;
mod perf;
mod rock_access;

use metrics::counter;
use tracing::debug;
use tracing::error;
use tracing::warn;

use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::Arc;

use rocksdb::ExportImportFilesMetaData;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;

use restate_core::ShutdownError;

// re-exports
pub use self::db_manager::RocksDbManager;
pub use self::db_spec::*;
pub use self::error::*;
pub use self::iterator::IterAction;
use self::iterator::RocksIterator;
pub use self::perf::RocksDbPerfGuard;
pub use self::rock_access::RocksAccess;

use self::background::StorageTask;
use self::background::StorageTaskKind;
use self::metric_definitions::*;

pub type RawRocksDb = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;
type BoxedCfMatcher = Box<dyn CfNameMatch + Send + Sync>;

/// Denotes whether an operation is considered latency sensitive or not
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum Priority {
    High,
    #[default]
    Low,
}

impl Priority {
    pub fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

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

#[derive(derive_more::Display, derive_more::Debug)]
#[display("{:?}", db)]
#[debug("{:?}", db)]
pub struct RocksDb {
    manager: &'static RocksDbManager,
    db: ManuallyDrop<RocksAccess>,
}

impl Drop for RocksDb {
    fn drop(&mut self) {
        // Safety: self.db is exclusives "taken" at drop time, so we know that it's
        // still valid. We don't provide &mut access to db, and it's not a public field.
        let inner_db = unsafe { ManuallyDrop::take(&mut self.db) };

        if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            self.manager.background_close_db(inner_db);
        } else {
            // inline shutdown if the last drop was outside of tokio (in tests mainly)
            inner_db.shutdown();
        }
    }
}

static_assertions::assert_impl_all!(RocksDb: Send, Sync);

impl RocksDb {
    pub fn name(&self) -> &DbName {
        &self.db.spec().name
    }

    pub(crate) async fn open(
        manager: &'static RocksDbManager,
        spec: DbSpec,
    ) -> Result<Arc<Self>, RocksError> {
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenDb)
            .op(move || {
                let _x = RocksDbPerfGuard::new("open-db");
                RocksAccess::open_db(
                    spec,
                    &manager.env,
                    &manager.write_buffer_manager,
                    &manager.cache,
                )
            })
            .build()
            .unwrap();

        let db = manager.async_spawn(task).await??;

        Ok(Arc::new(Self {
            manager,
            db: ManuallyDrop::new(db),
        }))
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

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatch,
    ) -> Result<(), RocksError> {
        self.write_batch_internal(
            name,
            priority,
            io_mode,
            write_options,
            move |db, write_options| db.write_batch(&write_batch, write_options),
        )
        .await
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn write_batch_with_index(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatchWithIndex,
    ) -> Result<(), RocksError> {
        self.write_batch_internal(
            name,
            priority,
            io_mode,
            write_options,
            move |db, write_options| db.write_batch_with_index(&write_batch, write_options),
        )
        .await
    }

    async fn write_batch_internal<OP>(
        self: &Arc<Self>,
        name: &'static str,
        priority: Priority,
        io_mode: IoMode,
        mut write_options: rocksdb::WriteOptions,
        write_op: OP,
    ) -> Result<(), RocksError>
    where
        OP: Fn(&RocksAccess, &rocksdb::WriteOptions) -> Result<(), rocksdb::Error> + Send + 'static,
    {
        //  depending on the IoMode, we decide how to do the write.
        match io_mode {
            IoMode::AllowBlockingIO => {
                let _x = RocksDbPerfGuard::new(name);
                debug!(
                    "Blocking IO is allowed for write_batch, stall detection will not be used in this operation!"
                );
                write_options.set_no_slowdown(false);
                write_op(&self.db, &write_options)?;
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_MAYBE_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(());
            }
            IoMode::AlwaysBackground => {
                // Operation will block, dispatch to background.
                let db = Arc::clone(self);
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || {
                        let _x = RocksDbPerfGuard::new(name);
                        write_op(&db.db, &write_options)
                    })
                    .build()
                    .unwrap();

                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_BACKGROUND,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);

                return Ok(self.manager.async_spawn(task).await??);
            }
            IoMode::OnlyIfNonBlocking => {
                let _x = RocksDbPerfGuard::new(name);
                write_options.set_no_slowdown(true);
                write_op(&self.db, &write_options)?;
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_NON_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(());
            }
            _ => {}
        }

        // Auto...
        // First, attempt to write without blocking
        write_options.set_no_slowdown(true);

        let perf_guard = RocksDbPerfGuard::new(name);
        let result = write_op(&self.db, &write_options);
        match result {
            Ok(_) => {
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_NON_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                Ok(())
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
                let db = Arc::clone(self);
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || {
                        let _x = RocksDbPerfGuard::new(name);
                        write_op(&db.db, &write_options)
                    })
                    .build()
                    .unwrap();

                return Ok(self.manager.async_spawn(task).await??);
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

        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::BackgroundIterator)
            .priority(priority)
            .op(move || {
                // note: the perf guard's lifetime encapsulates all operations in the iterator and
                // the total duration includes all the time spent blocking on the tx's capacity.
                let _x = RocksDbPerfGuard::new(name);
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

        manager.spawn(task)
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_wal(self: Arc<Self>, sync: bool) -> Result<(), RocksError> {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                let _x = RocksDbPerfGuard::new("flush-wal");
                self.db.flush_wal(sync)
            })
            .build()
            .unwrap();

        manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub fn run_bg_wal_sync(self: Arc<Self>) {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                let _x = RocksDbPerfGuard::new("bg-wal-sync");
                if let Err(e) = self.db.flush_wal(true) {
                    error!("Failed to flush rocksdb WAL: {}", e);
                }
            })
            .build()
            .unwrap();
        // always spawn wal flushes.
        manager.spawn_unchecked(task);
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_memtables(
        self: Arc<Self>,
        cfs: &[CfName],
        wait: bool,
    ) -> Result<(), RocksError> {
        let manager = self.manager;
        let mut owned_cfs = Vec::with_capacity(cfs.len());
        owned_cfs.extend_from_slice(cfs);
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushMemtables)
            .op(move || self.db.flush_memtables(&owned_cfs, wait))
            .build()
            .unwrap();
        manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn flush_all(self: Arc<Self>) -> Result<(), RocksError> {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushMemtables)
            .op(move || {
                let _x = RocksDbPerfGuard::new("manual-flush");
                self.db.flush_all()
            })
            .build()
            .unwrap();

        manager.async_spawn_unchecked(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn compact_all(self: Arc<Self>) {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::Compaction)
            .op(move || {
                let _x = RocksDbPerfGuard::new("manual-compaction");
                self.db.compact_all();
            })
            .build()
            .unwrap();

        let _ = manager.async_spawn_unchecked(task).await;
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
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenColumnFamily)
            .op(move || {
                self.db
                    .open_cf(name, &manager.write_buffer_manager, &manager.cache)
            })
            .build()
            .unwrap();

        manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn import_cf(
        self: Arc<Self>,
        name: CfName,
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::ImportColumnFamily)
            .priority(Priority::Low)
            .op(move || {
                let _x = RocksDbPerfGuard::new("import-column-family");
                self.db.import_cf(
                    name,
                    &manager.write_buffer_manager,
                    &manager.cache,
                    metadata,
                )
            })
            .build()
            .unwrap();

        manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub async fn export_cf(
        self: Arc<Self>,
        name: CfName,
        export_dir: PathBuf,
    ) -> Result<ExportImportFilesMetaData, RocksError> {
        let manager = self.manager;
        let task = StorageTask::default()
            .kind(StorageTaskKind::ExportColumnFamily)
            .priority(Priority::Low)
            .op(move || {
                let _x = RocksDbPerfGuard::new("export-column-family");

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

        manager.async_spawn(task).await?
    }

    /// Performs a clean shutdown of the database and waits for completion.
    ///
    /// This fails if there other live references to the database.
    pub async fn close(self: Arc<Self>) -> Result<(), Arc<Self>> {
        self.manager.close_db(self).await
    }
}

fn is_retryable_error(error_kind: rocksdb::ErrorKind) -> bool {
    matches!(
        error_kind,
        rocksdb::ErrorKind::Incomplete | rocksdb::ErrorKind::TryAgain | rocksdb::ErrorKind::Busy
    )
}
