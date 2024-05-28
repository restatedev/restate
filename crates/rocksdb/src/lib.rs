// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod background;
mod db_manager;
mod db_spec;
mod error;
mod metric_definitions;
mod perf;
mod rock_access;

use metrics::counter;
use metrics::gauge;
use metrics::histogram;
use restate_core::ShutdownError;
use restate_types::config::RocksDbOptions;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;

use self::background::ReadyStorageTask;
// re-exports
pub use self::db_manager::RocksDbManager;
pub use self::db_spec::*;
pub use self::error::*;
pub use self::rock_access::RocksAccess;

use self::background::StorageTask;
use self::background::StorageTaskKind;
use self::metric_definitions::*;

type BoxedCfMatcher = Box<dyn CfNameMatch + Send + Sync>;
type BoxedCfOptionUpdater = Box<dyn Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync>;

/// Denotes whether an operation is considered latency sensitive or not
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, strum_macros::IntoStaticStr)]
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
    /// Attempts to perform the operation without blocking IO in worker thread, if it's not
    /// possible, it'll spawn work in the background thread pool.
    #[default]
    Default,
}

#[derive(derive_more::Display, Clone)]
#[display(fmt = "{}", name)]
pub struct RocksDb {
    manager: &'static RocksDbManager,
    pub name: DbName,
    pub path: PathBuf,
    pub db_options: rocksdb::Options,
    cf_patterns: Arc<[(BoxedCfMatcher, BoxedCfOptionUpdater)]>,
    flush_on_shutdown: Arc<[BoxedCfMatcher]>,
    db: Arc<dyn RocksAccess + Send + Sync + 'static>,
}

static_assertions::assert_impl_all!(RocksDb: Send, Sync);

impl fmt::Debug for RocksDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RocksDb({} at {})", self.name, self.path.display())
    }
}

impl RocksDb {
    pub(crate) fn new<T>(manager: &'static RocksDbManager, spec: DbSpec<T>, db: Arc<T>) -> Self
    where
        T: RocksAccess + Send + Sync + 'static,
    {
        Self {
            manager,
            name: spec.name,
            path: spec.path,
            cf_patterns: spec.cf_patterns.into(),
            db,
            db_options: spec.db_options,
            flush_on_shutdown: spec.flush_on_shutdown.into(),
        }
    }

    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    ///
    /// todo: remove this once all access is migrated to this abstraction
    pub fn inner(&self) -> &Arc<dyn RocksAccess + Send + Sync + 'static> {
        &self.db
    }

    pub fn cfs(&self) -> Vec<CfName> {
        self.db.cfs()
    }

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn write_batch(
        &self,
        priority: Priority,
        io_mode: IoMode,
        mut write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatch,
    ) -> Result<(), RocksError> {
        //  depending on the IoMode, we decide how to do the write.
        match io_mode {
            IoMode::AllowBlockingIO => {
                debug!("Blocking IO is allowed for write_batch, stall detection will not be used in this operation!");
                write_options.set_no_slowdown(false);
                self.db.write_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_MAYBE_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(());
            }
            IoMode::OnlyIfNonBlocking => {
                write_options.set_no_slowdown(true);
                self.db.write_batch(&write_batch, &write_options)?;
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

        #[cfg(test)]
        let result = self.db.write_batch(&write_batch, &write_options);
        #[cfg(not(test))]
        let result =
            tokio::task::block_in_place(|| self.db.write_batch(&write_batch, &write_options));
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
                // Operation will block, dispatch to background.
                let db = self.db.clone();
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || db.write_batch(&write_batch, &write_options))
                    .build()
                    .unwrap();

                Ok(race_against_stall_detector(self.manager, task).await??)
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

    // unfortunate side effect of trait objects not supporting generics
    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn write_tx_batch(
        &self,
        priority: Priority,
        io_mode: IoMode,
        mut write_options: rocksdb::WriteOptions,
        write_batch: rocksdb::WriteBatchWithTransaction<true>,
    ) -> Result<(), RocksError> {
        //  depending on the IoMode, we decide how to do the write.
        match io_mode {
            IoMode::AllowBlockingIO => {
                debug!("Blocking IO is allowed for write_batch, stall detection will not be used in this operation!");
                write_options.set_no_slowdown(false);
                self.db.write_tx_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP,
                    DISPOSITION => DISPOSITION_MAYBE_BLOCKING,
                    OP_TYPE => StorageTaskKind::WriteBatch.as_static_str(),
                    PRIORITY => priority.as_static_str(),
                )
                .increment(1);
                return Ok(());
            }
            IoMode::OnlyIfNonBlocking => {
                write_options.set_no_slowdown(true);
                self.db.write_tx_batch(&write_batch, &write_options)?;
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
        #[cfg(any(test, feature = "test-util"))]
        let result = self.db.write_tx_batch(&write_batch, &write_options);
        #[cfg(not(feature = "test-util"))]
        let result =
            tokio::task::block_in_place(|| self.db.write_tx_batch(&write_batch, &write_options));
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
                // Operation will block, dispatch to background.
                let db = self.db.clone();
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || db.write_tx_batch(&write_batch, &write_options))
                    .build()
                    .unwrap();

                Ok(race_against_stall_detector(self.manager, task).await??)
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

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn flush_wal(&self, sync: bool) -> Result<(), RocksError> {
        let db = self.db.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || db.flush_wal(sync))
            .build()
            .unwrap();

        self.manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub fn run_bg_wal_sync(&self) {
        let db = self.db.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                if let Err(e) = db.flush_wal(true) {
                    error!("Failed to flush rocksdb WAL: {}", e);
                }
            })
            .build()
            .unwrap();
        // always spawn wal flushes.
        self.manager.spawn_unchecked(task);
    }

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn flush_memtables(&self, cfs: &[CfName], wait: bool) -> Result<(), RocksError> {
        let db = Arc::clone(&self.db);
        let mut owned_cfs = Vec::with_capacity(cfs.len());
        owned_cfs.extend_from_slice(cfs);
        let task = StorageTask::default()
            .kind(StorageTaskKind::FlushMemtables)
            .op(move || db.flush_memtables(&owned_cfs, wait))
            .build()
            .unwrap();
        self.manager.async_spawn(task).await?
    }

    pub fn get_histogram_data(&self, histogram: Histogram) -> HistogramData {
        self.db_options.get_histogram_data(histogram)
    }

    pub fn get_ticker_count(&self, ticker: Ticker) -> u64 {
        self.db_options.get_ticker_count(ticker)
    }

    pub fn get_statistics_str(&self) -> Option<String> {
        self.db_options.get_statistics()
    }

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn open_cf(&self, name: CfName, opts: &RocksDbOptions) -> Result<(), RocksError> {
        let default_cf_options = self.manager.default_cf_options(opts);
        let db = self.db.clone();
        let cf_patterns = self.cf_patterns.clone();
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenColumnFamily)
            .op(move || db.open_cf(name, default_cf_options, cf_patterns))
            .build()
            .unwrap();

        self.manager.async_spawn(task).await?
    }

    #[tracing::instrument(skip_all, fields(db = %self.name))]
    pub async fn shutdown(self: Arc<Self>) {
        let manager = self.manager;
        let op = move || {
            if let Err(e) = self.db.flush_wal(true) {
                warn!(
                    db = %self.name,
                    "Failed to flush local loglet rocksdb WAL: {}",
                    e
                );
            }

            let cfs_to_flush = self
                .cfs()
                .into_iter()
                .filter(|c| {
                    self.flush_on_shutdown
                        .iter()
                        .any(|matcher| matcher.cf_matches(c))
                })
                .collect::<Vec<_>>();
            if cfs_to_flush.is_empty() {
                debug!(
                    db = %self.name,
                    "No column families to flush for db on shutdown"
                );
                return;
            }

            debug!(
            db = %self.name,
            "Number of column families to flush on shutdown: {}", cfs_to_flush.len());
            if let Err(e) = self.db.flush_memtables(cfs_to_flush.as_slice(), true) {
                warn!(
                    db = %self.name,
                    "Failed to flush memtables: {}",
                    e
                );
            }
            self.db.cancel_all_background_work(true);
        };
        // intentionally ignore scheduling error
        let task = StorageTask::default()
            .kind(StorageTaskKind::Shutdown)
            .op(op)
            .build()
            .unwrap();
        let _ = manager.async_spawn_unchecked(task).await;
    }
}

fn is_retryable_error(error_kind: rocksdb::ErrorKind) -> bool {
    matches!(
        error_kind,
        rocksdb::ErrorKind::Incomplete | rocksdb::ErrorKind::TryAgain | rocksdb::ErrorKind::Busy
    )
}

async fn race_against_stall_detector<OP, R>(
    manager: &RocksDbManager,
    task: ReadyStorageTask<OP>,
) -> Result<R, ShutdownError>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let mut task = std::pin::pin!(manager.async_spawn(task));
    let mut stalled = false;
    let mut stalled_since = Instant::now();
    loop {
        tokio::select! {
            result = &mut task => {
                if stalled {
                    // reset the flare guage
                    gauge!(ROCKSDB_STALL_FLARE).decrement(1);
                    let elapsed = stalled_since.elapsed();
                    histogram!(ROCKSDB_STALL_DURATION).record(elapsed);
                    info!("[Stall Detector] Rocksdb write operation completed after a stall time of {:?}!", elapsed);
                }
                return result;
            }
            _ = tokio::time::sleep(manager.stall_detection_duration()), if !stalled => {
                stalled = true;
                stalled_since = Instant::now();
                gauge!(ROCKSDB_STALL_FLARE).increment(1);
                warn!("[Stall Detector] Rocksdb write operation exceeded rocksdb-write-stall-threshold, will continue waiting");
            }

        }
    }
}
