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
mod rock_access;

use metrics::counter;
use restate_types::config::RocksDbOptions;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;

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
pub enum Priority {
    High,
    #[default]
    Low,
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
#[display(fmt = "{}::{}", owner, name)]
pub struct RocksDb {
    manager: &'static RocksDbManager,
    pub owner: Owner,
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
        write!(
            f,
            "RocksDb({}::{} at {})",
            self.owner,
            self.name,
            self.path.display()
        )
    }
}

impl RocksDb {
    pub(crate) fn new<T>(manager: &'static RocksDbManager, spec: DbSpec<T>, db: Arc<T>) -> Self
    where
        T: RocksAccess + Send + Sync + 'static,
    {
        Self {
            manager,
            owner: spec.owner,
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
    pub fn inner(&self) -> &Arc<dyn RocksAccess + Send + Sync + 'static> {
        &self.db
    }

    pub fn cfs(&self) -> Vec<CfName> {
        self.db.cfs()
    }

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
                write_options.set_no_slowdown(false);
                self.db.write_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_MAYBE_BLOCKING, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                return Ok(());
            }
            IoMode::OnlyIfNonBlocking => {
                write_options.set_no_slowdown(true);
                self.db.write_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_NON_BLOCKING, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                return Ok(());
            }
            _ => {}
        }

        // Auto...
        // First, attempt to write without blocking
        write_options.set_no_slowdown(true);
        let result = self.db.write_batch(&write_batch, &write_options);
        match result {
            Ok(_) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_NON_BLOCKING, OP_TYPE => OP_WRITE).increment(1);
                Ok(())
            }
            Err(e) if is_retryable_error(e.kind()) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_MOVED_TO_BG, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                let start = std::time::Instant::now();
                // Operation will block, dispatch to background.
                let db = self.db.clone();
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .db_name(self.name.clone())
                    .owner(self.owner)
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || {
                        db.write_batch(&write_batch, &write_options)?;
                        info!(
                            "background write completed, completed in {:?}",
                            start.elapsed()
                        );
                        Ok(())
                    })
                    .build()
                    .unwrap();

                self.manager.async_spawn(task).await?
            }
            Err(e) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_FAILED, OP_TYPE => OP_WRITE)
                    .increment(1);
                Err(e.into())
            }
        }
    }

    // unfortunate side effect of trait objects not supporting generics
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
                write_options.set_no_slowdown(false);
                self.db.write_tx_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_MAYBE_BLOCKING, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                return Ok(());
            }
            IoMode::OnlyIfNonBlocking => {
                write_options.set_no_slowdown(true);
                self.db.write_tx_batch(&write_batch, &write_options)?;
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_NON_BLOCKING, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                return Ok(());
            }
            _ => {}
        }

        // Auto...
        // First, attempt to write without blocking
        write_options.set_no_slowdown(true);
        let result = self.db.write_tx_batch(&write_batch, &write_options);
        match result {
            Ok(_) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_NON_BLOCKING, OP_TYPE => OP_WRITE).increment(1);
                Ok(())
            }
            Err(e) if is_retryable_error(e.kind()) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_MOVED_TO_BG, OP_TYPE =>
                    OP_WRITE)
                .increment(1);
                let start = std::time::Instant::now();
                // Operation will block, dispatch to background.
                let db = self.db.clone();
                // In the background thread pool we can block on IO
                write_options.set_no_slowdown(false);
                let task = StorageTask::default()
                    .db_name(self.name.clone())
                    .owner(self.owner)
                    .priority(priority)
                    .kind(StorageTaskKind::WriteBatch)
                    .op(move || {
                        db.write_tx_batch(&write_batch, &write_options)?;
                        info!(
                            "background write completed, completed in {:?}",
                            start.elapsed()
                        );
                        Ok(())
                    })
                    .build()
                    .unwrap();

                self.manager.async_spawn(task).await?
            }
            Err(e) => {
                counter!(STORAGE_IO_OP, DISPOSITION => DISPOSITION_FAILED, OP_TYPE => OP_WRITE)
                    .increment(1);
                Err(e.into())
            }
        }
    }

    pub fn run_bg_wal_sync(&self) {
        let db = self.db.clone();
        let task = StorageTask::default()
            .db_name(self.name.clone())
            .owner(self.owner)
            .kind(StorageTaskKind::FlushWal)
            .op(move || {
                if let Err(e) = db.flush_wal(true) {
                    error!("Failed to flush rocksdb WAL in local loglet : {}", e);
                }
            })
            .build()
            .unwrap();
        // always spawn wal flushes.
        self.manager.spawn_unchecked(task);
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

    pub async fn open_cf(&self, name: CfName, opts: &RocksDbOptions) -> Result<(), RocksError> {
        let default_cf_options = self.manager.default_cf_options(opts);
        let db = self.db.clone();
        let cf_patterns = self.cf_patterns.clone();
        let task = StorageTask::default()
            .db_name(self.name.clone())
            .owner(self.owner)
            .kind(StorageTaskKind::OpenColumnFamily)
            .op(move || db.open_cf(name, default_cf_options, cf_patterns))
            .build()
            .unwrap();

        self.manager.async_spawn(task).await?
    }

    pub async fn shutdown(self: Arc<Self>) {
        let db_name = self.name.clone();
        let owner = self.owner;
        let manager = self.manager;
        let op = move || {
            if let Err(e) = self.db.flush_wal(true) {
                warn!(
                    db = %self.name,
                    owner = %self.owner,
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
                    owner = %self.owner,
                    "No column families to flush for db on shutdown"
                );
                return;
            }

            debug!(
            db = %self.name,
            owner = %self.owner,
            "Numbre of column families to flush on shutdown: {}", cfs_to_flush.len());
            if let Err(e) = self.db.flush_memtables(cfs_to_flush.as_slice(), true) {
                warn!(
                    db = %self.name,
                    owner = %self.owner,
                    "Failed to flush memtables: {}",
                    e
                );
            }
            self.db.cancel_all_background_work(true);
        };
        // intentionally ignore scheduling error
        let task = StorageTask::default()
            .db_name(db_name)
            .owner(owner)
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
