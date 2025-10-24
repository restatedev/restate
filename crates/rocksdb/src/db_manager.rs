// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use parking_lot::RwLock;
use rocksdb::{Cache, WriteBufferManager};
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_serde_util::ByteCount;
use restate_types::config::{CommonOptions, Configuration};

use crate::background::ReadyStorageTask;
use crate::{DbName, DbSpec, Priority, RocksAccess, RocksDb, RocksError, metric_definitions};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

/// Tracks rocksdb databases created by various components, memory budgeting, monitoring, and
/// acting as a single entry point for all running databases on the node.
///
/// It doesn't try to limit rocksdb use-cases from accessing the raw rocksdb.
#[derive(derive_more::Debug)]
#[debug("RocksDbManager")]
pub struct RocksDbManager {
    pub(crate) env: rocksdb::Env,
    /// a shared rocksdb block cache
    pub(crate) cache: Cache,
    // auto updates to changes in common.rocksdb_memory_limit and common.rocksdb_memtable_total_size_limit
    pub(crate) write_buffer_manager: WriteBufferManager,
    dbs: RwLock<HashMap<DbName, Weak<RocksDb>>>,
    shutting_down: AtomicBool,
    close_db_tasks: TaskTracker,
    high_pri_pool: threadpool::ThreadPool,
    low_pri_pool: threadpool::ThreadPool,
}

impl RocksDbManager {
    #[track_caller]
    pub fn get() -> &'static RocksDbManager {
        DB_MANAGER.get().expect("DBManager not initialized")
    }

    pub fn maybe_get() -> Option<&'static RocksDbManager> {
        DB_MANAGER.get()
    }

    /// Create a new instance of the database manager. This should not be executed concurrently,
    /// only run it once on program startup.
    ///
    /// Must run in task_center scope.
    pub fn init() -> &'static Self {
        // best-effort, it doesn't make concurrent access safe, but it's better than nothing.
        if let Some(manager) = DB_MANAGER.get() {
            return manager;
        }
        metric_definitions::describe_metrics();
        let opts = &Configuration::pinned().common;

        check_memory_limit(opts);

        let cache = Cache::new_lru_cache(opts.rocksdb_total_memory_size.get());
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.rocksdb_actual_total_memtables_size(),
            false,
            cache.clone(),
        );
        // Setup the shared rocksdb environment
        let mut env = rocksdb::Env::new().expect("rocksdb env is created");
        env.set_low_priority_background_threads(opts.rocksdb_bg_threads().get() as i32);
        env.set_high_priority_background_threads(opts.rocksdb_high_priority_bg_threads.get() as i32);
        env.set_background_threads(opts.rocksdb_bg_threads().get() as i32);

        // Create our own storage thread pools
        let high_pri_pool = threadpool::Builder::new()
            .thread_name("rs:io-hi".to_owned())
            .num_threads(opts.storage_high_priority_bg_threads().into())
            .build();

        let low_pri_pool = threadpool::Builder::new()
            .thread_name("rs:io-lo".to_owned())
            .num_threads(opts.storage_low_priority_bg_threads().into())
            .build();

        let dbs = RwLock::default();

        let manager = Self {
            env,
            cache,
            write_buffer_manager,
            dbs,
            shutting_down: AtomicBool::new(false),
            close_db_tasks: TaskTracker::default(),
            high_pri_pool,
            low_pri_pool,
        };

        DB_MANAGER.set(manager).expect("DBManager initialized once");
        // Start db monitoring.
        TaskCenter::spawn(
            TaskKind::SystemService,
            "db-manager",
            DbWatchdog::run(Self::get()),
        )
        .expect("run db watchdog");

        Self::get()
    }

    pub fn get_db(&self, name: DbName) -> Option<Arc<RocksDb>> {
        let read_guard = self.dbs.upgradable_read();
        let db = read_guard.get(&name)?.upgrade();
        if let Some(db) = db {
            Some(db)
        } else {
            let mut write_guard = parking_lot::RwLockUpgradableReadGuard::upgrade(read_guard);
            // clean it up unless someone else added it back
            let db = write_guard.get(&name)?.upgrade();
            match db {
                Some(db) => Some(db),
                None => {
                    write_guard.remove(&name);
                    None
                }
            }
        }
    }

    pub async fn open_db(&'static self, db_spec: DbSpec) -> Result<Arc<RocksDb>, RocksError> {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RocksError::Shutdown(ShutdownError));
        }

        // get latest options
        let name = db_spec.name.clone();
        let path = db_spec.path.clone();
        let wrapper = RocksDb::open(self, db_spec).await?;
        self.dbs
            .write()
            .insert(name.clone(), Arc::downgrade(&wrapper));

        debug!(
            db = %name,
            path = %path.display(),
            "Opened rocksdb database"
        );
        Ok(wrapper)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn reset(&'static self) -> anyhow::Result<()> {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Release);
        self.shutdown().await;
        self.dbs.write().clear();
        self.shutting_down
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn get_total_write_buffer_capacity(&self) -> u64 {
        self.write_buffer_manager.get_buffer_size() as u64
    }

    pub fn get_total_write_buffer_usage(&self) -> u64 {
        self.write_buffer_manager.get_usage() as u64
    }

    /// Returns aggregated memory usage for all databases if filter is empty
    pub fn get_memory_usage_stats(
        &self,
        filter: &[DbName],
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        builder.add_cache(&self.cache);

        if filter.is_empty() {
            for db in self.dbs.read().values() {
                let Some(db) = db.upgrade() else {
                    continue;
                };
                db.inner().record_memory_stats(&mut builder);
            }
        } else {
            for key in filter {
                if let Some(db) = self.dbs.read().get(key) {
                    let Some(db) = db.upgrade() else {
                        continue;
                    };
                    db.inner().record_memory_stats(&mut builder);
                }
            }
        }

        Ok(builder.build()?)
    }

    pub fn get_all_dbs(&self) -> Vec<Arc<RocksDb>> {
        self.dbs.read().values().filter_map(Weak::upgrade).collect()
    }

    /// Closes the database and waits for completion.
    pub(crate) async fn close_db(&self, db: Arc<RocksDb>) -> Result<(), Arc<RocksDb>> {
        let db = Arc::try_unwrap(db)?;
        // unconditionally remove the db from the map
        self.dbs.write().remove(db.name());
        let handle = self.close_db_tasks.spawn_blocking(move || {
            db.db.shutdown();
        });
        let _ = handle.await;
        Ok(())
    }

    /// Closes the database in the background
    ///
    /// This is intended to be used by the [`RocksDb`] instance Drop impl to close the database.
    /// If the database has already been removed from the map, then we'll assume that the shutdown
    /// routine has already been executed by a previous call to [`RocksDb::close`] or by
    /// [`RocksDbManager`]'s shutdown routine.
    ///
    /// if you need to wait for the shutdown, then use [`RocksDb::close`] instead.
    pub(crate) fn background_close_db(&self, db: RocksAccess) {
        let Some(_db) = self.dbs.write().remove(db.name()) else {
            // database has already been closed via other means
            return;
        };
        self.close_db_tasks.spawn_blocking(move || {
            db.shutdown();
        });
    }

    /// Ask all databases to shut down cleanly
    pub async fn shutdown(&'static self) {
        self.close_db_tasks.close();
        for (name, db) in self.dbs.write().drain() {
            let Some(db) = db.upgrade() else {
                continue;
            };

            self.close_db_tasks.spawn_blocking(move || {
                db.db.shutdown();
                name.clone()
            });
        }
        // wait for all tasks to complete
        self.close_db_tasks.wait().await;
        self.env.clone().join_all_threads();
        info!("Rocksdb manager shutdown completed");
    }

    /// Emergency shutdown is ongoing, this will ensure rocksdb's wal is fsynced.
    pub fn on_ungraceful_shutdown(&'static self) {
        let Some(guard) = self.dbs.try_read_for(Duration::from_secs(1)) else {
            eprintln!("[rocksdb] couldn't acquire rwlock to flush in time");
            return;
        };
        // WAL first
        for (name, db) in guard.iter() {
            let Some(db) = db.upgrade() else {
                continue;
            };
            if let Err(e) = db.db.flush_wal(true) {
                eprintln!("[rocksdb] failed to flush WAL of {name}: {e}");
            } else {
                eprintln!("[rocksdb] flushed WAL of {name}");
            }
        }

        // Best effort normal shutdown
        for (_, db) in guard.iter() {
            let Some(db) = db.upgrade() else {
                continue;
            };
            db.db.shutdown();
        }

        if !guard.is_empty() {
            eprintln!("[rocksdb] flushed all!");
        }
    }

    /// Spawn a rocksdb blocking operation in the background
    pub(crate) async fn async_spawn<OP, R>(
        &self,
        task: ReadyStorageTask<OP>,
    ) -> Result<R, ShutdownError>
    where
        OP: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ShutdownError);
        }

        self.async_spawn_unchecked(task).await
    }

    /// Ignores the shutdown signal. This should be used if an IO operation needs
    /// to be performed _during_ shutdown.
    pub(crate) async fn async_spawn_unchecked<OP, R>(
        &self,
        task: ReadyStorageTask<OP>,
    ) -> Result<R, ShutdownError>
    where
        OP: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let priority = task.priority;
        match priority {
            Priority::High => self.high_pri_pool.execute(task.into_async_runner(tx)),
            Priority::Low => self.low_pri_pool.execute(task.into_async_runner(tx)),
        }
        rx.await.map_err(|_| ShutdownError)
    }

    #[allow(dead_code)]
    pub(crate) fn spawn<OP>(&self, task: ReadyStorageTask<OP>) -> Result<(), ShutdownError>
    where
        OP: FnOnce() + Send + 'static,
    {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ShutdownError);
        }
        self.spawn_unchecked(task);
        Ok(())
    }

    pub(crate) fn spawn_unchecked<OP>(&self, task: ReadyStorageTask<OP>)
    where
        OP: FnOnce() + Send + 'static,
    {
        match task.priority {
            Priority::High => self.high_pri_pool.execute(task.into_runner()),
            Priority::Low => self.low_pri_pool.execute(task.into_runner()),
        }
    }
}

#[allow(dead_code)]
struct ConfigSubscription {
    name: DbName,
}

struct DbWatchdog {
    manager: &'static RocksDbManager,
    cache: Cache,
    current_common_opts: CommonOptions,
}

impl DbWatchdog {
    pub async fn run(manager: &'static RocksDbManager) -> anyhow::Result<()> {
        let prev_opts = Configuration::pinned().common.clone();
        let mut watchdog = Self {
            manager,
            cache: manager.cache.clone(),
            current_common_opts: prev_opts,
        };

        let shutdown_watch = cancellation_watcher();
        tokio::pin!(shutdown_watch);

        let config_watch = Configuration::watcher();
        tokio::pin!(config_watch);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_watch => {
                    // Shutdown requested.
                    manager
                        .shutting_down
                        .store(true, std::sync::atomic::Ordering::Release);
                    break;
                }
                _ = config_watch.changed() => {
                    watchdog.on_config_update();
                }
            }
        }

        Ok(())
    }

    fn on_config_update(&mut self) {
        // ignore if in shutdown
        if self
            .manager
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            info!("Ignoring config update as we are shutting down");
            return;
        }
        let new_common_opts = &Configuration::pinned().common;

        // Memory budget changed?
        if new_common_opts.rocksdb_total_memory_size
            != self.current_common_opts.rocksdb_total_memory_size
        {
            warn!(
                old = self.current_common_opts.rocksdb_total_memory_size,
                new = new_common_opts.rocksdb_total_memory_size,
                "[config update] Setting rocksdb total memory limit to {}",
                ByteCount::from(new_common_opts.rocksdb_total_memory_size)
            );
            check_memory_limit(new_common_opts);
            self.cache
                .set_capacity(new_common_opts.rocksdb_total_memory_size.get());
            self.manager
                .write_buffer_manager
                .set_buffer_size(new_common_opts.rocksdb_actual_total_memtables_size());
        }

        // update memtable total memory
        if new_common_opts.rocksdb_actual_total_memtables_size()
            != self
                .current_common_opts
                .rocksdb_actual_total_memtables_size()
        {
            warn!(
                old = self
                    .current_common_opts
                    .rocksdb_actual_total_memtables_size(),
                new = new_common_opts.rocksdb_actual_total_memtables_size(),
                "[config update] Setting rocksdb total memtables size limit to {}",
                ByteCount::from(new_common_opts.rocksdb_actual_total_memtables_size())
            );
            self.manager
                .write_buffer_manager
                .set_buffer_size(new_common_opts.rocksdb_actual_total_memtables_size());
        }

        // Databases choose to react to config updates as they see fit.
        // e.g. set write_buffer_size
        for db in self.manager.dbs.read().values() {
            let Some(db) = db.upgrade() else {
                continue;
            };
            db.note_config_update();
        }

        self.current_common_opts = new_common_opts.clone();
    }
}

fn check_memory_limit(opts: &CommonOptions) {
    if let Some(process_memory_size) = opts.process_total_memory_size() {
        let memory_ratio =
            opts.rocksdb_total_memory_size.get() as f64 / process_memory_size.get() as f64;
        if memory_ratio < 0.5 {
            warn!(
                "'rocksdb-total-memory-size' parameter is set to {}, less than half the process memory limit of {}. Roughly 75% of process memory should be given to RocksDB",
                ByteCount::from(opts.rocksdb_total_memory_size),
                ByteCount::from(process_memory_size),
            )
        } else if memory_ratio > 1.0 {
            error!(
                "'rocksdb-total-memory-size' parameter is set to {}, more than the process memory limit of {}. This guarantees an OOM under load; roughly 75% of process memory should be given to RocksDB",
                ByteCount::from(opts.rocksdb_total_memory_size),
                ByteCount::from(process_memory_size),
            )
        } else if memory_ratio > 0.9 {
            error!(
                "'rocksdb-total-memory-size' parameter is set to {}, more than 90% of the process memory limit of {}. This risks an OOM under load; roughly 75% of process memory should be given to RocksDB",
                ByteCount::from(opts.rocksdb_total_memory_size),
                ByteCount::from(process_memory_size),
            )
        }
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);
