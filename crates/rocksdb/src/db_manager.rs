// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use rocksdb::{Cache, RateLimiter, RateLimiterMode, WriteBufferManager};
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_types::config::{CommonOptions, Configuration};
use restate_util_bytecount::ByteCount;

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
    /// A shared IO write rate limiter
    pub(crate) rate_limiter: RateLimiter,
    /// a shared rocksdb block cache
    pub(crate) cache: Cache,
    // auto updates to changes in common.rocksdb_memory_limit and common.rocksdb_memtable_total_size_limit
    pub(crate) write_buffer_manager: WriteBufferManager,
    dbs: RwLock<HashMap<DbName, Weak<RocksDb>>>,
    /// Databases whose shutdown is still running, keyed by name.
    ///
    /// RocksDB only releases a database directory's file lock once its shutdown completes, so
    /// re-opening the same database has to wait for the in-flight close first. The semaphore
    /// holds no permits and is closed by the shutdown task to release waiters.
    closing_dbs: RwLock<HashMap<DbName, Arc<Semaphore>>>,
    shutting_down: AtomicBool,
    close_db_tasks: TaskTracker,
    high_pri_pool: threadpool::ThreadPool,
    low_pri_pool: threadpool::ThreadPool,
    // Keep at the end of the struct to ensure it's dropped last
    pub(crate) env: rocksdb::Env,
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

        // HCC is the newly recommended default for RocksDB.
        let cache = Cache::new_hyper_clock_cache(opts.rocksdb_total_memory_size().as_usize(), 0);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.rocksdb_total_memtables_size().as_usize(),
            false,
            cache.clone(),
        );
        // Setup the default shared rocksdb environment. These are just the initial pool sizes;
        // rocksdb grows them on demand at db-open to fit each database's max_background_flushes
        // (high-priority) and max_background_compactions (low-priority), never shrinking below.
        let mut env = rocksdb::Env::new().expect("rocksdb env is created");
        env.set_high_priority_background_threads(2);
        env.set_low_priority_background_threads(1);
        env.set_bottom_priority_background_threads(1);

        // Setup the global write rate limiter
        let rate_limiter = RateLimiter::new(
            opts.rocksdb_max_write_rate_per_second.as_u64() as i64,
            100 * 1000,
            10,
            RateLimiterMode::KWritesOnly,
            true,
        );

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
            rate_limiter,
            cache,
            write_buffer_manager,
            dbs,
            closing_dbs: RwLock::default(),
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

        // A previous instance of this database may still be shutting down, in which case rocksdb
        // has not released the directory's file lock yet and opening would fail.
        self.wait_for_pending_close(&name).await;

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
        self.closing_dbs.write().clear();
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
        // for safety.
        // keep databases alive while we are aggregating memory usage
        let mut pinned = vec![];
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        builder.add_cache(&self.cache);

        if filter.is_empty() {
            for db in self.dbs.read().values() {
                let Some(db) = db.upgrade() else {
                    continue;
                };
                pinned.push(db);
            }
        } else {
            for key in filter {
                if let Some(db) = self.dbs.read().get(key) {
                    let Some(db) = db.upgrade() else {
                        continue;
                    };
                    pinned.push(db);
                }
            }
        }

        for db in &pinned {
            builder.add_db(db.inner().as_raw_db());
        }

        Ok(builder.build()?)
    }

    pub fn get_all_dbs(&self) -> Vec<Arc<RocksDb>> {
        self.dbs.read().values().filter_map(Weak::upgrade).collect()
    }

    /// Waits for an in-flight close of the named database to complete, if there is one.
    async fn wait_for_pending_close(&self, name: &DbName) {
        let Some(semaphore) = self.closing_dbs.read().get(name).cloned() else {
            return;
        };

        debug!(db = %name, "Waiting for the previous instance of this database to finish closing");
        // never granted; resolves with an error once the shutdown task closes the semaphore
        let _ = semaphore.acquire().await;
    }

    /// Marks the named database as closing. Callers must pass the returned semaphore to
    /// [`Self::finish_pending_close`] once the shutdown has completed.
    fn register_pending_close(&self, name: DbName) -> Arc<Semaphore> {
        let semaphore = Arc::new(Semaphore::new(0));
        self.closing_dbs.write().insert(name, semaphore.clone());
        semaphore
    }

    fn finish_pending_close(&self, name: &DbName, semaphore: &Arc<Semaphore>) {
        {
            let mut guard = self.closing_dbs.write();
            // only clear our own registration; the database may have been opened and closed again
            if guard
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, semaphore))
            {
                guard.remove(name);
            }
        }
        // releases anyone that started waiting before we removed the entry above
        semaphore.close();
    }

    /// Closes the database and waits for completion.
    pub(crate) async fn close_db(&'static self, db: Arc<RocksDb>) -> Result<(), Arc<RocksDb>> {
        let db = Arc::try_unwrap(db)?;
        let name = db.name().clone();
        // unconditionally remove the db from the map
        self.dbs.write().remove(&name);
        let semaphore = self.register_pending_close(name.clone());
        let handle = self.close_db_tasks.spawn_blocking(move || {
            db.db.shutdown();
            self.finish_pending_close(&name, &semaphore);
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
    pub(crate) fn background_close_db(&'static self, db: RocksAccess) {
        let name = db.name().clone();
        let Some(_db) = self.dbs.write().remove(&name) else {
            // database has already been closed via other means
            return;
        };
        // Opening this database again has to wait for this shutdown: rocksdb holds the
        // directory's file lock until it completes.
        let semaphore = self.register_pending_close(name.clone());
        self.close_db_tasks.spawn_blocking(move || {
            db.shutdown();
            self.finish_pending_close(&name, &semaphore);
        });
    }

    /// Ask all databases to shut down cleanly
    pub async fn shutdown(&'static self) {
        // Stop accepting new work. Submitters using the `*_unchecked` variants can still get
        // through, which is why we join the pools below instead of relying on this alone.
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Release);

        // Wait for storage tasks that are already running before draining `self.dbs`: a database
        // that is still being opened has not been registered there yet (see `open_db`), so it
        // would otherwise escape the close loop entirely.
        self.join_storage_pools().await;

        self.close_db_tasks.close();
        for (name, db) in self.dbs.write().drain() {
            let Some(db) = db.upgrade() else {
                continue;
            };

            self.close_db_tasks.spawn_blocking(move || {
                db.db.shutdown();
                name
            });
        }
        // wait for all tasks to complete
        self.close_db_tasks.wait().await;

        // No storage task may still be inside rocksdb when we return. The process can begin
        // running C++ static destructors right after this, and a task still in `DB::Open` would
        // then read a freed option-registry static and crash with SIGSEGV.
        self.join_storage_pools().await;
        self.env.clone().join_all_threads();
        info!("Rocksdb manager shutdown completed");
    }

    /// Waits until neither storage pool has queued or running jobs.
    ///
    /// Bounded by the configured shutdown grace period: a stalled write is not a reason to hang
    /// the process forever, even though returning early re-opens the window described in
    /// [`Self::shutdown`].
    async fn join_storage_pools(&'static self) {
        let grace_period = Configuration::pinned().common.shutdown_grace_period();

        // `ThreadPool::join` blocks, so it must not run on a runtime worker.
        let join = tokio::task::spawn_blocking(move || {
            self.high_pri_pool.join();
            self.low_pri_pool.join();
        });

        match tokio::time::timeout(grace_period, join).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("Failed to join rocksdb storage thread pools: {err}"),
            Err(_) => warn!(
                "Rocksdb storage thread pools are still busy after {:?}, continuing shutdown",
                grace_period
            ),
        }
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
        if new_common_opts.rocksdb_total_memory_size()
            != self.current_common_opts.rocksdb_total_memory_size()
        {
            warn!(
                old = %self.current_common_opts.rocksdb_total_memory_size(),
                new = %new_common_opts.rocksdb_total_memory_size(),
                "[config update] Setting rocksdb total memory limit to {}",
                new_common_opts.rocksdb_total_memory_size()
            );
            check_memory_limit(new_common_opts);
            self.cache
                .set_capacity(new_common_opts.rocksdb_total_memory_size().as_usize());
            self.manager
                .write_buffer_manager
                .set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
        }

        // update memtable total memory
        if new_common_opts.rocksdb_total_memtables_size()
            != self.current_common_opts.rocksdb_total_memtables_size()
        {
            warn!(
                old = %self.current_common_opts.rocksdb_total_memtables_size(),
                new = %new_common_opts.rocksdb_total_memtables_size(),
                "[config update] Setting rocksdb total memtables size limit to {}",
                new_common_opts.rocksdb_total_memtables_size()
            );
            self.manager
                .write_buffer_manager
                .set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
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
            opts.rocksdb_total_memory_size().as_u64() as f64 / process_memory_size.get() as f64;
        if memory_ratio > 1.0 {
            error!(
                "'rocksdb-total-memory-size' parameter is set to {}, more than the process memory limit of {}. This guarantees an OOM under load; keep it under 50% of process memory",
                opts.rocksdb_total_memory_size(),
                ByteCount::from(process_memory_size),
            )
        } else if memory_ratio > 0.9 {
            error!(
                "'rocksdb-total-memory-size' parameter is set to {}, more than 90% of the process memory limit of {}. This risks an OOM under load; keep it under 50% of process memory",
                opts.rocksdb_total_memory_size(),
                ByteCount::from(process_memory_size),
            )
        }
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);
