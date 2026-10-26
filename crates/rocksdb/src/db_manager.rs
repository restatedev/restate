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
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_types::config::{CommonOptions, Configuration};

use crate::background::ReadyStorageTask;
use crate::db_env::check_memory_limit;
use crate::{
    DbEnv, DbName, DbSpec, Priority, RocksAccess, RocksDb, RocksError, metric_definitions,
};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

/// Tracks rocksdb databases created by various components, memory budgeting, monitoring, and
/// acting as a single entry point for all running databases on the node.
///
/// It doesn't try to limit rocksdb use-cases from accessing the raw rocksdb.
#[derive(derive_more::Debug)]
#[debug("RocksDbManager")]
pub struct RocksDbManager {
    dbs: RwLock<HashMap<DbName, Weak<RocksDb>>>,
    shutting_down: AtomicBool,
    close_db_tasks: TaskTracker,
    high_pri_pool: threadpool::ThreadPool,
    low_pri_pool: threadpool::ThreadPool,
    // Keep at the end of the struct to ensure it's dropped last
    db_env: DbEnv,
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

        let mut db_env = DbEnv::from_common_options(opts).expect("rocksdb env is created");

        // Initialize the process-wide pools only once at manager startup.
        // Initial sizes only: RocksDB grows these pools on database open to fit
        // max_background_flushes and max_background_compactions, never shrinking.
        if let Some(env) = &mut db_env.native_env {
            env.set_high_priority_background_threads(2);
            env.set_low_priority_background_threads(1);
        }

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
            db_env,
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

    /// Default shared resources, controlled by this manager's configuration watcher.
    /// Clone and customize the bundle to override or opt out of selected resources.
    pub fn db_env(&self) -> &DbEnv {
        &self.db_env
    }

    pub async fn open_db(&'static self, db_spec: DbSpec) -> Result<Arc<RocksDb>, RocksError> {
        self.open_db_with_env(db_spec, self.db_env.clone()).await
    }

    /// Opens with exactly the supplied resources, without filling absent fields
    /// from the default environment. Shared default resources still observe updates
    /// from this manager; custom resources are not enrolled in its resource tuning.
    pub async fn open_db_with_env(
        &'static self,
        db_spec: DbSpec,
        env: DbEnv,
    ) -> Result<Arc<RocksDb>, RocksError> {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RocksError::Shutdown(ShutdownError));
        }

        // get latest options
        let name = db_spec.name.clone();
        let path = db_spec.path.clone();
        let wrapper = RocksDb::open(self, db_spec, env).await?;
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

    /// Occupy every worker in a pool until the returned senders are dropped.
    /// Used to deterministically test queued work and cancelled result waiters.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn pause_background(&self, priority: Priority) -> Vec<std::sync::mpsc::Sender<()>> {
        let pool = match priority {
            Priority::High => &self.high_pri_pool,
            Priority::Low => &self.low_pri_pool,
        };
        let mut resumes = Vec::with_capacity(pool.max_count());
        for _ in 0..pool.max_count() {
            let (resume, paused) = std::sync::mpsc::channel();
            let (started, reached) = tokio::sync::oneshot::channel();
            resumes.push(resume);
            pool.execute(move || {
                let _ = started.send(());
                let _ = paused.recv();
            });
            reached.await.expect("background worker started");
        }
        resumes
    }

    /// Capacity of the default shared WBM, excluding independently supplied WBMs.
    pub fn get_total_write_buffer_capacity(&self) -> u64 {
        self.db_env
            .write_buffer_manager
            .as_ref()
            .map_or(0, |manager| manager.get_buffer_size() as u64)
    }

    /// Usage of the default shared WBM, excluding independently supplied WBMs.
    pub fn get_total_write_buffer_usage(&self) -> u64 {
        self.db_env
            .write_buffer_manager
            .as_ref()
            .map_or(0, |manager| manager.get_usage() as u64)
    }

    /// Returns aggregated memory usage for the selected databases and their supplied
    /// caches. Shared caches are counted once, using their full usage rather than
    /// attributing entries to individual databases. An empty filter also includes
    /// the manager's default cache, even when no database currently uses it.
    pub fn get_memory_usage_stats(
        &self,
        filter: &[DbName],
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        // for safety.
        // keep databases alive while we are aggregating memory usage
        let mut pinned = vec![];
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        if filter.is_empty() {
            if let Some(cache) = &self.db_env.cache {
                builder.add_cache(cache);
            }
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
            if let Some(cache) = &db.inner().db_env().cache {
                // The native builder deduplicates shared caches.
                builder.add_cache(cache);
            }
        }

        Ok(builder.build()?)
    }

    /// Returns aggregated memory usage for all databases if filter is empty
    pub fn get_db_memory_usage_stats(
        &self,
        db: &Arc<RocksDb>,
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        builder.add_db(db.inner().as_raw_db());
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
                name
            });
        }
        // wait for all tasks to complete
        self.close_db_tasks.wait().await;
        if let Some(env) = &self.db_env.native_env {
            env.clone().join_all_threads();
        }
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
        for db in guard.values() {
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
    current_common_opts: CommonOptions,
}

impl DbWatchdog {
    pub async fn run(manager: &'static RocksDbManager) -> anyhow::Result<()> {
        let prev_opts = Configuration::pinned().common.clone();
        let mut watchdog = Self {
            manager,
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
            if let Some(cache) = &self.manager.db_env.cache {
                cache
                    .clone()
                    .set_capacity(new_common_opts.rocksdb_total_memory_size().as_usize());
            }
            if let Some(manager) = &self.manager.db_env.write_buffer_manager {
                manager.set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
            }
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
            if let Some(manager) = &self.manager.db_env.write_buffer_manager {
                manager.set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
            }
        }

        // Databases choose to react to config updates as they see fit.
        // e.g. set write_buffer_size
        // A final Arc drop re-enters background_close_db, which takes the write
        // lock. Snapshot weak references and upgrade them outside the registry lock.
        let dbs: Vec<_> = self.manager.dbs.read().values().cloned().collect();
        for db in dbs {
            let Some(db) = db.upgrade() else {
                continue;
            };
            db.note_config_update();
        }

        self.current_common_opts = new_common_opts.clone();
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use restate_core::TaskCenterBuilder;
    use restate_platform::sync::Mutex;
    use restate_types::config::{Configuration, node_dir};
    use restate_types::protobuf::common::DatabaseKind;

    use super::{DbWatchdog, RocksDbManager};
    use crate::configuration::{DbConfigurator, create_default_db_options};
    use crate::{DbEnv, DbName, DbSpecBuilder, RocksAccess, RocksDb};

    #[derive(Clone, Default)]
    struct ReleaseOnConfigUpdate {
        owner: Arc<Mutex<Option<Arc<RocksDb>>>>,
    }

    impl DbConfigurator for ReleaseOnConfigUpdate {
        fn get_db_options(&self, db_name: &DbName, env: &DbEnv) -> rocksdb::Options {
            create_default_db_options(env, db_name)
        }

        fn note_config_update(&self, _db: &RocksAccess) {
            // On regression, retain the external owner so the test can report the
            // failure instead of deadlocking when the watchdog drops its Arc.
            if RocksDbManager::get().dbs.try_write().is_some() {
                // Leave the watchdog holding the last strong reference.
                self.owner.lock().take();
            }
        }
    }

    struct MemoryStatsConfigurator;

    impl DbConfigurator for MemoryStatsConfigurator {
        fn get_db_options(&self, db_name: &DbName, env: &DbEnv) -> rocksdb::Options {
            create_default_db_options(env, db_name)
        }
    }

    #[tokio::test]
    async fn memory_stats_filter_and_deduplicate_supplied_caches() -> anyhow::Result<()> {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);
        let tempdir = restate_test_util::tempdir()?;
        let custom_cache = rocksdb::Cache::new_lru_cache(1024 * 1024);
        // Avoid WBM cache reservations changing while measuring block-cache usage.
        let default_env = manager.db_env().clone().with_write_buffer_manager(None);
        let custom_env = default_env.clone().with_cache(Some(custom_cache.clone()));
        let mut databases = Vec::new();
        let mut names = Vec::new();
        for (index, env) in [
            default_env.clone(),
            default_env,
            custom_env.clone(),
            custom_env,
        ]
        .into_iter()
        .enumerate()
        {
            let name = DbName::new(&format!("memory-stats-{index}"));
            let spec = DbSpecBuilder::new(
                name.clone(),
                DatabaseKind::PartitionStore,
                tempdir.path().join(name.as_str()),
                MemoryStatsConfigurator,
            )
            .cf_patterns(Vec::new())
            .build()?;
            let db = manager.open_db_with_env(spec, env).await?;
            let raw = db.inner().as_raw_db();
            raw.put(b"key", [42; 4096])?;
            raw.flush()?;
            assert!(raw.get(b"key")?.is_some());
            names.push(name);
            databases.push(db);
        }

        let default_usage = manager.db_env().cache.as_ref().unwrap().get_usage() as u64;
        let custom_usage = custom_cache.get_usage() as u64;
        assert!(default_usage > 0);
        assert!(custom_usage > 0);
        assert_eq!(
            manager
                .get_memory_usage_stats(&[])?
                .approximate_cache_total(),
            default_usage + custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[..2])?
                .approximate_cache_total(),
            default_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[2..])?
                .approximate_cache_total(),
            custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[2..3])?
                .approximate_cache_total(),
            custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&[DbName::new("missing")])?
                .approximate_cache_total(),
            0,
        );

        for db in databases {
            db.close().await.expect("all database users released");
        }
        task_center.shutdown_node("test completed", 0).await;
        manager.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn config_update_can_release_last_database_reference() -> anyhow::Result<()> {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);
        let mut owners = Vec::new();
        for name in ["config-update-first", "config-update-second"] {
            let configurator = ReleaseOnConfigUpdate::default();
            let spec = DbSpecBuilder::new(
                DbName::new(name),
                DatabaseKind::PartitionStore,
                node_dir().join(name),
                configurator.clone(),
            )
            .cf_patterns(Vec::new())
            .build()?;
            let db = manager.open_db(spec).await?;
            *configurator.owner.lock() = Some(db);
            owners.push(configurator.owner);
        }

        let mut watchdog = DbWatchdog {
            manager,
            current_common_opts: Configuration::pinned().common.clone(),
        };
        watchdog.on_config_update();

        let released_in_callback = owners.iter().all(|owner| owner.lock().is_none());
        let databases_unregistered = manager.dbs.read().is_empty();
        // Also release retained owners on regression, after the registry lock is gone.
        for owner in owners {
            owner.lock().take();
        }
        task_center.shutdown_node("test completed", 0).await;
        manager.shutdown().await;

        assert!(
            released_in_callback,
            "config callbacks must run without the database registry lock"
        );
        assert!(
            databases_unregistered,
            "the watchdog must release its final database references before returning"
        );
        Ok(())
    }
}
