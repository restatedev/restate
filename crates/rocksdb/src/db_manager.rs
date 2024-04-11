// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytesize::ByteSize;
use dashmap::DashMap;
use rocksdb::{Cache, WriteBufferManager};

use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};
use restate_types::arc_util::Updateable;
use restate_types::config::{CommonOptions, Configuration, RocksDbOptions};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{
    amend_cf_options, amend_db_options, DbName, DbSpec, Owner, RocksAccess, RocksDb, RocksError,
};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

enum WatchdogCommand {
    Register(ConfigSubscription),
    #[cfg(any(test, feature = "test-util"))]
    ResetAll,
}

/// Tracks rocksdb databases created by various components, memory budgeting, monitoring, and
/// acting as a single entry point for all running databases on the node.
///
/// It doesn't try to limit rocksdb use-cases from accessing the raw rocksdb.
pub struct RocksDbManager {
    /// a shared rocksdb block cache
    cache: Cache,
    // auto updates to changes in common.rocksdb_memory_limit and common.rocksdb_memtable_total_size_limit
    write_buffer_manager: WriteBufferManager,
    dbs: DashMap<(Owner, DbName), Arc<RocksDb>>,
    watchdog_tx: mpsc::Sender<WatchdogCommand>,
    shutting_down: AtomicBool,
}

impl Debug for RocksDbManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbManager").finish()
    }
}

impl RocksDbManager {
    pub fn get() -> &'static RocksDbManager {
        DB_MANAGER.get().expect("DBManager not initialized")
    }

    /// Create a new instance of the database manager
    ///
    /// Must run in task_center scope.
    pub fn init(mut base_opts: impl Updateable<CommonOptions> + Send + 'static) -> &'static Self {
        let opts = base_opts.load();
        let cache = Cache::new_lru_cache(opts.rocksdb_total_memory_limit as usize);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.rocksdb_total_memtables_size_limit as usize,
            true,
            cache.clone(),
        );
        let dbs = DashMap::default();

        let (watchdog_tx, watchdog_rx) = mpsc::channel(10);

        let manager = Self {
            cache,
            write_buffer_manager,
            dbs,
            watchdog_tx,
            shutting_down: AtomicBool::new(false),
        };

        DB_MANAGER.set(manager).expect("DBManager initialized once");
        // Start db monitoring.
        task_center()
            .spawn(
                TaskKind::SystemService,
                "db-manager",
                None,
                DbWatchdog::run(Self::get(), watchdog_rx, base_opts),
            )
            .expect("run db watchdog");

        Self::get()
    }

    pub fn get_db(&self, owner: Owner, name: DbName) -> Option<Arc<RocksDb>> {
        self.dbs.get(&(owner, name)).as_deref().cloned()
    }

    pub async fn open_db<T: RocksAccess + Send + Sync + 'static>(
        &'static self,
        mut updateable_opts: impl Updateable<RocksDbOptions> + Send + 'static,
        mut db_spec: DbSpec<T>,
    ) -> Result<Arc<T>, RocksError> {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RocksError::Shutdown(ShutdownError));
        }

        tokio::task::spawn_blocking(move || {
            // get latest options
            let options = updateable_opts.load().clone();
            let name = db_spec.name.clone();
            let owner = db_spec.owner;
            // use the spec default options as base then apply the config from the updateable.
            amend_db_options(&mut db_spec.db_options, &options);
            // write butter is controlled by write buffer manager
            db_spec
                .db_options
                .set_write_buffer_manager(&self.write_buffer_manager);
            // todo: set avoid_unnecessary_blocking_io = true;

            // for every column family, generate cf_options with the same strategy
            for (_, cf_opts) in &mut db_spec.column_families {
                amend_cf_options(cf_opts, &options, &self.cache);
            }

            let db = Arc::new(RocksAccess::open_db(&db_spec)?);
            let wrapper = Arc::new(RocksDb::new(db_spec, db.clone()));

            self.dbs.insert((owner, name.clone()), wrapper);

            if let Err(e) =
                self.watchdog_tx
                    .blocking_send(WatchdogCommand::Register(ConfigSubscription {
                        owner,
                        name: name.clone(),
                        updateable_rocksdb_opts: Box::new(updateable_opts),
                        last_applied_opts: options,
                    }))
            {
                warn!(
                    db = %name,
                    owner = %owner,
                    "Failed to register database with watchdog: {}, this database will \
                        not receive config updates but the system will continue to run as normal",
                    e
                );
            }
            Ok(db)
        })
        .await
        .map_err(|_| RocksError::Shutdown(ShutdownError))?
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn reset(&self) -> anyhow::Result<()> {
        self.watchdog_tx
            .send(WatchdogCommand::ResetAll)
            .await
            .map_err(|_| RocksError::Shutdown(ShutdownError))?;
        Ok(())
    }

    /// Returns aggregated memory usage for all databases if filter is empty
    pub fn get_memory_usage_stats(
        &self,
        filter: &[(Owner, DbName)],
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        builder.add_cache(&self.cache);

        if filter.is_empty() {
            for db in self.dbs.iter() {
                db.db.record_memory_stats(&mut builder);
            }
        } else {
            for key in filter {
                if let Some(db) = self.dbs.get(key) {
                    db.db.record_memory_stats(&mut builder);
                }
            }
        }

        Ok(builder.build()?)
    }

    pub fn get_all_dbs(&self) -> Vec<Arc<RocksDb>> {
        self.dbs.iter().map(|k| k.clone()).collect()
    }
}

#[allow(dead_code)]
struct ConfigSubscription {
    owner: Owner,
    name: DbName,
    updateable_rocksdb_opts: Box<dyn Updateable<RocksDbOptions> + Send + 'static>,
    last_applied_opts: RocksDbOptions,
}

struct DbWatchdog {
    manager: &'static RocksDbManager,
    cache: Cache,
    watchdog_rx: tokio::sync::mpsc::Receiver<WatchdogCommand>,
    updateable_common_opts: Box<dyn Updateable<CommonOptions> + Send>,
    current_common_opts: CommonOptions,
    subscriptions: Vec<ConfigSubscription>,
}

impl DbWatchdog {
    pub async fn run(
        manager: &'static RocksDbManager,
        watchdog_rx: tokio::sync::mpsc::Receiver<WatchdogCommand>,
        mut updateable_common_opts: impl Updateable<CommonOptions> + Send + 'static,
    ) -> anyhow::Result<()> {
        let prev_opts = updateable_common_opts.load().clone();
        let mut watchdog = Self {
            manager,
            cache: manager.cache.clone(),
            watchdog_rx,
            updateable_common_opts: Box::new(updateable_common_opts),
            current_common_opts: prev_opts,
            subscriptions: Vec::new(),
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
                    break;
                }
                Some(cmd) = watchdog.watchdog_rx.recv() => {
                    watchdog.handle_command(cmd)
                }
                _ = config_watch.changed() => {
                    watchdog.on_config_update();
                }
            }
        }

        watchdog.shutdown();
        Ok(())
    }

    fn shutdown(&mut self) {
        self.manager
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Release);
        // Ask all databases to shutdown cleanly.
        let start = Instant::now();
        for v in &self.manager.dbs {
            info!(
                db = %v.name,
                owner = %v.owner,
                "Shutting down rocksdb database"
            );
            if let Err(e) = v.db.flush_wal(true) {
                warn!(
                    db = %v.name,
                    owner = %v.owner,
                    "Failed to flush local loglet rocksdb WAL: {}",
                    e
                );
            }
            if let Err(e) = v.db.flush_memtables(v.cfs().as_slice(), true) {
                warn!(
                    db = %v.name,
                    owner = %v.owner,
                    "Failed to flush memtables: {}",
                    e
                );
            }
            v.db.cancel_all_background_work(true);
        }
        info!("Clean rocksdb shutdown took {:?}", start.elapsed());
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            #[cfg(any(test, feature = "test-util"))]
            WatchdogCommand::ResetAll => {
                self.shutdown();
                self.manager.dbs.clear();
                self.subscriptions.clear();
                self.manager
                    .shutting_down
                    .store(false, std::sync::atomic::Ordering::Release);
            }
            WatchdogCommand::Register(sub) => self.subscriptions.push(sub),
        }
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
        // Memory budget changed?
        let new_common_opts = self.updateable_common_opts.load();
        if new_common_opts.rocksdb_total_memory_limit
            != self.current_common_opts.rocksdb_total_memory_limit
        {
            info!(
                old = self.current_common_opts.rocksdb_total_memory_limit,
                new = new_common_opts.rocksdb_total_memory_limit,
                "[config update] Setting rocksdb total memory limit to {}",
                ByteSize::b(new_common_opts.rocksdb_total_memory_limit)
            );
            self.cache
                .set_capacity(new_common_opts.rocksdb_total_memory_limit as usize);
        }

        // update memtable total memory
        if new_common_opts.rocksdb_total_memtables_size_limit
            != self.current_common_opts.rocksdb_total_memtables_size_limit
        {
            info!(
                old = self.current_common_opts.rocksdb_total_memtables_size_limit,
                new = new_common_opts.rocksdb_total_memtables_size_limit,
                "[config update] Setting rocksdb total memtables size limit to {}",
                ByteSize::b(new_common_opts.rocksdb_total_memtables_size_limit)
            );
            self.manager
                .write_buffer_manager
                .set_buffer_size(new_common_opts.rocksdb_total_memtables_size_limit as usize);
        }

        // todo: Apply other changes to the databases.
        // e.g. set write_buffer_size
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);
