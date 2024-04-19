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
use rocksdb::{BlockBasedOptions, Cache, WriteBufferManager};

use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};
use restate_types::arc_util::Updateable;
use restate_types::config::{CommonOptions, Configuration, RocksDbOptions, StatisticsLevel};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{DbName, DbSpec, Owner, RocksAccess, RocksDb, RocksError};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

enum WatchdogCommand {
    Register(ConfigSubscription),
    #[cfg(any(test, feature = "test-util"))]
    ResetAll(tokio::sync::oneshot::Sender<()>),
}

/// Tracks rocksdb databases created by various components, memory budgeting, monitoring, and
/// acting as a single entry point for all running databases on the node.
///
/// It doesn't try to limit rocksdb use-cases from accessing the raw rocksdb.
pub struct RocksDbManager {
    env: rocksdb::Env,
    /// a shared rocksdb block cache
    cache: Cache,
    // auto updates to changes in common.rocksdb_memory_limit and common.rocksdb_memtable_total_size_limit
    write_buffer_manager: WriteBufferManager,
    dbs: DashMap<(Owner, DbName), Arc<RocksDb>>,
    watchdog_tx: mpsc::UnboundedSender<WatchdogCommand>,
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

    /// Create a new instance of the database manager. This should not be executed concurrently,
    /// only run it once on program startup.
    ///
    /// Must run in task_center scope.
    pub fn init(mut base_opts: impl Updateable<CommonOptions> + Send + 'static) -> &'static Self {
        // best-effort, it doesn't make concurrent access safe, but it's better than nothing.
        if let Some(manager) = DB_MANAGER.get() {
            return manager;
        }
        let opts = base_opts.load();
        let cache = Cache::new_lru_cache(opts.rocksdb_total_memory_limit as usize);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.rocksdb_total_memtables_size_limit as usize,
            true,
            cache.clone(),
        );

        // Setup the shared rocksdb environment
        let mut env = rocksdb::Env::new().expect("rocksdb env is created");
        env.set_low_priority_background_threads(opts.rocksdb_bg_threads().get() as i32);
        env.set_high_priority_background_threads(opts.rocksdb_high_priority_bg_threads.get() as i32);

        let dbs = DashMap::default();

        // unbounded channel since commands are rare and we don't want to block
        let (watchdog_tx, watchdog_rx) = mpsc::unbounded_channel();

        let manager = Self {
            env,
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

    // todo: move this to async after allowing bifrost to async-create providers.
    pub fn open_db<T: RocksAccess + Send + Sync + 'static>(
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

        // get latest options
        let options = updateable_opts.load().clone();
        let name = db_spec.name.clone();
        let owner = db_spec.owner;
        // use the spec default options as base then apply the config from the updateable.
        self.amend_db_options(&mut db_spec.db_options, &options);

        let (db, cfs) = RocksAccess::open_db(&db_spec, self.default_cf_options(&options))?;
        let db = Arc::new(db);

        let path = db_spec.path.clone();
        let wrapper = Arc::new(RocksDb::new(db_spec, db.clone(), cfs));

        self.dbs.insert((owner, name.clone()), wrapper);

        if let Err(e) = self
            .watchdog_tx
            .send(WatchdogCommand::Register(ConfigSubscription {
                owner,
                name: name.clone(),
                updateable_rocksdb_opts: Box::new(updateable_opts),
                last_applied_opts: options,
            }))
        {
            warn!(
                db = %name,
                owner = %owner,
                path = %path.display(),
                "Failed to register database with watchdog: {}, this database will \
                    not receive config updates but the system will continue to run as normal",
                e
            );
        }
        info!(
            db = %name,
            owner = %owner,
            path = %path.display(),
            "Opened rocksdb database"
        );
        Ok(db)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn reset(&self) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.watchdog_tx
            .send(WatchdogCommand::ResetAll(tx))
            .map_err(|_| RocksError::Shutdown(ShutdownError))?;
        // safe to unwrap since we use this only in tests
        rx.await.unwrap();
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

    pub async fn shutdown(&'static self) {
        // Ask all databases to shutdown cleanly.
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for v in &self.dbs {
            tasks.spawn(async move {
                v.shutdown().await;
                (v.name.clone(), v.owner)
            });
        }
        // wait for all tasks to complete
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok((name, owner)) => {
                    info!(
                        db = %name,
                        owner = %owner,
                        "Rocksdb database shutdown completed, {} remaining", tasks.len());
                }
                Err(e) => {
                    warn!("Failed to shutdown db: {}", e);
                }
            }
        }
        info!("Rocksdb shutdown took {:?}", start.elapsed());
    }

    fn amend_db_options(&self, db_options: &mut rocksdb::Options, opts: &RocksDbOptions) {
        db_options.set_env(&self.env);
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_max_background_jobs(opts.rocksdb_max_background_jobs().get() as i32);

        // write butter is controlled by write buffer manager
        db_options.set_write_buffer_manager(&self.write_buffer_manager);

        // todo: set avoid_unnecessary_blocking_io = true;

        if !opts.rocksdb_disable_statistics() {
            db_options.enable_statistics();
            db_options
                .set_statistics_level(convert_statistics_level(opts.rocksdb_statistics_level()));
        }

        // Disable WAL archiving.
        // the following two options has to be both 0 to disable WAL log archive.
        db_options.set_wal_size_limit_mb(0);
        db_options.set_wal_ttl_seconds(0);

        if !opts.rocksdb_disable_wal() {
            // Disable automatic WAL flushing.
            // We will call flush manually, when we commit a storage transaction.
            //
            db_options.set_manual_wal_flush(opts.rocksdb_batch_wal_flushes());
            // Once the WAL logs exceed this size, rocksdb start will start flush memtables to disk.
            db_options.set_max_total_wal_size(opts.rocksdb_max_total_wal_size());
        }
        //
        // Let rocksdb decide for level sizes.
        //
        db_options.set_level_compaction_dynamic_level_bytes(true);
        db_options.set_compaction_readahead_size(opts.rocksdb_compaction_readahead_size());
        //
        // [Not important setting, consider removing], allows to shard compressed
        // block cache to up to 64 shards in memory.
        //
        db_options.set_table_cache_num_shard_bits(6);

        // Use Direct I/O for reads, do not use OS page cache to cache compressed blocks.
        db_options.set_use_direct_reads(true);
        db_options.set_use_direct_io_for_flush_and_compaction(true);
    }

    fn default_cf_options(&self, opts: &RocksDbOptions) -> rocksdb::Options {
        let mut cf_options = rocksdb::Options::default();
        // write buffer
        //
        cf_options.set_write_buffer_size(opts.rocksdb_write_buffer_size());
        //
        // bloom filters and block cache.
        //
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, true);
        // use the latest Rocksdb table format.
        // https://github.com/facebook/rocksdb/blob/f059c7d9b96300091e07429a60f4ad55dac84859/include/rocksdb/table.h#L275
        block_opts.set_format_version(5);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_block_cache(&self.cache);
        cf_options.set_block_based_table_factory(&block_opts);

        cf_options
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
    watchdog_rx: mpsc::UnboundedReceiver<WatchdogCommand>,
    updateable_common_opts: Box<dyn Updateable<CommonOptions> + Send>,
    current_common_opts: CommonOptions,
    subscriptions: Vec<ConfigSubscription>,
}

impl DbWatchdog {
    pub async fn run(
        manager: &'static RocksDbManager,
        watchdog_rx: mpsc::UnboundedReceiver<WatchdogCommand>,
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
                    manager
                        .shutting_down
                        .store(true, std::sync::atomic::Ordering::Release);
                    break;
                }
                Some(cmd) = watchdog.watchdog_rx.recv() => {
                    watchdog.handle_command(cmd).await;
                }
                _ = config_watch.changed() => {
                    watchdog.on_config_update();
                }
            }
        }

        Ok(())
    }

    async fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            #[cfg(any(test, feature = "test-util"))]
            WatchdogCommand::ResetAll(response) => {
                self.manager
                    .shutting_down
                    .store(true, std::sync::atomic::Ordering::Release);
                self.manager.shutdown().await;
                self.manager.dbs.clear();
                self.subscriptions.clear();
                self.manager
                    .shutting_down
                    .store(false, std::sync::atomic::Ordering::Release);
                // safe to unwrap since we use this only in tests
                response.send(()).unwrap();
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

fn convert_statistics_level(input: StatisticsLevel) -> rocksdb::statistics::StatsLevel {
    use rocksdb::statistics::StatsLevel;
    match input {
        StatisticsLevel::DisableAll => StatsLevel::DisableAll,
        StatisticsLevel::ExceptHistogramOrTimers => StatsLevel::ExceptHistogramOrTimers,
        StatisticsLevel::ExceptTimers => StatsLevel::ExceptTimers,
        StatisticsLevel::ExceptDetailedTimers => StatsLevel::ExceptDetailedTimers,
        StatisticsLevel::ExceptTimeForMutex => StatsLevel::ExceptTimeForMutex,
        StatisticsLevel::All => StatsLevel::All,
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);
