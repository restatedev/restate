// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(target_os = "linux")]
use rocksdb::MemoryAllocator;
use rocksdb::{Cache, HyperClockCacheOptions, RateLimiter, RateLimiterMode, WriteBufferManager};
use tracing::error;
#[cfg(target_os = "linux")]
use tracing::warn;

use restate_types::config::{CommonOptions, RocksDbWriteRateLimiterMode};
use restate_util_bytecount::ByteCount;

use crate::RocksError;

/// Optional resources supplied to a database and its column families.
///
/// `Some` shares the supplied resource; `None` leaves database-specific configuration
/// and native defaults in control. Missing resources never implicitly inherit from
/// a manager. In particular, an absent cache does not disable block caching, and an
/// absent native environment does not imply isolated background thread pools.
///
/// Clones share resources, including subsequent changes made by their controller.
/// This bundle does not subscribe to configuration updates, select an application
/// I/O executor, or close databases when dropped. A supplied write buffer manager
/// retains its existing accounting policy, independently of the supplied block cache.
#[derive(Clone, Default)]
pub struct DbEnv {
    /// Shared block cache, if supplied.
    pub cache: Option<Cache>,
    /// Shared memtable accounting and budget, if supplied.
    pub write_buffer_manager: Option<WriteBufferManager>,
    /// Shared native I/O rate limiter, if supplied.
    pub rate_limiter: Option<RateLimiter>,
    /// Native filesystem and background-work environment, if supplied.
    pub native_env: Option<rocksdb::Env>,
}

impl DbEnv {
    /// Replaces the supplied block cache, preserving all other resources.
    ///
    /// `None` opts out of the supplied cache, not out of native block caching.
    /// This does not rebind an inherited write buffer manager's accounting cache.
    #[must_use]
    pub fn with_cache(mut self, cache: Option<Cache>) -> Self {
        self.cache = cache;
        self
    }

    /// Replaces the supplied write buffer manager, preserving all other resources.
    #[must_use]
    pub fn with_write_buffer_manager(mut self, manager: Option<WriteBufferManager>) -> Self {
        self.write_buffer_manager = manager;
        self
    }

    /// Replaces the supplied rate limiter, preserving all other resources.
    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: Option<RateLimiter>) -> Self {
        self.rate_limiter = limiter;
        self
    }

    /// Replaces the supplied native environment, preserving all other resources.
    #[must_use]
    pub fn with_native_env(mut self, env: Option<rocksdb::Env>) -> Self {
        self.native_env = env;
        self
    }

    /// Creates the production resource bundle from an explicit configuration snapshot.
    ///
    /// All resources are populated. The write buffer manager charges memtables to
    /// the supplied cache. This does not start a configuration watcher, and the
    /// native environment uses RocksDB's process-wide default background pools.
    ///
    /// # Errors
    /// Returns an error if the native environment cannot be created.
    pub fn from_common_options(opts: &CommonOptions) -> Result<Self, RocksError> {
        check_memory_limit(opts);

        let cache_opts =
            HyperClockCacheOptions::new(opts.rocksdb_total_memory_size().as_usize(), 0);

        // On Apple platforms jemalloc uses prefixed symbols, so RocksDB cannot use
        // its jemalloc no-dump allocator.
        #[cfg(target_os = "linux")]
        let mut cache_opts = cache_opts;
        #[cfg(target_os = "linux")]
        match MemoryAllocator::new_jemalloc_nodump() {
            Ok(allocator) => cache_opts.set_memory_allocator(&allocator),
            Err(e) => warn!(
                "RocksDB is not built with jemalloc no-dump support. Coredumps will include the full block cache: {e}",
            ),
        }

        let cache = Cache::new_hyper_clock_cache_opts(&cache_opts);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.rocksdb_total_memtables_size().as_usize(),
            false,
            cache.clone(),
        );

        // Initial sizes only: RocksDB grows these pools on database open to fit
        // max_background_flushes and max_background_compactions, never shrinking.
        let mut native_env = rocksdb::Env::new()?;
        native_env.set_high_priority_background_threads(2);
        native_env.set_low_priority_background_threads(1);

        let rate_limiter = RateLimiter::new(
            opts.rocksdb_max_write_rate_per_second.as_u64() as i64,
            100 * 1000,
            10,
            RateLimiterMode::KWritesOnly,
            opts.rocksdb_write_rate_limiter_mode == RocksDbWriteRateLimiterMode::AutoTuned,
        );

        Ok(Self {
            cache: Some(cache),
            write_buffer_manager: Some(write_buffer_manager),
            rate_limiter: Some(rate_limiter),
            native_env: Some(native_env),
        })
    }

    /// Creates a fresh in-memory filesystem for test databases.
    ///
    /// Clones share that filesystem; separate calls create independent filesystems.
    /// Other resources are absent. Native background work still delegates to the
    /// default environment; this does not create an application I/O executor.
    /// The Rust binding may still create an empty host database directory.
    ///
    /// # Errors
    /// Returns an error if the in-memory native environment cannot be created.
    #[cfg(feature = "test-util")]
    pub fn in_memory() -> Result<Self, RocksError> {
        Ok(Self {
            native_env: Some(rocksdb::Env::mem_env()?),
            ..Self::default()
        })
    }
}

pub(crate) fn check_memory_limit(opts: &CommonOptions) {
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

static_assertions::assert_impl_all!(DbEnv: Send, Sync);

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use restate_types::protobuf::common::DatabaseKind;

    use super::*;
    use crate::configuration::{
        CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
    };
    use crate::{CfName, CfPrefixPattern, DbName, DbSpec, DbSpecBuilder, RocksAccess};

    fn supplied(env: &DbEnv) -> [bool; 4] {
        [
            env.native_env.is_some(),
            env.cache.is_some(),
            env.write_buffer_manager.is_some(),
            env.rate_limiter.is_some(),
        ]
    }

    #[derive(Clone)]
    struct Configurator {
        expected: [bool; 4],
        seen_cfs: Arc<Mutex<Vec<String>>>,
    }

    impl DbConfigurator for Configurator {
        fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
            assert_eq!(supplied(env), self.expected);
            create_default_db_options(env, name)
        }
    }

    impl CfConfigurator for Configurator {
        fn get_cf_options(&self, _: &DbName, cf: &str, env: &DbEnv) -> rocksdb::Options {
            assert_eq!(supplied(env), self.expected);
            self.seen_cfs.lock().push(cf.to_owned());
            let mut options = create_default_cf_options(env.write_buffer_manager.as_ref());
            let mut blocks = rocksdb::BlockBasedOptions::default();
            if let Some(cache) = &env.cache {
                blocks.set_block_cache(cache);
            }
            options.set_block_based_table_factory(&blocks);
            options
        }
    }

    fn spec(path: &Path, configurator: &Configurator) -> DbSpec {
        DbSpecBuilder::new(
            DbName::new("db-env"),
            DatabaseKind::PartitionStore,
            path.to_owned(),
            configurator.clone(),
        )
        .add_cf_pattern(CfPrefixPattern::new("data"), configurator.clone())
        .build()
        .unwrap()
    }

    #[test]
    fn customization_preserves_unspecified_shared_resources() -> anyhow::Result<()> {
        assert_eq!(supplied(&DbEnv::default()), [false; 4]);
        let base = DbEnv::from_common_options(&CommonOptions::default())?;
        assert_eq!(supplied(&base), [true; 4]);
        assert!(base.write_buffer_manager.as_ref().unwrap().cost_to_cache());

        let without_cache = base.clone().with_cache(None);
        assert_eq!(supplied(&without_cache), [true, false, true, true]);
        without_cache
            .write_buffer_manager
            .as_ref()
            .unwrap()
            .set_buffer_size(8 * 1024 * 1024);
        assert_eq!(
            base.write_buffer_manager
                .as_ref()
                .unwrap()
                .get_buffer_size(),
            8 * 1024 * 1024,
        );

        let replacement = base
            .clone()
            .with_cache(Some(Cache::new_lru_cache(1024 * 1024)));
        replacement
            .cache
            .as_ref()
            .unwrap()
            .clone()
            .set_capacity(2 * 1024 * 1024);
        assert_eq!(
            replacement.cache.as_ref().unwrap().get_capacity(),
            2 * 1024 * 1024
        );
        assert_eq!(
            base.cache.as_ref().unwrap().get_capacity(),
            CommonOptions::default()
                .rocksdb_total_memory_size()
                .as_usize(),
        );
        let cleared = replacement
            .with_cache(None)
            .with_write_buffer_manager(None)
            .with_rate_limiter(None)
            .with_native_env(None);
        assert_eq!(supplied(&cleared), [false; 4]);
        assert_eq!(supplied(&base), [true; 4]);
        Ok(())
    }

    #[test]
    fn databases_and_new_cfs_accept_independently_optional_resources() -> anyhow::Result<()> {
        let tempdir = restate_test_util::tempdir()?;
        let environments = [
            DbEnv::default(),
            DbEnv::default().with_cache(Some(Cache::new_lru_cache(1024 * 1024))),
            DbEnv::default().with_write_buffer_manager(Some(
                WriteBufferManager::new_write_buffer_manager(8 * 1024 * 1024, false),
            )),
            DbEnv::from_common_options(&CommonOptions::default())?,
        ];
        for (index, env) in environments.into_iter().enumerate() {
            let configurator = Configurator {
                expected: supplied(&env),
                seen_cfs: Arc::default(),
            };
            let db = RocksAccess::open_db(
                spec(&tempdir.path().join(index.to_string()), &configurator),
                env,
            )?;
            db.open_cf(CfName::new("data"))?;
            let cf = db.cf_handle("data").unwrap();
            db.as_raw_db().put_cf(&cf, b"key", b"value")?;
            assert_eq!(db.as_raw_db().get_cf(&cf, b"key")?, Some(b"value".to_vec()));
            assert_eq!(supplied(db.db_env()), configurator.expected);
            assert_eq!(*configurator.seen_cfs.lock(), ["data"]);
        }
        Ok(())
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn in_memory_resources_follow_open_create_import_and_reopen() -> anyhow::Result<()> {
        use restate_core::TaskCenterBuilder;

        use crate::RocksDbManager;

        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);
        assert_eq!(supplied(manager.db_env()), [true; 4]);
        let initial_usage = manager.get_total_write_buffer_usage();
        let tempdir = restate_test_util::tempdir()?;
        let path = tempdir.path().join("memory-db");
        let export = tempdir.path().join("memory-export");
        let configurator = Configurator {
            expected: [true, false, false, false],
            seen_cfs: Arc::default(),
        };
        let db = manager
            .open_db_with_env(spec(&path, &configurator), DbEnv::in_memory()?)
            .await?;
        db.clone().open_cf(CfName::new("data-source")).await?;
        {
            let cf = db.inner().cf_handle("data-source").unwrap();
            db.inner().as_raw_db().put_cf(&cf, b"key", b"value")?;
        }
        let metadata = db
            .clone()
            .export_cf(CfName::new("data-source"), export.clone())
            .await?;
        db.clone()
            .import_cf(CfName::new("data-imported"), metadata)
            .await?;
        assert_eq!(
            *configurator.seen_cfs.lock(),
            ["data-source", "data-imported"]
        );
        assert_eq!(manager.get_total_write_buffer_usage(), initial_usage);
        let retained_env = db.inner().db_env().clone();
        db.close().await.expect("all database users released");

        // A clone retains the filesystem after physical database destruction.
        let reopened = RocksAccess::open_db(spec(&path, &configurator), retained_env)?;
        {
            let cf = reopened.cf_handle("data-imported").unwrap();
            assert_eq!(
                reopened.as_raw_db().get_cf(&cf, b"key")?,
                Some(b"value".to_vec()),
            );
        }
        drop(reopened);

        // A fresh environment at the same path has an independent filesystem.
        let fresh = RocksAccess::open_db(spec(&path, &configurator), DbEnv::in_memory()?)?;
        assert!(fresh.cf_handle("data-imported").is_none());
        // The Rust binding creates the host DB directory before native open, even
        // with a memory environment. Database files themselves must stay in memory.
        assert!(std::fs::read_dir(&path)?.next().is_none());
        assert!(!export.exists());
        drop(fresh);
        task_center.shutdown_node("test completed", 0).await;
        manager.shutdown().await;
        Ok(())
    }
}
