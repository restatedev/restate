// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::{BlockBasedOptions, Cache, WriteBufferManager};

use restate_types::config::{RocksDbLogLevel, RocksDbOptions, StatisticsLevel};

use crate::RocksAccess;

/// A trait for customizing database options when it's being opened and enables live reaction to
/// configuration changes.
pub trait DbConfigurator {
    fn get_db_options(
        &self,
        db_name: &str,
        env: &rocksdb::Env,
        global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options;

    /// Called when configuration is updated.
    fn note_config_update(&self, db: &RocksAccess, db_name: &str);
}

pub struct DefaultDbConfigurator {
    /// Applied after database options is created from config/env.
    post_config: Box<dyn Fn(&mut rocksdb::Options) + Send + Sync + 'static>,
    /// Where to source the configuration from.
    config_source: Box<dyn Fn() -> RocksDbOptions + Send + Sync + 'static>,
}

impl DefaultDbConfigurator {
    pub fn new(config_source: impl Fn() -> RocksDbOptions + Send + Sync + 'static) -> Self {
        Self {
            post_config: Box::new(|_| {}),
            config_source: Box::new(config_source),
        }
    }

    pub fn new_with_post_config(
        config_source: impl Fn() -> RocksDbOptions + Send + Sync + 'static,
        post_config: impl Fn(&mut rocksdb::Options) + Send + Sync + 'static,
    ) -> Self {
        Self {
            post_config: Box::new(post_config),
            config_source: Box::new(config_source),
        }
    }
}

impl DbConfigurator for DefaultDbConfigurator {
    fn get_db_options(
        &self,
        _db_name: &str,
        env: &rocksdb::Env,
        _global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let mut db_options = rocksdb::Options::default();
        let config = (self.config_source)();
        // load config from the input configuration
        // amend default options from rocksdb_manager
        db_options.set_env(env);
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_max_background_jobs(config.rocksdb_max_background_jobs().get() as i32);

        // write buffer is controlled by write buffer manager
        db_options.set_write_buffer_manager(write_buffer_manager);

        db_options.set_avoid_unnecessary_blocking_io(true);

        if !config.rocksdb_disable_statistics() {
            db_options.enable_statistics();
            db_options
                .set_statistics_level(convert_statistics_level(config.rocksdb_statistics_level()));
        }

        // no need to retain 1000 log files by default.
        if !config.rocksdb_disable_wal() {
            // RocksDB does not support recycling wal log files if wal is disabled when writing
            db_options.set_recycle_log_file_num(4);
        }
        // Disable WAL archiving.
        // the following two options has to be both 0 to disable WAL log archive.
        db_options.set_wal_size_limit_mb(0);
        db_options.set_wal_ttl_seconds(0);

        //
        // Let rocksdb decide for level sizes.
        //
        db_options.set_level_compaction_dynamic_level_bytes(true);
        db_options.set_compaction_readahead_size(config.rocksdb_compaction_readahead_size().get());
        //
        // [Not important setting, consider removing], allows to shard compressed
        // block cache to up to 64 shards in memory.
        //
        db_options.set_table_cache_num_shard_bits(6);

        // Speed up database open, useful for large databases and slow disk.
        db_options.set_skip_stats_update_on_db_open(true);

        // Use Direct I/O for reads, do not use OS page cache to cache compressed blocks.
        db_options.set_use_direct_reads(!config.rocksdb_disable_direct_io_for_reads());
        db_options.set_use_direct_io_for_flush_and_compaction(
            !config.rocksdb_disable_direct_io_for_flush_and_compaction(),
        );

        // Configure info logs
        db_options.set_keep_log_file_num(config.rocksdb_log_keep_file_num());
        db_options.set_max_log_file_size(config.rocksdb_log_max_file_size().as_usize());
        db_options.set_log_level(convert_log_level(config.rocksdb_log_level()));
        (self.post_config)(&mut db_options);

        db_options
    }

    fn note_config_update(&self, _db: &RocksAccess, _db_name: &str) {}
}

/// A trait for customizing the column family option when it's being opened.
///
/// A blanked implementation exists for `Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync`
/// to allow easy default -> custom_options transformation.
pub trait CfConfigurator {
    fn get_cf_options(
        &self,
        db_name: &str,
        cf_name: &str,
        global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options;
}

pub struct DefaultCfConfigurator {
    /// Applied after database options is created from config/env.
    post_config: Box<dyn Fn(&mut rocksdb::Options) + Send + Sync + 'static>,
    /// Where to source the configuration from.
    config_source: Box<dyn Fn() -> RocksDbOptions + Send + Sync + 'static>,
}

impl DefaultCfConfigurator {
    pub fn new(config_source: impl Fn() -> RocksDbOptions + Send + Sync + 'static) -> Self {
        Self {
            post_config: Box::new(|_| {}),
            config_source: Box::new(config_source),
        }
    }

    pub fn new_with_post_config(
        config_source: impl Fn() -> RocksDbOptions + Send + Sync + 'static,
        post_config: impl Fn(&mut rocksdb::Options) + Send + Sync + 'static,
    ) -> Self {
        Self {
            post_config: Box::new(post_config),
            config_source: Box::new(config_source),
        }
    }
}

impl CfConfigurator for DefaultCfConfigurator {
    fn get_cf_options(
        &self,
        _db_name: &str,
        _cf_name: &str,
        global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let opts = (self.config_source)();
        let mut cf_options = generate_cf_options(&opts, write_buffer_manager, global_cache);
        (self.post_config)(&mut cf_options);
        cf_options
    }
}

pub fn generate_cf_options(
    opts: &RocksDbOptions,
    write_buffer_manager: &WriteBufferManager,
    global_cache: &rocksdb::Cache,
) -> rocksdb::Options {
    let mut cf_options = rocksdb::Options::default();
    // write buffer
    cf_options.set_write_buffer_manager(write_buffer_manager);
    cf_options.set_max_background_jobs(opts.rocksdb_max_background_jobs().get() as i32);
    cf_options.set_avoid_unnecessary_blocking_io(true);

    cf_options.set_optimize_filters_for_hits(true);
    // bloom filters and block cache.
    //
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, true);
    // use the latest Rocksdb table format.
    // https://github.com/facebook/rocksdb/blob/56359da69132d769e97f0a7cc89681d3500e166d/include/rocksdb/table.h#L571
    block_opts.set_format_version(6);
    block_opts.set_optimize_filters_for_memory(true);
    block_opts.set_index_block_restart_interval(4);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_block_size(opts.rocksdb_block_size().get());

    block_opts.set_block_cache(global_cache);
    cf_options.set_block_based_table_factory(&block_opts);

    cf_options
}

pub fn convert_statistics_level(input: StatisticsLevel) -> rocksdb::statistics::StatsLevel {
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

pub fn convert_log_level(input: RocksDbLogLevel) -> rocksdb::LogLevel {
    use rocksdb::LogLevel;
    match input {
        RocksDbLogLevel::Debug => LogLevel::Debug,
        RocksDbLogLevel::Error => LogLevel::Error,
        RocksDbLogLevel::Fatal => LogLevel::Fatal,
        RocksDbLogLevel::Header => LogLevel::Header,
        RocksDbLogLevel::Info => LogLevel::Info,
        RocksDbLogLevel::Warn => LogLevel::Warn,
    }
}
