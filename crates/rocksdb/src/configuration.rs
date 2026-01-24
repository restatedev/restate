// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use crate::logging::LoggingEventListener;
use crate::{DbName, RocksAccess};

/// A trait for customizing database options when it's being opened and enables live reaction to
/// configuration changes.
pub trait DbConfigurator {
    fn get_db_options(
        &self,
        db_name: &DbName,
        env: &rocksdb::Env,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options;

    fn apply_db_opts_from_config(
        &self,
        db_options: &mut rocksdb::Options,
        config: &RocksDbOptions,
    ) {
        db_options.set_max_background_jobs(config.rocksdb_max_background_jobs().get() as i32);
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
        db_options.set_compaction_readahead_size(config.rocksdb_compaction_readahead_size().get());

        // Use Direct I/O for reads, do not use OS page cache to cache compressed blocks.
        db_options.set_use_direct_reads(!config.rocksdb_disable_direct_io_for_reads());
        db_options.set_use_direct_io_for_flush_and_compaction(
            !config.rocksdb_disable_direct_io_for_flush_and_compaction(),
        );

        // Configure info logs
        db_options.set_keep_log_file_num(config.rocksdb_log_keep_file_num());
        db_options.set_max_log_file_size(config.rocksdb_log_max_file_size().as_usize());
        db_options.set_log_level(convert_log_level(config.rocksdb_log_level()));
    }

    /// Called when configuration is updated.
    fn note_config_update(&self, _db: &RocksAccess) {}
}

pub fn create_default_db_options(
    env: &rocksdb::Env,
    db_name: &DbName,
    create_db_if_missing: bool,
    write_buffer_manager: &rocksdb::WriteBufferManager,
) -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    db_options.set_env(env);
    if create_db_if_missing {
        db_options.create_if_missing(true);
    }
    db_options.create_missing_column_families(true);
    // write buffer is controlled by write buffer manager
    db_options.set_write_buffer_manager(write_buffer_manager);
    db_options.set_avoid_unnecessary_blocking_io(true);
    // Disable WAL archiving.
    // the following two options has to be both 0 to disable WAL log archive.
    db_options.set_wal_size_limit_mb(0);
    db_options.set_wal_ttl_seconds(0);
    //
    // Let rocksdb decide for level sizes.
    //
    db_options.set_level_compaction_dynamic_level_bytes(true);
    //
    // [Not important setting, consider removing], allows to shard compressed
    // block cache to up to 64 shards in memory.
    //
    db_options.set_table_cache_num_shard_bits(6);

    // Speed up database open, useful for large databases and slow disk.
    db_options.set_skip_stats_update_on_db_open(true);

    // Disable WAL archiving.
    // the following two options has to be both 0 to disable WAL log archive.
    db_options.set_wal_size_limit_mb(0);
    db_options.set_wal_ttl_seconds(0);

    db_options.add_event_listener(LoggingEventListener::new(db_name.clone()));

    db_options
}

pub fn create_default_block_options(
    opts: &RocksDbOptions,
    block_cache: Option<&rocksdb::Cache>,
) -> rocksdb::BlockBasedOptions {
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

    if let Some(block_cache) = block_cache {
        block_opts.set_block_cache(block_cache);
    }
    block_opts
}

pub fn create_default_cf_options(
    write_buffer_manager: Option<&WriteBufferManager>,
) -> rocksdb::Options {
    let mut cf_options = rocksdb::Options::default();
    // write buffer
    if let Some(write_buffer_manager) = write_buffer_manager {
        cf_options.set_write_buffer_manager(write_buffer_manager);
    }
    cf_options.set_avoid_unnecessary_blocking_io(true);
    cf_options.set_optimize_filters_for_hits(true);

    cf_options
}

/// A trait for customizing the column family option when it's being opened.
///
/// A blanked implementation exists for `Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync`
/// to allow easy default -> custom_options transformation.
pub trait CfConfigurator {
    fn get_cf_options(
        &self,
        db_name: &DbName,
        cf_name: &str,
        global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options;
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
