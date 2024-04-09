// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use restate_types::arc_util::Updateable;
use restate_types::config::{LocalLogletOptions, RocksDbOptions};
use rocksdb::{BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, DB};
use tracing::{debug, warn};

use super::keys::{MetadataKey, MetadataKind};
use super::log_state::{log_state_full_merge, log_state_partial_merge, LogState};
use super::log_store_writer::LogStoreWriter;

pub(crate) static DATA_CF: &str = "logstore_data";
pub(crate) static METADATA_CF: &str = "logstore_metadata";

#[derive(Debug, Clone, thiserror::Error)]
pub enum LogStoreError {
    #[error(transparent)]
    // unfortunately, we have to use Arc here, because the bincode error is not Clone.
    Encode(#[from] Arc<bincode::error::EncodeError>),
    #[error(transparent)]
    // unfortunately, we have to use Arc here, because the bincode error is not Clone.
    Decode(#[from] Arc<bincode::error::DecodeError>),
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
}

#[derive(Debug, Clone)]
pub struct RocksDbLogStore {
    db: Arc<DB>,
}

impl RocksDbLogStore {
    pub fn new(
        mut updateable_options: impl Updateable<LocalLogletOptions> + Send + 'static,
    ) -> Result<Self, LogStoreError> {
        // todo start a task that updates rocksdb options
        let options = &updateable_options.load();
        let rock_opts = &options.rocksdb;
        // todo: get shared rocksdb cache
        // temporary
        let cache = Some(Cache::new_lru_cache(0));

        let mut metadata_cf_options = cf_common_options(rock_opts, cache.clone());
        metadata_cf_options.set_min_write_buffer_number_to_merge(10);
        metadata_cf_options.set_max_successive_merges(10);
        // Merge operator for log state updates
        metadata_cf_options.set_merge_operator(
            "LogStateMerge",
            log_state_full_merge,
            log_state_partial_merge,
        );

        let cfs = [
            rocksdb::ColumnFamilyDescriptor::new(
                DATA_CF,
                cf_common_options(rock_opts, cache.clone()),
            ),
            rocksdb::ColumnFamilyDescriptor::new(METADATA_CF, metadata_cf_options),
        ];
        let db_options = db_options(rock_opts);

        let db = DB::open_cf_descriptors(&db_options, options.data_dir(), cfs)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn data_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(DATA_CF).expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(METADATA_CF).expect("METADATA_CF exists")
    }

    pub fn get_log_state(&self, log_id: u64) -> Result<Option<LogState>, LogStoreError> {
        let metadata_cf = self.metadata_cf();
        let value = self.db.get_pinned_cf(
            metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
        )?;

        if let Some(value) = value {
            Ok(Some(LogState::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    pub fn create_writer(&self, manual_wal_flush: bool) -> LogStoreWriter {
        LogStoreWriter::new(self.db.clone(), manual_wal_flush)
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn shutdown(&self) {
        let start = Instant::now();
        if let Err(e) = self.db.flush_wal(true) {
            warn!("Failed to flush local loglet rocksdb WAL: {}", e);
        }
        self.db.cancel_all_background_work(true);
        debug!(
            "Local loglet clean rocksdb shutdown took {:?}",
            start.elapsed(),
        );
    }
}

fn db_options(opts: &RocksDbOptions) -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    if opts.rocksdb_num_threads() > 0 {
        db_options.increase_parallelism(opts.rocksdb_num_threads() as i32);
        db_options.set_max_background_jobs(opts.rocksdb_num_threads() as i32);
    }

    // todo: integrate with node-ctrl monitoring
    if !opts.rocksdb_disable_statistics() {
        db_options.enable_statistics();
        // Reasonable default, but we might expose this as a config in the future.
        db_options.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
    }

    // Disable WAL archiving.
    // the following two options has to be both 0 to disable WAL log archive.
    //
    db_options.set_wal_size_limit_mb(0);
    db_options.set_wal_ttl_seconds(0);
    //
    // Disable automatic WAL flushing if flush_wal_on_commit is enabled
    // We will call flush manually, when we commit a storage transaction.
    if !opts.rocksdb_disable_wal() {
        db_options.set_manual_wal_flush(opts.rocksdb_batch_wal_flushes());
        //
        // Once the WAL logs exceed this size, rocksdb start will start flush memtables to disk
        // We set this value to 10GB by default, to make sure that we don't flush
        // memtables prematurely.
        db_options.set_max_total_wal_size(opts.rocksdb_max_total_wal_size());
    }
    //
    // write buffer
    //
    // we disable the total write buffer size, since in restate the column family
    // number is static.
    //
    db_options.set_db_write_buffer_size(0);
    //
    // Let rocksdb decide for level sizes.
    //
    db_options.set_level_compaction_dynamic_level_bytes(true);
    db_options.set_compaction_readahead_size(1 << 21);
    //
    // We use Tokio's background threads to access rocksdb, and
    // at most we have 512 threads.
    //
    db_options.set_table_cache_num_shard_bits(9);
    //
    // no need to retain 1000 log files by default.
    //
    db_options.set_keep_log_file_num(10);
    //
    // Allow mmap read and write.
    //
    db_options.set_allow_mmap_reads(true);
    db_options.set_allow_mmap_writes(true);

    db_options
}

fn cf_common_options(opts: &RocksDbOptions, cache: Option<Cache>) -> rocksdb::Options {
    let mut cf_options = rocksdb::Options::default();
    //
    //
    // write buffer
    //
    if opts.rocksdb_write_buffer_size() > 0 {
        cf_options.set_write_buffer_size(opts.rocksdb_write_buffer_size())
    }

    cf_options.set_compaction_style(DBCompactionStyle::Universal);
    //
    // bloom filters and block cache.
    //
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, true);
    // use the latest Rocksdb table format.
    // https://github.com/facebook/rocksdb/blob/f059c7d9b96300091e07429a60f4ad55dac84859/include/rocksdb/table.h#L275
    block_opts.set_format_version(5);
    if let Some(cache) = cache {
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_block_cache(&cache);
    }
    cf_options.set_block_based_table_factory(&block_opts);
    //
    // Set compactions per level
    //
    cf_options.set_num_levels(3);
    cf_options.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::None,
        DBCompressionType::Zstd,
    ]);

    cf_options
}
