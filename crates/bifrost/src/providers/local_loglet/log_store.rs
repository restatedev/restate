// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_types::errors::MaybeRetryableError;
use rocksdb::{BoundColumnFamily, DB, DBCompressionType, SliceTransform};
use static_assertions::const_assert;

use restate_rocksdb::{
    CfExactPattern, CfName, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_types::config::LocalLogletOptions;
use restate_types::live::{LiveLoad, LiveLoadExt};
use restate_types::storage::{StorageDecodeError, StorageEncodeError};

use super::keys::{DATA_KEY_PREFIX_LENGTH, MetadataKey, MetadataKind};
use super::log_state::{LogState, log_state_full_merge, log_state_partial_merge};
use super::log_store_writer::LogStoreWriter;

// matches the default directory name
pub(crate) const DB_NAME: &str = "local-loglet";

pub(crate) const DATA_CF: &str = "logstore_data";
pub(crate) const METADATA_CF: &str = "logstore_metadata";

const DATA_CF_BUDGET_RATIO: f64 = 0.85;

const_assert!(DATA_CF_BUDGET_RATIO < 1.0);

#[derive(Debug, thiserror::Error)]
pub enum LogStoreError {
    #[error(transparent)]
    Encode(#[from] StorageEncodeError),
    #[error(transparent)]
    Decode(#[from] StorageDecodeError),
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
    #[error(transparent)]
    RocksDbManager(#[from] RocksError),
}

impl MaybeRetryableError for LogStoreError {
    fn retryable(&self) -> bool {
        match self {
            LogStoreError::Encode(_) => false,
            LogStoreError::Decode(_) => false,
            LogStoreError::Rocksdb(_) => true,
            LogStoreError::RocksDbManager(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RocksDbLogStore {
    rocksdb: Arc<RocksDb>,
}

impl RocksDbLogStore {
    pub async fn create(
        mut options: impl LiveLoad<Live = LocalLogletOptions> + 'static,
    ) -> Result<Self, LogStoreError> {
        let db_manager = RocksDbManager::get();

        let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

        let opts = options.live_load();
        let data_dir = opts.data_dir();

        let db_spec = DbSpecBuilder::new(DbName::new(DB_NAME), data_dir, db_options())
            .add_cf_pattern(
                CfExactPattern::new(DATA_CF),
                cf_data_options(opts.rocksdb_memory_budget()),
            )
            .add_cf_pattern(
                CfExactPattern::new(METADATA_CF),
                cf_metadata_options(opts.rocksdb_memory_budget()),
            )
            // not very important but it's to reduce the number of merges by flushing.
            // it's also a small cf so it should be quick.
            .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
            .ensure_column_families(cfs)
            .build()
            .expect("valid spec");
        let rocksdb = db_manager
            .open_db(options.map(|options| &options.rocksdb).boxed(), db_spec)
            .await?;
        Ok(Self { rocksdb })
    }

    pub fn data_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(DATA_CF)
            .expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn get_log_state(&self, loglet_id: u64) -> Result<Option<LogState>, LogStoreError> {
        let metadata_cf = self.metadata_cf();
        let value = self.rocksdb.inner().as_raw_db().get_pinned_cf(
            &metadata_cf,
            MetadataKey::new(loglet_id, MetadataKind::LogState).to_bytes(),
        )?;

        if let Some(value) = value {
            Ok(Some(LogState::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    pub fn create_writer(&self) -> LogStoreWriter {
        LogStoreWriter::new(self.rocksdb.clone())
    }

    pub fn db(&self) -> &DB {
        self.rocksdb.inner().as_raw_db()
    }
}

fn db_options() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();

    // Enable atomic flushes.
    // If WAL is disabled, this ensure we do not persist inconsistent data.
    // If WAL is enabled, this ensures that flushing either cf flushes both.
    // This is valuable because otherwise the metadata cf will flush rarely, and that would keep the WAL around
    // until shutdown, full of data cf bytes that have already been flushed, wasting disk space.
    opts.set_atomic_flush(true);

    // This is Rocksdb's default, it's added here for clarity.
    //
    // Rationale: If WAL tail is corrupted, it's likely that it has failed during write, that said,
    // we can use absolute consistency but on a single-node setup, we don't have a way to recover
    // from it, so it's not useful for us.
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);
    opts.set_wal_compression_type(DBCompressionType::Zstd);
    // most reads are sequential
    opts.set_advise_random_on_open(false);

    opts
}

fn cf_data_options(
    memory_budget: usize,
) -> impl Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync + 'static {
    move |mut opts| {
        // memory budget is in bytes. We divide the budget between the data cf and metadata cf.
        // data 10% to metadata 90% to data.
        let memtables_budget = (memory_budget as f64 * DATA_CF_BUDGET_RATIO).floor() as usize;
        assert!(
            memtables_budget > 0,
            "memory budget should be greater than 0"
        );

        set_memory_related_opts(&mut opts, memtables_budget);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_num_levels(7);

        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
        ]);

        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(DATA_KEY_PREFIX_LENGTH));
        opts.set_memtable_prefix_bloom_ratio(0.2);
        //
        opts
    }
}

fn set_memory_related_opts(opts: &mut rocksdb::Options, memtables_budget: usize) {
    // We set the budget to allow 1 mutable + 3 immutable.
    opts.set_write_buffer_size(memtables_budget / 4);

    // merge 2 memtables when flushing to L0
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_max_write_buffer_number(4);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(memtables_budget as u64 / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(memtables_budget as u64);
}

fn cf_metadata_options(
    memory_budget: usize,
) -> impl Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync + 'static {
    move |mut opts| {
        let memtables_budget =
            (memory_budget as f64 * (1.0 - DATA_CF_BUDGET_RATIO)).floor() as usize;
        assert!(
            memtables_budget > 0,
            "memory budget should be greater than 0"
        );
        set_memory_related_opts(&mut opts, memtables_budget);
        //
        // Set compactions per level
        //
        opts.set_num_levels(3);
        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
        ]);
        opts.set_memtable_whole_key_filtering(true);
        opts.set_max_write_buffer_number(4);
        opts.set_max_successive_merges(10);
        // Merge operator for log state updates
        opts.set_merge_operator(
            "LogStateMerge",
            log_state_full_merge,
            log_state_partial_merge,
        );
        opts
    }
}
