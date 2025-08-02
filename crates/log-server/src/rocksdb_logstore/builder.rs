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

use rocksdb::{DBCompressionType, SliceTransform};
use static_assertions::const_assert;

use restate_core::ShutdownError;
use restate_rocksdb::{CfExactPattern, CfName, DbName, DbSpecBuilder, RocksDb, RocksDbManager};
use restate_types::config::LogServerOptions;
use restate_types::health::HealthStatus;
use restate_types::live::{BoxLiveLoad, LiveLoad, LiveLoadExt};
use restate_types::protobuf::common::LogServerStatus;

use super::writer::LogStoreWriter;
use super::{DATA_CF, DB_NAME, METADATA_CF};
use super::{RocksDbLogStore, RocksDbLogStoreError};
use crate::rocksdb_logstore::keys::KeyPrefix;
use crate::rocksdb_logstore::metadata_merge::{metadata_full_merge, metadata_partial_merge};

const DATA_CF_BUDGET_RATIO: f64 = 0.85;
const_assert!(DATA_CF_BUDGET_RATIO < 1.0);

#[derive(Clone)]
pub struct RocksDbLogStoreBuilder {
    rocksdb: Arc<RocksDb>,
    updateable_options: BoxLiveLoad<LogServerOptions>,
}

impl RocksDbLogStoreBuilder {
    pub async fn create(
        mut updateable_options: impl LiveLoad<Live = LogServerOptions> + Clone + 'static,
    ) -> Result<Self, RocksDbLogStoreError> {
        let options = updateable_options.live_load();
        let data_dir = options.data_dir();
        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

        let db_spec = DbSpecBuilder::new(db_name, data_dir, db_options(options))
            .add_cf_pattern(
                CfExactPattern::new(DATA_CF),
                cf_data_options(options.rocksdb_memory_budget()),
            )
            .add_cf_pattern(
                CfExactPattern::new(METADATA_CF),
                cf_metadata_options(options.rocksdb_memory_budget()),
            )
            // not very important but it's to reduce the number of merges by flushing.
            // it's also a small cf so it should be quick.
            .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
            .add_to_flush_on_shutdown(CfExactPattern::new(DATA_CF))
            .ensure_column_families(cfs)
            .build()
            .expect("valid spec");
        let rocksdb = db_manager
            .open_db(
                updateable_options.clone().map(|config| &config.rocksdb),
                db_spec,
            )
            .await?;

        Ok(Self {
            rocksdb,
            updateable_options: updateable_options.boxed(),
        })
    }

    pub async fn start(
        self,
        health_status: HealthStatus<LogServerStatus>,
    ) -> Result<RocksDbLogStore, ShutdownError> {
        let RocksDbLogStoreBuilder {
            rocksdb,
            updateable_options,
        } = self;
        // todo (asoli) load up our loglet metadata cache.
        let writer_handle = LogStoreWriter::new(
            rocksdb.clone(),
            updateable_options.clone(),
            health_status.clone(),
        )
        .start()?;

        Ok(RocksDbLogStore {
            health_status,
            _updateable_options: updateable_options,
            rocksdb,
            writer_handle,
        })
    }
}

fn db_options(log_server_opts: &LogServerOptions) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    // This is Rocksdb's default, it's added here for clarity.
    //
    // Rationale: If WAL tail is corrupted, it's likely that it has failed during write, that said,
    // we can use absolute consistency but on a single-node setup, we don't have a way to recover
    // from it, so it's not useful for us. Even for a replicated loglet setup, the recovery is
    // likely to depend on the last durably committed record for a quorum of nodes in the write
    // set.
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);
    opts.set_wal_compression_type(DBCompressionType::Zstd);
    // most reads are sequential
    opts.set_advise_random_on_open(false);

    opts.set_max_total_wal_size(
        log_server_opts
            .rocksdb_max_wal_size()
            .try_into()
            .expect("fits into u64"),
    );

    opts.set_enable_pipelined_write(true);
    opts.set_max_subcompactions(log_server_opts.rocksdb_max_sub_compactions());

    opts
}

fn cf_data_options(
    memory_budget: usize,
) -> impl Fn(rocksdb::Options) -> rocksdb::Options + Send + Sync + 'static {
    move |mut opts| {
        // memory budget is in bytes. We divide the budget between the data cf and metadata cf.
        let memtables_budget = (memory_budget as f64 * DATA_CF_BUDGET_RATIO).floor() as usize;
        assert!(
            memtables_budget > 0,
            "memory budget should be greater than 0"
        );

        set_memory_related_opts(&mut opts, memtables_budget);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_num_levels(7);

        opts.set_compression_per_level(&[
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);

        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(KeyPrefix::size()));
        opts.set_memtable_prefix_bloom_ratio(0.2);
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
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);
        opts.set_memtable_whole_key_filtering(true);
        opts.set_max_write_buffer_number(4);
        opts.set_max_successive_merges(10);
        // Merge operator for some metadata updates
        opts.set_merge_operator("MetadataMerge", metadata_full_merge, metadata_partial_merge);
        opts
    }
}
