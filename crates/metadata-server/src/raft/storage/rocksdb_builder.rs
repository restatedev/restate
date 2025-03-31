// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::storage::{DATA_CF, DATA_DIR, DB_NAME, METADATA_CF};
use restate_rocksdb::{
    CfExactPattern, CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::config::{MetadataServerOptions, RocksDbOptions, data_dir};
use restate_types::live::BoxedLiveLoad;
use rocksdb::DBCompressionType;
use static_assertions::const_assert;
use std::sync::Arc;

const DATA_CF_BUDGET_RATIO: f64 = 0.85;
const_assert!(DATA_CF_BUDGET_RATIO < 1.0);

pub async fn build_rocksdb(
    options: &MetadataServerOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
) -> Result<Arc<RocksDb>, RocksError> {
    let data_dir = data_dir(DATA_DIR);
    let db_name = DbName::new(DB_NAME);
    let db_manager = RocksDbManager::get();
    let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

    let db_spec = DbSpecBuilder::new(db_name, data_dir, db_options(options))
        .add_cf_pattern(
            CfPrefixPattern::new(DATA_CF),
            cf_data_options(options.rocksdb_memory_budget()),
        )
        .add_cf_pattern(
            CfPrefixPattern::new(METADATA_CF),
            cf_metadata_options(options.rocksdb_memory_budget()),
        )
        // not very important but it's to reduce the number of merges by flushing.
        // it's also a small cf so it should be quick.
        .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
        .add_to_flush_on_shutdown(CfExactPattern::new(DATA_CF))
        .ensure_column_families(cfs)
        .build()
        .expect("valid spec");

    let db_name = db_spec.name().clone();
    let _ = db_manager.open_db(rocksdb_options, db_spec).await?;
    Ok(db_manager
        .get_db(db_name)
        .expect("raft metadata store db is open"))
}

pub fn db_options(_options: &MetadataServerOptions) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();

    // Enable atomic flushes.
    // If WAL is disabled, this ensure we do not persist inconsistent data.
    // If WAL is enabled, this ensures that flushing either cf flushes both.
    // This is valuable because otherwise the metadata cf will flush rarely, and that would keep the WAL around
    // until shutdown, full of data cf bytes that have already been flushed, wasting disk space.
    opts.set_atomic_flush(true);

    // Make sure that we don't accept any potentially lost writes since it might cause us to make
    // contradicting promises to our metadata server peers.
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::AbsoluteConsistency);

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
        let memtables_budget = (memory_budget as f64 * DATA_CF_BUDGET_RATIO).floor() as usize;
        assert!(
            memtables_budget > 0,
            "memory budget should be greater than 0"
        );

        set_memory_related_opts(&mut opts, memtables_budget);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_num_levels(7);

        opts.set_compression_per_level(&[
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
        ]);

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
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
        ]);
        opts.set_memtable_whole_key_filtering(true);
        opts.set_max_write_buffer_number(4);
        opts.set_max_successive_merges(10);
        opts
    }
}
