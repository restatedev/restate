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

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType};
use static_assertions::const_assert;

use restate_rocksdb::{
    CfExactPattern, CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::config::{Configuration, MetadataServerOptions, data_dir};

use crate::raft::storage::{DATA_CF, DATA_DIR, DB_NAME, METADATA_CF};

const DATA_CF_BUDGET_RATIO: f64 = 0.85;
const_assert!(DATA_CF_BUDGET_RATIO < 1.0);

pub async fn build_rocksdb() -> Result<Arc<RocksDb>, RocksError> {
    let data_dir = data_dir(DATA_DIR);
    let db_name = DbName::new(DB_NAME);
    let db_manager = RocksDbManager::get();
    let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

    let db_spec = DbSpecBuilder::new(db_name, data_dir, RocksConfigurator)
        .add_cf_pattern(CfPrefixPattern::new(DATA_CF), RocksConfigurator)
        .add_cf_pattern(CfPrefixPattern::new(METADATA_CF), RocksConfigurator)
        // not very important but it's to reduce the number of merges by flushing.
        // it's also a small cf so it should be quick.
        .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
        .add_to_flush_on_shutdown(CfExactPattern::new(DATA_CF))
        .ensure_column_families(cfs)
        .build()
        .expect("valid spec");

    db_manager.open_db(db_spec).await
}

struct RocksConfigurator;

impl restate_rocksdb::configuration::DbConfigurator for RocksConfigurator {
    fn get_db_options(
        &self,
        db_name: &DbName,
        env: &rocksdb::Env,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let mut db_options = restate_rocksdb::configuration::create_default_db_options(
            env,
            db_name,
            true, /* create_db_if_missing */
            write_buffer_manager,
        );
        // load config from the input configuration
        let metadata_server_config = &Configuration::pinned().metadata_server;
        // amend default options from rocksdb_manager
        self.apply_db_opts_from_config(&mut db_options, &metadata_server_config.rocksdb);

        // Metadata server customizations

        // Enable atomic flushes.
        // If WAL is disabled, this ensure we do not persist inconsistent data.
        // If WAL is enabled, this ensures that flushing either cf flushes both.
        // This is valuable because otherwise the metadata cf will flush rarely, and that would keep the WAL around
        // until shutdown, full of data cf bytes that have already been flushed, wasting disk space.
        db_options.set_atomic_flush(true);

        // We cannot prevent that tail writes don't fully complete in the presence of hard crashes.
        // That's why we tolerate corrupted tail records knowing that we haven't acted on them yet. The
        // downside is that this is only a heuristic and cannot distinguish between a corrupted tail and
        // an incomplete write (see https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes#ktoleratecorruptedtailrecords).
        db_options.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);

        db_options.set_wal_compression_type(DBCompressionType::Zstd);
        // most reads are sequential
        db_options.set_advise_random_on_open(false);

        db_options
    }
}

impl restate_rocksdb::configuration::CfConfigurator for RocksConfigurator {
    fn get_cf_options(
        &self,
        _db_name: &DbName,
        cf_name: &str,
        global_cache: &Cache,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let config = &Configuration::pinned().metadata_server;
        let mut cf_options =
            restate_rocksdb::configuration::create_default_cf_options(Some(write_buffer_manager));
        let block_options = restate_rocksdb::configuration::create_default_block_options(
            &config.rocksdb,
            Some(global_cache),
        );

        cf_options.set_disable_auto_compactions(config.rocksdb.rocksdb_disable_auto_compactions());
        if let Some(compaction_period) = config.rocksdb.rocksdb_periodic_compaction_seconds() {
            cf_options.set_periodic_compaction_seconds(compaction_period);
        }

        if cf_name == DATA_CF {
            cf_data_options(&mut cf_options, &block_options, config);
        } else if cf_name == METADATA_CF {
            cf_metadata_options(&mut cf_options, &block_options, config);
        }

        cf_options
    }
}

fn cf_data_options(
    cf_options: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    metadata_server_config: &MetadataServerOptions,
) {
    cf_options.set_block_based_table_factory(block_options);

    let memory_budget = metadata_server_config.rocksdb_memory_budget();
    // memory budget is in bytes. We divide the budget between the data cf and metadata cf.
    let memtables_budget = (memory_budget as f64 * DATA_CF_BUDGET_RATIO).floor() as usize;
    assert!(
        memtables_budget > 0,
        "memory budget should be greater than 0"
    );

    set_memory_related_opts(cf_options, memtables_budget);
    cf_options.set_compaction_style(rocksdb::DBCompactionStyle::Level);
    cf_options.set_num_levels(7);

    cf_options.set_compression_per_level(&[
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ]);
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
    cf_options: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    metadata_server_config: &MetadataServerOptions,
) {
    cf_options.set_block_based_table_factory(block_options);

    let memory_budget = metadata_server_config.rocksdb_memory_budget();
    let memtables_budget = (memory_budget as f64 * (1.0 - DATA_CF_BUDGET_RATIO)).floor() as usize;
    assert!(
        memtables_budget > 0,
        "memory budget should be greater than 0"
    );
    set_memory_related_opts(cf_options, memtables_budget);
    //
    // Set compactions per level
    //
    cf_options.set_num_levels(3);
    cf_options.set_compression_per_level(&[
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ]);
    cf_options.set_memtable_whole_key_filtering(true);
    cf_options.set_max_write_buffer_number(4);
    cf_options.set_max_successive_merges(10);
}
