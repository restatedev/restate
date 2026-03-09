// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, SliceTransform};
use tracing::{info, warn};

use restate_core::ShutdownError;
use restate_rocksdb::{CfExactPattern, CfName, DbName, DbSpecBuilder, RocksDb, RocksDbManager};
use restate_serde_util::ByteCount;
use restate_types::config::{Configuration, LogServerOptions};
use restate_types::health::HealthStatus;
use restate_types::protobuf::common::LogServerStatus;

use super::writer::LogStoreWriterBuilder;
use super::{DATA_CF, DB_NAME, METADATA_CF};
use super::{RocksDbLogStore, RocksDbLogStoreError};
use crate::logstore::LogStoreState;
use crate::rocksdb_logstore::keys::KeyPrefix;
use crate::rocksdb_logstore::metadata_merge::{metadata_full_merge, metadata_partial_merge};

#[derive(Clone)]
pub struct RocksDbLogStoreBuilder {
    rocksdb: Arc<RocksDb>,
}

impl RocksDbLogStoreBuilder {
    pub async fn create() -> Result<Self, RocksDbLogStoreError> {
        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

        let db_spec = DbSpecBuilder::new(
            db_name,
            Configuration::pinned().log_server.data_dir(),
            RocksConfigurator,
        )
        .add_cf_pattern(CfExactPattern::new(DATA_CF), RocksConfigurator)
        .add_cf_pattern(CfExactPattern::new(METADATA_CF), RocksConfigurator)
        // not very important but it's to reduce the number of merges by flushing.
        // it's also a small cf so it should be quick.
        .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
        .add_to_flush_on_shutdown(CfExactPattern::new(DATA_CF))
        .ensure_column_families(cfs)
        .build()
        .expect("valid spec");
        let rocksdb = db_manager.open_db(db_spec).await?;

        Ok(Self { rocksdb })
    }

    pub async fn start(
        self,
        health_status: HealthStatus<LogServerStatus>,
    ) -> Result<RocksDbLogStore, ShutdownError> {
        let RocksDbLogStoreBuilder { rocksdb } = self;
        let store_state = LogStoreState::new(health_status);
        let writer_handle =
            LogStoreWriterBuilder::new(rocksdb.clone(), store_state.clone()).start()?;

        Ok(RocksDbLogStore {
            rocksdb,
            store_state,
            writer_handle,
        })
    }
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
        let log_server_config = &Configuration::pinned().log_server;
        // amend default options from rocksdb_manager
        self.apply_db_opts_from_config(&mut db_options, &log_server_config.rocksdb);

        restate_rocksdb::configuration::set_background_work_budget(
            &mut db_options,
            log_server_config.rocksdb_max_background_flushes(),
            log_server_config.rocksdb_max_background_compactions(),
        );

        // log-server specific customizations

        // This is Rocksdb's default, it's added here for clarity.
        //
        // Rationale: If WAL tail is corrupted, it's likely that it has failed during write, that said,
        // we can use absolute consistency but on a single-node setup, we don't have a way to recover
        // from it, so it's not useful for us. Even for a replicated loglet setup, the recovery is
        // likely to depend on the last durably committed record for a quorum of nodes in the write
        // set.
        db_options.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);
        db_options.set_wal_compression_type(DBCompressionType::Zstd);
        // most reads are sequential
        db_options.set_advise_random_on_open(false);

        db_options.set_max_total_wal_size(log_server_config.rocksdb_max_wal_size().as_u64());

        db_options.set_enable_pipelined_write(true);
        db_options.set_max_subcompactions(log_server_config.rocksdb_max_sub_compactions());

        db_options
    }

    fn note_config_update(&self, db: &restate_rocksdb::RocksAccess) {
        // memory budget is in bytes. We divide the budget between the data cf and metadata cf.
        let (data_budget, metadata_budget) = {
            let config = &Configuration::pinned().log_server;
            let data_budget = config.rocksdb_data_memtables_budget();
            let metadata_budget = config.rocksdb_metadata_memtables_budget();
            (data_budget, metadata_budget)
        };

        set_memtable_budget(db, DATA_CF, data_budget.as_usize());
        set_memtable_budget(db, METADATA_CF, metadata_budget.as_usize());
        // check if usage is higher than the new budget, then force a flush.

        let total_data_usage = db
            .get_property_int_cf(DATA_CF, "rocksdb.cur-size-all-mem-tables")
            .unwrap()
            .unwrap_or_default() as usize;

        let total_metadata_usage = db
            .get_property_int_cf(METADATA_CF, "rocksdb.cur-size-all-mem-tables")
            .unwrap()
            .unwrap_or_default() as usize;

        let will_flush = total_data_usage > data_budget.as_usize()
            || total_metadata_usage > metadata_budget.as_usize();
        info!(
            "Updating log-server memory budget. data_usage:{}/{}, metadata_usage:{}/{}, will_flush: {}",
            ByteCount::from(total_data_usage),
            data_budget,
            ByteCount::from(total_metadata_usage),
            metadata_budget,
            will_flush
        );
        if will_flush {
            db.flush_memtables(
                &[CfName::from(DATA_CF), CfName::from(METADATA_CF)],
                // do not wait for flush to complete to avoid blocking the runtime.
                false,
            )
            .unwrap_or_else(|e| warn!("Failed to flush memtables: {}", e));
        }
    }
}

fn set_memtable_budget(db: &restate_rocksdb::RocksAccess, cf: &str, memory_budget: usize) {
    let max_bytes_for_level_base = memory_budget;
    let single_memtable_budget = memory_budget / 4;
    let target_file_size_base = memory_budget / 8;

    let max_bytes_for_level_base_str = max_bytes_for_level_base.to_string();
    let single_memtable_budget_str = single_memtable_budget.to_string();
    let target_file_size_base_str = target_file_size_base.to_string();
    // setting data-cf memory budget
    if let Err(err) = db.set_options_cf(
        cf,
        &[
            ("write_buffer_size", &single_memtable_budget_str),
            ("target_file_size_base", &target_file_size_base_str),
            ("max_bytes_for_level_base", &max_bytes_for_level_base_str),
        ],
    ) {
        warn!(
            "Failed to update memory budget for {}/{cf}: {err}",
            db.name(),
        );
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
        let config = &Configuration::pinned().log_server;
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
    opts: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    log_server_config: &LogServerOptions,
) {
    opts.set_block_based_table_factory(block_options);

    let memtables_budget = log_server_config.rocksdb_data_memtables_budget().as_usize();

    set_memory_related_opts(opts, memtables_budget);
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

    if log_server_config.rocksdb_enable_blob_separation {
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(512 * 1024); // 512KiB minimum to use blob files
        opts.set_enable_blob_gc(true);
        opts.set_blob_compression_type(DBCompressionType::Zstd);
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
    opts: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    log_server_config: &LogServerOptions,
) {
    opts.set_block_based_table_factory(block_options);
    let memtables_budget = log_server_config
        .rocksdb_metadata_memtables_budget()
        .as_usize();

    set_memory_related_opts(opts, memtables_budget);
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
}
