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

use rocksdb::{BlockBasedOptions, Cache, SliceTransform};
use tracing::{info, warn};

use restate_core::ShutdownError;
use restate_rocksdb::{
    CfExactPattern, CfName, DbName, DbSpecBuilder, OpenMode, RocksDb, RocksDbManager,
};
use restate_serde_util::ByteCount;
use restate_types::config::{Configuration, LogServerOptions};
use restate_types::health::HealthStatus;
use restate_types::protobuf::common::{DatabaseKind, LogServerStatus};

use super::writer::LogStoreWriterBuilder;
use super::{DATA_CF, METADATA_CF};
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
        let kind = DatabaseKind::LogServer;
        let db_name = DbName::new(kind.db_name());
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

        let db_spec = DbSpecBuilder::new(
            db_name,
            kind,
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
    fn get_db_open_mode(&self) -> OpenMode {
        if Configuration::pinned().log_server.read_only {
            OpenMode::ReadOnly
        } else {
            OpenMode::ReadWrite
        }
    }

    fn get_db_options(
        &self,
        db_name: &DbName,
        env: &rocksdb::Env,
        write_buffer_manager: &rocksdb::WriteBufferManager,
        limiter: &rocksdb::RateLimiter,
    ) -> rocksdb::Options {
        let mut db_options = restate_rocksdb::configuration::create_default_db_options(
            env,
            db_name,
            true, /* create_db_if_missing */
            write_buffer_manager,
            limiter,
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
        if !log_server_config.rocksdb.rocksdb_disable_wal_compression() {
            db_options.set_wal_compression_type(rocksdb::DBCompressionType::Zstd);
        }
        // most reads are sequential
        db_options.set_advise_random_on_open(false);

        db_options.set_max_total_wal_size(log_server_config.rocksdb_max_wal_size().as_u64());

        // Flush both CFs atomically. This ensures the metadata CF (trim points, seals)
        // and the data CF are always consistent on disk without requiring WAL replay to
        // reconstruct the relationship. The metadata CF is sized so that it never triggers
        // a flush on its own — it piggybacks on data CF flushes.
        //
        // Note: atomic_flush is incompatible with enable_pipelined_write, and it forces
        // min_write_buffer_number_to_merge=1 on all CFs (which is what we want anyway).
        db_options.set_atomic_flush(true);
        db_options.set_max_subcompactions(log_server_config.rocksdb_max_sub_compactions());

        db_options
    }

    fn note_config_update(&self, db: &restate_rocksdb::RocksAccess) {
        let data_budget = {
            let config = &Configuration::pinned().log_server;
            config.rocksdb_data_memtables_budget()
        };

        update_data_cf_budget(db, data_budget.as_usize());
        // Keep metadata CF write_buffer_size in sync so it never triggers a flush on
        // its own (atomic_flush means any CF triggering a flush drags all CFs along).
        update_metadata_cf_write_buffer_size(db, data_budget.as_usize());

        // If current memtable usage exceeds the new budget, trigger a flush so that
        // memory is reclaimed promptly. With atomic_flush, flushing the data CF will
        // also flush the metadata CF.
        let total_data_usage = db
            .get_property_int_cf(DATA_CF, "rocksdb.cur-size-all-mem-tables")
            .unwrap()
            .unwrap_or_default() as usize;

        let will_flush = total_data_usage > data_budget.as_usize();
        info!(
            "Updating log-server data CF memory budget. usage:{}/{}, will_flush: {}",
            ByteCount::from(total_data_usage),
            data_budget,
            will_flush
        );
        if will_flush {
            db.flush_memtables(
                &[CfName::from(DATA_CF)],
                // do not wait for flush to complete to avoid blocking the runtime.
                false,
            )
            .unwrap_or_else(|e| warn!("Failed to flush memtables: {}", e));
        }
    }
}

/// Dynamically updates the data CF sizing options to match the new memory budget.
///
/// Must stay in sync with the initial values set by [`cf_data_options`]:
///   - `write_buffer_size = budget / 4`
///   - `target_file_size_base = budget / 8`
///   - `max_bytes_for_level_base = budget * 2` (= trigger × write_buffer_size = 8 × budget/4)
///
/// `level0_file_num_compaction_trigger` (8) is a constant that doesn't depend on the
/// budget — the ratio `8 × (budget/4) = 2 × budget` holds for any budget value.
fn update_data_cf_budget(db: &restate_rocksdb::RocksAccess, memory_budget: usize) {
    let write_buffer_size = (memory_budget / 4).to_string();
    let target_file_size_base = (memory_budget / 8).to_string();
    let max_bytes_for_level_base = (memory_budget * 2).to_string();

    if let Err(err) = db.set_options_cf(
        DATA_CF,
        &[
            ("write_buffer_size", &write_buffer_size),
            ("target_file_size_base", &target_file_size_base),
            ("max_bytes_for_level_base", &max_bytes_for_level_base),
        ],
    ) {
        warn!(
            "Failed to update data CF memory budget for {}/{DATA_CF}: {err}",
            db.name(),
        );
    }
}

/// Keeps the metadata CF write_buffer_size in sync with the data CF so that
/// the metadata CF never independently triggers a flush under atomic_flush.
fn update_metadata_cf_write_buffer_size(db: &restate_rocksdb::RocksAccess, data_budget: usize) {
    let write_buffer_size = (data_budget / 4).to_string();
    if let Err(err) = db.set_options_cf(METADATA_CF, &[("write_buffer_size", &write_buffer_size)]) {
        warn!(
            "Failed to update metadata CF write_buffer_size for {}/{METADATA_CF}: {err}",
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

        cf_options.set_disable_auto_compactions(config.rocksdb.rocksdb_disable_auto_compactions());
        if let Some(compaction_period) = config.rocksdb.rocksdb_periodic_compaction_seconds() {
            cf_options.set_periodic_compaction_seconds(compaction_period);
        }

        if cf_name == DATA_CF {
            let block_options = restate_rocksdb::configuration::create_default_block_options(
                &config.rocksdb,
                Some(global_cache),
            );
            cf_data_options(&mut cf_options, &block_options, config);
        } else if cf_name == METADATA_CF {
            let block_options = metadata_block_options(&config.rocksdb, global_cache);
            cf_metadata_options(&mut cf_options, &block_options, config);
        }

        cf_options
    }
}

/// Block options for the metadata CF. Uses a smaller block size than the default (4KiB vs
/// 64KiB) since this CF is small and accessed via point lookups where large blocks waste
/// read bandwidth.
fn metadata_block_options(
    opts: &restate_types::config::RocksDbOptions,
    global_cache: &Cache,
) -> BlockBasedOptions {
    let mut block_options =
        restate_rocksdb::configuration::create_default_block_options(opts, Some(global_cache));
    block_options.set_block_size(4 * 1024);
    block_options
}

fn cf_data_options(
    opts: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    log_server_config: &LogServerOptions,
) {
    opts.set_block_based_table_factory(block_options);

    let memtables_budget = log_server_config.rocksdb_data_memtables_budget().as_usize();

    set_memory_related_opts(opts, memtables_budget);
    // With atomic_flush enabled, min_write_buffer_number_to_merge is forced to 1 by
    // RocksDB. Each flush produces one L0 file of ~(budget / 4).
    //
    // We intentionally delay L0→L1 compaction (trigger=8) to give trimming a chance to
    // reclaim data before compaction runs. Reads are rare thanks to the RecordCache, so
    // higher L0 read amplification is acceptable. At trigger=8, L0 accumulates up to
    // ~2×budget before compaction, with comfortable headroom to the default slowdown (20)
    // and stop (36) triggers.
    opts.set_level_zero_file_num_compaction_trigger(8);
    // L1 target = trigger × write_buffer_size = 8 × (budget/4) = 2 × budget.
    opts.set_max_bytes_for_level_base(memtables_budget as u64 * 2);
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
    opts.set_num_levels(7);

    let l0_l1 = if log_server_config
        .rocksdb
        .rocksdb_disable_l0_l1_compression()
    {
        rocksdb::DBCompressionType::None
    } else {
        rocksdb::DBCompressionType::Zstd
    };
    let levels = restate_rocksdb::configuration::build_compression_per_level(
        7,
        l0_l1,
        rocksdb::DBCompressionType::Zstd,
    );
    opts.set_compression_per_level(&levels);

    // Override the global default. We want bloom filters on the last level because
    // trimmed ranges (via DeleteRange) can cause scans to hit levels where data no longer
    // exists, and last-level bloom filters help skip those blocks.
    opts.set_optimize_filters_for_hits(false);

    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(KeyPrefix::size()));
    opts.set_memtable_prefix_bloom_ratio(0.2);

    if log_server_config.rocksdb_enable_blob_separation {
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(log_server_config.rocksdb_blob_min_size().as_u64());
        let blob_compression = if log_server_config.rocksdb_disable_blob_compression {
            rocksdb::DBCompressionType::None
        } else {
            rocksdb::DBCompressionType::Zstd
        };
        opts.set_blob_compression_type(blob_compression);

        // Blob GC: relocate live blobs during compaction to reclaim space after trimming.
        opts.set_enable_blob_gc(true);
        // All blob files are eligible for GC. Data is append-only so blob GC is purely
        // about cleanup after DeleteRange trimming — there's no benefit to limiting
        // eligibility to the oldest fraction.
        opts.set_blob_gc_age_cutoff(1.0);
        // Trigger forced compactions when >=50% of data in eligible blob files is garbage.
        // Without this (default 1.0 = disabled), SSTs referencing trimmed blob data may
        // not be picked for compaction naturally, delaying disk space reclamation.
        opts.set_blob_gc_force_threshold(0.5);
        // Use the same readahead size for blob files as for SST files during compaction.
        // Blob GC reads blobs from old files during compaction; readahead improves
        // throughput, especially with direct I/O enabled.
        opts.set_blob_compaction_readahead_size(
            log_server_config
                .rocksdb
                .rocksdb_compaction_readahead_size()
                .get() as u64,
        );
    }
}

/// Sets common memtable and SST sizing for the data column family.
///
/// With 4 write buffers (1 mutable + 3 immutable), each memtable is `budget / 4`.
/// The caller must separately set `level_zero_file_num_compaction_trigger` and
/// `max_bytes_for_level_base` to satisfy the invariant:
///   `trigger × write_buffer_size ≈ max_bytes_for_level_base`
fn set_memory_related_opts(opts: &mut rocksdb::Options, memtables_budget: usize) {
    opts.set_write_buffer_size(memtables_budget / 4);
    opts.set_max_write_buffer_number(4);
    opts.set_target_file_size_base(memtables_budget as u64 / 8);
}

fn cf_metadata_options(
    opts: &mut rocksdb::Options,
    block_options: &BlockBasedOptions,
    log_server_config: &LogServerOptions,
) {
    opts.set_block_based_table_factory(block_options);

    // The metadata CF uses the data CF's write_buffer_size so it never independently
    // triggers a flush (with atomic_flush, any CF triggering drags all CFs along).
    // Metadata writes are tiny (~20 bytes per op), so actual memory use is negligible.
    let data_memtables_budget = log_server_config.rocksdb_data_memtables_budget().as_usize();
    opts.set_write_buffer_size(data_memtables_budget / 4);
    opts.set_max_write_buffer_number(4);

    // The metadata CF holds a handful of small keys per loglet (trim point, sequencer,
    // seal). Total data is typically a few hundred KB even with thousands of loglets —
    // SST files will never approach the default target_file_size_base or level limits,
    // so we leave those at RocksDB defaults. Compact eagerly to keep L0 clean for point
    // lookups (e.g. load_loglet_state).
    opts.set_level_zero_file_num_compaction_trigger(2);
    opts.set_num_levels(3);
    // The metadata CF is tiny — disable compression entirely to avoid unnecessary CPU
    // overhead on flushes and point lookups. The disk savings are negligible.
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    opts.set_memtable_whole_key_filtering(true);
    // Merge operator for trim-point updates (monotonic max). The merge is trivially cheap
    // (max on 8-byte offsets) and trim points are rarely read, so we leave
    // max_successive_merges at the default (0 = disabled) to avoid unnecessary memtable
    // lookups on the write path. Merge chains are naturally bounded by flush frequency.
    opts.set_merge_operator("MetadataMerge", metadata_full_merge, metadata_partial_merge);
}
