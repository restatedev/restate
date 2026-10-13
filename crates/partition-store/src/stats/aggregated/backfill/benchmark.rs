// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Opt-in, release-lite measurements against a downloaded historical snapshot.
//! BACKFILL_SNAPSHOT points to a directory containing metadata.json and its SST files.
//! BACKFILL_SCRATCH selects a dedicated scratch parent on the intended storage device.
//! BACKFILL_MODE is scan or full; BACKFILL_BUFFER_MIB and BACKFILL_READAHEAD_KIB tune
//! the experiment. Every invocation imports a fresh copy and preserves the source snapshot.
//! BACKFILL_DISABLE_COMPACTIONS=1 isolates scanner work from import-triggered compactions.

use std::path::PathBuf;

use rocksdb::{PerfContext, PerfMetric, PerfStatsLevel};

use restate_rocksdb::RocksDbManager;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_types::config::{Configuration, RocksDbOptions, set_current_config};
use restate_types::partition_table::Partition;

use crate::PartitionStoreManager;
use crate::snapshots::{LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir};
use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageStatusCounts;

use super::*;

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BACKFILL_SNAPSHOT and BACKFILL_SCRATCH; run with release-lite"]
async fn historical_snapshot() {
    let source = PathBuf::from(std::env::var_os("BACKFILL_SNAPSHOT").expect("BACKFILL_SNAPSHOT"));
    let scratch = PathBuf::from(std::env::var_os("BACKFILL_SCRATCH").expect("BACKFILL_SCRATCH"));
    let mode = std::env::var("BACKFILL_MODE").unwrap_or_else(|_| "full".into());
    assert!(matches!(mode.as_str(), "scan" | "full"));
    let disable_compactions = std::env::var("BACKFILL_DISABLE_COMPACTIONS").is_ok_and(|v| v == "1");
    let buffer_mib: usize = std::env::var("BACKFILL_BUFFER_MIB")
        .unwrap_or_else(|_| "8".into())
        .parse()
        .unwrap();
    let readahead_kib: usize = std::env::var("BACKFILL_READAHEAD_KIB")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    std::fs::create_dir_all(&scratch).unwrap();
    let working = tempfile::tempdir_in(scratch).unwrap();
    let imported = tempfile::tempdir_in(working.path()).unwrap();
    let mut metadata: PartitionSnapshotMetadata =
        serde_json::from_slice(&std::fs::read(source.join("metadata.json")).unwrap()).unwrap();
    for file in &mut metadata.files {
        let filename = file.name.trim_start_matches('/');
        assert_eq!(PathBuf::from(filename).file_name().unwrap(), filename);
        std::fs::copy(source.join(filename), imported.path().join(filename)).unwrap();
        file.directory = imported.path().to_string_lossy().into_owned();
    }
    let mut config = Configuration::default();
    config.common.set_base_dir(working.path().join("store"));
    config.common.set_node_name("backfill-benchmark");
    config.common.set_cluster_name("backfill-benchmark");
    // Match the test cluster's direct-read policy, but give this single-partition
    // process a modest, fixed cache budget. Auto compaction is enabled unless the
    // experiment explicitly isolates the read path from import-triggered compactions.
    config.worker.storage.rocksdb = serde_json::from_value::<RocksDbOptions>(serde_json::json!({
        "rocksdb-disable-direct-io-for-reads": false,
        "rocksdb-disable-auto-compactions": disable_compactions
    }))
    .unwrap();
    config
        .worker
        .storage
        .set_rocksdb_memory_budget(restate_types::memory::NonZeroByteCount::new(
            std::num::NonZeroUsize::new(2 * 1024 * 1024 * 1024).unwrap(),
        ));
    set_current_config(config);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partition = Partition::new(metadata.partition_id, metadata.key_range);
    let store = manager
        .open_from_snapshot(
            &partition,
            LocalPartitionSnapshot {
                base_dir: SnapshotDir::new(imported.keep()),
                log_id: metadata.log_id,
                min_applied_lsn: metadata.min_applied_lsn,
                db_comparator_name: metadata.db_comparator_name,
                files: metadata.files,
                key_range: metadata.key_range,
            },
        )
        .await
        .unwrap();
    let db = store.partition_db().clone();
    let scan_only = mode == "scan";
    let result = db
        .rocksdb()
        .clone()
        .run_background_read_op(
            "historical-backfill-benchmark",
            StorageTaskKind::BackgroundIterator,
            Priority::Low,
            move |_| {
                rocksdb::perf::set_perf_stats(PerfStatsLevel::EnableTimeExceptForMutex);
                let mut perf = PerfContext::default();
                perf.reset();
                let cancel = CancellationToken::new();
                let io_before = proc_fields("/proc/self/io");
                let report = if scan_only {
                    build_measured::<false>(
                        &db,
                        &cancel,
                        buffer_mib * 1024 * 1024,
                        readahead_kib * 1024,
                    )
                } else {
                    build_measured::<true>(
                        &db,
                        &cancel,
                        buffer_mib * 1024 * 1024,
                        readahead_kib * 1024,
                    )
                }
                .unwrap();
                let io_after = proc_fields("/proc/self/io");
                let memory = proc_fields("/proc/self/status");
                let read_bytes = perf.metric(PerfMetric::BlockReadByte);
                let read_ns = perf.metric(PerfMetric::BlockReadTime);
                let decompress_ns = perf.metric(PerfMetric::BlockDecompressTime);
                let perf_report = perf.report(true);
                let mut counted_entries = 0u64;
                let mut stat_keys = 0u64;
                if !scan_only {
                    let mut options = ReadOptions::default();
                    options.set_total_order_seek(true);
                    options.fill_cache(false);
                    options.set_iterate_range(rocksdb::PrefixRange(KeyKind::Stats.as_bytes()));
                    let mut iterator = db
                        .rocksdb()
                        .inner()
                        .as_raw_db()
                        .raw_iterator_cf_opt(db.cf_handle(), options);
                    iterator.seek_to_first();
                    while let Some((key, value)) = iterator.item() {
                        let (prefix, _) = StatKeyPrefix::decode_prefix(key).unwrap();
                        let counts = StageStatusCounts::deserialize_from(value).unwrap();
                        stat_keys += 1;
                        if prefix.stat_id() == Some(ServiceLoad::STAT_ID) {
                            counted_entries += counts.iter().map(|(_, count)| count).sum::<u64>();
                        }
                        iterator.next();
                    }
                    iterator.status().unwrap();
                    assert_eq!(counted_entries, report.entries);
                }
                serde_json::json!({
                    "mode": mode, "buffer_mib": buffer_mib, "readahead_kib": readahead_kib,
                    "disable_compactions": disable_compactions,
                    "rss_kib": memory.get("VmRSS"), "peak_rss_kib": memory.get("VmHWM"),
                    "io_before": io_before, "io_after": io_after,
                    "backfill": report, "block_read_bytes": read_bytes,
                    "block_read_secs": read_ns as f64 / 1e9,
                    "decompress_secs": decompress_ns as f64 / 1e9,
                    "stat_keys": stat_keys, "counted_entries": counted_entries, "perf": perf_report,
                })
            },
        )
        .await
        .unwrap();
    println!("BACKFILL_RESULT {}", result);
    drop(store);
    manager.close_partition_store(partition.id()).await;
    RocksDbManager::get().shutdown().await;
}

fn proc_fields(path: &str) -> BTreeMap<String, u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            let value = value.split_whitespace().next()?.parse().ok()?;
            Some((name.to_owned(), value))
        })
        .collect()
}
