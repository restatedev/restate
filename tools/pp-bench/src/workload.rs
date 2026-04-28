// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Core benchmark loop: sets up the state machine and partition store,
//! runs warmup, then measures apply + commit throughput.
//!
//! Runs entirely on the caller's single-threaded `LocalRuntime`. The apply
//! loop, the interval reporter, and the metrics HTTP server all share this
//! one thread and interleave at `.await` points.
//!
//! Note: this does NOT exactly match production. In production the partition
//! processor runs on its own dedicated `current_thread` runtime in a separate
//! OS thread, isolated from infrastructure tasks (metrics server, tracing,
//! network I/O) which run on a multi-thread runtime. Here we collapse
//! everything onto one thread to keep the tool simple. The trade-off is that
//! background tasks (interval reporter ticks, metrics scrape handling) compete
//! with the apply loop for thread time, which can perturb measurements
//! slightly compared to the isolated production runtime.

use std::time::Instant;

use hdrhistogram::Histogram;

use restate_cli_util::{c_println, c_success};
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_time_util::FriendlyDuration;
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_vqueues::VQueuesMetaCache;
use restate_worker::partition::state_machine::{ActionCollector, StateMachine};

use crate::RunOpts;
use crate::command_gen;
use crate::command_pool::CommandPool;
use crate::report::{
    BenchCounters, IntervalReporter, RunSummary, SystemSnapshot, print_json_summary, print_summary,
};

/// The RocksDB database name used by the partition store.
const DB_NAME: &str = "db";

pub async fn run(
    opts: &RunOpts,
    report_interval: FriendlyDuration,
    raw_rocksdb_stats: bool,
    json: bool,
) -> anyhow::Result<()> {
    // -----------------------------------------------------------------------
    // 1. Load or generate commands
    // -----------------------------------------------------------------------
    let mut pool = if let Some(ref path) = opts.command_file {
        c_println!("Loading commands from {}", path.display());
        let loaded = command_gen::load_commands_from_file(path)?;
        c_println!("Command pool: {} commands loaded", loaded.commands.len());
        if let Some(ref lsns) = loaded.lsns {
            CommandPool::with_lsns(loaded.commands, lsns.clone())
        } else {
            CommandPool::new(loaded.commands)
        }
    } else {
        let total_needed = opts.warmup + opts.num_commands;
        c_println!(
            "Generating {} commands in-memory (workload={:?}, seed={})",
            total_needed,
            opts.spec.workload,
            opts.spec.seed,
        );
        let commands = command_gen::generate_commands_inline(&opts.spec, total_needed);
        c_println!("Command pool: {} commands loaded", commands.len());
        CommandPool::new(commands)
    };

    // -----------------------------------------------------------------------
    // 2. Set up StateMachine + PartitionStore
    // -----------------------------------------------------------------------
    RocksDbManager::init();

    let manager = PartitionStoreManager::create().await?;
    let mut partition_store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await?;

    let mut state_machine = StateMachine::new(
        0,    /* inbox_seq_number */
        0,    /* outbox_seq_number */
        None, /* outbox_head_seq_number */
        KeyRange::FULL,
        SemanticRestateVersion::unknown(),
        None, /* schema */
    );

    // Initialize the vqueue cache from the (empty) partition store — this follows
    // the production init path and avoids requiring test-util features.
    let mut vq_cache = VQueuesMetaCache::create(partition_store.partition_db().clone()).await?;

    let workload_name = format!("{:?}", opts.spec.workload);
    let batch_size = opts.batch_size;
    let warmup = opts.warmup;
    let num_commands = opts.num_commands;

    // -----------------------------------------------------------------------
    // 3. Spawn the interval reporter on the same single-threaded runtime as
    //    the apply loop. The reporter only progresses when the apply loop
    //    yields (at .await points on RocksDB I/O), so reporting cadence is
    //    approximate.
    // -----------------------------------------------------------------------
    let counters = BenchCounters::new();
    let (reporter, latency_collector) =
        IntervalReporter::new(report_interval, DB_NAME, counters.clone());
    let reporter_handle = tokio::task::spawn_local(reporter.run());

    // -----------------------------------------------------------------------
    // 4. Warmup
    // -----------------------------------------------------------------------
    if warmup > 0 {
        c_println!("Warming up ({} commands)...", warmup);
        let mut lsn = Lsn::OLDEST;
        let mut action_collector = ActionCollector::default();

        let warmup_batch_count = warmup.div_ceil(batch_size as u64);
        let mut cmds_applied: u64 = 0;

        for _ in 0..warmup_batch_count {
            let mut txn = partition_store.transaction();
            let batch_cmds = (batch_size as u64).min(warmup - cmds_applied);
            for _ in 0..batch_cmds {
                let (cmd, _real_lsn) = pool.next_command();
                state_machine
                    .apply(
                        cmd,
                        MillisSinceEpoch::now(),
                        lsn,
                        &mut txn,
                        &mut action_collector,
                        &mut vq_cache,
                        false,
                    )
                    .await?;
                lsn = lsn.next();
            }
            txn.commit().await?;
            cmds_applied += batch_cmds;
        }
        c_success!("Warmup complete ({} commands applied)", cmds_applied);
    }

    // -----------------------------------------------------------------------
    // 5. Measurement
    // -----------------------------------------------------------------------
    let start_snapshot = SystemSnapshot::capture(DB_NAME);
    let start_time = Instant::now();

    let mut latencies = Histogram::<u64>::new(3).unwrap();
    let mut lsn = Lsn::from(1_000_000);
    let mut action_collector = ActionCollector::default();

    let num_batches = num_commands.div_ceil(batch_size as u64);
    let mut total_cmds: u64 = 0;

    for _ in 0..num_batches {
        let batch_cmds = (batch_size as u64).min(num_commands - total_cmds);
        let batch_start = Instant::now();

        let mut txn = partition_store.transaction();
        for _ in 0..batch_cmds {
            let (cmd, _real_lsn) = pool.next_command();
            state_machine
                .apply(
                    cmd,
                    MillisSinceEpoch::now(),
                    lsn,
                    &mut txn,
                    &mut action_collector,
                    &mut vq_cache,
                    false,
                )
                .await?;
            lsn = lsn.next();
        }
        txn.commit().await?;

        let batch_nanos = batch_start.elapsed().as_nanos() as u64;
        let _ = latencies.record(batch_nanos);
        latency_collector.record(batch_nanos);
        counters.inc_commands(batch_cmds);
        counters.inc_batches();

        total_cmds += batch_cmds;
    }

    let wall_time = start_time.elapsed();
    let end_snapshot = SystemSnapshot::capture(DB_NAME);

    // -----------------------------------------------------------------------
    // 6. Stop the interval reporter
    // -----------------------------------------------------------------------
    drop(latency_collector);
    reporter_handle.abort();
    let _ = reporter_handle.await;

    // -----------------------------------------------------------------------
    // 7. Summary
    // -----------------------------------------------------------------------
    let summary = RunSummary {
        workload: &workload_name,
        total_commands: num_commands,
        total_batches: counters.batches(),
        batch_size,
        warmup_commands: warmup,
        wall_time: FriendlyDuration::from(wall_time),
        latencies: &latencies,
        start_snapshot: &start_snapshot,
        end_snapshot: &end_snapshot,
        db_name: DB_NAME,
        raw_rocksdb_stats,
    };

    if json {
        print_json_summary(&summary);
    } else {
        print_summary(&summary);
    }

    RocksDbManager::get().shutdown().await;
    Ok(())
}
