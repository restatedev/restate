// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mixed workload: concurrent writes, reads, and trims across multiple loglets.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::Result;
use hdrhistogram::Histogram;
use tokio::sync::Barrier;
use tracing::info;

use restate_core::network::ServiceMessage;
use restate_core::{TaskCenter, TaskKind};
use restate_log_server::loglet_worker::{LogletWorker, LogletWorkerHandle};
use restate_log_server::logstore::LogStore;
use restate_log_server::metadata::LogletStateMap;
use restate_time_util::FriendlyDuration;
use restate_types::GenerationalNodeId;
use restate_types::logs::{KeyFilter, LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::net::log_server::*;

use crate::payload::{PayloadPool, PayloadSpec};
use crate::report::{
    BenchCounters, IntervalReporter, RunSummary, SystemSnapshot, print_read_summary, print_summary,
};

const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
const DB_NAME: &str = "log-server";
const POOL_BATCHES: usize = 1024;

#[derive(Debug, Clone, clap::Parser)]
pub struct MixedWorkloadOpts {
    /// Duration to run the workload (e.g. "60s", "5m").
    #[arg(long, default_value = "60s")]
    pub duration: FriendlyDuration,

    /// Number of loglets.
    #[arg(long, default_value = "10")]
    pub num_loglets: u32,

    /// Number of records per Store batch.
    #[arg(long, default_value = "10")]
    pub records_per_batch: usize,

    /// Enable read tasks (one per loglet, reading behind the writer).
    #[arg(long)]
    pub enable_reads: bool,

    /// How many records behind the writer the reader stays.
    #[arg(long, default_value = "10000")]
    pub read_lag: u64,

    /// Enable periodic trim operations.
    #[arg(long)]
    pub enable_trims: bool,

    /// Interval between trim operations.
    #[arg(long, default_value = "10s")]
    pub trim_interval: FriendlyDuration,

    /// How many records behind the writer the trim point stays.
    #[arg(long, default_value = "100000")]
    pub trim_lag: u64,

    /// Load payload from a pre-generated file (see `generate-payload` subcommand).
    /// When set, --payload-size/--entropy/--key-style/--seed are ignored.
    #[arg(long)]
    pub payload_file: Option<PathBuf>,

    #[clap(flatten)]
    pub payload: PayloadSpec,
}

struct SharedState {
    /// Per-loglet write offset (records written so far).
    offsets: Vec<AtomicU64>,
    stop: AtomicBool,
}

pub async fn run<S: LogStore + Sync>(
    opts: &MixedWorkloadOpts,
    log_store: S,
    state_map: &LogletStateMap,
    report_interval: FriendlyDuration,
    raw_rocksdb_stats: bool,
) -> Result<()> {
    let records_per_batch = opts.records_per_batch.max(1);

    // Create workers
    let mut workers: Vec<LogletWorkerHandle> = Vec::with_capacity(opts.num_loglets as usize);
    for i in 0..opts.num_loglets {
        let loglet_id = LogletId::new_unchecked(u64::from(i) + 1);
        let loglet_state = state_map.get_or_load(loglet_id, &log_store).await?;
        let worker = LogletWorker::start(loglet_id, log_store.clone(), loglet_state)?;
        workers.push(worker);
    }

    let shared = Arc::new(SharedState {
        offsets: (0..opts.num_loglets).map(|_| AtomicU64::new(1)).collect(),
        stop: AtomicBool::new(false),
    });

    info!(
        duration = %opts.duration,
        num_loglets = opts.num_loglets,
        records_per_batch,
        enable_reads = opts.enable_reads,
        enable_trims = opts.enable_trims,
        payload_size = %opts.payload.payload_size,
        entropy = opts.payload.entropy,
        "Starting mixed workload"
    );

    // We use a barrier to start all tasks simultaneously after setup.
    let mut task_count = opts.num_loglets as usize; // writers
    if opts.enable_reads {
        task_count += opts.num_loglets as usize;
    }
    if opts.enable_trims {
        task_count += opts.num_loglets as usize;
    }
    let barrier = Arc::new(Barrier::new(task_count + 1)); // +1 for the main task

    // Set up reporting
    let counters = BenchCounters::new();
    let (reporter, latency_collector) =
        IntervalReporter::new(report_interval, DB_NAME, counters.clone());
    let latency_collector = Arc::new(latency_collector);
    let reporter_handle = tokio::spawn(reporter.run());

    let mut write_tasks = Vec::new();
    let mut read_tasks = Vec::new();

    // Spawn writer tasks
    for (i, worker) in workers.iter().enumerate() {
        let loglet_id = LogletId::new_unchecked(i as u64 + 1);
        let shared = shared.clone();
        let barrier = barrier.clone();
        let payload_spec = opts.payload.clone();
        let payload_file = opts.payload_file.clone();
        let data_tx = worker.data_tx();
        let counters = counters.clone();
        let collector = latency_collector.clone();
        let payload_size = opts.payload.payload_size.as_u64();

        let handle =
            TaskCenter::spawn_unmanaged(TaskKind::LogServerRole, "bench-writer", async move {
                let mut pool = if let Some(ref path) = payload_file {
                    PayloadPool::from_file(path, Some(records_per_batch))?
                } else {
                    PayloadPool::new(&payload_spec, records_per_batch, POOL_BATCHES)
                };
                let mut next_offset = LogletOffset::OLDEST;

                barrier.wait().await;

                while !shared.stop.load(Ordering::Relaxed) {
                    let payloads: Arc<[Record]> = pool.next_batch();
                    let batch_len = payloads.len() as u32;
                    let store_msg = Store {
                        header: LogServerRequestHeader::new(loglet_id, next_offset),
                        timeout_at: None,
                        sequencer: SEQUENCER,
                        known_archived: LogletOffset::INVALID,
                        first_offset: next_offset,
                        flags: StoreFlags::empty(),
                        payloads: payloads.into(),
                    };
                    let (msg, reply) = ServiceMessage::fake_rpc(
                        store_msg,
                        Some(loglet_id.into()),
                        SEQUENCER,
                        None,
                    );

                    let batch_start = std::time::Instant::now();
                    data_tx.send(msg);
                    let stored: Stored = reply.await?;
                    collector.record(batch_start.elapsed().as_nanos() as u64);

                    if stored.status != Status::Ok {
                        anyhow::bail!("Store failed: {:?}", stored.status);
                    }

                    next_offset = next_offset + batch_len;
                    shared.offsets[i].store(u64::from(next_offset), Ordering::Relaxed);

                    // Update shared counters
                    counters.ops.fetch_add(batch_len as u64, Ordering::Relaxed);
                    counters
                        .payload_bytes
                        .fetch_add(batch_len as u64 * payload_size, Ordering::Relaxed);
                }

                let total_records = u64::from(next_offset) - 1;
                Ok::<_, anyhow::Error>((loglet_id, total_records))
            })?;
        write_tasks.push(handle);
    }

    // Spawn reader tasks
    if opts.enable_reads {
        for (i, worker) in workers.iter().enumerate() {
            let loglet_id = LogletId::new_unchecked(i as u64 + 1);
            let shared = shared.clone();
            let barrier = barrier.clone();
            let read_lag = opts.read_lag;
            let data_tx = worker.data_tx();

            let handle =
                TaskCenter::spawn_unmanaged(TaskKind::LogServerRole, "bench-reader", async move {
                    let mut latencies = Histogram::<u64>::new(3)?;
                    let mut read_offset = LogletOffset::OLDEST;
                    let mut total_records_read: u64 = 0;

                    barrier.wait().await;

                    while !shared.stop.load(Ordering::Relaxed) {
                        let writer_offset = shared.offsets[i].load(Ordering::Relaxed);
                        let target = writer_offset.saturating_sub(read_lag);
                        if u64::from(read_offset) >= target {
                            tokio::task::yield_now().await;
                            continue;
                        }

                        let to_raw = (u64::from(read_offset) + 1000)
                            .min(target)
                            .min(u32::MAX as u64) as u32;
                        let get_msg = GetRecords {
                            header: LogServerRequestHeader::new(loglet_id, read_offset),
                            from_offset: read_offset,
                            to_offset: LogletOffset::from(to_raw),
                            total_limit_in_bytes: Some(64 * 1024),
                            filter: KeyFilter::Any,
                        };

                        let (msg, reply) = ServiceMessage::fake_rpc(
                            get_msg,
                            Some(loglet_id.into()),
                            SEQUENCER,
                            None,
                        );

                        let read_start = std::time::Instant::now();
                        data_tx.send(msg);
                        let records: Records = reply.await?;
                        latencies.record(read_start.elapsed().as_nanos() as u64)?;

                        total_records_read += records.records.len() as u64;
                        read_offset = records.next_offset;
                    }

                    Ok::<_, anyhow::Error>((total_records_read, latencies))
                })?;
            read_tasks.push(handle);
        }
    }

    // Spawn trim tasks
    if opts.enable_trims {
        for (i, worker) in workers.iter().enumerate() {
            let loglet_id = LogletId::new_unchecked(i as u64 + 1);
            let shared = shared.clone();
            let barrier = barrier.clone();
            let trim_lag = opts.trim_lag;
            let trim_interval = opts.trim_interval;
            let info_tx = worker.meta_tx();

            TaskCenter::spawn_unmanaged(TaskKind::LogServerRole, "bench-trimmer", async move {
                barrier.wait().await;
                while !shared.stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(*trim_interval).await;
                    let writer_offset = shared.offsets[i].load(Ordering::Relaxed);
                    let trim_to = writer_offset.saturating_sub(trim_lag);
                    if trim_to <= 1 {
                        continue;
                    }
                    let trim_offset = LogletOffset::from(trim_to.min(u32::MAX as u64) as u32);
                    let trim_msg = Trim {
                        header: LogServerRequestHeader::new(loglet_id, trim_offset),
                        trim_point: trim_offset,
                    };
                    let (msg, reply) =
                        ServiceMessage::fake_rpc(trim_msg, Some(loglet_id.into()), SEQUENCER, None);
                    info_tx.send(msg);
                    let _ = reply.await;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        }
    }

    let start_snapshot = SystemSnapshot::capture(DB_NAME);

    // Start all tasks
    barrier.wait().await;
    info!("All tasks started, running for {}", opts.duration);

    // Wait for duration
    tokio::time::sleep(*opts.duration).await;
    shared.stop.store(true, Ordering::Relaxed);
    info!("Duration elapsed, stopping tasks...");

    // Stop reporter and collect combined histogram
    drop(latency_collector);
    reporter_handle.abort();
    let combined = match reporter_handle.await {
        Ok(h) => h,
        Err(_) => Histogram::<u64>::new(3)?,
    };

    let end_snapshot = SystemSnapshot::capture(DB_NAME);

    // Collect writer results
    let mut total_write_records: u64 = 0;
    for task in write_tasks {
        let (_loglet_id, records) = task.await??;
        total_write_records += records;
    }
    let total_batches = total_write_records / records_per_batch as u64;

    print_summary(&RunSummary {
        title: "Mixed Workload Results",
        total_ops: total_write_records,
        total_batches,
        records_per_batch,
        payload_size: opts.payload.payload_size,
        entropy: opts.payload.entropy,
        num_loglets: opts.num_loglets,
        wall_time: opts.duration,
        latencies: &combined,
        start_snapshot: &start_snapshot,
        end_snapshot: &end_snapshot,
        db_name: DB_NAME,
        raw_rocksdb_stats,
    });

    // Collect reader results
    if opts.enable_reads {
        let mut total_read_records: u64 = 0;
        let mut combined_read_latencies = Histogram::<u64>::new(3)?;
        for task in read_tasks {
            let (records, latencies) = task.await??;
            total_read_records += records;
            combined_read_latencies.add(&latencies)?;
        }
        print_read_summary(&combined_read_latencies, total_read_records, opts.duration);
    }

    // Drain workers
    for worker in workers {
        worker.drain().await?;
    }

    Ok(())
}
