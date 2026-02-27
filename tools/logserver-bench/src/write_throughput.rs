// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sequential write throughput benchmark.
//!
//! Drives Store messages into per-loglet workers and measures write latency and throughput.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::Result;
use tracing::info;

use restate_core::network::ServiceMessage;
use restate_log_server::loglet_worker::{LogletWorker, LogletWorkerHandle};
use restate_log_server::logstore::LogStore;
use restate_log_server::metadata::LogletStateMap;
use restate_time_util::FriendlyDuration;
use restate_types::GenerationalNodeId;
use restate_types::logs::{LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::net::log_server::{LogServerRequestHeader, Status, Store, StoreFlags, Stored};

use crate::payload::{PayloadPool, PayloadSpec};
use crate::report::{BenchCounters, IntervalReporter, RunSummary, SystemSnapshot, print_summary};

const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
const DB_NAME: &str = "log-server";
/// Number of pre-generated payload batches to cycle through.
const POOL_BATCHES: usize = 1024;

#[derive(Debug, Clone, clap::Parser)]
pub struct WriteThroughputOpts {
    /// Total number of records to write. Mutually exclusive with --duration.
    /// Defaults to 1,000,000 when neither --num-records nor --duration is given.
    #[arg(long, conflicts_with = "duration")]
    pub num_records: Option<u64>,

    /// Run continuously for a duration (e.g. "5m", "1h"). Mutually exclusive with --num-records.
    #[arg(long, conflicts_with = "num_records")]
    pub duration: Option<FriendlyDuration>,

    /// Number of records per Store message batch.
    #[arg(long, default_value = "10")]
    pub records_per_batch: usize,

    /// Number of loglets to write to concurrently.
    #[arg(long, default_value = "1")]
    pub num_loglets: u32,

    /// Number of records to write as warmup (not measured).
    #[arg(long, default_value = "50000")]
    pub warmup_records: u64,

    /// Load payload from a pre-generated file (see `generate-payload` subcommand).
    /// When set, --payload-size/--entropy/--key-style/--seed are ignored.
    #[arg(long)]
    pub payload_file: Option<PathBuf>,

    #[clap(flatten)]
    pub payload: PayloadSpec,
}

const DEFAULT_NUM_RECORDS: u64 = 1_000_000;

struct LogletContext {
    worker: LogletWorkerHandle,
    loglet_id: LogletId,
    next_offset: LogletOffset,
}

pub async fn run<S: LogStore + Sync>(
    opts: &WriteThroughputOpts,
    log_store: S,
    state_map: &LogletStateMap,
    report_interval: FriendlyDuration,
    raw_rocksdb_stats: bool,
) -> Result<()> {
    let records_per_batch = opts.records_per_batch.max(1);
    let mut pool = if let Some(ref path) = opts.payload_file {
        PayloadPool::from_file(path, Some(records_per_batch))?
    } else {
        PayloadPool::new(&opts.payload, records_per_batch, POOL_BATCHES)
    };

    // Create loglet workers
    let mut loglets: Vec<LogletContext> = Vec::with_capacity(opts.num_loglets as usize);
    for i in 0..opts.num_loglets {
        let loglet_id = LogletId::new_unchecked((i + 1) as u64);
        let loglet_state = state_map.get_or_load(loglet_id, &log_store).await?;
        let worker = LogletWorker::start(loglet_id, log_store.clone(), loglet_state)?;
        loglets.push(LogletContext {
            worker,
            loglet_id,
            next_offset: LogletOffset::OLDEST,
        });
    }

    // Determine stop condition
    let max_batches = if opts.duration.is_some() {
        None
    } else {
        Some(opts.num_records.unwrap_or(DEFAULT_NUM_RECORDS) / records_per_batch as u64)
    };

    if let Some(max) = max_batches {
        info!(
            num_loglets = opts.num_loglets,
            records_per_batch,
            num_records = max * records_per_batch as u64,
            payload_size = %opts.payload.payload_size,
            entropy = opts.payload.entropy,
            "Starting write throughput benchmark (record count mode)"
        );
    } else {
        info!(
            num_loglets = opts.num_loglets,
            records_per_batch,
            duration = %opts.duration.unwrap(),
            payload_size = %opts.payload.payload_size,
            entropy = opts.payload.entropy,
            "Starting write throughput benchmark (duration mode)"
        );
    }

    // Warmup phase
    if opts.warmup_records > 0 {
        info!(warmup_records = opts.warmup_records, "Warmup phase");
        let warmup_batches = opts.warmup_records / records_per_batch as u64;
        let num_loglets = loglets.len();
        for batch_i in 0..warmup_batches {
            let ctx = &mut loglets[batch_i as usize % num_loglets];
            send_store_batch(ctx, &mut pool).await?;
        }
        info!("Warmup complete");
    }

    // Set up reporting
    let counters = BenchCounters::new();
    let (reporter, latency_collector) =
        IntervalReporter::new(report_interval, DB_NAME, counters.clone());
    let reporter_handle = tokio::spawn(reporter.run());

    let start_snapshot = SystemSnapshot::capture(DB_NAME);
    let num_loglets = loglets.len();
    let payload_size = opts.payload.payload_size.as_u64();
    let deadline = opts.duration.map(|d| Instant::now() + *d);

    let start = Instant::now();
    let mut batch_count: u64 = 0;
    loop {
        if let Some(max) = max_batches
            && batch_count >= max
        {
            break;
        }
        if let Some(dl) = deadline
            && Instant::now() >= dl
        {
            break;
        }

        let ctx = &mut loglets[batch_count as usize % num_loglets];
        let batch_start = Instant::now();
        send_store_batch(ctx, &mut pool).await?;
        latency_collector.record(batch_start.elapsed().as_nanos() as u64);

        batch_count += 1;
        let completed_records = batch_count * records_per_batch as u64;
        counters.ops.store(completed_records, Ordering::Relaxed);
        counters
            .payload_bytes
            .store(completed_records * payload_size, Ordering::Relaxed);
    }
    let wall_time = start.elapsed();

    // Stop reporter and collect combined histogram
    drop(latency_collector);
    reporter_handle.abort();
    let combined = match reporter_handle.await {
        Ok(h) => h,
        Err(_) => hdrhistogram::Histogram::<u64>::new(3)?,
    };

    let end_snapshot = SystemSnapshot::capture(DB_NAME);
    let total_records = batch_count * records_per_batch as u64;

    print_summary(&RunSummary {
        title: "Write Throughput Results",
        total_ops: total_records,
        total_batches: batch_count,
        records_per_batch,
        payload_size: opts.payload.payload_size,
        entropy: opts.payload.entropy,
        num_loglets: opts.num_loglets,
        wall_time: wall_time.into(),
        latencies: &combined,
        start_snapshot: &start_snapshot,
        end_snapshot: &end_snapshot,
        db_name: DB_NAME,
        raw_rocksdb_stats,
    });

    // Drain workers
    for ctx in loglets {
        ctx.worker.drain().await?;
    }

    Ok(())
}

async fn send_store_batch(ctx: &mut LogletContext, pool: &mut PayloadPool) -> Result<()> {
    let payloads: Arc<[Record]> = pool.next_batch();
    let batch_len = payloads.len() as u32;

    let store_msg = Store {
        header: LogServerRequestHeader::new(ctx.loglet_id, ctx.next_offset),
        timeout_at: None,
        sequencer: SEQUENCER,
        known_archived: LogletOffset::INVALID,
        first_offset: ctx.next_offset,
        flags: StoreFlags::empty(),
        payloads: payloads.into(),
    };

    let (msg, reply) =
        ServiceMessage::fake_rpc(store_msg, Some(ctx.loglet_id.into()), SEQUENCER, None);
    ctx.worker.data_tx().send(msg);
    let stored: Stored = reply.await?;

    anyhow::ensure!(
        stored.status == Status::Ok,
        "Store failed with status {:?}",
        stored.status
    );

    ctx.next_offset = ctx.next_offset.checked_add(batch_len).unwrap().into();
    Ok(())
}
