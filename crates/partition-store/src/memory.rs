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
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use ahash::{HashMap, HashMapExt};
use tracing::{info, trace, warn};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_serde_util::ByteCount;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;

use crate::{PartitionDb, SharedState};

const INITIAL_NUM_PARTITIONS: usize = 4;
const DEBUG_MEMORY_REPORTING: bool = false;

pub(crate) struct MemoryBudget {
    // manages memory budgets for rocksdb-based partition stores.
    budget_per_partition: AtomicUsize,
}

impl MemoryBudget {
    fn new() -> Arc<Self> {
        let budget_per_partition = AtomicUsize::new(
            Configuration::pinned()
                .worker
                .storage
                .rocksdb_memory_budget()
                / INITIAL_NUM_PARTITIONS,
        );

        Arc::new(Self {
            budget_per_partition,
        })
    }

    pub fn get_total_memory_budget(&self) -> usize {
        let config = Configuration::pinned();
        config.worker.storage.rocksdb_memory_budget()
    }

    pub fn current_per_partition_budget(&self) -> usize {
        self.budget_per_partition
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_per_partition_budget(&self, budget: usize) {
        self.budget_per_partition
            .store(budget, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct MemoryController {
    // manages memory budgets for rocksdb-based partition stores.
    pub(crate) memory_budget: Arc<MemoryBudget>,
}

impl MemoryController {
    pub fn start(psm_state: Arc<SharedState>) -> Result<MemoryController, ShutdownError> {
        let memory_budget = MemoryBudget::new();
        TaskCenter::spawn(
            TaskKind::Background,
            "partition-memory-controller",
            memory_controller_task(memory_budget.clone(), psm_state),
        )?;

        Ok(Self { memory_budget })
    }
}

async fn memory_controller_task(
    memory_budget: Arc<MemoryBudget>,
    psm_state: Arc<SharedState>,
) -> anyhow::Result<()> {
    let mut cancel = std::pin::pin!(cancellation_watcher());
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_total_budget = memory_budget.get_total_memory_budget();
    let mut config_watch = Configuration::watcher();

    loop {
        tokio::select! {
            _ = &mut cancel => {
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = rebalance_memory(&memory_budget, &psm_state).await {
                    warn!("Failed to rebalance partition stores memory: {}", e);
                }
            }
            () = config_watch.changed() => {
                let total_budget = memory_budget.get_total_memory_budget();
                if total_budget != last_total_budget {
                    info!("Detected a change in memory budget for partitions in configuration file (from {} -> {}), will rebalance",
                        ByteCount::from(last_total_budget),
                        ByteCount::from(total_budget)
                    );
                    last_total_budget = total_budget;
                    interval.reset_after(Duration::from_secs(1));
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct CollectedMemoryUsage {
    partitions: HashMap<PartitionId, (MemoryUsage, PartitionDb)>,
    num_open_partitions: usize,
    total_usage: usize,
    usage_from_closed: usize,
    total_reclaimable: usize,
    total_pinned: usize,
    partitions_exceeding_budget: usize,
    num_pending_flushes: usize,
}

async fn collect_memory_usage(
    psm_state: &SharedState,
    partition_budget: usize,
) -> anyhow::Result<CollectedMemoryUsage> {
    let dbs = psm_state.get_maybe_open_dbs().await;
    if dbs.is_empty() {
        return Ok(Default::default());
    }

    let mut partitions: HashMap<PartitionId, (MemoryUsage, PartitionDb)> =
        HashMap::with_capacity(dbs.len());
    let mut num_open_partitions = 0;
    let mut total_usage = 0usize;
    let mut usage_from_closed = 0usize;
    let mut total_reclaimable = 0usize;
    let mut total_pinned = 0usize;
    let mut partitions_exceeding_budget = 0usize;
    let mut num_pending_flushes = 0usize;

    for db_state in dbs.iter() {
        assert!(db_state.maybe_open());
        let db = db_state.db().unwrap().clone();
        let usage = MemoryUsage::create(db_state)?;
        // a partition will be considered open if it's Open or Closed for less than 30 seconds.
        if db_state
            .closed_since()
            .is_none_or(|elapsed| elapsed < Duration::from_secs(30))
        {
            num_open_partitions += 1;
        }

        total_usage += usage.total_bytes;
        if !usage.is_open() {
            usage_from_closed += usage.total_bytes;
        }

        total_reclaimable += if usage.is_open() {
            usage.immutable_bytes()
        } else {
            usage.total_bytes
        };

        total_pinned += usage.pinned_bytes();
        if usage.total_bytes > partition_budget {
            partitions_exceeding_budget += 1;
        }
        num_pending_flushes += if usage.is_flush_pending { 1 } else { 0 };

        trace!(partition_id = %db.partition().id(), "  - Partition memory: {}", usage);
        partitions.insert(db.partition().id(), (usage, db));
    }

    Ok(CollectedMemoryUsage {
        partitions,
        num_open_partitions,
        total_usage,
        usage_from_closed,
        total_reclaimable,
        total_pinned,
        partitions_exceeding_budget,
        num_pending_flushes,
    })
}

async fn rebalance_memory(
    memory_budget: &MemoryBudget,
    psm_state: &SharedState,
) -> anyhow::Result<()> {
    let total_budget = memory_budget.get_total_memory_budget();
    let current_per_partition_budget = memory_budget.current_per_partition_budget();
    let collected = collect_memory_usage(psm_state, current_per_partition_budget).await?;

    if DEBUG_MEMORY_REPORTING {
        report_memory_usage(&collected, total_budget);
    }

    if collected.num_open_partitions > 0 {
        // possibly update memory budget
        let new_partition_budget = total_budget / collected.num_open_partitions;
        // budget has changed since last time we checked
        if new_partition_budget != current_per_partition_budget {
            if !DEBUG_MEMORY_REPORTING {
                report_memory_usage(&collected, total_budget);
            }
            memory_budget.set_per_partition_budget(new_partition_budget);

            info!(
                "Rebalancing the memory budget over {} open partitions. Budget per partition changed from {} -> {}. \
                    total_usage: {}/{}",
                collected.num_open_partitions,
                ByteCount::from(current_per_partition_budget),
                ByteCount::from(new_partition_budget),
                ByteCount::from(collected.total_usage),
                ByteCount::from(total_budget)
            );
            for (_usage, partition_db) in collected.partitions.values() {
                // Note: we deliberately don't filter out closed partitions since they might be
                // opened again and we don't want to leave them behind. They'll be flushed to disk
                // if their memory usage exceeds the predefined threshold (512KiB) and their new
                // memtable will sit idle roughly at 8KiB regardless of what we configure here.
                //
                // If/when it gets re-opened, we'll be monitoring its memory usage against the
                // latest budget anyway and we'll be able to flush it prematurely if it exceeds the
                // per-partition budget. This or WBM might hit it first.
                partition_db.update_memory_budget(new_partition_budget);
            }
        }
    }

    // refresh the partition_budget since previous step may have changed it
    let current_per_partition_budget = memory_budget.current_per_partition_budget();

    let mut reclaim_candidates: Vec<_> = Vec::with_capacity(collected.partitions.len());
    // did we exceed the total memory budget?
    for (usage, partition_db) in collected.partitions.values() {
        // is this an offending partition? only if it exceeds its budget by 10%
        if !usage.is_flush_pending
            && usage.total_bytes
                > (current_per_partition_budget + current_per_partition_budget.div_ceil(10))
        {
            reclaim_candidates.push((usage, partition_db));
        }
        if !usage.is_open() {
            // 512KiB threshold for closed partitions. This is primarily a heuristic to avoid
            // re-flushing the same partitions over and over again.
            if usage.total_bytes > 512 * 1024 * 1024 {
                reclaim_candidates.push((usage, partition_db));
            }
        }
    }

    for (usage, partition_db) in reclaim_candidates {
        info!(
            "Flushing partition {} to reclaim memory. partition_usage: {}/{}",
            partition_db.partition().id(),
            ByteCount::from(usage.total_bytes),
            ByteCount::from(current_per_partition_budget),
        );
        partition_db.flush_memtables(true).await?;
    }

    Ok(())
}

fn report_memory_usage(collected: &CollectedMemoryUsage, total_budget: usize) {
    info!(
        "Partition memory report. open: {}, closed: {}, total_usage: {}/{}, \
            usage_from_closed_partitions: {}, total_reclaimable_bytes: {}, pinned_bytes: {}, \
            num_partitions_exceeding_budget: {}, flush_pending: {}",
        collected.num_open_partitions,
        collected.partitions.len() - collected.num_open_partitions,
        ByteCount::from(collected.total_usage),
        ByteCount::from(total_budget),
        ByteCount::from(collected.usage_from_closed),
        ByteCount::from(collected.total_reclaimable),
        ByteCount::from(collected.total_pinned),
        collected.partitions_exceeding_budget,
        collected.num_pending_flushes
    );
}

/// A snapshot of the memory usage of a partition database.
struct MemoryUsage {
    /// Whether the partition is open or closed.
    closed_since: Option<Duration>,
    /// - "rocksdb.cur-size-active-mem-table": bytes in active memtable
    mutable_bytes: usize,
    /// - "rocksdb.cur-size-all-mem-tables": (bytes in active + immutable memtables)
    total_bytes: usize,
    ///-  "rocksdb.size-all-mem-tables" (immutable + pinned)
    immutable_and_pinned_bytes: usize,
    /// - "rocksdb.mem-table-flush-pending": 1 if a flush is pending
    is_flush_pending: bool,
}

impl MemoryUsage {
    const fn immutable_bytes(&self) -> usize {
        self.total_bytes.saturating_sub(self.mutable_bytes)
    }

    const fn pinned_bytes(&self) -> usize {
        self.immutable_and_pinned_bytes
            .saturating_sub(self.total_bytes)
    }

    fn create(partition_state: &crate::partition_db::State) -> anyhow::Result<Self> {
        assert!(partition_state.maybe_open());
        let partition_db = partition_state.db().unwrap();

        let cf_names = partition_db.cf_names();
        let rocks_db = partition_db.rocksdb();
        let raw_rocks_db = rocks_db.inner();

        let mut memory_usage = Self {
            closed_since: partition_state.closed_since(),
            mutable_bytes: 0,
            total_bytes: 0,
            immutable_and_pinned_bytes: 0,
            is_flush_pending: false,
        };

        for cf in cf_names {
            memory_usage.total_bytes += raw_rocks_db
                .get_property_int_cf(&cf, "rocksdb.cur-size-all-mem-tables")?
                .unwrap_or_default() as usize;

            memory_usage.immutable_and_pinned_bytes += raw_rocks_db
                .get_property_int_cf(&cf, "rocksdb.size-all-mem-tables")?
                .unwrap_or_default()
                as usize;

            memory_usage.mutable_bytes += raw_rocks_db
                .get_property_int_cf(&cf, "rocksdb.cur-size-active-mem-table")?
                .unwrap_or_default() as usize;

            memory_usage.is_flush_pending |= raw_rocks_db
                .get_property_int_cf(&cf, "rocksdb.mem-table-flush-pending")?
                .unwrap_or_default()
                == 1;
        }

        Ok(memory_usage)
    }

    /// a partition will be considered open if it's Open or Closed for less than 30 seconds.
    fn is_open(&self) -> bool {
        self.closed_since
            .is_none_or(|elapsed| elapsed < Duration::from_secs(30))
    }
}

impl std::fmt::Display for MemoryUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let immutable = self.total_bytes - self.mutable_bytes;
        write!(
            f,
            "mutable={}, immutable={}, pinned={}, total={}, flush_pending?={}",
            ByteCount::from(self.mutable_bytes),
            ByteCount::from(immutable),
            ByteCount::from(self.immutable_and_pinned_bytes - immutable),
            ByteCount::from(self.total_bytes),
            self.is_flush_pending
        )
    }
}
