// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use metrics::{counter, gauge, histogram};
use restate_types::live::LiveLoad;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace, warn};

use restate_core::cancellation_watcher;
use restate_storage_api::StorageError;
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_types::config::{Configuration, StorageOptions};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};

use crate::PartitionStoreManager;

#[derive(Clone, Debug)]
struct ExponentialMovingAverage {
    value: f64,
    alpha: f64,
}

impl ExponentialMovingAverage {
    fn new_with_alpha(alpha: f64) -> Self {
        Self { value: 0.0, alpha }
    }

    fn update(&mut self, new_value: f64) {
        self.value = self.alpha * new_value + (1.0 - self.alpha) * self.value;
    }

    fn value(&self) -> f64 {
        self.value
    }
}

#[derive(Clone, Debug)]
struct PartitionState {
    memtable_size: u64,
    applied_lsn: Lsn,
    persisted_lsn: Lsn,
    last_flush_time: Instant,
}

#[derive(Debug)]
pub struct MemtableFlushController {
    partition_store_manager: PartitionStoreManager,
    partition_states: HashMap<PartitionId, PartitionState>,
    persisted_lsns: BTreeMap<PartitionId, Lsn>,
    total_memtables_budget: u64,
    memtable_size_soft_limit: u64,

    persisted_lsns_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,

    startup_time: Instant,
    latest_total_partition_store_memtables_size: u64,
    last_status_log: Instant,
    last_flush_time: Instant,
    last_stats_update: Instant,
    allocation_rate: ExponentialMovingAverage,
    burst_allocation_rate: ExponentialMovingAverage,
    memory_pressure: f64,
}

impl MemtableFlushController {
    pub fn create(
        partition_store_manager: PartitionStoreManager,
        mut storage_opts: impl LiveLoad<StorageOptions> + Send + 'static,
        persisted_lsns_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    ) -> Self {
        let options = storage_opts.live_load();

        MemtableFlushController {
            partition_store_manager,
            partition_states: HashMap::default(),
            persisted_lsns: BTreeMap::default(),
            total_memtables_budget: u64::try_from(options.rocksdb_memtables_budget())
                .expect("fits in u64"),
            memtable_size_soft_limit: 64 << 20,

            persisted_lsns_tx,

            startup_time: Instant::now(),
            latest_total_partition_store_memtables_size: 0,
            last_status_log: Instant::now(),
            last_flush_time: Instant::now(),
            last_stats_update: Instant::now(),
            allocation_rate: ExponentialMovingAverage::new_with_alpha(0.01),
            burst_allocation_rate: ExponentialMovingAverage::new_with_alpha(0.2),
            memory_pressure: 0.,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Starting flush controller");

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();

        let mut state_check_interval = time::interval(Duration::from_millis(10));
        state_check_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                _ = state_check_interval.tick() => {
                    if let Err(err) = self.update_partition_states().await {
                        warn!("Failed to update partition states: {err}");
                        continue;
                    }

                    if self.should_trigger_flush_cycle() {
                        // Select which partitions to flush
                        let partitions_to_flush = self.select_partitions_to_flush();

                        if !partitions_to_flush.is_empty() {
                            if let Err(err) = self.flush_partitions(&partitions_to_flush).await {
                                warn!("Failed to flush partitions: {err}");
                            }
                        }
                    }
                }
                _ = config_watcher.changed() => {
                    debug!("Configuration update not supported!");
                }
            }
        }

        Ok(())
    }

    async fn update_partition_states(&mut self) -> anyhow::Result<()> {
        let partition_stores = self
            .partition_store_manager
            .get_all_partition_stores()
            .await;

        let mut partition_memtables_size_sum = 0;
        let states = &mut self.partition_states;

        for mut partition_store in partition_stores {
            let partition_id = partition_store.partition_id();

            let state = states
                .entry(partition_id)
                .or_insert_with(|| PartitionState {
                    memtable_size: 0,
                    applied_lsn: Lsn::INVALID,
                    persisted_lsn: Lsn::INVALID,
                    last_flush_time: Instant::now(),
                });

            let current_lsn = partition_store
                .get_applied_lsn()
                .await?
                .unwrap_or(Lsn::INVALID);
            state.applied_lsn = current_lsn;

            let size = partition_store.get_memtables_size();
            state.memtable_size = size;
            partition_memtables_size_sum += size;

            if let Some(persisted_lsn) = self.persisted_lsns.get(&partition_id) {
                state.persisted_lsn = *persisted_lsn;
            }
        }

        self.persisted_lsns_tx.send(self.persisted_lsns.clone())?;

        let now = Instant::now();
        let delta = partition_memtables_size_sum
            .saturating_sub(self.latest_total_partition_store_memtables_size)
            as f64
            / now.duration_since(self.last_stats_update).as_secs_f64();
        self.allocation_rate.update(delta);
        self.burst_allocation_rate.update(delta);
        self.last_stats_update = now;

        self.latest_total_partition_store_memtables_size = partition_memtables_size_sum;

        let total_capacity = self.total_memtables_budget;
        let current_usage = self.latest_total_partition_store_memtables_size;
        let memory_pressure = current_usage as f64 / total_capacity as f64;
        self.memory_pressure = memory_pressure;

        gauge!("restate.memtable_flush_controller.memory_pressure").set(memory_pressure);
        gauge!("restate.memtable_flush_controller.memory_usage").set(current_usage as f64);

        Ok(())
    }

    fn should_trigger_flush_cycle(&mut self) -> bool {
        let total_capacity = self.total_memtables_budget;
        let current_usage = self.latest_total_partition_store_memtables_size;
        let memory_pressure = self.memory_pressure;
        let now = Instant::now();

        if self.last_status_log.elapsed() > Duration::from_secs(1) {
            debug!(
                current_usage = ByteSize::b(current_usage).display().iec().to_string(),
                memory_pressure,
                allocation_rate = format!(
                    "{}/s",
                    ByteSize::b(self.allocation_rate.value() as u64)
                        .display()
                        .iec()
                        .to_string()
                ),
                total_capacity = ByteSize::b(total_capacity).display().iec().to_string(),
                "Partition memory usage"
            );
            self.last_status_log = now;
        }

        if self
            .partition_states
            .iter()
            .any(|(_, state)| state.memtable_size >= (self.memtable_size_soft_limit * 9 / 10))
        {
            debug!(
                current_usage = ByteSize::b(current_usage).display().iec().to_string(),
                memory_pressure = (current_usage as f64 / total_capacity as f64),
                total_capacity = ByteSize::b(total_capacity).display().iec().to_string(),
                "Triggering flush for target memtable size"
            );
            return true;
        }

        // Are we in danger of running out of memtable buffers in the near future? Don't use
        // projected pressure until we've been running for a bit though
        let projected_pressure = if self.startup_time.elapsed() > Duration::from_secs(10) {
            (current_usage as f64 + self.allocation_rate.value()) / total_capacity as f64
        } else {
            self.memory_pressure
        };

        // TODO: this should be based off of individual partitions' growth rates
        if projected_pressure > 0.8 {
            debug!(
                current_usage = ByteSize::b(current_usage).display().iec().to_string(),
                total_capacity = ByteSize::b(total_capacity).display().iec().to_string(),
                memory_pressure,
                projected_pressure,
                "Triggering memory pressure flush"
            );
            self.last_status_log = now;
            return true;
        }

        let time_based_flush_check_interval = Duration::from_secs(300);
        if now.duration_since(self.last_flush_time) > time_based_flush_check_interval {
            trace!(
                last_flush = ?self.last_flush_time.elapsed(),
                "Time based flush"
            );
            self.last_status_log = now;
            return true;
        }

        false
    }

    fn select_partitions_to_flush(&mut self) -> Vec<PartitionId> {
        let now = Instant::now();

        // let total_capacity = self.total_memtables_budget;
        // let current_usage = self.latest_total_partition_store_memtables_size;

        // projected memory pressure at some time into the near future:
        // let projected_pressure =
        //     (current_usage as f64 + self.allocation_rate.value() * 2.) / total_capacity as f64;

        let memtable_size_soft_limit = self.memtable_size_soft_limit as f64;

        let mut partition_scores: Vec<(PartitionId, u64, f64)> = self
            .partition_states
            .iter()
            .map(|(id, state)| {
                let memtable_size = state.memtable_size as f64;
                let memtable_usage_ratio = memtable_size / memtable_size_soft_limit;
                let memory_score = memtable_usage_ratio *
                    // sigmoid infection after 80% that grows rapidly as we head towards the limit
                    (1.0 + 10.0 * (memtable_usage_ratio - 0.8).max(0.0).powf(2.0));

                // We want older memtables to score high, provided they meet a certain min size
                let time_score = if memtable_size < memtable_size_soft_limit * 0.001 {
                    // ignore tables < ~64k (at 64MB buffer cap)
                    0.
                } else if memtable_size < memtable_size_soft_limit * 0.5 {
                    // consider anything up to half "full" as a low-priority target
                    now.duration_since(state.last_flush_time).as_secs_f64() / 900.
                } else {
                    now.duration_since(state.last_flush_time).as_secs_f64() / 60.
                };

                let total_score = /* projected_pressure.exp2() * */ memory_score + time_score;
                trace!(
                    partition_id = ?id,
                    total_score,
                    memory_score,
                    time_score,
                    "Score"
                );

                (*id, state.memtable_size, total_score)
            })
            .collect();

        partition_scores.sort_by(|(_, _, score_a), (_, _, score_b)| {
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_capacity = self.total_memtables_budget;
        let current_usage = self.latest_total_partition_store_memtables_size;
        let memory_pressure = self.memory_pressure;

        // Moving averages based heuristic. Try to set a goal amount of memory to free up - in the
        // future we might scale this based on the observed moving average, too
        let free_up_target_size = if current_usage > total_capacity * 9 / 10 {
            // try free up to 20% of the total available capacity in this cycle
            total_capacity * 2 / 10
        } else if memory_pressure < 0.5 {
            // we might flush some partitions based on high score (memory / individual size), but we
            // don't have a specific goal to hit
            0
        } else {
            // try stay ahead of the long-term moving average allocation rate
            (self.allocation_rate.value() * 1.5) as u64
        };

        let mut running_flush_total_size = 0;
        let selected: Vec<_> = partition_scores
            .iter()
            .take_while(|(_, size, score)| {
                // flush high-scoring partitions even during low memory pressure
                if *score > 0.9 || running_flush_total_size < free_up_target_size {
                    running_flush_total_size += *size;
                    true
                } else {
                    false
                }
            })
            .map(|(id, ..)| *id)
            .collect();

        if !selected.is_empty() {
            debug!(
                memory_pressure,
                flush_goal = ByteSize::b(free_up_target_size).display().iec().to_string(),
                num_partitions = selected.len(),
                targets = ?partition_scores[..selected.len()],
                "Partitions to flush"
            );
        }

        selected
    }

    async fn flush_partitions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<(), StorageError> {
        let mut modified = false;

        let start_time = Instant::now();
        let total_count = partition_ids.len();
        let mut success_count = 0;

        let concurrent_flushes = Arc::new(Semaphore::new(4));
        let mut flush_tasks: JoinSet<anyhow::Result<_>> = JoinSet::new();

        for &partition_id in partition_ids {
            if let Some(mut partition_store) = self
                .partition_store_manager
                .get_partition_store(partition_id)
                .await
            {
                let Some(applied_lsn) = partition_store.get_applied_lsn().await? else {
                    warn!(?partition_id, "Could not get applied LSN");
                    continue;
                };

                let previously_persisted_lsn = self
                    .persisted_lsns
                    .get(&partition_id)
                    .cloned()
                    .unwrap_or(Lsn::INVALID);

                if applied_lsn <= previously_persisted_lsn {
                    trace!(
                        partition_id = %partition_id,
                        current_lsn = %applied_lsn,
                        previously_persisted_lsn = %previously_persisted_lsn,
                        "No need to flush, still at last persisted LSN"
                    );
                    if let Some(state) = self.partition_states.get_mut(&partition_id) {
                        state.last_flush_time = Instant::now();
                    }
                    continue;
                }

                let Some(partition_store) = self
                    .partition_store_manager
                    .get_partition_store(partition_id)
                    .await
                else {
                    warn!(
                        ?partition_id,
                        "Partition store for partition is gone, skipping flush"
                    );
                    continue;
                };

                let snapshot = self
                    .partition_states
                    .get(&partition_id)
                    .expect("partition state present")
                    .clone();

                let concurrent_flushes = concurrent_flushes.clone();
                flush_tasks.spawn(async move {
                    let _permit = concurrent_flushes.acquire().await?;
                    let flush_start = Instant::now();

                    partition_store.flush_memtables(true).await?;

                    let flush_duration = flush_start.elapsed();
                    histogram!(
                        "restate.memtable_flush_controller.partition_flush_duration.seconds"
                    )
                    .record(flush_duration);

                    let size_before_flush = snapshot.memtable_size;
                    let last_flush = snapshot.last_flush_time.elapsed();
                    let size_after_flush = partition_store.get_memtables_size();

                    debug!(
                        %partition_id,
                        ?last_flush,
                        persisted_lsn = %applied_lsn,
                        ?flush_duration,
                        size_before = ByteSize::b(size_before_flush).display().iec().to_string(),
                        size_after = ByteSize::b(size_after_flush).display().iec().to_string(),
                        "Flushed partition"
                    );

                    histogram!(
                        "restate.memtable_flush_controller.partition_flushed_memtables_size.bytes"
                    )
                    .record(size_before_flush as f64);

                    Ok((partition_id, flush_start, applied_lsn, size_after_flush))
                });
            }
        }

        while let Some(result) = flush_tasks.join_next().await {
            match result {
                Ok(Ok((partition_id, flush_time, persisted_lsn, size_after_flush))) => {
                    let _ = self.persisted_lsns.insert(partition_id, persisted_lsn);
                    modified = true;
                    success_count += 1;

                    if let Some(state) = self.partition_states.get_mut(&partition_id) {
                        state.persisted_lsn = persisted_lsn;
                        state.memtable_size = size_after_flush;
                        state.last_flush_time = flush_time;
                    }
                }
                Ok(Err(err)) => {
                    warn!("Flush task failed: {}", err);
                }
                Err(err) => {
                    warn!("Flush task failed: {}", err);
                }
            }
        }

        counter!("restate.memtable_flush_controller.partitions_flushed").increment(success_count);
        histogram!("restate.memtable_flush_controller.flush_cycle_duration.seconds")
            .record(start_time.elapsed());

        if modified {
            // let _ = self.watch_tx.send(new_persisted_lsns);

            // Updating the latest total post-flush will give us more accurate running averages
            self.latest_total_partition_store_memtables_size = self
                .partition_states
                .values()
                .map(|s| s.memtable_size)
                .sum();

            debug!(
                elapsed = ?start_time.elapsed(),
                success_count,
                total_count,
                "Completed flush cycle"
            );
        }

        // Avoid rapid time-based flush trigger if we didn't find anything worth flushing
        self.last_flush_time = Instant::now();

        Ok(())
    }
}
