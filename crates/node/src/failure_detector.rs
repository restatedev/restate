// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod fd_state;
mod node_state;

use std::num::NonZeroUsize;
use std::time::Duration;

use ahash::HashMap;
use futures::stream::FuturesUnordered;
use metrics::counter;
use tokio::time::Instant;
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, info, trace, warn};

use restate_core::network::NetworkSender;
use restate_core::{
    Metadata, MetadataKind, ShutdownError, TaskCenter, TaskKind,
    network::{
        BackPressureMode, Incoming, MessageRouterBuilder, RawSvcRpc, RawSvcUnary, ServiceMessage,
        ServiceReceiver, Verdict,
    },
    task_center::{DefaultRuntimeTaskStats, TaskCenterMonitoring},
};
use restate_memory::NonZeroByteCount;
use restate_types::health::NodeStatus;
use restate_types::live::LiveLoad;
use restate_types::net::RpcRequest;
use restate_types::net::node::GetClusterState;
use restate_types::net::node::Gossip;
use restate_types::net::node::GossipFlags;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::time::MillisSinceEpoch;
use restate_types::{
    config::GossipOptions,
    net::node::{GetNodeState, GossipService, NodeStateResponse},
};
use restate_worker_api::ProcessorsManagerHandle;

use crate::metric_definitions::GOSSIP_SENT;

use self::fd_state::Error;
use self::fd_state::FdState;

const BUSIEST_PARTITION_PROCESSOR_RUNTIMES: usize = 3;

pub struct FailureDetector<T> {
    networking: T,
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    replica_set_states: PartitionReplicaSetStates,
    gossip_svc_rx: ServiceReceiver<GossipService>,
    gossip_interval: tokio::time::Interval,
    // when did we send the last gossip message with extras
    intervals_since_last_extras: u32,
    last_dumped: Instant,
    last_runtime_snapshot: RuntimeSnapshot,
}

#[derive(Debug)]
struct RuntimeWorkerSnapshot {
    poll_count: u64,
    busy_duration: Duration,
}

#[derive(Debug)]
struct RuntimeSnapshot {
    workers: Vec<RuntimeWorkerSnapshot>,
    tokio_spawned_tasks: u64,
    task_stats: DefaultRuntimeTaskStats,
    partition_processors: HashMap<String, RuntimeWorkerSnapshot>,
}

struct RuntimeWork {
    global_queue_depth: usize,
    worker_poll_count: Vec<u64>,
    worker_busy_duration: Vec<Duration>,
    worker_mean_poll_time: Vec<Duration>,
    worker_local_queue_depth: Vec<usize>,
    tokio_alive_tasks: usize,
    tokio_spawned_tasks: u64,
    tracked_task_active: u64,
    tracked_task_spawned: u64,
    approx_unattributed_task_active: usize,
    approx_unattributed_task_spawned: u64,
    partition_processors: PartitionProcessorRuntimeWork,
}

struct TaskKindWork {
    kind: &'static str,
    spawned: u64,
    active: u64,
    peak_active: u64,
    poll_count_total: u64,
    poll_count_delta: u64,
    poll_wall_duration_total: Duration,
    poll_wall_duration_delta: Duration,
}

impl std::fmt::Debug for TaskKindWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskKindWork")
            .field("kind", &self.kind)
            .field("spawned", &self.spawned)
            .field("active", &self.active)
            .field("peak_active", &self.peak_active)
            .field("poll_count_total", &self.poll_count_total)
            .field("poll_count_delta", &self.poll_count_delta)
            .field("poll_wall_duration_total", &self.poll_wall_duration_total)
            .field("poll_wall_duration_delta", &self.poll_wall_duration_delta)
            .finish()
    }
}

struct PartitionProcessorRuntimeWork {
    count: usize,
    worker_poll_count: u64,
    worker_busy_duration: Duration,
}

#[derive(Debug, Eq, PartialEq)]
struct PartitionProcessorRuntimeDelta {
    name: String,
    worker_poll_count: u64,
    worker_busy_duration: Duration,
}

impl RuntimeSnapshot {
    fn capture(partition_processors: HashMap<String, RuntimeWorkerSnapshot>) -> Self {
        let metrics = TaskCenter::with_current(|tc| tc.default_runtime_metrics());
        let task_stats = TaskCenter::with_current(|tc| tc.default_runtime_task_stats());
        let workers = (0..metrics.num_workers())
            .map(|worker| RuntimeWorkerSnapshot {
                poll_count: metrics.worker_poll_count(worker),
                busy_duration: metrics.worker_total_busy_duration(worker),
            })
            .collect();
        Self {
            workers,
            tokio_spawned_tasks: metrics.spawned_tasks_count(),
            task_stats,
            partition_processors,
        }
    }

    fn work_since(&self, previous: &Self) -> RuntimeWork {
        let metrics = TaskCenter::with_current(|tc| tc.default_runtime_metrics());
        let mut worker_poll_count = Vec::with_capacity(self.workers.len());
        let mut worker_busy_duration = Vec::with_capacity(self.workers.len());
        let mut worker_mean_poll_time = Vec::with_capacity(self.workers.len());
        let mut worker_local_queue_depth = Vec::with_capacity(self.workers.len());
        for (worker, (current, previous)) in self.workers.iter().zip(&previous.workers).enumerate()
        {
            worker_poll_count.push(current.poll_count.saturating_sub(previous.poll_count));
            worker_busy_duration.push(current.busy_duration.saturating_sub(previous.busy_duration));
            worker_mean_poll_time.push(metrics.worker_mean_poll_time(worker));
            worker_local_queue_depth.push(metrics.worker_local_queue_depth(worker));
        }
        let tracked_task_spawned = self
            .task_stats
            .task_kinds
            .iter()
            .zip(&previous.task_stats.task_kinds)
            .map(|((_, current), (_, previous))| current.spawned.saturating_sub(previous.spawned))
            .sum();
        let tokio_spawned_tasks = self
            .tokio_spawned_tasks
            .saturating_sub(previous.tokio_spawned_tasks);

        RuntimeWork {
            global_queue_depth: metrics.global_queue_depth(),
            worker_poll_count,
            worker_busy_duration,
            worker_mean_poll_time,
            worker_local_queue_depth,
            tokio_alive_tasks: self.task_stats.tokio_alive_tasks,
            tokio_spawned_tasks,
            tracked_task_active: self.task_stats.tracked_active_tasks,
            tracked_task_spawned,
            approx_unattributed_task_active: self
                .task_stats
                .tokio_alive_tasks
                .saturating_sub(self.task_stats.tracked_active_tasks as usize),
            approx_unattributed_task_spawned: tokio_spawned_tasks
                .saturating_sub(tracked_task_spawned),
            partition_processors: partition_processor_runtime_work(
                &self.partition_processors,
                &previous.partition_processors,
            ),
        }
    }

    fn task_kind_work(&self, previous: &Self) -> Vec<TaskKindWork> {
        self.task_stats
            .task_kinds
            .iter()
            .zip(&previous.task_stats.task_kinds)
            .filter_map(|((kind, current), (_, previous))| {
                let spawned = current.spawned.saturating_sub(previous.spawned);
                let poll_count_delta = current.poll_count.saturating_sub(previous.poll_count);
                let poll_wall_duration_delta = current
                    .poll_wall_duration
                    .saturating_sub(previous.poll_wall_duration);
                (spawned > 0
                    || current.active > 0
                    || current.peak_active > 0
                    || poll_count_delta > 0
                    || !poll_wall_duration_delta.is_zero())
                .then_some(TaskKindWork {
                    kind: kind.into(),
                    spawned,
                    active: current.active,
                    peak_active: current.peak_active,
                    poll_count_total: current.poll_count,
                    poll_count_delta,
                    poll_wall_duration_total: current.poll_wall_duration,
                    poll_wall_duration_delta,
                })
            })
            .collect()
    }

    fn busiest_partition_processors(
        &self,
        previous: &Self,
        limit: usize,
    ) -> Vec<PartitionProcessorRuntimeDelta> {
        busiest_partition_processors(
            &self.partition_processors,
            &previous.partition_processors,
            limit,
        )
    }
}

fn partition_processor_runtime_work(
    current: &HashMap<String, RuntimeWorkerSnapshot>,
    previous: &HashMap<String, RuntimeWorkerSnapshot>,
) -> PartitionProcessorRuntimeWork {
    current.iter().fold(
        PartitionProcessorRuntimeWork {
            count: current.len(),
            worker_poll_count: 0,
            worker_busy_duration: Duration::default(),
        },
        |mut work, (name, current)| {
            let previous = previous.get(name);
            work.worker_poll_count += previous.map_or(current.poll_count, |previous| {
                current.poll_count.saturating_sub(previous.poll_count)
            });
            work.worker_busy_duration += previous.map_or(current.busy_duration, |previous| {
                current.busy_duration.saturating_sub(previous.busy_duration)
            });
            work
        },
    )
}

fn busiest_partition_processors(
    current: &HashMap<String, RuntimeWorkerSnapshot>,
    previous: &HashMap<String, RuntimeWorkerSnapshot>,
    limit: usize,
) -> Vec<PartitionProcessorRuntimeDelta> {
    let mut runtimes: Vec<_> = current
        .iter()
        .map(|(name, current)| {
            let previous = previous.get(name);
            PartitionProcessorRuntimeDelta {
                name: name.clone(),
                worker_poll_count: previous.map_or(current.poll_count, |previous| {
                    current.poll_count.saturating_sub(previous.poll_count)
                }),
                worker_busy_duration: previous.map_or(current.busy_duration, |previous| {
                    current.busy_duration.saturating_sub(previous.busy_duration)
                }),
            }
        })
        .collect();
    runtimes.sort_unstable_by(|left, right| {
        right
            .worker_busy_duration
            .cmp(&left.worker_busy_duration)
            .then_with(|| left.name.cmp(&right.name))
    });
    runtimes.truncate(limit);
    runtimes
}

#[derive(Default)]
struct LoopWork {
    gossip_messages: u32,
    gossip_messages_elapsed: Duration,
    get_node_state_requests: u32,
    get_node_state_requests_elapsed: Duration,
    get_cluster_state_requests: u32,
    get_cluster_state_requests_elapsed: Duration,
    startup_cluster_state_replies: u32,
    startup_cluster_state_replies_elapsed: Duration,
    node_status_changes: u32,
    node_status_changes_elapsed: Duration,
    nodes_config_changes: u32,
    nodes_config_changes_elapsed: Duration,
    unrecognized_messages: u32,
    unrecognized_messages_elapsed: Duration,
}

impl<T: NetworkSender> FailureDetector<T> {
    pub fn new(
        opts: &GossipOptions,
        networking: T,
        router_builder: &mut MessageRouterBuilder,
        replica_set_states: PartitionReplicaSetStates,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        // Gossip uses a small dedicated pool (1 MiB) with load shedding.
        let gossip_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool("gossip", ||
                // No config-driven resizing.
                NonZeroByteCount::new(NonZeroUsize::new(1024 * 1024).unwrap()))
        });
        let gossip_svc_rx =
            router_builder.register_service_with_pool(gossip_pool, BackPressureMode::Lossy);
        let mut gossip_interval = tokio::time::interval(*opts.gossip_tick_interval);
        gossip_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            networking,
            processor_manager_handle,
            replica_set_states,
            gossip_svc_rx,
            gossip_interval,
            intervals_since_last_extras: u32::MAX,
            last_dumped: Instant::now(),
            last_runtime_snapshot: Self::capture_runtime_snapshot(),
        }
    }

    pub fn start(
        self,
        opts: impl LiveLoad<Live = GossipOptions> + 'static,
    ) -> Result<(), ShutdownError> {
        // Note that the failure detector is an unmanaged task because we want it to continue
        // running until the very end of the node's lifecycle. If this was spawn(), then task
        // center will need to wait for the task to terminate before it can shutdown.
        TaskCenter::spawn_unmanaged(TaskKind::FailureDetector, "failure-detector", async {
            if let Err(e) = self.run(opts).await {
                // We request shutdown of the node. FD can only fail in unrecoverable errors.
                //
                // The handling is manual because this is an unmanaged task.
                TaskCenter::current().shutdown_node(&e.to_string(), 1).await;
            }
        })?;
        Ok(())
    }

    pub async fn run(
        mut self,
        mut opts: impl LiveLoad<Live = GossipOptions> + 'static,
    ) -> anyhow::Result<()> {
        debug!("Failure Detector Starting");
        let (my_node_id, mut nodes_config, mut nodes_config_watch) = Metadata::with_current(|m| {
            (
                m.my_node_id(),
                m.updateable_nodes_config(),
                m.watch(MetadataKind::NodesConfiguration),
            )
        });

        let mut shutting_down = false;
        let (my_node_health, cs_updater) =
            TaskCenter::with_current(|tc| (tc.health().clone(), tc.cluster_state_updater()));
        let fd_state_created_at = Instant::now();
        let mut fd_state = FdState::new(
            my_node_id,
            nodes_config.live_load(),
            self.replica_set_states.clone(),
            cs_updater,
        );
        // We are starting up. Let others know as early as possible so they can update their
        // nodes configuration, and implicitly start the suspect timer for this node.
        let mut my_node_status_watch = my_node_health.node_status().subscribe();

        // We send the first bring-up before we enable gossip network service.
        let node_status = *my_node_status_watch.borrow_and_update();
        self.broadcast_bring_up(node_status, &mut fd_state);

        // spawn get-cluster-state to pre-seed our view of the cluster
        let mut get_cs_futs = FuturesUnordered::new();
        for (_, node) in fd_state.peers() {
            if let Ok(reply_token) = node.send_get_cluster_state(&self.networking) {
                get_cs_futs.push(reply_token);
            }
        }

        // We should only gossip after we have fully started and stop during
        // shutdown.
        if !node_status.is_alive() {
            trace!("Failure detector is waiting for the node to fully start");
            let node_status = *my_node_status_watch
                .wait_for(|status| *status != NodeStatus::StartingUp)
                .await?;
            // maybe we are shutting down.
            if !node_status.is_alive() {
                return Ok(());
            }
            // broadcast again that we have started
            self.broadcast_bring_up(node_status, &mut fd_state);
        }
        info!(
            startup_elapsed = ?fd_state_created_at.elapsed(),
            "Failure Detector Started"
        );
        // Explicit reset because the interval could have been created long time ago, and we don't
        // want to erroneously report that a stall was detected.
        self.gossip_interval.reset_immediately();
        self.last_runtime_snapshot = Self::capture_runtime_snapshot();

        // Start receiving gossip messages
        let mut network_rx = self.gossip_svc_rx.take().start();
        let mut loop_work = LoopWork::default();

        loop {
            tokio::select! {
                Ok(()) = my_node_status_watch.changed(), if !shutting_down => {
                    let processing_started_at = Instant::now();
                    loop_work.node_status_changes += 1;
                    // we should only see shutdowns.
                    let status = *my_node_status_watch.borrow_and_update();
                    debug_assert!(matches!(status, NodeStatus::ShuttingDown | NodeStatus::Unknown), "{status:?}");
                    self.broadcast_failover(&mut fd_state);
                    shutting_down = true;
                    loop_work.node_status_changes_elapsed = loop_work
                        .node_status_changes_elapsed
                        .saturating_add(processing_started_at.elapsed());
                }
                Ok(()) = nodes_config_watch.changed() => {
                    let processing_started_at = Instant::now();
                    loop_work.nodes_config_changes += 1;
                    // can fail the task if we have been preempted
                    fd_state.refresh_nodes_config(nodes_config.live_load())?;
                    loop_work.nodes_config_changes_elapsed = loop_work
                        .nodes_config_changes_elapsed
                        .saturating_add(processing_started_at.elapsed());
                }
                Some(Ok(cs_reply)) = get_cs_futs.next() => {
                    let processing_started_at = Instant::now();
                    loop_work.startup_cluster_state_replies += 1;
                    let opts = opts.live_load();
                    if cs_reply.status != restate_types::net::node::CsReplyStatus::Ok {
                        loop_work.startup_cluster_state_replies_elapsed = loop_work
                            .startup_cluster_state_replies_elapsed
                            .saturating_add(processing_started_at.elapsed());
                        continue;
                    }

                    if !fd_state.is_stable(opts) && !fd_state.am_i_alive() {
                        fd_state.update_from_cluster_state_message(opts, cs_reply);
                    }
                    // we are not interested in further replies.
                    get_cs_futs.clear();
                    loop_work.startup_cluster_state_replies_elapsed = loop_work
                        .startup_cluster_state_replies_elapsed
                        .saturating_add(processing_started_at.elapsed());
                }
                tick_instant = self.gossip_interval.tick() => {
                    let opts = opts.live_load();
                    self.tick(
                        opts,
                        tick_instant,
                        &mut fd_state,
                        nodes_config.live_load(),
                        std::mem::take(&mut loop_work),
                    )?;
                }
                Some(op) = network_rx.next() => {
                    let processing_started_at = Instant::now();
                    let opts = opts.live_load();
                    match op {
                        ServiceMessage::Unary(msg) => {
                            loop_work.gossip_messages += 1;
                            self.on_gossip_message(opts, msg, &mut fd_state);
                            loop_work.gossip_messages_elapsed = loop_work
                                .gossip_messages_elapsed
                                .saturating_add(processing_started_at.elapsed());
                        }
                        ServiceMessage::Rpc(msg) if msg.msg_type() == GetNodeState::TYPE => {
                            loop_work.get_node_state_requests += 1;
                            // V1 GetNodeState messages
                            self.on_get_node_state_rpc(msg);
                            loop_work.get_node_state_requests_elapsed = loop_work
                                .get_node_state_requests_elapsed
                                .saturating_add(processing_started_at.elapsed());
                        }
                        ServiceMessage::Rpc(msg) if msg.msg_type() == GetClusterState::TYPE => {
                            loop_work.get_cluster_state_requests += 1;
                            // V2 GetClusterState messages
                            self.on_get_cluster_state_rpc(&fd_state, opts, msg);
                            loop_work.get_cluster_state_requests_elapsed = loop_work
                                .get_cluster_state_requests_elapsed
                                .saturating_add(processing_started_at.elapsed());
                        }
                        _ => {
                            loop_work.unrecognized_messages += 1;
                            op.fail(Verdict::MessageUnrecognized);
                            loop_work.unrecognized_messages_elapsed = loop_work
                                .unrecognized_messages_elapsed
                                .saturating_add(processing_started_at.elapsed());
                        }
                    }
                }
            }
        }
    }

    /// A gossip tick, most of the time happens every gossip_tick_interval unless
    /// something else resets the interval.
    fn tick(
        &mut self,
        opts: &GossipOptions,
        tick_instant: Instant,
        state: &mut FdState,
        nodes_config: &NodesConfiguration,
        loop_work: LoopWork,
    ) -> Result<(), Error> {
        let processing_started_at = Instant::now();
        state.refresh_nodes_config(nodes_config)?;
        // Used as proxy for overload/stall detection
        let tick_lag = tick_instant.elapsed();
        if tick_lag >= Duration::from_secs(5) {
            warn!(
                "Severe lag ({:?}) was detected in failure detector internal timer, \
                    this indicates an overload or a stall.",
                tick_lag,
            );
        }
        let intervals_passed = state.gossip_tick(opts);
        let failure_significant = intervals_passed >= opts.gossip_failure_threshold.get();
        let runtime_snapshot = Self::capture_runtime_snapshot();
        let runtime_work = runtime_snapshot.work_since(&self.last_runtime_snapshot);
        if intervals_passed > 1 && !failure_significant {
            debug!(
                intervals_passed,
                ?tick_lag,
                fd_state = ?state,
                "Failure detector processed multiple gossip intervals in one tick"
            );
        }

        // If we are not stable yet, we shouldn't make state machine transitions.
        //
        // Note that it's still okay to send gossip messages even if we have not
        // moved our state machines (we are not stable yet). The state machines are
        // mainly to update our interpretation of who's alive and who's dead but it
        // doesn't impact the information we send out to peers in the gossip message.
        let detector_stable = state.is_stable(opts);
        if detector_stable {
            state.detect_peer_failures(opts);
        } else {
            // If we are not stable, we still want to update our own state.
            // `gossip_tick()` will always set our node's gossip_age to zero.
            //
            // We special case the standalone setup to avoid going into suspect on startup.
            state.update_my_node_state(opts);
        }

        let sent_counter = counter!(GOSSIP_SENT);
        let mut gossip_send_attempted = 0;
        let mut gossip_send_succeeded = 0;
        let mut gossip_send_failed = 0;
        let gossip_send_started_at = Instant::now();
        // At least one interval has passed, let's send a gossip round
        if intervals_passed > 0 {
            let mut sent = 0;
            let include_extras = self.intervals_since_last_extras
                >= opts.gossip_extras_exchange_frequency.get()
                && nodes_config.len() > 1;
            // What to do with V1 nodes? Those don't have the unary handler for
            // GossipService so messages will be lost. It's relatively low-risk until more nodes
            // are started up.
            let msg = state.make_gossip_message(opts, include_extras, nodes_config);
            for target_node in state.select_targets_for_gossip(nodes_config, &self.networking) {
                gossip_send_attempted += 1;
                match target_node.send_gossip(&self.networking, msg.clone()) {
                    Err(err) => {
                        gossip_send_failed += 1;
                        trace!(peer = %target_node.gen_node_id, "Couldn't send gossip to peer: {err}");
                    }
                    Ok(_) => {
                        gossip_send_succeeded += 1;
                        sent += 1;
                        sent_counter.increment(1);
                        if sent >= opts.gossip_num_peers.get() {
                            break;
                        }
                    }
                }
            }
            if sent == 0 && nodes_config.len() > 1 {
                trace!(
                    "Finished a full round of attempts without finding a suitable target node to gossip to!"
                );
            }

            if sent > 0 && include_extras {
                self.intervals_since_last_extras = 0;
            } else {
                self.intervals_since_last_extras =
                    self.intervals_since_last_extras.saturating_add(1);
            }
        }
        let gossip_send_elapsed = gossip_send_started_at.elapsed();

        if self.last_dumped.elapsed() > Duration::from_secs(1) {
            state.report_stats(opts);
            self.last_dumped = Instant::now();
        }

        let processing_elapsed = processing_started_at.elapsed();
        if failure_significant {
            let task_kind_work = runtime_snapshot.task_kind_work(&self.last_runtime_snapshot);
            warn!(
                intervals_passed,
                failure_threshold = opts.gossip_failure_threshold.get(),
                detector_stable,
                ?tick_lag,
                ?processing_elapsed,
                ?gossip_send_elapsed,
                gossip_messages_since_last_tick = loop_work.gossip_messages,
                gossip_messages_processing_elapsed_since_last_tick = ?loop_work.gossip_messages_elapsed,
                get_node_state_requests_since_last_tick = loop_work.get_node_state_requests,
                get_node_state_requests_processing_elapsed_since_last_tick = ?loop_work.get_node_state_requests_elapsed,
                get_cluster_state_requests_since_last_tick = loop_work.get_cluster_state_requests,
                get_cluster_state_requests_processing_elapsed_since_last_tick = ?loop_work.get_cluster_state_requests_elapsed,
                startup_cluster_state_replies_since_last_tick = loop_work.startup_cluster_state_replies,
                startup_cluster_state_replies_processing_elapsed_since_last_tick = ?loop_work.startup_cluster_state_replies_elapsed,
                node_status_changes_since_last_tick = loop_work.node_status_changes,
                node_status_changes_processing_elapsed_since_last_tick = ?loop_work.node_status_changes_elapsed,
                nodes_config_changes_since_last_tick = loop_work.nodes_config_changes,
                nodes_config_changes_processing_elapsed_since_last_tick = ?loop_work.nodes_config_changes_elapsed,
                unrecognized_messages_since_last_tick = loop_work.unrecognized_messages,
                unrecognized_messages_processing_elapsed_since_last_tick = ?loop_work.unrecognized_messages_elapsed,
                gossip_send_attempted,
                gossip_send_succeeded,
                gossip_send_failed,
                default_runtime_global_queue_depth = runtime_work.global_queue_depth,
                default_runtime_worker_poll_count = ?runtime_work.worker_poll_count,
                default_runtime_worker_busy_duration = ?runtime_work.worker_busy_duration,
                default_runtime_worker_mean_poll_time = ?runtime_work.worker_mean_poll_time,
                default_runtime_worker_local_queue_depth = ?runtime_work.worker_local_queue_depth,
                default_runtime_tokio_alive_tasks = runtime_work.tokio_alive_tasks,
                default_runtime_tokio_spawned_tasks_delta = runtime_work.tokio_spawned_tasks,
                default_runtime_tracked_task_active = runtime_work.tracked_task_active,
                default_runtime_tracked_task_spawned_delta = runtime_work.tracked_task_spawned,
                default_runtime_approx_unattributed_task_active = runtime_work.approx_unattributed_task_active,
                default_runtime_approx_unattributed_task_spawned_delta = runtime_work.approx_unattributed_task_spawned,
                default_runtime_task_kind_work = ?task_kind_work,
                partition_processor_runtime_count = runtime_work.partition_processors.count,
                partition_processor_runtime_worker_poll_count_delta = runtime_work.partition_processors.worker_poll_count,
                partition_processor_runtime_worker_busy_duration_delta = ?runtime_work.partition_processors.worker_busy_duration,
                partition_processor_runtime_busiest_by_busy_duration_delta = ?runtime_snapshot.busiest_partition_processors(&self.last_runtime_snapshot, BUSIEST_PARTITION_PROCESSOR_RUNTIMES),
                fd_state = ?state,
                "Failure detector processed a failure-significant number of gossip intervals in one tick"
            );
        }
        if processing_elapsed >= opts.gossip_tick_interval {
            warn!(
                ?processing_elapsed,
                gossip_tick_interval = ?opts.gossip_tick_interval,
                "Failure detector tick processing exceeded the gossip tick interval"
            );
        }

        self.last_runtime_snapshot = runtime_snapshot;

        Ok(())
    }

    fn capture_runtime_snapshot() -> RuntimeSnapshot {
        let partition_processors = TaskCenter::with_current(|tc| {
            tc.managed_runtime_metrics()
                .into_iter()
                .filter(|(name, _)| name.starts_with("pp-"))
                .filter_map(|(name, metrics)| {
                    (metrics.num_workers() > 0).then(|| {
                        (
                            name.to_string(),
                            RuntimeWorkerSnapshot {
                                poll_count: (0..metrics.num_workers())
                                    .map(|worker| metrics.worker_poll_count(worker))
                                    .sum(),
                                busy_duration: (0..metrics.num_workers())
                                    .map(|worker| metrics.worker_total_busy_duration(worker))
                                    .fold(Duration::default(), |total, busy| total + busy),
                            },
                        )
                    })
                })
                .collect()
        });
        RuntimeSnapshot::capture(partition_processors)
    }

    /// handle incoming gossip messages
    fn on_gossip_message(
        &mut self,
        opts: &GossipOptions,
        msg: Incoming<RawSvcUnary<GossipService>>,
        state: &mut FdState,
    ) {
        let processing_started_at = Instant::now();
        let Ok(msg) = msg.try_into_typed::<Gossip>() else {
            return;
        };
        let peer_nc_version = msg.metadata_version().get(MetadataKind::NodesConfiguration);
        let peer = msg.peer();
        let msg = msg.into_body();

        if !state.can_admit_message(opts, peer, peer_nc_version, &msg) {
            return;
        }
        trace!(%peer, "Received a gossip message {:?}", msg);
        state.update_from_gossip_message(opts, peer, peer_nc_version, msg);

        let processing_elapsed = processing_started_at.elapsed();
        if processing_elapsed >= opts.gossip_tick_interval {
            warn!(
                %peer,
                ?processing_elapsed,
                gossip_tick_interval = ?opts.gossip_tick_interval,
                "Admitted gossip message processing exceeded the gossip tick interval"
            );
        }
    }

    /// Handle V1's GetNodeState rpc request
    fn on_get_node_state_rpc(&mut self, message: Incoming<RawSvcRpc<GossipService>>) {
        let request = match message.try_into_typed::<GetNodeState>() {
            Ok(request) => request,
            Err(msg) => {
                msg.fail(Verdict::MessageUnrecognized);
                return;
            }
        };
        let handle = self.processor_manager_handle.clone();
        let uptime = TaskCenter::with_current(|t| t.age());
        tokio::spawn(async move {
            let partition_state = if let Some(handle) = handle {
                handle.get_state().await.ok()
            } else {
                None
            };

            request.into_reciprocal().send(NodeStateResponse {
                partition_processor_state: partition_state,
                uptime,
            });
        });
    }

    /// Handle V2's GetClusterState rpc request
    fn on_get_cluster_state_rpc(
        &mut self,
        state: &FdState,
        opts: &GossipOptions,
        message: Incoming<RawSvcRpc<GossipService>>,
    ) {
        use restate_types::net::node::{
            ClusterStateReply, CsNode, CsReplyStatus, NodeState, PartitionReplicaSet,
        };

        let request = match message.try_into_typed::<GetClusterState>() {
            Ok(request) => request,
            Err(msg) => {
                msg.fail(Verdict::MessageUnrecognized);
                return;
            }
        };

        if !state.is_stable(opts) || state.is_lonely(opts) || !state.am_i_alive() {
            request
                .into_reciprocal()
                .send(ClusterStateReply::not_ready());
        } else {
            let nodes = state
                .all_node_states()
                .map(|(node_id, state)| CsNode {
                    node_id,
                    state: NodeState::from(state),
                })
                .collect();
            let partitions = state
                .partitions()
                .map(|(id, membership)| PartitionReplicaSet {
                    id,
                    current_leader: membership.current_leader(),
                    observed_current_membership: membership.observed_current_membership,
                    observed_next_membership: membership.observed_next_membership,
                })
                .collect();

            request.into_reciprocal().send(ClusterStateReply {
                status: CsReplyStatus::Ok,
                nodes,
                partitions,
            });
        }
    }

    fn broadcast_bring_up(&mut self, node_status: NodeStatus, state: &mut FdState) {
        let mut flags = GossipFlags::Special;
        flags |= match node_status {
            NodeStatus::StartingUp => GossipFlags::BringUp,
            NodeStatus::Alive => GossipFlags::ReadyToServe,
            NodeStatus::ShuttingDown | NodeStatus::Unknown => return,
        };

        let message = Gossip {
            instance_ts: state.my_instance_ts,
            sent_at: MillisSinceEpoch::now(),
            flags,
            nodes: Vec::new(),
            partitions: Vec::new(),
        };

        for (_, node) in state.peers() {
            let _sent = node.send_gossip(&self.networking, message.clone());
        }
    }

    fn broadcast_failover(&mut self, state: &mut FdState) -> bool {
        state.set_failover();

        let flags = GossipFlags::Special | GossipFlags::FailingOver;
        let message = Gossip {
            instance_ts: state.my_instance_ts,
            sent_at: MillisSinceEpoch::now(),
            flags,
            nodes: Vec::new(),
            partitions: Vec::new(),
        };

        for (_, node) in state.peers() {
            let _sent = node.send_gossip(&self.networking, message.clone());
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime(poll_count: u64, busy_duration: Duration) -> RuntimeWorkerSnapshot {
        RuntimeWorkerSnapshot {
            poll_count,
            busy_duration,
        }
    }

    #[test]
    fn partition_processor_deltas_sum_and_report_busiest_runtimes() {
        let previous: HashMap<_, _> = [
            ("pp-a".to_owned(), runtime(4, Duration::from_millis(10))),
            ("pp-b".to_owned(), runtime(10, Duration::from_millis(10))),
            (
                "pp-removed".to_owned(),
                runtime(100, Duration::from_secs(1)),
            ),
        ]
        .into_iter()
        .collect();
        let current: HashMap<_, _> = [
            ("pp-a".to_owned(), runtime(10, Duration::from_millis(30))),
            ("pp-b".to_owned(), runtime(20, Duration::from_millis(15))),
            ("pp-new".to_owned(), runtime(1, Duration::from_millis(8))),
        ]
        .into_iter()
        .collect();

        let aggregate = partition_processor_runtime_work(&current, &previous);
        assert_eq!(aggregate.count, 3);
        assert_eq!(aggregate.worker_poll_count, 17);
        assert_eq!(aggregate.worker_busy_duration, Duration::from_millis(33));

        assert_eq!(
            busiest_partition_processors(&current, &previous, 2),
            vec![
                PartitionProcessorRuntimeDelta {
                    name: "pp-a".to_owned(),
                    worker_poll_count: 6,
                    worker_busy_duration: Duration::from_millis(20),
                },
                PartitionProcessorRuntimeDelta {
                    name: "pp-new".to_owned(),
                    worker_poll_count: 1,
                    worker_busy_duration: Duration::from_millis(8),
                },
            ]
        );
    }
}
