// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use std::time::Duration;

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
    task_center::TaskCenterMonitoring,
    worker_api::ProcessorsManagerHandle,
};
use restate_types::health::NodeStatus;
use restate_types::live::LiveLoad;
use restate_types::net::node::Gossip;
use restate_types::net::node::GossipFlags;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::time::MillisSinceEpoch;
use restate_types::{
    config::GossipOptions,
    net::node::{GetNodeState, GossipService, NodeStateResponse},
};

use self::fd_state::Error;
use self::fd_state::FdState;

pub struct FailureDetector<T> {
    networking: T,
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    gossip_svc_rx: ServiceReceiver<GossipService>,
    gossip_interval: tokio::time::Interval,
    last_dumped: Instant,
}

impl<T: NetworkSender> FailureDetector<T> {
    pub fn new(
        opts: &GossipOptions,
        networking: T,
        router_builder: &mut MessageRouterBuilder,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        let gossip_svc_rx = router_builder.register_service(128, BackPressureMode::Lossy);
        let mut gossip_interval = tokio::time::interval(*opts.gossip_tick_interval);
        gossip_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            networking,
            processor_manager_handle,
            gossip_svc_rx,
            gossip_interval,
            last_dumped: Instant::now(),
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
        let mut fd_state = FdState::new(my_node_id, nodes_config.live_load(), cs_updater);
        // We are starting up. let others know as early as possible so they can update their
        // nodes configuration, and implicitly start the suspect timer for this node.
        //
        // This watch is primed, so the first call to `changed()` will return _a_ value.
        // this will be the trigger for the first broadcast message to send out.
        let mut my_node_status_watch = my_node_health.node_status().subscribe();

        // We send the first bring-up before we enable gossip network service.
        let node_status = *my_node_status_watch.borrow_and_update();
        self.broadcast_bring_up(node_status, &mut fd_state);

        // We should only gossip after we have fully started and stop during
        // shutdown.
        if !node_status.is_alive() {
            trace!("Failure detector is waiting for the node to fully start");
            let node_status = *my_node_status_watch
                .wait_for(|status| *status != NodeStatus::StartingUp)
                .await?;
            // maybe we are shutting down.
            if !node_status.is_alive() {
                trace!("Failure detector is shutting down before a fully start");
                return Ok(());
            }
            // broadcast again that we have started
            self.broadcast_bring_up(node_status, &mut fd_state);
        }
        info!("Failure Detector Started");
        // Explicit reset because the interval could have been created long time ago, and we don't
        // want to erreneously report that a stall was detected.
        self.gossip_interval.reset_immediately();

        // Start receiving gossip messages
        let mut network_rx = self.gossip_svc_rx.take().start();

        loop {
            tokio::select! {
                Ok(()) = my_node_status_watch.changed(), if !shutting_down => {
                    // we should only see shutdowns.
                    let status = *my_node_status_watch.borrow_and_update();
                    debug_assert!(matches!(status, NodeStatus::ShuttingDown | NodeStatus::Unknown), "{status:?}");
                    self.broadcast_failover(&mut fd_state);
                    shutting_down = true;
                }
                Ok(()) = nodes_config_watch.changed() => {
                    // can fail the task if we have been preempted
                    fd_state.refresh_nodes_config(nodes_config.live_load())?;
                }
                tick_instant = self.gossip_interval.tick() => {
                    let opts = opts.live_load();
                    self.tick(opts, tick_instant, &mut fd_state, nodes_config.live_load())?;
                }
                Some(op) = network_rx.next() => {
                    let opts = opts.live_load();
                    match op {
                        ServiceMessage::Unary(msg) => {
                            self.on_gossip_message(opts, msg, &mut fd_state);
                        }
                        ServiceMessage::Rpc(msg) => {
                            // V1 GetNodeState messages
                            self.on_rpc(msg);
                        }
                        _ => {
                            op.fail(Verdict::MessageUnrecognized);
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
    ) -> Result<(), Error> {
        state.refresh_nodes_config(nodes_config)?;
        // can be used as proxy for overload
        let tick_lag = tick_instant.elapsed();
        if tick_lag >= Duration::from_secs(5) {
            warn!(
                "Severe lag ({:?}) was detected in failure detector internal timer, \
                    this indicates an overload or a stall.",
                tick_lag,
            );
        }
        let interval_passed = state.gossip_tick(opts);

        // If we are not stable yet, we shouldn't make state machine transitions.
        if state.is_stable(opts) {
            // Note that it's still okay to send gossip messages even if we have not
            // moved our state machines (we are not stable yet). The state machines are
            // mainly to update our interpretation of who's alive and who's dead but it
            // doesn't impact the information we send out to peers in the gossip message.
            state.detect_peer_failures(opts);
        } else {
            // If we are not stable, we still want to update our own state.
            // `gossip_tick()` will always set our node's gossip_age to zero.
            //
            // We special case the standalone setup to avoid going into suspect on startup.
            state.update_my_node_state(opts);
        }

        // At least one interval has passed, let's send a gossip round
        if interval_passed {
            let mut sent = 0;
            // todo 1: include extras every N intervals.
            //
            // What to do with V1 nodes? Those don't have the unary handler for
            // GossipService so messages will be lost. It's relatively low-risk until more nodes
            // are started up.
            let targets = state.select_targets_for_gossip(nodes_config);
            if !targets.is_empty() {
                trace!(
                    "potential targets: {:?}",
                    targets.iter().map(|n| n.gen_node_id).collect::<Vec<_>>()
                );
                let msg = state.make_gossip_message(opts, false, nodes_config);
                for target_node in state.select_targets_for_gossip(nodes_config) {
                    match target_node.send_gossip(&self.networking, msg.clone()) {
                        Err(err) => {
                            trace!(peer = %target_node.gen_node_id, "Couldn't send gossip to peer: {err}");
                        }
                        Ok(_) => {
                            sent += 1;
                            if sent >= opts.gossip_num_peers.get() {
                                break;
                            }
                        }
                    }
                }
            }
            if sent == 0 && nodes_config.len() > 1 {
                trace!(
                    "Finished a full round of attempts without finding a suitable target node to gossip to!"
                );
            }
        }

        // todo: consider removing this or switching to trace once development of FD is
        // concluded
        if self.last_dumped.elapsed() > Duration::from_secs(30) {
            debug!("{}", state.dump_as_string(opts));
            self.last_dumped = Instant::now();
        }

        Ok(())
    }

    /// handle incoming gossip messages
    fn on_gossip_message(
        &mut self,
        opts: &GossipOptions,
        msg: Incoming<RawSvcUnary<GossipService>>,
        state: &mut FdState,
    ) {
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
        state.update_from_gossip_message(opts, peer, peer_nc_version, &msg);
    }

    /// Handle V1's GetNodeState rpc request
    fn on_rpc(&mut self, message: Incoming<RawSvcRpc<GossipService>>) {
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
            extras: Vec::new(),
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
            extras: Vec::new(),
        };

        for (_, node) in state.peers() {
            let _sent = node.send_gossip(&self.networking, message.clone());
        }

        true
    }
}
