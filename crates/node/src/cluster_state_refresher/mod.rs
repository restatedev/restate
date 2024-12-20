// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use tokio::{
    sync::watch,
    time::{self, Instant, Interval, MissedTickBehavior},
};
use tracing::{debug, instrument, trace};

use restate_core::{
    cancellation_watcher,
    network::{
        Incoming, MessageRouterBuilder, MessageStream, NetworkSender, Networking, Outgoing,
        TransportConnect,
    },
    Metadata, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    cluster::cluster_state::{AliveNode, ClusterState, DeadNode, NodeState, SuspectNode},
    config::{CommonOptions, Configuration},
    net::cluster_state::{NodePing, NodePong},
    time::MillisSinceEpoch,
    GenerationalNodeId, PlainNodeId,
};

const SUSPECT_THRESHOLD_FACTOR: u32 = 2;
const DEAD_THRESHOLD_FACTOR: u32 = 4;

#[derive(Clone)]
struct SeenState {
    generational_node_id: GenerationalNodeId,
    seen_at: MillisSinceEpoch,
}

impl SeenState {
    fn new(generational_node_id: GenerationalNodeId) -> Self {
        SeenState {
            generational_node_id,
            seen_at: MillisSinceEpoch::now(),
        }
    }
}

#[derive(Clone, Default)]
struct NodeTracker {
    seen: Option<SeenState>,
    last_attempt_at: Option<MillisSinceEpoch>,
}

pub struct ClusterStateRefresher<T> {
    metadata: Metadata,
    ping_requests: MessageStream<NodePing>,
    ping_responses: MessageStream<NodePong>,
    networking: Networking<T>,
    nodes: BTreeMap<PlainNodeId, NodeTracker>,
    heartbeat_interval: Duration,
    cluster_state_watch_tx: watch::Sender<Arc<ClusterState>>,
    start_time: MillisSinceEpoch,
}

impl<T> ClusterStateRefresher<T>
where
    T: TransportConnect,
{
    pub(crate) fn new(
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let config = Configuration::pinned();
        ClusterStateRefresher {
            metadata,
            ping_requests: router_builder.subscribe_to_stream(128),
            ping_responses: router_builder.subscribe_to_stream(128),
            networking,
            nodes: BTreeMap::default(),
            heartbeat_interval: config.common.heartbeat_interval.into(),
            cluster_state_watch_tx: watch::Sender::new(Arc::new(ClusterState::empty())),
            start_time: MillisSinceEpoch::UNIX_EPOCH,
        }
    }

    pub fn cluster_state_watch(&self) -> watch::Receiver<Arc<ClusterState>> {
        self.cluster_state_watch_tx.subscribe()
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut config_watcher = Configuration::watcher();

        let mut cancelled = std::pin::pin!(cancellation_watcher());
        let mut heartbeat_interval =
            Self::create_heartbeat_interval(&Configuration::pinned().common);

        self.start_time = MillisSinceEpoch::now();
        loop {
            tokio::select! {
                _ = &mut cancelled => {
                    break;
                }
                _ = heartbeat_interval.tick() => {
                    self.on_heartbeat().await?;
                }
                _ = config_watcher.changed() => {
                    debug!("Updating heartbeat settings.");
                    let config = Configuration::pinned();
                    self.heartbeat_interval = config.common.heartbeat_interval.into();
                    heartbeat_interval =
                        Self::create_heartbeat_interval(&config.common);
                }
                Some(request) = self.ping_requests.next() => {
                    self.on_ping(request).await?;
                }
                Some(responses) = self.ping_responses.next() => {
                    self.on_pong(responses).await?;
                }

            }
        }

        Ok(())
    }

    fn create_heartbeat_interval(options: &CommonOptions) -> Interval {
        let mut heartbeat_interval = time::interval_at(
            Instant::now() + options.heartbeat_interval.into(),
            options.heartbeat_interval.into(),
        );
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        heartbeat_interval
    }

    #[instrument(level="debug", parent=None, skip_all)]
    async fn on_heartbeat(&mut self) -> Result<(), ShutdownError> {
        // notify all watcher about the current known state
        // of the cluster.
        self.update_cluster_state().await?;

        let nodes = self.metadata.nodes_config_ref();
        for (node_id, _) in nodes.iter() {
            if node_id == self.metadata.my_node_id().as_plain() {
                continue;
            }

            let local_node_state = self.nodes.entry(node_id).or_default();
            let last_seen_since = local_node_state
                .seen
                .as_ref()
                .map(|seen| seen.seen_at.elapsed());

            match last_seen_since {
                Some(elapsed) if elapsed < self.heartbeat_interval => {
                    continue;
                }
                _ => {
                    // never been seen before, or last seen is greater than heartbeat interval
                    local_node_state.last_attempt_at = Some(MillisSinceEpoch::now());
                    let networking = self.networking.clone();
                    let _ = TaskCenter::spawn_child(
                        TaskKind::Disposable,
                        "send-ping-request",
                        async move {
                            // ignore send errors
                            let _ = networking.send(Outgoing::new(node_id, NodePing {})).await;
                            Ok(())
                        },
                    );
                }
            }
        }

        Ok(())
    }

    fn node_state_for(&self, node_tracker: &NodeTracker) -> Option<NodeState> {
        let suspect_threshold = self.heartbeat_interval * SUSPECT_THRESHOLD_FACTOR;
        let dead_threshold = self.heartbeat_interval * DEAD_THRESHOLD_FACTOR;

        match node_tracker.seen {
            None => {
                // never seen this node before
                if node_tracker.last_attempt_at.is_some()
                    && self.start_time.elapsed() > dead_threshold
                {
                    Some(NodeState::Dead(DeadNode {
                        last_seen_alive: None,
                    }))
                } else {
                    // we are not sure
                    None
                }
            }
            Some(ref seen) => {
                let elapsed = seen.seen_at.elapsed();
                if elapsed < suspect_threshold {
                    Some(NodeState::Alive(AliveNode {
                        generational_node_id: seen.generational_node_id,
                        last_heartbeat_at: seen.seen_at,
                    }))
                } else if elapsed >= suspect_threshold && elapsed < dead_threshold {
                    Some(NodeState::Suspect(SuspectNode {
                        generational_node_id: seen.generational_node_id,
                        last_attempt: seen.seen_at,
                    }))
                } else {
                    Some(NodeState::Dead(DeadNode {
                        last_seen_alive: Some(seen.seen_at),
                    }))
                }
            }
        }
    }

    async fn update_cluster_state(&self) -> Result<(), ShutdownError> {
        let nodes = self.metadata.nodes_config_ref();

        let mut cluster_state = ClusterState {
            last_refreshed: Some(Instant::now().into_std()),
            logs_metadata_version: self.metadata.logs_version(),
            nodes_config_version: self.metadata.nodes_config_version(),
            partition_table_version: self.metadata.partition_table_version(),
            nodes: nodes
                .iter()
                .filter_map(|(node_id, _)| {
                    let node_data = self.nodes.get(&node_id);
                    let node_state = match node_data {
                        None => None,
                        Some(data) => self.node_state_for(data),
                    };

                    node_state.map(|state| (node_id, state))
                })
                .collect(),
        };

        cluster_state.nodes.insert(
            self.metadata.my_node_id().as_plain(),
            NodeState::Alive(AliveNode {
                generational_node_id: self.metadata.my_node_id(),
                last_heartbeat_at: MillisSinceEpoch::now(),
            }),
        );

        trace!("cluster state: {:?}", cluster_state);

        //todo: should we notify listener only if node liveness changes?
        self.cluster_state_watch_tx
            .send_replace(Arc::new(cluster_state));

        Ok(())
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_pong(&mut self, mut msg: Incoming<NodePong>) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Handling pong response");

        let tracker = self.nodes.entry(msg.peer().as_plain()).or_default();
        tracker.seen = Some(SeenState::new(msg.peer()));

        Ok(())
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_ping(&mut self, mut msg: Incoming<NodePing>) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Handling ping request");
        let peer = msg.peer();

        let tracker = self.nodes.entry(peer.as_plain()).or_default();
        tracker.seen = Some(SeenState::new(peer));

        let _ = msg.to_rpc_response(NodePong {}).try_send();

        Ok(())
    }
}
