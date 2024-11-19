// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    cmp::Ordering,
    collections::{btree_map::Entry, BTreeMap},
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio::{
    sync::watch,
    time::{self, MissedTickBehavior},
};
use tracing::{debug, info, instrument, trace};

use restate_core::{
    cancellation_watcher,
    network::{
        Incoming, MessageRouterBuilder, MessageStream, NetworkSender, Networking, Outgoing,
        TransportConnect,
    },
    task_center,
    worker_api::ProcessorsManagerHandle,
    Metadata, MetadataKind, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    cluster::cluster_state::{AliveNode, ClusterState, ClusterStateWatch, DeadNode, NodeState},
    config::Configuration,
    net::gossip::{GossipPayload, GossipRequest, GossipResponse, NodesSnapshot},
    time::MillisSinceEpoch,
    PlainNodeId, Version,
};

const ALLOWED_STALLING_FACTOR: u32 = 3;

pub struct Gossip<T> {
    gossip_requests: MessageStream<GossipRequest>,
    gossip_responses: MessageStream<GossipResponse>,
    networking: Networking<T>,
    nodes: BTreeMap<PlainNodeId, NodeState>,
    metadata: Metadata,
    heartbeat_interval: Duration,
    task_center: TaskCenter,
    logs_version: Version,
    partition_table_version: Version,
    nodes_config_version: Version,
    cluster_state_watch_tx: watch::Sender<ClusterState>,

    // handlers
    processor_manager_handle: Option<ProcessorsManagerHandle>,
}

impl<T> Gossip<T>
where
    T: TransportConnect,
{
    pub(crate) fn new(
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let config = Configuration::pinned();
        Gossip {
            gossip_requests: router_builder.subscribe_to_stream(128),
            gossip_responses: router_builder.subscribe_to_stream(128),
            networking,
            nodes: BTreeMap::default(),
            metadata,
            heartbeat_interval: config.common.heartbeat_interval.into(),
            task_center: task_center(),
            logs_version: Version::MIN,
            partition_table_version: Version::MIN,
            nodes_config_version: Version::MIN,
            cluster_state_watch_tx: watch::Sender::new(ClusterState::empty()),
            processor_manager_handle: None,
        }
    }

    pub fn cluster_state_watch(&self) -> ClusterStateWatch {
        self.cluster_state_watch_tx.subscribe()
    }

    pub fn with_processor_manager_handle(&mut self, handle: ProcessorsManagerHandle) {
        self.processor_manager_handle = Some(handle);
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut cancelled = std::pin::pin!(cancellation_watcher());
        let mut heartbeat_interval = time::interval(self.heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut logs_watcher = self.metadata.watch(MetadataKind::Logs);
        let mut partition_processor_watcher = self.metadata.watch(MetadataKind::PartitionTable);
        let mut nodes_config_watcher = self.metadata.watch(MetadataKind::NodesConfiguration);
        // TODO: handle responses

        loop {
            tokio::select! {
                _ = &mut cancelled => {
                    break;
                }
                _ = heartbeat_interval.tick() => {
                    self.on_heartbeat(false).await?;
                }
                _ = logs_watcher.changed() => {
                    self.logs_version = *logs_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                _ = partition_processor_watcher.changed() => {
                    self.partition_table_version= *partition_processor_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                _ = nodes_config_watcher.changed() => {
                    self.nodes_config_version = *nodes_config_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                Some(request) = self.gossip_requests.next() => {
                    self.on_gossip_request(request).await?;
                }
                Some(responses) = self.gossip_responses.next() => {
                    self.on_gossip_response(responses).await?;
                }

            }
        }

        Ok(())
    }

    /// checks if metadata is at latest "seen" versions of
    /// logs, partition table, and nodes_config
    fn is_latest_version(&self) -> bool {
        self.metadata.logs_version() >= self.logs_version
            && self.metadata.partition_table_version() >= self.partition_table_version
            && self.metadata.nodes_config_version() >= self.nodes_config_version
    }

    #[instrument(level="debug", parent=None, skip(self), fields(
        logs_version=%self.logs_version,
        partition_table_version=%self.partition_table_version,
        nodes_config_version=%self.nodes_config_version,
    ))]
    async fn on_heartbeat(&mut self, all_nodes: bool) -> Result<(), ShutdownError> {
        let nodes = self.metadata.nodes_config_snapshot();

        let mut broadcast_set = vec![];
        for (node_id, _) in nodes.iter() {
            if node_id == self.metadata.my_node_id().as_plain() {
                continue;
            }

            let local_node_state = self.nodes.get(&node_id);

            let last_seen_since = local_node_state
                .and_then(|state| state.last_seen())
                .map(|timestamp| timestamp.elapsed());

            match last_seen_since {
                None => {
                    // never seen!
                    // we try to ping it, but we don't set
                    // its status as "dead".
                }
                Some(since) => {
                    // todo: make the multiplication factor configurable
                    if !all_nodes && since < self.heartbeat_interval {
                        // we have recent state of the node and we don't have
                        // to ping all nodes
                        continue;
                    }

                    if since > self.heartbeat_interval * ALLOWED_STALLING_FACTOR {
                        // mark node as dead if it was believed to be alive
                        if let Some(NodeState::Alive(node)) = local_node_state {
                            debug!(%node_id, ?since, "Node is dead");
                            self.nodes.insert(
                                node_id,
                                NodeState::Dead(DeadNode {
                                    last_seen_alive: Some(node.last_heartbeat_at),
                                }),
                            );
                        }
                    }
                }
            }

            broadcast_set.push(node_id);
        }

        // include my own state
        self.nodes.insert(
            self.metadata.my_node_id().into(),
            NodeState::Alive(AliveNode {
                last_heartbeat_at: MillisSinceEpoch::now(),
                generational_node_id: self.metadata.my_node_id(),
                partitions: if let Some(ref handle) = self.processor_manager_handle {
                    handle.get_state().await?
                } else {
                    BTreeMap::default()
                },
            }),
        );

        self.publish_cluster_state();

        trace!(
            "Gossip request to {} nodes: {:?}",
            broadcast_set.len(),
            broadcast_set
        );

        for node in broadcast_set {
            let msg = Outgoing::new(
                node,
                GossipRequest {
                    payload: GossipPayload::Snapshot {
                        nodes: self.nodes.clone(),
                    },
                },
            );

            let networking = self.networking.clone();
            let _ = self.task_center.spawn_child(
                TaskKind::Disposable,
                "send-gossip-request",
                None,
                async move {
                    // ignore send errors
                    let _ = networking.send(msg).await;
                    Ok(())
                },
            );
        }

        Ok(())
    }

    fn publish_cluster_state(&self) {
        let cluster_state = ClusterState {
            last_refreshed: Some(Instant::now()),
            logs_metadata_version: self.logs_version,
            nodes_config_version: self.nodes_config_version,
            partition_table_version: self.partition_table_version,
            nodes: self.nodes.clone(),
        };

        self.cluster_state_watch_tx.send_if_modified(|current| {
            let changed = current != &cluster_state;
            *current = cluster_state;
            // waiter on the watch will not get triggered if there as no change
            // in the actual structure of the cluster state (changes in node state)
            // or partition table.
            // But we sill override the value to have latest time stamps

            if changed {
                info!(
                    "Cluster state changed {}/{}",
                    current.alive_nodes().count(),
                    current.nodes.len()
                );
            }
            changed
        });
    }

    /// update our local view with received state
    async fn merge_local_view<P: Into<PlainNodeId>>(
        &mut self,
        sender: P,
        payload: GossipPayload,
    ) -> Result<NodesSnapshot, ShutdownError> {
        let peer = sender.into();

        let mut diff = NodesSnapshot::default();
        match payload {
            GossipPayload::Snapshot {
                nodes: mut peer_view,
            } => {
                // full snapshot of the peer view of cluster state.
                let nodes_config = self.metadata.nodes_config_snapshot();

                for (node_id, _) in nodes_config.iter() {
                    let Some(mut peer_node_view) = peer_view.remove(&node_id) else {
                        // peer does not know about this node, do I ?
                        if let Some(local_view) = self.nodes.get(&node_id) {
                            diff.insert(node_id, local_view.clone());
                        }

                        continue;
                    };

                    if node_id == self.metadata.my_node_id().as_plain() {
                        continue;
                    }

                    if peer == node_id {
                        assert!(
                            matches!(peer_node_view, NodeState::Alive(_)),
                            "sender must be alive"
                        );

                        if let NodeState::Alive(ref mut alive) = peer_node_view {
                            alive.last_heartbeat_at = MillisSinceEpoch::now();
                        }
                    }

                    //merge node state.
                    let entry = self.nodes.entry(node_id);
                    match entry {
                        Entry::Vacant(vacant) => {
                            vacant.insert(peer_node_view);
                            continue;
                        }
                        Entry::Occupied(mut occupied) => {
                            let local_node_view: &NodeState = occupied.get();
                            match local_node_view.last_seen().cmp(&peer_node_view.last_seen()) {
                                Ordering::Equal => {}
                                Ordering::Less => {
                                    occupied.insert(peer_node_view);
                                }
                                Ordering::Greater => {
                                    // our view of this node is
                                    // newer than the sender.
                                    diff.insert(node_id, local_node_view.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        self.publish_cluster_state();

        trace!(
            "Cluster state updated. Learned about {}/{} nodes",
            self.nodes.len() - diff.len(),
            self.nodes.len()
        );

        Ok(diff)
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_gossip_response(
        &mut self,
        mut msg: Incoming<GossipResponse>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Gossip response");

        self.merge_local_view(msg.peer(), msg.into_body().payload)
            .await?;

        Ok(())
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_gossip_request(
        &mut self,
        mut msg: Incoming<GossipRequest>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Gossip request");
        let peer = msg.peer();

        let reciprocal = msg.create_reciprocal();

        let body = msg.into_body();
        let mut diff = self.merge_local_view(peer, body.payload).await?;

        // include my own state
        diff.insert(
            self.metadata.my_node_id().into(),
            NodeState::Alive(AliveNode {
                last_heartbeat_at: MillisSinceEpoch::now(),
                generational_node_id: self.metadata.my_node_id(),
                partitions: if let Some(ref handle) = self.processor_manager_handle {
                    handle.get_state().await?
                } else {
                    BTreeMap::default()
                },
            }),
        );

        let outgoing = reciprocal.prepare(GossipResponse {
            payload: GossipPayload::Snapshot { nodes: diff },
        });

        let _ = outgoing.try_send();
        Ok(())
    }
}
