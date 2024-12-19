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
    collections::{btree_map::Entry, BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

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
    worker_api::ProcessorsManagerHandle,
    Metadata, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    cluster::cluster_state::{AliveNode, ClusterState, DeadNode, NodeState, SuspectNode},
    config::{CommonOptions, Configuration},
    net::cluster_state::{
        ClusterStateRequest, ClusterStateResponse, GossipPayload, NodeData, NodeRecord,
    },
    time::MillisSinceEpoch,
    PlainNodeId,
};

const SUSPECT_THRESHOLD_FACTOR: u32 = 2;
const DEAD_THRESHOLD_FACTOR: u32 = 4;

type Peers = HashMap<PlainNodeId, NodeRecord>;

pub struct ClusterStateRefresher<T> {
    metadata: Metadata,
    gossip_requests: MessageStream<ClusterStateRequest>,
    gossip_responses: MessageStream<ClusterStateResponse>,
    networking: Networking<T>,
    nodes: BTreeMap<PlainNodeId, NodeData>,
    heartbeat_interval: Duration,
    cluster_state_watch_tx: watch::Sender<Arc<ClusterState>>,

    broadcast_set: HashSet<PlainNodeId>,
    // handlers
    processor_manager_handle: Option<ProcessorsManagerHandle>,
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
            gossip_requests: router_builder.subscribe_to_stream(128),
            gossip_responses: router_builder.subscribe_to_stream(128),
            networking,
            nodes: BTreeMap::default(),
            heartbeat_interval: config.common.heartbeat_interval.into(),
            cluster_state_watch_tx: watch::Sender::new(Arc::new(ClusterState::empty())),
            broadcast_set: HashSet::default(),
            processor_manager_handle: None,
        }
    }

    pub fn cluster_state_watch(&self) -> watch::Receiver<Arc<ClusterState>> {
        self.cluster_state_watch_tx.subscribe()
    }

    pub fn with_processor_manager_handle(&mut self, handle: ProcessorsManagerHandle) {
        self.processor_manager_handle = Some(handle);
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut config_watcher = Configuration::watcher();

        let mut cancelled = std::pin::pin!(cancellation_watcher());
        let mut heartbeat_interval =
            Self::create_heartbeat_interval(&Configuration::pinned().common);

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

            let local_node_state = self.nodes.get(&node_id);
            let last_seen_since = local_node_state.map(|s| s.last_update_time.elapsed());
            match last_seen_since {
                None => {
                    // never seen!
                    // we try to ping it
                }
                Some(since) => {
                    if since < self.heartbeat_interval {
                        // we have recent state of the node and we don't have
                        // to ping all nodes
                        continue;
                    }
                }
            }

            self.broadcast_set.insert(node_id);
        }

        trace!(
            "Gossip request to {} nodes: {:?}",
            self.broadcast_set.len(),
            self.broadcast_set
        );

        let request = ClusterStateRequest {
            payload: GossipPayload {
                record: NodeRecord::Data(self.my_state().await?),
                cluster: self
                    .nodes
                    .iter()
                    .map(|(id, state)| (*id, state.as_node_hash().into()))
                    .collect(),
            },
        };

        for node in self.broadcast_set.drain() {
            let msg = Outgoing::new(node, request.clone());

            let networking = self.networking.clone();
            let _ =
                TaskCenter::spawn_child(TaskKind::Disposable, "send-gossip-request", async move {
                    // ignore send errors
                    let _ = networking.send(msg).await;
                    Ok(())
                });
        }

        Ok(())
    }

    async fn my_state(&self) -> Result<NodeData, ShutdownError> {
        Ok(NodeData {
            last_update_time: MillisSinceEpoch::now(),
            generational_node_id: self.metadata.my_node_id(),
            partitions: if let Some(ref handle) = self.processor_manager_handle {
                handle.get_state().await?
            } else {
                BTreeMap::default()
            },
        })
    }

    async fn update_cluster_state(&self) -> Result<(), ShutdownError> {
        let nodes = self.metadata.nodes_config_ref();

        let suspect_duration = self.heartbeat_interval * SUSPECT_THRESHOLD_FACTOR;
        let dead_duration = self.heartbeat_interval * DEAD_THRESHOLD_FACTOR;

        let mut cluster_state = ClusterState {
            last_refreshed: Some(Instant::now().into_std()),
            logs_metadata_version: self.metadata.logs_version(),
            nodes_config_version: self.metadata.nodes_config_version(),
            partition_table_version: self.metadata.partition_table_version(),
            nodes: nodes
                .iter()
                .map(|(node_id, _)| {
                    let node_data = self.nodes.get(&node_id);
                    let node_state = match node_data {
                        None => NodeState::Dead(DeadNode {
                            last_seen_alive: None,
                        }),
                        Some(data) => {
                            let elapsed = data.last_update_time.elapsed();
                            if elapsed < suspect_duration {
                                NodeState::Alive(AliveNode {
                                    generational_node_id: data.generational_node_id,
                                    last_heartbeat_at: data.last_update_time,
                                    partitions: data.partitions.clone(),
                                })
                            } else if elapsed >= suspect_duration && elapsed < dead_duration {
                                NodeState::Suspect(SuspectNode {
                                    generational_node_id: data.generational_node_id,
                                    // todo(azmy): this is wrong because timestamp
                                    // is actually last seen timestamp so it
                                    // can't be used as last_attempt.
                                    last_attempt: data.last_update_time,
                                })
                            } else {
                                NodeState::Dead(DeadNode {
                                    last_seen_alive: Some(data.last_update_time),
                                })
                            }
                        }
                    };

                    (node_id, node_state)
                })
                .collect(),
        };

        // todo(azmy): change how we fetch state from message passing
        // to memory sharing so we it doesn't have to be async.
        cluster_state.nodes.insert(
            self.metadata.my_node_id().into(),
            NodeState::Alive(self.my_state().await?.into()),
        );

        self.cluster_state_watch_tx.send_if_modified(|state| {
            *state = Arc::new(cluster_state);

            trace!(
                "Gossip alive({:?}) dead({:?})",
                state
                    .alive_nodes()
                    .map(|n| n.generational_node_id.to_string())
                    .collect::<Vec<String>>(),
                state
                    .dead_nodes()
                    .map(|n| n.to_string())
                    .collect::<Vec<String>>()
            );

            true
        });

        Ok(())
    }

    /// update our local view with received state
    async fn merge_views<P: Into<PlainNodeId>>(
        &mut self,
        sender: P,
        payload: GossipPayload,
    ) -> Result<Peers, ShutdownError> {
        let peer = sender.into();

        let GossipPayload {
            record,
            mut cluster,
        } = payload;

        let mut diff = Peers::default();
        match record {
            NodeRecord::Data(mut data) => {
                data.last_update_time = MillisSinceEpoch::now();
                self.nodes.insert(peer, data);
            }
            NodeRecord::Hash(hash) => {
                if let Some(data) = self.nodes.get_mut(&peer) {
                    if hash.hash_value == data.hash_value() {
                        data.last_update_time = MillisSinceEpoch::now()
                    }
                }
            }
        }

        // full snapshot of the peer view of cluster state.
        let nodes_config = self.metadata.nodes_config_ref();

        for (node_id, _) in nodes_config.iter() {
            if node_id == self.metadata.my_node_id().as_plain() || node_id == peer {
                continue;
            }

            let local_node_view = self.nodes.entry(node_id);

            let Some(peer_node_view) = cluster.remove(&node_id) else {
                // peer does not know about this node, do we know about it?
                if let Entry::Occupied(local_view) = local_node_view {
                    diff.insert(node_id, local_view.get().clone().into());
                }

                continue;
            };

            match (local_node_view, peer_node_view) {
                (Entry::Vacant(entry), NodeRecord::Data(data)) => {
                    entry.insert(data);
                }
                (Entry::Vacant(_), NodeRecord::Hash(_)) => {
                    self.broadcast_set.insert(node_id);
                }
                (Entry::Occupied(mut local), NodeRecord::Data(remote)) => {
                    let local = local.get_mut();
                    match (
                        remote.last_update_time > local.last_update_time,
                        remote.hash_value() == local.hash_value(),
                    ) {
                        (true, _) => {
                            *local = remote;
                        }
                        (false, true) => {
                            diff.insert(node_id, local.as_node_hash().into());
                        }
                        (false, false) => {
                            diff.insert(node_id, NodeRecord::Data(local.clone()));
                        }
                    }
                }
                (Entry::Occupied(mut local), NodeRecord::Hash(remote)) => {
                    let local = local.get_mut();

                    match (
                        remote.last_update_time > local.last_update_time,
                        remote.hash_value == local.hash_value(),
                    ) {
                        (true, true) => {
                            // only update the local timestamp
                            local.last_update_time = remote.last_update_time;
                        }
                        (true, false) => {
                            // we need to update our view.
                            self.broadcast_set.insert(node_id);
                        }
                        (false, true) => {
                            // local is more recent but same data
                            diff.insert(node_id, NodeRecord::Hash(local.as_node_hash()));
                        }
                        (false, false) => {
                            // we have a more recent view
                            diff.insert(node_id, NodeRecord::Data(local.clone()));
                        }
                    }
                }
            }
        }

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
        mut msg: Incoming<ClusterStateResponse>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Handling gossip response");

        self.merge_views(msg.peer(), msg.into_body().payload)
            .await?;

        Ok(())
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_gossip_request(
        &mut self,
        mut msg: Incoming<ClusterStateRequest>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Handling gossip request");
        let peer = msg.peer();

        let reciprocal = msg.create_reciprocal();

        let my_data = self.my_state().await?;
        let hashed = my_data.as_node_hash();

        let body = msg.into_body();
        let peer_view = body
            .payload
            .cluster
            .get(&self.metadata.my_node_id().as_plain());

        let record = match peer_view {
            None => NodeRecord::Data(my_data),
            Some(record) if record.hash_value() != hashed.hash_value => NodeRecord::Data(my_data),
            Some(_) => NodeRecord::Hash(hashed),
        };

        let diff = self.merge_views(peer, body.payload).await?;

        let msg = ClusterStateResponse {
            payload: GossipPayload {
                record,
                cluster: diff,
            },
        };

        let outgoing = reciprocal.prepare(msg);
        let _ = outgoing.try_send();
        Ok(())
    }
}
