// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::watch;
use tracing::trace;

use restate_core::network::{NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{
    Metadata, ShutdownError, TaskCenter, TaskCenterFutureExt, TaskHandle, TaskKind,
};
use restate_types::Version;
use restate_types::cluster::cluster_state::{AliveNode, DeadNode, LegacyClusterState, NodeState};
use restate_types::config::Configuration;
use restate_types::net::connect_opts::CommonClientConnectionOptions;
use restate_types::net::node::GetNodeState;
use restate_types::time::MillisSinceEpoch;

pub struct ClusterStateRefresher<T> {
    network_sender: Networking<T>,
    in_flight_refresh: Option<TaskHandle<anyhow::Result<()>>>,
    cluster_state_update_rx: watch::Receiver<Arc<LegacyClusterState>>,
    cluster_state_update_tx: Arc<watch::Sender<Arc<LegacyClusterState>>>,
}

impl<T: TransportConnect> ClusterStateRefresher<T> {
    pub fn new(network_sender: Networking<T>) -> Self {
        let initial_state = LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: BTreeMap::new(),
        };
        let (cluster_state_update_tx, cluster_state_update_rx) =
            watch::channel(Arc::from(initial_state));

        Self {
            network_sender,
            in_flight_refresh: None,
            cluster_state_update_rx,
            cluster_state_update_tx: Arc::new(cluster_state_update_tx),
        }
    }

    pub fn get_cluster_state(&self) -> Arc<LegacyClusterState> {
        Arc::clone(&self.cluster_state_update_rx.borrow())
    }

    pub fn cluster_state_watcher(&self) -> ClusterStateWatcher {
        ClusterStateWatcher {
            cluster_state_watcher: self.cluster_state_update_rx.clone(),
        }
    }

    pub async fn next_cluster_state_update(&mut self) -> Arc<LegacyClusterState> {
        self.cluster_state_update_rx
            .changed()
            .await
            .expect("sender should always exist");
        Arc::clone(&self.cluster_state_update_rx.borrow_and_update())
    }

    pub fn schedule_refresh(&mut self) -> Result<(), ShutdownError> {
        // if in-flight refresh is happening, then ignore.
        if let Some(handle) = &self.in_flight_refresh {
            if handle.is_finished() {
                self.in_flight_refresh = None;
            } else {
                // still in flight.
                return Ok(());
            }
        }

        self.in_flight_refresh = Self::start_refresh_task(
            self.network_sender.clone(),
            Arc::clone(&self.cluster_state_update_tx),
        )?;

        Ok(())
    }

    fn start_refresh_task(
        network_sender: Networking<T>,
        cluster_state_tx: Arc<watch::Sender<Arc<LegacyClusterState>>>,
    ) -> Result<Option<TaskHandle<anyhow::Result<()>>>, ShutdownError> {
        let refresh = async move {
            trace!("Refreshing cluster state");
            let last_state = Arc::clone(&cluster_state_tx.borrow());
            let heartbeat_timeout = Configuration::pinned().networking.keep_alive_timeout();
            let metadata = Metadata::current();
            // make sure we have a partition table that equals or newer than last refresh
            let partition_table_version = metadata
                .wait_for_version(
                    restate_core::MetadataKind::PartitionTable,
                    last_state.partition_table_version,
                )
                .await?;
            let _ = metadata
                .wait_for_version(
                    restate_core::MetadataKind::NodesConfiguration,
                    last_state.nodes_config_version,
                )
                .await;
            let nodes_config = metadata.nodes_config_snapshot();

            let mut nodes = BTreeMap::new();
            let mut join_set = tokio::task::JoinSet::new();
            for (_, node_config) in nodes_config.iter() {
                let node_id = node_config.current_generation;
                let network_sender = network_sender.clone();
                join_set
                    .build_task()
                    .name("get-nodes-state")
                    .spawn(
                        async move {
                            (
                                node_id,
                                network_sender
                                    .call_rpc(
                                        node_id,
                                        Swimlane::General,
                                        GetNodeState::default(),
                                        None,
                                        Some(heartbeat_timeout),
                                    )
                                    .await,
                            )
                        }
                        .in_current_tc_as_task(TaskKind::InPlace, "get-nodes-state"),
                    )
                    .expect("to spawn task");
            }
            while let Some(Ok((node_id, result))) = join_set.join_next().await {
                match result {
                    Ok(msg) => {
                        nodes.insert(
                            node_id.as_plain(),
                            NodeState::Alive(AliveNode {
                                last_heartbeat_at: MillisSinceEpoch::now(),
                                generational_node_id: node_id,
                                partitions: msg.partition_processor_state.unwrap_or_default(),
                                uptime: msg.uptime,
                            }),
                        );
                    }
                    Err(err) => {
                        // todo: implement a more robust failure detector
                        // This is a naive mechanism for failure detection and is just a stop-gap measure.
                        // A single connection error or timeout will cause a node to be marked as dead.
                        trace!("Node {node_id} is marked dead: {err}");
                        let last_seen_alive = last_state.nodes.get(&node_id.as_plain()).and_then(
                            |state| match state {
                                NodeState::Alive(AliveNode {
                                    last_heartbeat_at, ..
                                }) => Some(*last_heartbeat_at),
                                NodeState::Dead(DeadNode { last_seen_alive }) => *last_seen_alive,
                            },
                        );

                        nodes.insert(
                            node_id.as_plain(),
                            NodeState::Dead(DeadNode { last_seen_alive }),
                        );
                    }
                };
            }

            let state = LegacyClusterState {
                last_refreshed: Some(Instant::now()),
                nodes_config_version: nodes_config.version(),
                partition_table_version,
                nodes,
                logs_metadata_version: metadata.logs_version(),
            };

            // publish the new state
            trace!("New cluster state is acquired, publishing");
            cluster_state_tx.send(Arc::new(state))?;
            Ok(())
        };

        let handle = TaskCenter::spawn_unmanaged(
            restate_core::TaskKind::Disposable,
            "cluster-state-refresh",
            refresh,
        )?;

        // If this returned None, it means that the task completed or has been
        // cancelled before we get to this point.
        Ok(Some(handle))
    }
}

#[derive(Debug, Clone)]
pub struct ClusterStateWatcher {
    cluster_state_watcher: watch::Receiver<Arc<LegacyClusterState>>,
}

impl ClusterStateWatcher {
    pub async fn next_cluster_state(&mut self) -> Result<Arc<LegacyClusterState>, ShutdownError> {
        self.cluster_state_watcher
            .changed()
            .await
            .map_err(|_| ShutdownError)?;
        Ok(Arc::clone(&self.cluster_state_watcher.borrow_and_update()))
    }

    pub fn current(&self) -> Arc<LegacyClusterState> {
        Arc::clone(&self.cluster_state_watcher.borrow())
    }

    pub fn watch(&self) -> watch::Receiver<Arc<LegacyClusterState>> {
        self.cluster_state_watcher.clone()
    }
}
