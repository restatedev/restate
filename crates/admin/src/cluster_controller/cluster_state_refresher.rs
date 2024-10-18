// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{MessageRouterBuilder, Networking, Outgoing, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenter, TaskHandle};
use restate_types::cluster::cluster_state::{AliveNode, ClusterState, DeadNode, NodeState};
use restate_types::net::partition_processor_manager::GetProcessorsState;
use restate_types::nodes_config::Role;
use restate_types::time::MillisSinceEpoch;
use restate_types::Version;

pub struct ClusterStateRefresher<T> {
    task_center: TaskCenter,
    metadata: Metadata,
    network_sender: Networking<T>,
    get_state_router: RpcRouter<GetProcessorsState>,
    in_flight_refresh: Option<TaskHandle<anyhow::Result<()>>>,
    cluster_state_update_rx: watch::Receiver<Arc<ClusterState>>,
    cluster_state_update_tx: Arc<watch::Sender<Arc<ClusterState>>>,
}

impl<T: TransportConnect> ClusterStateRefresher<T> {
    pub fn new(
        task_center: TaskCenter,
        metadata: Metadata,
        network_sender: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let get_state_router = RpcRouter::new(router_builder);

        let initial_state = ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: BTreeMap::new(),
        };
        let (cluster_state_update_tx, cluster_state_update_rx) =
            watch::channel(Arc::from(initial_state));

        Self {
            task_center,
            metadata,
            network_sender,
            get_state_router,
            in_flight_refresh: None,
            cluster_state_update_rx,
            cluster_state_update_tx: Arc::new(cluster_state_update_tx),
        }
    }

    pub fn get_cluster_state(&self) -> Arc<ClusterState> {
        Arc::clone(&self.cluster_state_update_rx.borrow())
    }

    pub fn cluster_state_watcher(&self) -> ClusterStateWatcher {
        ClusterStateWatcher {
            cluster_state_watcher: self.cluster_state_update_rx.clone(),
        }
    }

    pub async fn next_cluster_state_update(&mut self) -> Arc<ClusterState> {
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
            self.task_center.clone(),
            self.get_state_router.clone(),
            self.network_sender.clone(),
            Arc::clone(&self.cluster_state_update_tx),
            self.metadata.clone(),
        )?;

        Ok(())
    }

    fn start_refresh_task(
        tc: TaskCenter,
        get_state_router: RpcRouter<GetProcessorsState>,
        network_sender: Networking<T>,
        cluster_state_tx: Arc<watch::Sender<Arc<ClusterState>>>,
        metadata: Metadata,
    ) -> Result<Option<TaskHandle<anyhow::Result<()>>>, ShutdownError> {
        let task_center = tc.clone();
        let refresh = async move {
            let last_state = Arc::clone(&cluster_state_tx.borrow());
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
            for (node_id, node) in nodes_config.iter() {
                // We are only interested in worker nodes.
                if !node.has_role(Role::Worker) {
                    continue;
                }

                let rpc_router = get_state_router.clone();
                let tc = tc.clone();
                let network_sender = network_sender.clone();
                join_set
                    .build_task()
                    .name("get-processors-state")
                    .spawn(async move {
                        tc.run_in_scope("get-processor-state", None, async move {
                            match network_sender.node_connection(node_id).await {
                                Ok(connection) => {
                                    let outgoing =
                                        Outgoing::new(node_id, GetProcessorsState::default())
                                            .assign_connection(connection);

                                    (
                                        node_id,
                                        rpc_router
                                            .call_outgoing_timeout(
                                                outgoing,
                                                std::time::Duration::from_secs(1), // todo: make configurable
                                            )
                                            .await,
                                    )
                                }
                                Err(network_error) => (node_id, Err(network_error)),
                            }
                        })
                        .await
                    })
                    .expect("to spawn task");
            }
            while let Some(Ok((node_id, result))) = join_set.join_next().await {
                match result {
                    Ok(response) => {
                        let peer = *response.peer();
                        let msg = response.into_body();
                        nodes.insert(
                            node_id,
                            NodeState::Alive(AliveNode {
                                last_heartbeat_at: MillisSinceEpoch::now(),
                                generational_node_id: peer,
                                partitions: msg.state,
                            }),
                        );
                    }
                    _ => {
                        // todo: implement a more robust failure detector
                        // This is a naive mechanism for failure detection and is just a stop-gap measure.
                        // A single connection error or timeout will cause a node to be marked as dead.
                        let last_seen_alive =
                            last_state
                                .nodes
                                .get(&node_id)
                                .and_then(|state| match state {
                                    NodeState::Alive(AliveNode {
                                        last_heartbeat_at, ..
                                    }) => Some(*last_heartbeat_at),
                                    NodeState::Dead(DeadNode { last_seen_alive }) => {
                                        *last_seen_alive
                                    }
                                });

                        nodes.insert(node_id, NodeState::Dead(DeadNode { last_seen_alive }));
                    }
                };
            }

            let state = ClusterState {
                last_refreshed: Some(Instant::now()),
                nodes_config_version: nodes_config.version(),
                partition_table_version,
                nodes,
                logs_metadata_version: metadata.logs_version(),
            };

            // publish the new state
            cluster_state_tx.send(Arc::new(state))?;
            Ok(())
        };

        let handle = task_center.spawn_unmanaged(
            restate_core::TaskKind::Disposable,
            "cluster-state-refresh",
            None,
            refresh,
        )?;

        // If this returned None, it means that the task completed or has been
        // cancelled before we get to this point.
        Ok(Some(handle))
    }
}

#[derive(Debug, Clone)]
pub struct ClusterStateWatcher {
    cluster_state_watcher: watch::Receiver<Arc<ClusterState>>,
}

impl ClusterStateWatcher {
    pub async fn next_cluster_state(&mut self) -> Result<Arc<ClusterState>, ShutdownError> {
        self.cluster_state_watcher
            .changed()
            .await
            .map_err(|_| ShutdownError)?;
        Ok(Arc::clone(&self.cluster_state_watcher.borrow_and_update()))
    }
}
