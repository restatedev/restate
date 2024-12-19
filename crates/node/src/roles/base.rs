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

use anyhow::Context;
use futures::StreamExt;
use tokio::sync::watch;

use restate_core::{
    cancellation_watcher,
    network::{
        Incoming, MessageRouterBuilder, MessageStream, NetworkError, Networking, TransportConnect,
    },
    worker_api::ProcessorsManagerHandle,
    Metadata, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    cluster::cluster_state::ClusterState,
    net::node::{GetNodeState, NodeStateResponse},
};

use crate::cluster_state_refresher::ClusterStateRefresher;

pub struct BaseRole<T> {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    incoming_node_state: MessageStream<GetNodeState>,
    cluster_state_refresher: Option<ClusterStateRefresher<T>>,
}

impl<T> BaseRole<T>
where
    T: TransportConnect,
{
    pub fn create(
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let incoming_node_state = router_builder.subscribe_to_stream(2);
        let cluster_state_refresher =
            ClusterStateRefresher::new(metadata, networking, router_builder);
        Self {
            processor_manager_handle: None,
            incoming_node_state,
            cluster_state_refresher: Some(cluster_state_refresher),
        }
    }

    #[allow(dead_code)]
    pub fn cluster_state_watch(&self) -> watch::Receiver<Arc<ClusterState>> {
        self.cluster_state_refresher
            .as_ref()
            .expect("is set")
            .cluster_state_watch()
    }

    pub fn with_processor_manager_handle(&mut self, handle: ProcessorsManagerHandle) -> &mut Self {
        self.cluster_state_refresher
            .as_mut()
            .expect("is set")
            .with_processor_manager_handle(handle.clone());

        self.processor_manager_handle = Some(handle);
        self
    }

    pub fn start(mut self) -> anyhow::Result<()> {
        let cluster_state_refresher = self.cluster_state_refresher.take().expect("is set");

        TaskCenter::spawn_child(TaskKind::SystemService, "cluster-state-refresher", async {
            let cancelled = cancellation_watcher();
            tokio::select! {
                result = cluster_state_refresher.run() => {
                    result
                }
                _ = cancelled => {
                    Ok(())
                }
            }
        })
        .context("Failed to start cluster state refresher")?;

        TaskCenter::spawn_child(TaskKind::RoleRunner, "base-role-service", async {
            let cancelled = cancellation_watcher();

            tokio::select! {
                result = self.run() => {
                    result
                }
                _ = cancelled => {
                    Ok(())
                }
            }
        })
        .context("Failed to start base service")?;

        Ok(())
    }

    async fn run(mut self) -> anyhow::Result<()> {
        while let Some(request) = self.incoming_node_state.next().await {
            // handle request
            self.handle_get_node_state(request).await?;
        }

        Ok(())
    }

    async fn handle_get_node_state(
        &self,
        msg: Incoming<GetNodeState>,
    ) -> Result<(), ShutdownError> {
        let partition_state = if let Some(ref handle) = self.processor_manager_handle {
            Some(handle.get_state().await?)
        } else {
            None
        };

        // only return error if Shutdown
        if let Err(NetworkError::Shutdown(err)) = msg
            .to_rpc_response(NodeStateResponse {
                partition_processor_state: partition_state,
            })
            .try_send()
            .map_err(|err| err.source)
        {
            return Err(err);
        }

        Ok(())
    }
}
