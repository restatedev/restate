// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use codederror::CodedError;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::time::Instant;

use restate_network::Networking;
use restate_node_protocol::cluster_controller::{
    Action, AttachRequest, AttachResponse, RunPartition,
};
use restate_node_protocol::common::{KeyRange, RequestId};
use restate_types::arc_util::Updateable;
use restate_types::config::AdminOptions;
use restate_types::partition_table::FixedPartitionTable;

use restate_core::network::{MessageRouterBuilder, NetworkSender};
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskCenter};
use restate_node_protocol::MessageEnvelope;
use restate_types::processors::RunMode;
use restate_types::{GenerationalNodeId, Version};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

use crate::cluster_state::{ClusterState, ClusterStateRefresher};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

pub struct Service {
    task_center: TaskCenter,
    metadata: Metadata,
    networking: Networking,
    incoming_messages: BoxStream<'static, MessageEnvelope<AttachRequest>>,
    cluster_state_refresher: ClusterStateRefresher,
    command_tx: mpsc::Sender<ClusterControllerCommand>,
    command_rx: mpsc::Receiver<ClusterControllerCommand>,
}

impl Service {
    pub fn new(
        task_center: TaskCenter,
        metadata: Metadata,
        networking: Networking,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let incoming_messages = router_builder.subscribe_to_stream(10);
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher = ClusterStateRefresher::new(
            task_center.clone(),
            metadata.clone(),
            networking.clone(),
            router_builder,
        );

        Service {
            task_center,
            metadata,
            networking,
            incoming_messages,
            cluster_state_refresher,
            command_tx,
            command_rx,
        }
    }
}

enum ClusterControllerCommand {
    GetClusterState(oneshot::Sender<Arc<ClusterState>>),
}

pub struct ClusterControllerHandle {
    tx: mpsc::Sender<ClusterControllerCommand>,
}

impl ClusterControllerHandle {
    pub async fn get_cluster_state(&self) -> Result<Arc<ClusterState>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        // ignore the error, we own both tx and rx at this point.
        let _ = self
            .tx
            .send(ClusterControllerCommand::GetClusterState(tx))
            .await;
        rx.await.map_err(|_| ShutdownError)
    }
}

impl Service {
    pub fn handle(&self) -> ClusterControllerHandle {
        ClusterControllerHandle {
            tx: self.command_tx.clone(),
        }
    }

    pub async fn run(
        mut self,
        mut updateable_config: impl Updateable<AdminOptions>,
    ) -> anyhow::Result<()> {
        let options = updateable_config.load();
        // Make sure we have partition table before starting
        let _ = self.metadata.wait_for_partition_table(Version::MIN).await?;
        let mut heartbeat =
            tokio::time::interval_at(Instant::now(), options.heartbeat_interval.into());
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.on_get_cluster_state(cmd);
                }
                Some(message) = self.incoming_messages.next() => {
                    let (from, message) = message.split();
                    self.on_attach_request(from, message)?;
                }
              _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn on_get_cluster_state(&self, command: ClusterControllerCommand) {
        match command {
            ClusterControllerCommand::GetClusterState(tx) => {
                let _ = tx.send(self.cluster_state_refresher.get_cluster_state());
            }
        }
    }

    fn on_attach_request(
        &mut self,
        from: GenerationalNodeId,
        request: AttachRequest,
    ) -> Result<(), ShutdownError> {
        let partition_table = self
            .metadata
            .partition_table()
            .expect("partition table is loaded before run");
        let networking = self.networking.clone();
        let response = self.create_attachment_response(&partition_table, from, request.request_id);
        self.task_center.spawn(
            restate_core::TaskKind::Disposable,
            "attachment-response",
            None,
            async move { Ok(networking.send(from.into(), &response).await?) },
        )?;
        Ok(())
    }

    fn create_attachment_response(
        &self,
        partition_table: &FixedPartitionTable,
        _node: GenerationalNodeId,
        request_id: RequestId,
    ) -> AttachResponse {
        // simulating a plan after initial attachement
        let actions = partition_table
            .partitioner()
            .map(|(partition_id, key_range)| {
                Action::RunPartition(RunPartition {
                    partition_id,
                    key_range_inclusive: KeyRange {
                        from: *key_range.start(),
                        to: *key_range.end(),
                    },
                    mode: RunMode::Leader,
                })
            })
            .collect();
        AttachResponse {
            request_id,
            actions,
        }
    }
}
