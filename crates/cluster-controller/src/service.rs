// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;
use futures::stream::BoxStream;
use futures::StreamExt;

use restate_network::Networking;
use restate_node_protocol::common::RequestId;
use restate_node_protocol::worker::{
    Action, AttachmentResponse, KeyRange, RunMode, RunPartition, WorkerMessage,
};
use restate_types::partition_table::FixedPartitionTable;

use restate_core::network::NetworkSender;
use restate_core::{cancellation_watcher, task_center, Metadata, ShutdownError, TaskCenter};
use restate_node_protocol::cluster_controller::ClusterControllerMessage;
use restate_node_protocol::MessageEnvelope;
use restate_types::{GenerationalNodeId, Version};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

pub struct Service {
    metadata: Metadata,
    networking: Networking,
    incoming_messages: BoxStream<'static, MessageEnvelope<ClusterControllerMessage>>,
}

impl Service {
    pub fn new(
        metadata: Metadata,
        networking: Networking,
        incoming_messages: BoxStream<'static, MessageEnvelope<ClusterControllerMessage>>,
    ) -> Self {
        Service {
            metadata,
            networking,
            incoming_messages,
        }
    }
}

// todo: Replace with proper handle
pub struct ClusterControllerHandle;

impl Service {
    pub fn handle(&self) -> ClusterControllerHandle {
        ClusterControllerHandle
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // Make sure we have partition table before starting
        let _ = self.metadata.wait_for_partition_table(Version::MIN).await?;

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let tc = task_center();
        loop {
            tokio::select! {
                Some(message) = self.incoming_messages.next() => {
                    let (from, message) = message.split();
                    self.handle_network_message(&tc, from, message).await?;
                }
              _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    async fn handle_network_message(
        &mut self,
        tc: &TaskCenter,
        from: GenerationalNodeId,
        msg: ClusterControllerMessage,
    ) -> Result<(), ShutdownError> {
        match msg {
            ClusterControllerMessage::Attach(details) => {
                let partition_table = self
                    .metadata
                    .partition_table()
                    .expect("partition table is loaded before run");
                let networking = self.networking.clone();
                let response =
                    self.create_attachment_response(&partition_table, from, details.request_id);
                tc.spawn(
                    restate_core::TaskKind::Disposable,
                    "attachment-response",
                    None,
                    async move {
                        Ok(networking
                            .send(from.into(), &WorkerMessage::AttachmentResponse(response))
                            .await?)
                    },
                )?;
            }
        }
        Ok(())
    }

    fn create_attachment_response(
        &self,
        partition_table: &FixedPartitionTable,
        _node: GenerationalNodeId,
        request_id: RequestId,
    ) -> AttachmentResponse {
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
        AttachmentResponse {
            request_id,
            actions,
        }
    }
}
