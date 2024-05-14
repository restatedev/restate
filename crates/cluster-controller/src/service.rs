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
use restate_node_protocol::cluster_controller::{
    Action, AttachRequest, AttachResponse, RunMode, RunPartition,
};
use restate_node_protocol::common::{KeyRange, RequestId};
use restate_types::partition_table::FixedPartitionTable;

use restate_core::network::{MessageRouterBuilder, NetworkSender};
use restate_core::{cancellation_watcher, task_center, Metadata, ShutdownError, TaskCenter};
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
    incoming_messages: BoxStream<'static, MessageEnvelope<AttachRequest>>,
}

impl Service {
    pub fn new(
        metadata: Metadata,
        networking: Networking,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let incoming_messages = router_builder.subscribe_to_stream(10);
        Service {
            metadata,
            networking,
            incoming_messages,
        }
    }
}

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
                    self.handle_attach_request(&tc, from, message)?;
                }
              _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn handle_attach_request(
        &mut self,
        tc: &TaskCenter,
        from: GenerationalNodeId,
        request: AttachRequest,
    ) -> Result<(), ShutdownError> {
        let partition_table = self
            .metadata
            .partition_table()
            .expect("partition table is loaded before run");
        let networking = self.networking.clone();
        let response = self.create_attachment_response(&partition_table, from, request.request_id);
        tc.spawn(
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
