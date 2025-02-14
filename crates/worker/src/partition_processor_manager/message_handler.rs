// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{Incoming, MessageHandler};
use restate_core::worker_api::ProcessorsManagerHandle;
use restate_core::{TaskCenter, TaskKind};
use restate_types::net::partition_processor_manager::{
    CreateSnapshotRequest, CreateSnapshotResponse, Snapshot, SnapshotError,
};
use tracing::warn;

/// RPC message handler for Partition Processor management operations.
pub struct PartitionProcessorManagerMessageHandler {
    processors_manager_handle: ProcessorsManagerHandle,
}

impl PartitionProcessorManagerMessageHandler {
    pub fn new(
        processors_manager_handle: ProcessorsManagerHandle,
    ) -> PartitionProcessorManagerMessageHandler {
        Self {
            processors_manager_handle,
        }
    }
}

impl MessageHandler for PartitionProcessorManagerMessageHandler {
    type MessageType = CreateSnapshotRequest;

    async fn on_message(&self, msg: Incoming<Self::MessageType>) {
        let processors_manager_handle = self.processors_manager_handle.clone();
        TaskCenter::spawn_child(
            TaskKind::Disposable,
            "create-snapshot-request-rpc",
            async move {
                let create_snapshot_result = processors_manager_handle
                    .create_snapshot(msg.body().partition_id)
                    .await;

                match create_snapshot_result.as_ref() {
                    Ok(snapshot) => msg.to_rpc_response(CreateSnapshotResponse {
                        result: Ok(Snapshot {
                            snapshot_id: snapshot.snapshot_id,
                            min_applied_lsn: snapshot.min_applied_lsn,
                        }),
                    }),
                    Err(err) => msg.to_rpc_response(CreateSnapshotResponse {
                        result: Err(SnapshotError::SnapshotCreationFailed(err.to_string())),
                    }),
                }
                .send()
                .await
                .map_err(|e| {
                    warn!(result = ?create_snapshot_result, "Failed to send response: {}", e);
                    anyhow::anyhow!("Failed to send response to create snapshot request: {}", e)
                })
            },
        )
        .map_err(|e| {
            warn!("Failed to spawn request handler: {}", e);
        })
        .ok();
    }
}
