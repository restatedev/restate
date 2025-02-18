// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;

use restate_core::{
    network::{Incoming, MessageRouterBuilder, MessageStream},
    task_center::TaskCenterMonitoring,
    worker_api::ProcessorsManagerHandle,
    ShutdownError, TaskCenter, TaskKind,
};
use restate_types::net::node::{GetNodeState, NodeStateResponse};
use restate_types::protobuf::common::NodeStatus;

pub struct BaseRole {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    incoming_node_state: MessageStream<GetNodeState>,
}

impl BaseRole {
    pub fn create(
        router_builder: &mut MessageRouterBuilder,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        let incoming_node_state = router_builder.subscribe_to_stream(128);

        Self {
            processor_manager_handle,
            incoming_node_state,
        }
    }

    pub fn start(self) -> Result<(), ShutdownError> {
        TaskCenter::spawn_unmanaged(TaskKind::RoleRunner, "base-role-service", async {
            let node_status = TaskCenter::with_current(|tc| tc.health().node_status());
            node_status.update(NodeStatus::Alive);
            self.run().await
        })?;

        Ok(())
    }

    async fn run(mut self) -> anyhow::Result<()> {
        while let Some(request) = self.incoming_node_state.next().await {
            self.handle_get_node_state(request).await;
        }

        Ok(())
    }

    async fn handle_get_node_state(&self, msg: Incoming<GetNodeState>) {
        let partition_state = if let Some(ref handle) = self.processor_manager_handle {
            handle.get_state().await.ok()
        } else {
            None
        };

        let _ = msg
            .to_rpc_response(NodeStateResponse {
                partition_processor_state: partition_state,
                uptime: TaskCenter::with_current(|t| t.age()),
            })
            .try_send();
    }
}
