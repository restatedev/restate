// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::debug;

use restate_core::{
    ShutdownError, TaskCenter, TaskKind,
    network::{
        BackPressureMode, Buffered, Drain, Handler, Incoming, MessageRouterBuilder, RawSvcRpc,
        Verdict,
    },
    task_center::TaskCenterMonitoring,
    worker_api::ProcessorsManagerHandle,
};
use restate_types::net::node::{GetNodeState, GossipService, NodeStateResponse};
use restate_types::protobuf::common::NodeStatus;

pub struct FailureDetector {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    gossip_rx: Buffered<GossipService>,
}

impl FailureDetector {
    pub fn new(
        router_builder: &mut MessageRouterBuilder,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        let gossip_rx = router_builder.register_buffered_service(1024, BackPressureMode::Lossy);

        Self {
            processor_manager_handle,
            gossip_rx,
        }
    }

    pub fn start(self) -> Result<(), ShutdownError> {
        // gossip service is running as unmanaged task to delay its termination until the very end
        TaskCenter::spawn_unmanaged(
            TaskKind::SystemService,
            "fd-network-service",
            self.gossip_rx.run(GossipHandler {
                processor_manager_handle: self.processor_manager_handle,
            }),
        )?;

        Ok(())
    }
}

struct GossipHandler {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
}

impl Handler for GossipHandler {
    type Service = GossipService;
    async fn on_start(&mut self) {
        debug!("Gossip handler started");
    }

    async fn on_drain(&mut self) -> Drain {
        debug!("Gossip handler drain requested");
        Drain::Immediate
    }

    async fn on_stop(&mut self) {
        let node_status = TaskCenter::with_current(|tc| tc.health().node_status());
        node_status.update(NodeStatus::Unknown);
        debug!("Gossip handler stopped");
    }

    /// handle rpc request
    async fn on_rpc(&mut self, message: Incoming<RawSvcRpc<Self::Service>>) {
        let request = match message.try_into_typed::<GetNodeState>() {
            Ok(request) => request,
            Err(msg) => {
                msg.fail(Verdict::MessageUnrecognized);
                return;
            }
        };
        let partition_state = if let Some(ref handle) = self.processor_manager_handle {
            handle.get_state().await.ok()
        } else {
            None
        };

        request.into_reciprocal().send(NodeStateResponse {
            partition_processor_state: partition_state,
            uptime: TaskCenter::with_current(|t| t.age()),
        });
    }
}
