// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use anyhow::Context;
use futures::{Stream, StreamExt};

use restate_core::{
    cancellation_watcher,
    network::{Incoming, MessageRouterBuilder, NetworkError},
    task_center,
    worker_api::ProcessorsManagerHandle,
    ShutdownError, TaskKind,
};
use restate_types::net::node::{GetNodeState, NodeStateResponse};

pub struct BaseRole {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    incoming_node_state:
        Pin<Box<dyn Stream<Item = Incoming<GetNodeState>> + Send + Sync + 'static>>,
}

impl BaseRole {
    pub fn create(
        router_builder: &mut MessageRouterBuilder,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        let incoming_node_state = router_builder.subscribe_to_stream(2);

        Self {
            processor_manager_handle,
            incoming_node_state,
        }
    }

    pub fn start(self) -> anyhow::Result<()> {
        let tc = task_center();
        tc.spawn_child(TaskKind::RoleRunner, "base-role-service", None, async {
            let cancelled = cancellation_watcher();

            tokio::select! {
                result = self.run() => {
                    result
                }
                _ = cancelled =>{
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
        let parition_state = if let Some(ref handle) = self.processor_manager_handle {
            Some(handle.get_state().await?)
        } else {
            None
        };

        // only return error if Shutdown
        if let Err(NetworkError::Shutdown(err)) = msg
            .to_rpc_response(NodeStateResponse {
                paritions_processor_state: parition_state,
            })
            .try_send()
            .map_err(|err| err.source)
        {
            return Err(err);
        }

        Ok(())
    }
}
