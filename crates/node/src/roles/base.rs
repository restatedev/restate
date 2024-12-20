// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use futures::StreamExt;

use restate_core::{
    cancellation_watcher,
    network::{Incoming, MessageRouterBuilder, MessageStream, NetworkError},
    worker_api::ProcessorsManagerHandle,
    ShutdownError, TaskCenter, TaskKind,
};
use restate_types::net::node::{GetPartitionsProcessorsState, PartitionsProcessorsStateResponse};

pub struct BaseRole {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    processors_state_request_stream: MessageStream<GetPartitionsProcessorsState>,
}

impl BaseRole {
    pub fn create(
        router_builder: &mut MessageRouterBuilder,
        processor_manager_handle: Option<ProcessorsManagerHandle>,
    ) -> Self {
        let processors_state_request_stream = router_builder.subscribe_to_stream(2);

        Self {
            processor_manager_handle,
            processors_state_request_stream,
        }
    }

    pub fn start(self) -> anyhow::Result<()> {
        TaskCenter::spawn_child(TaskKind::RoleRunner, "base-role-service", async {
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
        while let Some(request) = self.processors_state_request_stream.next().await {
            // handle request
            self.handle_get_partitions_processors_state(request).await?;
        }

        Ok(())
    }

    async fn handle_get_partitions_processors_state(
        &self,
        msg: Incoming<GetPartitionsProcessorsState>,
    ) -> Result<(), ShutdownError> {
        let partition_state = if let Some(ref handle) = self.processor_manager_handle {
            Some(handle.get_state().await?)
        } else {
            None
        };

        // only return error if Shutdown
        if let Err(NetworkError::Shutdown(err)) = msg
            .to_rpc_response(PartitionsProcessorsStateResponse {
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
