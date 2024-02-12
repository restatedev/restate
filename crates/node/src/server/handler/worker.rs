// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::Bifrost;
use restate_node_services::worker::worker_server::Worker;
use restate_node_services::worker::{BifrostVersion, StateMutationRequest, TerminationRequest};
use restate_worker::WorkerCommandSender;
use restate_worker_api::Handle;
use tonic::{Request, Response, Status};

pub struct WorkerHandler {
    bifrost: Bifrost,
    worker_cmd_tx: WorkerCommandSender,
}

impl WorkerHandler {
    pub fn new(bifrost: Bifrost, worker_cmd_tx: WorkerCommandSender) -> Self {
        Self {
            bifrost,
            worker_cmd_tx,
        }
    }
}

#[async_trait::async_trait]
impl Worker for WorkerHandler {
    async fn get_bifrost_version(
        &self,
        _request: Request<()>,
    ) -> Result<Response<BifrostVersion>, Status> {
        let version = self.bifrost.metadata_version();
        return Ok(Response::new(BifrostVersion {
            version: version.into(),
        }));
    }

    async fn terminate_invocation(
        &self,
        request: Request<TerminationRequest>,
    ) -> Result<Response<()>, Status> {
        let (invocation_termination, _) = bincode::serde::decode_from_slice(
            &request.into_inner().invocation_termination,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        self.worker_cmd_tx
            .terminate_invocation(invocation_termination)
            .await
            .map_err(|_| Status::unavailable("worker shut down"))?;

        Ok(Response::new(()))
    }

    async fn mutate_state(
        &self,
        request: Request<StateMutationRequest>,
    ) -> Result<Response<()>, Status> {
        let (state_mutation, _) = bincode::serde::decode_from_slice(
            &request.into_inner().state_mutation,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        self.worker_cmd_tx
            .external_state_mutation(state_mutation)
            .await
            .map_err(|_| Status::unavailable("worker shut down"))?;

        Ok(Response::new(()))
    }
}
