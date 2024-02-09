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
use restate_node_services::worker::BifrostVersion;
use tonic::{Request, Response, Status};

// -- GRPC Service Handlers --
pub struct WorkerHandler {
    bifrost: Bifrost,
}

impl WorkerHandler {
    pub fn new(bifrost: Bifrost) -> Self {
        Self { bifrost }
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
}
