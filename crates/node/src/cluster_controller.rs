// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Options;
use codederror::CodedError;
use std::convert::Infallible;
use tonic::transport::Uri;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ClusterControllerRoleError {
    #[error("cluster controller failed: {0}")]
    ClusterController(
        #[from]
        #[code]
        restate_cluster_controller::Error,
    ),
}

#[derive(Debug)]
pub struct ClusterControllerRole {
    endpoint: Uri,
    controller: restate_cluster_controller::Service,
}

impl ClusterControllerRole {
    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), ClusterControllerRoleError> {
        info!("Running cluster controller role");

        let shutdown_signal = shutdown_watch.signaled();
        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        let controller_fut = self.controller.run(inner_shutdown_watch);
        tokio::pin!(controller_fut);

        tokio::select! {
            _ = shutdown_signal => {
                info!("Stopping controller role");
                // ignore result because we are shutting down
                let _ = tokio::join!(inner_shutdown_signal.drain(), controller_fut);
            },
            controller_result = &mut controller_fut => {
                controller_result?;
                panic!("Unexpected termination of controller");
            }

        }

        Ok(())
    }

    pub fn controller_endpoint(&self) -> &Uri {
        &self.endpoint
    }
}

impl TryFrom<Options> for ClusterControllerRole {
    type Error = Infallible;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let endpoint = Uri::builder()
            .scheme("http")
            .authority(options.cluster_controller.bind_address.to_string())
            .path_and_query("/")
            .build()
            .expect("uri should be valid");

        Ok(ClusterControllerRole {
            endpoint,
            controller: restate_cluster_controller::Service::new(options.cluster_controller),
        })
    }
}
