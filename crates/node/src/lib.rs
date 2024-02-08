// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_controller;
mod options;
pub mod worker;

use codederror::CodedError;
use futures::TryFutureExt;
use restate_types::NodeId;
use std::convert::Infallible;
use std::str::FromStr;
use std::time::Duration;
use tokio::task::{JoinError, JoinSet};
use tonic::codegen::http::uri::InvalidUri;
use tonic::transport::{Channel, Uri};
use tracing::{info, instrument};

use crate::cluster_controller::ClusterControllerRole;
use crate::worker::WorkerRole;
pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
use restate_node_ctrl::service::NodeCtrlService;
use restate_node_ctrl_proto::cluster_controller::cluster_controller_client::ClusterControllerClient;
use restate_node_ctrl_proto::cluster_controller::AttachmentRequest;
use restate_types::retries::RetryPolicy;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        worker::WorkerRoleError,
    ),
    #[error("controller failed: {0}")]
    Controller(
        #[from]
        #[code]
        cluster_controller::ClusterControllerRoleError,
    ),
    #[error("node ctrl service failed: {0}")]
    NodeCtrlService(
        #[from]
        #[code]
        restate_node_ctrl::Error,
    ),
    #[error("failed to attach to cluster at '{0}': {1}")]
    #[code(unknown)]
    Attachment(Uri, tonic::Status),
    #[error("node component panicked: {0}")]
    #[code(unknown)]
    ComponentPanic(JoinError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("building worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        worker::WorkerRoleBuildError,
    ),
    #[error("invalid controller endpoint: {0}")]
    #[code(unknown)]
    InvalidControllerEndpoint(#[from] InvalidUri),
}

pub struct Node {
    node_id: NodeId,
    cluster_controller_endpoint: Uri,

    cluster_controller_role: Option<ClusterControllerRole>,
    worker_role: WorkerRole,
    node_ctrl: NodeCtrlService,
}

impl Node {
    pub fn new(
        node_id: impl Into<NodeId>,
        cluster_controller_location: ClusterControllerLocation,
        options: Options,
    ) -> Result<Self, BuildError> {
        let cluster_controller_role = if let ClusterControllerLocation::Local =
            cluster_controller_location
        {
            Some(ClusterControllerRole::try_from(options.clone()).expect("should be infallible"))
        } else {
            None
        };

        let worker_role = WorkerRole::try_from(options.clone())?;

        let node_ctrl = options.node_ctrl.build(
            Some(worker_role.rocksdb_storage().clone()),
            worker_role.bifrost_handle(),
            cluster_controller_role
                .as_ref()
                .map(|cluster_controller| cluster_controller.handle()),
        );

        let cluster_controller_endpoint =
            if let ClusterControllerLocation::Remote(cluster_controller_address) =
                cluster_controller_location
            {
                cluster_controller_address.parse()?
            } else {
                node_ctrl.endpoint()
            };

        Ok(Node {
            node_id: node_id.into(),
            cluster_controller_endpoint,
            cluster_controller_role,
            worker_role,
            node_ctrl,
        })
    }

    #[instrument(level = "debug", skip_all, fields(node.id = %self.node_id))]
    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let shutdown_signal = shutdown_watch.signaled();
        tokio::pin!(shutdown_signal);

        let (component_shutdown_signal, component_shutdown_watch) = drain::channel();

        let mut component_set: JoinSet<Result<&'static str, Error>> = JoinSet::new();

        component_set.spawn(
            self.node_ctrl
                .run(component_shutdown_watch.clone())
                .map_ok(|_| "node-ctrl")
                .map_err(Error::NodeCtrlService),
        );
        self.cluster_controller_role.map(|cluster_controller| {
            component_set.spawn(
                cluster_controller
                    .run(component_shutdown_watch.clone())
                    .map_ok(|_| "cluster-controller-role")
                    .map_err(Error::Controller),
            )
        });

        tokio::select! {
            _ = &mut shutdown_signal => {
                drop(component_shutdown_watch);
                component_shutdown_signal.drain().await;
                component_set.shutdown().await;
                return Ok(());
            },
            Some(component_result) = component_set.join_next() => {
                let component_name = component_result.map_err(Error::ComponentPanic)??;
                panic!("Unexpected termination of '{component_name}'");
            }
            attachment_result = Self::attach_node(self.node_id, self.cluster_controller_endpoint) => {
                attachment_result?
            }
        }

        component_set.spawn(
            self.worker_role
                .run(component_shutdown_watch)
                .map_ok(|_| "worker-role")
                .map_err(Error::Worker),
        );

        tokio::select! {
            _ = shutdown_signal => {
                info!("Shutting node down");
                component_shutdown_signal.drain().await;
                component_set.shutdown().await;
            },
            Some(component_result) = component_set.join_next() => {
                let component_name = component_result.map_err(Error::ComponentPanic)??;
                panic!("Unexpected termination of '{component_name}'");
            }
        }

        Ok(())
    }

    async fn attach_node(node_id: NodeId, cluster_controller_endpoint: Uri) -> Result<(), Error> {
        info!("Attach to cluster at '{cluster_controller_endpoint}'");
        let channel = Channel::builder(cluster_controller_endpoint.clone())
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy();
        let cc_client = ClusterControllerClient::new(channel);

        RetryPolicy::exponential(Duration::from_millis(50), 2.0, 10, None)
            .retry_operation(|| async {
                cc_client
                    .clone()
                    .attach_node(AttachmentRequest {
                        node_id: Some(node_id.into()),
                    })
                    .await
            })
            .await
            .map_err(|err| Error::Attachment(cluster_controller_endpoint, err))?;

        Ok(())
    }
}

/// Specifying where the cluster controller runs. Options are:
///
/// * Local: Spawning the controller as part of this process
/// * Remote: The controller runs on a remote host
#[derive(Debug)]
pub enum ClusterControllerLocation {
    Local,
    Remote(String),
}

impl FromStr for ClusterControllerLocation {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = if s.to_lowercase() == "local" {
            ClusterControllerLocation::Local
        } else {
            ClusterControllerLocation::Remote(s.to_string())
        };

        Ok(result)
    }
}
