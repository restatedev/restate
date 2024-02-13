// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod options;
mod roles;
mod server;
pub mod task_center;

use codederror::CodedError;
use restate_types::tasks::TaskKind;
use restate_types::time::MillisSinceEpoch;
use restate_types::{NodeId, PlainNodeId};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::task::JoinError;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{info, warn};

use crate::roles::{ClusterControllerRole, WorkerRole};
use crate::server::NodeServer;
pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
use restate_node_services::cluster_controller::cluster_controller_client::ClusterControllerClient;
use restate_node_services::cluster_controller::AttachmentRequest;
use restate_types::nodes_config::{NetworkAddress, NodeConfig, NodesConfiguration, Role};
use restate_types::retries::RetryPolicy;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};

use self::task_center::{task_center, TaskCenter};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        roles::WorkerRoleError,
    ),
    #[error("controller failed: {0}")]
    Controller(
        #[from]
        #[code]
        roles::ClusterControllerRoleError,
    ),
    #[error("node ctrl service failed: {0}")]
    NodeCtrlService(
        #[from]
        #[code]
        server::Error,
    ),
    #[error("invalid cluster controller address: {0}")]
    #[code(unknown)]
    InvalidClusterControllerAddress(http::Error),
    #[error("failed to attach to cluster at '{0}': {1}")]
    #[code(unknown)]
    Attachment(NetworkAddress, tonic::Status),
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
        roles::WorkerRoleBuildError,
    ),
    #[error("node neither runs cluster controller nor its address has been configured")]
    #[code(unknown)]
    UnknownClusterController,
}

pub struct Node {
    options: Options,
    cluster_controller_address: NetworkAddress,

    cluster_controller_role: Option<ClusterControllerRole>,
    worker_role: Option<WorkerRole>,
    server: NodeServer,
}

impl Node {
    pub fn new(options: Options) -> Result<Self, BuildError> {
        let opts = options.clone();
        let cluster_controller_role = if options.roles.contains(Role::ClusterController) {
            Some(ClusterControllerRole::try_from(options.clone()).expect("should be infallible"))
        } else {
            None
        };

        let worker_role = if options.roles.contains(Role::Worker) {
            Some(WorkerRole::try_from(options.clone())?)
        } else {
            None
        };

        let server = options.server.build(
            worker_role
                .as_ref()
                .map(|worker| (worker.rocksdb_storage().clone(), worker.bifrost_handle())),
            cluster_controller_role
                .as_ref()
                .map(|cluster_controller| cluster_controller.handle()),
        );

        let cluster_controller_address = if let Some(cluster_controller_address) =
            options.cluster_controller_address
        {
            if cluster_controller_role.is_some() {
                warn!("This node is running the cluster controller but has also a remote cluster controller address configured. \
                This indicates a potential misconfiguration. Trying to connect to the remote cluster controller.");
            }

            cluster_controller_address
        } else if cluster_controller_role.is_some() {
            NetworkAddress::DnsName(format!("127.0.0.1:{}", server.port()))
        } else {
            return Err(BuildError::UnknownClusterController);
        };

        Ok(Node {
            options: opts,
            cluster_controller_address,
            cluster_controller_role,
            worker_role,
            server,
        })
    }

    pub fn boot(self, tc: &TaskCenter) -> Result<(), anyhow::Error> {
        tc.spawn(
            TaskKind::RpcServer,
            "node-server",
            None,
            true, /* shutdown_node_on_failure */
            self.server.run(),
        )?;

        if let Some(cluster_controller_role) = self.cluster_controller_role {
            tc.spawn(
                TaskKind::RoleRunner,
                "cluster-controller-role",
                None,
                true, /* shutdown_node_on_failure */
                cluster_controller_role.run(),
            )?;
        }

        if let Some(worker_role) = self.worker_role {
            tc.spawn(
                TaskKind::RoleRunner,
                "worker-init",
                None,
                true, /* shutdown_node_on_failure */
                async {
                    Self::attach_node(self.options, self.cluster_controller_address).await?;
                    // Startup the worker
                    task_center().spawn(
                        TaskKind::RoleRunner,
                        "worker-role",
                        None,
                        true, /* shutdown_node_on_failure */
                        worker_role.run(),
                    )?;
                    Ok(())
                },
            )?;
        }

        Ok(())
    }

    async fn attach_node(
        options: Options,
        cluster_controller_address: NetworkAddress,
    ) -> Result<(), Error> {
        info!(
            "Attaching '{}' (insist on ID?={:?}) to cluster controller at '{cluster_controller_address}'",
            options.node_name,
            options.node_id,
        );

        let channel = Self::create_channel_from_network_address(&cluster_controller_address)
            .map_err(Error::InvalidClusterControllerAddress)?;

        let cc_client = ClusterControllerClient::new(channel);

        let _response = RetryPolicy::exponential(Duration::from_millis(50), 2.0, 10, None)
            .retry_operation(|| async {
                cc_client
                    .clone()
                    .attach_node(AttachmentRequest {
                        node_id: options.node_id.map(Into::into),
                        node_name: options.node_name.clone(),
                    })
                    .await
            })
            .await
            .map_err(|err| Error::Attachment(cluster_controller_address, err))?;

        // todo: Generational NodeId should come from attachment result
        let now = MillisSinceEpoch::now();
        let my_node_id: NodeId = options
            .node_id
            .unwrap_or(PlainNodeId::from(1))
            .with_generation(now.as_u64() as u32)
            .into();
        // We are attached, we can set our own NodeId.
        my_node_id.set_as_my_node_id();
        info!(
            "Node attached to cluster controller. My Node ID is {}",
            my_node_id
        );
        // Temporary: nodes configuration from current node.
        let mut nodes_config = NodesConfiguration::default();
        let address: NetworkAddress = NetworkAddress::TcpSocketAddr(options.server.bind_address);
        let node = NodeConfig::new(
            options.node_name,
            my_node_id
                .as_generational()
                .expect("my NodeId is generational"),
            address,
            options.roles,
        );
        nodes_config.upsert_node(node);
        nodes_config.set_as_current_unconditional();
        info!(
            "Loaded nodes configuration version {}",
            NodesConfiguration::current_version()
        );
        Ok(())
    }

    fn create_channel_from_network_address(
        cluster_controller_address: &NetworkAddress,
    ) -> Result<Channel, http::Error> {
        let channel = match cluster_controller_address {
            NetworkAddress::Uds(uds_path) => {
                let uds_path = uds_path.clone();
                // dummy endpoint required to specify an uds connector, it is not used anywhere
                Endpoint::try_from("/")
                    .expect("/ should be a valid Uri")
                    .connect_with_connector_lazy(service_fn(move |_: Uri| {
                        UnixStream::connect(uds_path.clone())
                    }))
            }
            NetworkAddress::TcpSocketAddr(socket_addr) => {
                let uri = Self::create_uri(socket_addr)?;
                Self::create_lazy_channel_from_uri(uri)
            }
            NetworkAddress::DnsName(dns_name) => {
                let uri = Self::create_uri(dns_name)?;
                Self::create_lazy_channel_from_uri(uri)
            }
        };
        Ok(channel)
    }

    fn create_uri(authority: impl ToString) -> Result<Uri, http::Error> {
        Uri::builder()
            // todo: Make the scheme configurable
            .scheme("http")
            .authority(authority.to_string())
            .path_and_query("/")
            .build()
    }

    fn create_lazy_channel_from_uri(uri: Uri) -> Channel {
        // todo: Make the channel settings configurable
        Channel::builder(uri)
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy()
    }
}
