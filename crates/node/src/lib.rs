// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cluster_marker;
mod init;
mod network_server;
mod roles;

use anyhow::Context;
use bytestring::ByteString;
use prost_dto::IntoProst;
use std::num::NonZeroU16;
use tracing::{debug, error, info, trace, warn};

use crate::cluster_marker::ClusterValidationError;
use crate::init::NodeInit;
use crate::network_server::NetworkServer;
use crate::roles::{AdminRole, BaseRole, IngressRole, WorkerRole};
use codederror::CodedError;
use restate_bifrost::BifrostService;
use restate_core::metadata_store::{
    retry_on_retryable_error, Precondition, ReadWriteError, WriteError,
};
use restate_core::network::{
    GrpcConnector, MessageRouterBuilder, NetworkServerBuilder, Networking,
};
use restate_core::partitions::{spawn_partition_routing_refresher, PartitionRoutingRefresher};
use restate_core::{spawn_metadata_manager, MetadataBuilder, MetadataManager, TaskCenter};
use restate_core::{Metadata, TaskKind};
#[cfg(feature = "replicated-loglet")]
use restate_log_server::LogServerService;
use restate_metadata_server::{
    BoxedMetadataServer, MetadataServer, MetadataStoreClient, ReadModifyWriteError,
};
use restate_types::config::{CommonOptions, Configuration};
use restate_types::live::Live;
use restate_types::logs::metadata::{Logs, LogsConfiguration, ProviderConfiguration};
#[cfg(feature = "replicated-loglet")]
use restate_types::logs::RecordCache;
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, PARTITION_TABLE_KEY};
use restate_types::nodes_config::{
    LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration, Role,
};
use restate_types::partition_table::{PartitionReplication, PartitionTable, PartitionTableBuilder};
use restate_types::protobuf::common::{
    AdminStatus, IngressStatus, LogServerStatus, NodeRpcStatus, WorkerStatus,
};
use restate_types::storage::StorageEncode;
use restate_types::{GenerationalNodeId, Version, Versioned};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("node failed to start due to failed safety check: {0}")]
    #[code(unknown)]
    SafetyCheck(String),
    #[error(
        "missing nodes configuration; can only create it if '--allow-bootstrap true' is specified"
    )]
    #[code(unknown)]
    MissingNodesConfiguration,
    #[error("detected concurrent node registration for node '{0}'; stepping down")]
    #[code(unknown)]
    ConcurrentNodeRegistration(String),
    #[error("could not read/write from/to metadata store: {0}")]
    #[code(unknown)]
    MetadataStore(#[from] ReadWriteError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("Invalid configuration: {0}")]
    #[code(unknown)]
    InvalidConfiguration(anyhow::Error),

    #[error("building worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        roles::WorkerRoleBuildError,
    ),
    #[error("building cluster controller failed: {0}")]
    ClusterController(
        #[from]
        #[code]
        roles::AdminRoleBuildError,
    ),
    #[error("building log-server failed: {0}")]
    LogServer(
        #[from]
        #[code]
        restate_log_server::LogServerBuildError,
    ),
    #[error("node neither runs cluster controller nor its address has been configured")]
    #[code(unknown)]
    UnknownClusterController,
    #[error("cluster bootstrap failed: {0}")]
    #[code(unknown)]
    Bootstrap(String),
    #[error("failed validating and updating cluster marker: {0}")]
    #[code(unknown)]
    ClusterValidation(#[from] ClusterValidationError),

    #[error("failed to initialize metadata store client: {0}")]
    #[code(unknown)]
    MetadataStoreClient(anyhow::Error),

    #[error("building metadata store failed: {0}")]
    #[code(unknown)]
    MetadataStore(#[from] anyhow::Error),
}

pub struct Node {
    server_builder: NetworkServerBuilder,
    updateable_config: Live<Configuration>,
    metadata_manager: MetadataManager,
    partition_routing_refresher: PartitionRoutingRefresher,
    metadata_store_client: MetadataStoreClient,
    bifrost: BifrostService,
    metadata_server_role: Option<BoxedMetadataServer>,
    base_role: BaseRole,
    admin_role: Option<AdminRole<GrpcConnector>>,
    worker_role: Option<WorkerRole>,
    ingress_role: Option<IngressRole<GrpcConnector>>,
    #[cfg(feature = "replicated-loglet")]
    log_server: Option<LogServerService>,
    networking: Networking<GrpcConnector>,
    is_provisioned: bool,
}

impl Node {
    pub async fn create(updateable_config: Live<Configuration>) -> Result<Self, BuildError> {
        let mut server_builder = NetworkServerBuilder::default();
        let config = updateable_config.pinned();

        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        // We need to se the global metadata as soon as possible since the metadata store client's
        // auto update functionality won't be started if the global metadata is not set.
        let is_set = TaskCenter::try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        let is_provisioned =
            cluster_marker::validate_and_update_cluster_marker(config.common.cluster_name())?;

        let (metadata_store_role, metadata_store_client) = if config.has_role(Role::MetadataServer)
        {
            restate_metadata_server::create_metadata_server_and_client(
                updateable_config.clone(),
                TaskCenter::with_current(|tc| tc.health().metadata_server_status()),
                &mut server_builder,
            )
            .await
            .map(|(server, client)| (Some(server), client))?
        } else {
            let metadata_store_client =
                restate_metadata_server::create_client(config.common.metadata_client.clone())
                    .await
                    .map_err(BuildError::MetadataStoreClient)?;
            (None, metadata_store_client)
        };

        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        let mut router_builder = MessageRouterBuilder::default();
        let networking = Networking::new(metadata.clone(), config.networking.clone());
        metadata_manager.register_in_message_router(&mut router_builder);
        let partition_routing_refresher = PartitionRoutingRefresher::default();

        #[cfg(feature = "replicated-loglet")]
        let record_cache = RecordCache::new(
            Configuration::pinned()
                .bifrost
                .record_cache_memory_size
                .as_usize(),
        );

        // Setup bifrost
        // replicated-loglet
        #[cfg(feature = "replicated-loglet")]
        let replicated_loglet_factory = restate_bifrost::providers::replicated_loglet::Factory::new(
            metadata_store_client.clone(),
            networking.clone(),
            record_cache.clone(),
            &mut router_builder,
        );
        let bifrost_svc =
            BifrostService::new(metadata_manager.writer()).enable_local_loglet(&updateable_config);

        #[cfg(feature = "replicated-loglet")]
        let bifrost_svc = bifrost_svc.with_factory(replicated_loglet_factory);

        #[cfg(feature = "memory-loglet")]
        let bifrost_svc = bifrost_svc.enable_in_memory_loglet();

        #[cfg(not(feature = "replicated-loglet"))]
        warn_if_log_store_left_artifacts(&config);

        let bifrost = bifrost_svc.handle();

        #[cfg(feature = "replicated-loglet")]
        let log_server = if config.has_role(Role::LogServer) {
            Some(
                LogServerService::create(
                    TaskCenter::with_current(|tc| tc.health().log_server_status()),
                    updateable_config.clone(),
                    metadata.clone(),
                    record_cache,
                    &mut router_builder,
                    &mut server_builder,
                )
                .await?,
            )
        } else {
            None
        };

        let worker_role = if config.has_role(Role::Worker) {
            Some(
                WorkerRole::create(
                    TaskCenter::with_current(|tc| tc.health().worker_status()),
                    metadata.clone(),
                    partition_routing_refresher.partition_routing(),
                    updateable_config.clone(),
                    &mut router_builder,
                    networking.clone(),
                    bifrost_svc.handle(),
                    metadata_store_client.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        if !config
            .ingress
            .experimental_feature_enable_separate_ingress_role
            && config.has_role(Role::HttpIngress)
        {
            return Err(BuildError::InvalidConfiguration(anyhow::anyhow!(
                "The http-ingress role is used but experimental-feature-enable-separate-ingress-role is not set."
            )));
        }

        let ingress_role = if config
            .ingress
            .experimental_feature_enable_separate_ingress_role
            && config.has_role(Role::HttpIngress)
            // todo remove once the safe fallback version supports the HttpIngress role
            || !config
            .ingress
            .experimental_feature_enable_separate_ingress_role
            && config.has_role(Role::Worker)
        {
            Some(IngressRole::create(
                updateable_config
                    .clone()
                    .map(|config| &config.ingress)
                    .boxed(),
                TaskCenter::with_current(|tc| tc.health().ingress_status()),
                networking.clone(),
                metadata.updateable_schema(),
                metadata.updateable_partition_table(),
                partition_routing_refresher.partition_routing(),
                &mut router_builder,
            ))
        } else {
            None
        };

        let admin_role = if config.has_role(Role::Admin) {
            Some(
                AdminRole::create(
                    TaskCenter::with_current(|tc| tc.health().admin_status()),
                    bifrost.clone(),
                    updateable_config.clone(),
                    partition_routing_refresher.partition_routing(),
                    networking.clone(),
                    metadata,
                    metadata_manager.writer(),
                    &mut server_builder,
                    &mut router_builder,
                    worker_role
                        .as_ref()
                        .map(|worker_role| worker_role.storage_query_context().clone()),
                )
                .await?,
            )
        } else {
            None
        };

        let base_role = BaseRole::create(
            &mut router_builder,
            worker_role
                .as_ref()
                .map(|role| role.partition_processor_manager_handle()),
        );

        // Ensures that message router is updated after all services have registered themselves in
        // the builder.
        let message_router = router_builder.build();
        networking
            .connection_manager()
            .set_message_router(message_router);

        Ok(Node {
            updateable_config,
            metadata_manager,
            partition_routing_refresher,
            bifrost: bifrost_svc,
            metadata_server_role: metadata_store_role,
            metadata_store_client,
            base_role,
            admin_role,
            ingress_role,
            worker_role,
            #[cfg(feature = "replicated-loglet")]
            log_server,
            server_builder,
            networking,
            is_provisioned,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        let config = self.updateable_config.pinned();

        let metadata_writer = self.metadata_manager.writer();

        // Start metadata manager
        spawn_metadata_manager(self.metadata_manager)?;

        // spawn the node rpc server first to enable connecting to the metadata store
        TaskCenter::spawn(TaskKind::RpcServer, "node-rpc-server", {
            let common_options = config.common.clone();
            let connection_manager = self.networking.connection_manager().clone();
            let metadata_store_client = self.metadata_store_client.clone();
            async move {
                NetworkServer::run(
                    connection_manager,
                    self.server_builder,
                    common_options,
                    metadata_store_client,
                )
                .await?;
                Ok(())
            }
        })?;

        // wait until the node rpc server is up and running before continuing
        let node_rpc_status = TaskCenter::with_current(|tc| tc.health().node_rpc_status());
        node_rpc_status.wait_for_value(NodeRpcStatus::Ready).await;

        if let Some(metadata_server) = self.metadata_server_role {
            TaskCenter::spawn(
                TaskKind::MetadataServer,
                "metadata-server",
                metadata_server.run(Some(metadata_writer.clone())),
            )?;
        }

        if config.common.auto_provision {
            if self.is_provisioned {
                debug!(
                    "No need to auto-provision cluster '{}' because it has been provisioned before",
                    config.common.cluster_name()
                );
            } else {
                TaskCenter::spawn(TaskKind::SystemBoot, "auto-provision-cluster", {
                    let cluster_configuration = ClusterConfiguration::from_configuration(&config);
                    let metadata_store_client = self.metadata_store_client.clone();
                    let common_opts = config.common.clone();
                    async move {
                        let response = provision_cluster_metadata(
                            &metadata_store_client,
                            &common_opts,
                            &cluster_configuration,
                        )
                        .await;

                        match response {
                            Ok(provisioned) => {
                                if provisioned {
                                    info!(
                                        "Cluster '{}' has been automatically provisioned",
                                        common_opts.cluster_name()
                                    );
                                } else {
                                    debug!("The cluster is already provisioned");
                                }
                            }
                            Err(err) => {
                                warn!(
                                    %err,
                                    "Failed to auto provision the cluster. In order to continue you have to provision the cluster manually"
                                );
                            }
                        }

                        Ok(())
                    }
                })?;
            }
        }

        let initialization_timeout = config.common.initialization_timeout.into();

        tokio::time::timeout(
            initialization_timeout,
            NodeInit::new(
                &self.metadata_store_client,
                &metadata_writer,
                self.is_provisioned,
            )
            .init(),
        )
        .await
            .context("Giving up trying to initialize the node. Make sure that it can reach the metadata store and don't forget to provision the cluster on a fresh start")?
            .context("Failed initializing the node")?;

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let my_node_id = Metadata::with_current(|m| m.my_node_id());
        let my_node_config = nodes_config
            .find_node_by_id(my_node_id)
            .expect("should be present");

        // Start partition routing information refresher
        spawn_partition_routing_refresher(self.partition_routing_refresher)?;

        // todo this is a temporary solution to announce the updated NodesConfiguration to the
        //  configured admin nodes. It should be removed once we have a gossip-based node status
        //  protocol. Notifying the admin nodes is done on a best effort basis in case one admin
        //  node is currently not available. This is ok, because the admin nodes will periodically
        //  sync the NodesConfiguration from the metadata store themselves.
        for admin_node in nodes_config.get_admin_nodes() {
            // we don't have to announce us to ourselves
            if admin_node.current_generation != my_node_id {
                let admin_node_id = admin_node.current_generation;
                let networking = self.networking.clone();

                TaskCenter::spawn_unmanaged(
                    TaskKind::Disposable,
                    "announce-node-at-admin-node",
                    async move {
                        if let Err(err) = networking.node_connection(admin_node_id).await {
                            debug!("Failed connecting to admin node '{admin_node_id}' and announcing myself. This can indicate network problems: {err}");
                        }
                    },
                )?;
            }
        }

        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in new tc scope to have access to metadata()
        self.bifrost.start().await?;

        #[cfg(feature = "replicated-loglet")]
        if let Some(log_server) = self.log_server {
            log_server.start(metadata_writer).await?;
        }

        if let Some(admin_role) = self.admin_role {
            TaskCenter::spawn(TaskKind::SystemBoot, "admin-init", admin_role.start())?;
        }

        if let Some(worker_role) = self.worker_role {
            TaskCenter::spawn(TaskKind::SystemBoot, "worker-init", worker_role.start())?;
        }

        if let Some(ingress_role) = self.ingress_role {
            TaskCenter::spawn(TaskKind::IngressServer, "ingress-http", ingress_role.run())?;
        }

        self.base_role.start()?;

        let my_roles = my_node_config.roles;
        // Report that the node is running when all roles are ready
        let _ = TaskCenter::spawn(TaskKind::Disposable, "status-report", async move {
            let health = TaskCenter::with_current(|tc| tc.health().clone());
            health
                .node_rpc_status()
                .wait_for_value(NodeRpcStatus::Ready)
                .await;
            trace!("Node-to-node networking is ready");
            for role in my_roles {
                match role {
                    Role::Worker => {
                        health
                            .worker_status()
                            .wait_for_value(WorkerStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::Admin => {
                        health
                            .admin_status()
                            .wait_for_value(AdminStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::MetadataServer => {
                        health
                            .metadata_server_status()
                            .wait_for(|status| status.is_running())
                            .await;
                        trace!("Metadata role is reporting ready");
                    }

                    Role::LogServer => {
                        health
                            .log_server_status()
                            .wait_for_value(LogServerStatus::Ready)
                            .await;
                        trace!("Log-server is reporting ready");
                    }
                    Role::HttpIngress => {
                        health
                            .ingress_status()
                            .wait_for_value(IngressStatus::Ready)
                            .await;
                        trace!("Ingress is reporting ready");
                    }
                }
            }
            info!("Restate node roles [{}] were started", my_roles);
            Ok(())
        });

        Ok(())
    }

    pub fn bifrost(&self) -> restate_bifrost::Bifrost {
        self.bifrost.handle()
    }

    pub fn metadata_store_client(&self) -> MetadataStoreClient {
        self.metadata_store_client.clone()
    }

    pub fn metadata_writer(&self) -> restate_core::MetadataWriter {
        self.metadata_manager.writer()
    }
}

#[derive(Clone, Debug, IntoProst)]
#[prost(target = "restate_types::protobuf::cluster::ClusterConfiguration")]
pub struct ClusterConfiguration {
    #[into_prost(map = "num_partitions_to_u32")]
    pub num_partitions: NonZeroU16,
    pub partition_replication: PartitionReplication,
    #[prost(required)]
    pub bifrost_provider: ProviderConfiguration,
}

fn num_partitions_to_u32(num_partitions: NonZeroU16) -> u32 {
    u32::from(num_partitions.get())
}

impl ClusterConfiguration {
    pub fn from_configuration(configuration: &Configuration) -> Self {
        ClusterConfiguration {
            num_partitions: configuration.common.default_num_partitions,
            partition_replication: configuration.admin.default_partition_replication.clone(),
            bifrost_provider: ProviderConfiguration::from_configuration(configuration),
        }
    }
}

/// Provision the cluster metadata. Returns `true` if the cluster was newly provisioned. Returns
/// `false` if the cluster is already provisioned.
///
/// This method returns an error if any of the initial metadata couldn't be written to the
/// metadata store. In this case, the method does not try to clean the already written metadata
/// up. Instead, the caller can retry to complete the provisioning.
async fn provision_cluster_metadata(
    metadata_store_client: &MetadataStoreClient,
    common_opts: &CommonOptions,
    cluster_configuration: &ClusterConfiguration,
) -> anyhow::Result<bool> {
    let (initial_nodes_configuration, initial_partition_table, initial_logs) =
        generate_initial_metadata(common_opts, cluster_configuration);

    let result = retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        metadata_store_client.provision(&initial_nodes_configuration)
    })
    .await?;

    retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        write_initial_logs_dont_fail_if_it_exists(
            metadata_store_client,
            BIFROST_CONFIG_KEY.clone(),
            initial_logs.clone(),
        )
    })
    .await
    .context("failed provisioning the initial logs")?;

    // NOTE:
    // The partition table metadata must be initialized only after the bifrost (logs) metadata.
    // Otherwise, the logs controller may begin creating logs with incorrect configurations
    // initialized by bifrost.
    retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        write_initial_value_dont_fail_if_it_exists(
            metadata_store_client,
            PARTITION_TABLE_KEY.clone(),
            &initial_partition_table,
        )
    })
    .await
    .context("failed provisioning the initial partition table")?;

    Ok(result)
}

fn create_initial_nodes_configuration(common_opts: &CommonOptions) -> NodesConfiguration {
    let mut initial_nodes_configuration =
        NodesConfiguration::new(Version::MIN, common_opts.cluster_name().to_owned());
    let node_config = NodeConfig::new(
        common_opts.node_name().to_owned(),
        common_opts
            .force_node_id
            .map(|force_node_id| force_node_id.with_generation(1))
            .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID),
        common_opts.location().clone(),
        common_opts.advertised_address.clone(),
        common_opts.roles,
        LogServerConfig::default(),
        MetadataServerConfig::default(),
    );
    initial_nodes_configuration.upsert_node(node_config);
    initial_nodes_configuration
}

fn generate_initial_metadata(
    common_opts: &CommonOptions,
    cluster_configuration: &ClusterConfiguration,
) -> (NodesConfiguration, PartitionTable, Logs) {
    let mut initial_partition_table_builder = PartitionTableBuilder::default();
    initial_partition_table_builder
        .with_equally_sized_partitions(cluster_configuration.num_partitions.get())
        .expect("Empty partition table should not have conflicts");
    initial_partition_table_builder
        .set_partition_replication(cluster_configuration.partition_replication.clone());
    let initial_partition_table = initial_partition_table_builder.build();

    let initial_logs = Logs::with_logs_configuration(LogsConfiguration::from(
        cluster_configuration.bifrost_provider.clone(),
    ));

    let initial_nodes_configuration = create_initial_nodes_configuration(common_opts);

    (
        initial_nodes_configuration,
        initial_partition_table,
        initial_logs,
    )
}

async fn write_initial_value_dont_fail_if_it_exists<T: Versioned + StorageEncode>(
    metadata_store_client: &MetadataStoreClient,
    key: ByteString,
    initial_value: &T,
) -> Result<(), WriteError> {
    match metadata_store_client
        .put(key, initial_value, Precondition::DoesNotExist)
        .await
    {
        Ok(_) => Ok(()),
        Err(WriteError::FailedPrecondition(_)) => {
            // we might have failed on a previous attempt after writing this value; so let's continue
            Ok(())
        }
        Err(err) => Err(err),
    }
}

async fn write_initial_logs_dont_fail_if_it_exists(
    metadata_store_client: &MetadataStoreClient,
    key: ByteString,
    initial_value: Logs,
) -> Result<(), ReadWriteError> {
    let value = metadata_store_client
        .read_modify_write(key, |current| match current {
            None => Ok(initial_value.clone()),
            Some(current_value) => {
                if current_value.configuration() == initial_value.configuration() {
                    Err(ReadModifyWriteError::FailedOperation(AlreadyInitialized))
                } else if current_value.version() == Version::MIN && current_value.num_logs() == 0 {
                    let builder = initial_value.clone().into_builder();
                    // make sure version is incremented to MIN + 1
                    Ok(builder.build())
                } else {
                    Err(ReadModifyWriteError::FailedOperation(AlreadyInitialized))
                }
            }
        })
        .await;

    match value {
        Ok(_) | Err(ReadModifyWriteError::FailedOperation(_)) => Ok(()),
        Err(ReadModifyWriteError::ReadWrite(err)) => Err(err),
    }
}

#[derive(Debug)]
struct AlreadyInitialized;

#[cfg(not(feature = "replicated-loglet"))]
fn warn_if_log_store_left_artifacts(config: &Configuration) {
    if config.log_server.data_dir().exists() {
        tracing::warn!("Log server data directory '{}' exists, \
            but log-server is not implemented in this version of restate-server. \
            This may indicate that the log-server role was previously enabled and the data directory was not cleaned up. If this was created by v1.1.1 of restate-server, please remove this directory to avoid potential future conflicts.", config.log_server.data_dir().display());
    }
}
