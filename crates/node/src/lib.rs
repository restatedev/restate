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
mod failure_detector;
mod init;
mod metric_definitions;
mod network_server;
mod roles;

use std::time::Duration;

use anyhow::Context;
use prost_dto::IntoProst;
use std::sync::Arc;
use tracing::{debug, error, info, trace, warn};

use codederror::CodedError;
use restate_bifrost::BifrostService;
use restate_core::network::{
    GrpcConnector, MessageRouterBuilder, NetworkServerBuilder, Networking,
};
use restate_core::partitions::PartitionRouting;
use restate_core::{Metadata, MetadataKind, MetadataWriter, TaskKind};
use restate_core::{MetadataBuilder, MetadataManager, TaskCenter, spawn_metadata_manager};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_log_server::LogServerService;
use restate_metadata_server::{
    BoxedMetadataServer, MetadataServer, MetadataStoreClient, ReadModifyWriteError,
};
use restate_metadata_store::{ReadWriteError, WriteError, retry_on_retryable_error};
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::config::{CommonOptions, Configuration};
use restate_types::health::NodeStatus;
use restate_types::live::Live;
use restate_types::live::LiveLoadExt;
use restate_types::logs::RecordCache;
use restate_types::logs::metadata::{Logs, LogsConfiguration, ProviderConfiguration, ProviderKind};
use restate_types::metadata::{GlobalMetadata, Precondition};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::{PartitionReplication, PartitionTable, PartitionTableBuilder};
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::protobuf::common::{
    AdminStatus, IngressStatus, LogServerStatus, NodeRpcStatus, WorkerStatus,
};
use restate_types::{GenerationalNodeId, Version, Versioned};

use self::failure_detector::FailureDetector;
use crate::cluster_marker::ClusterValidationError;
use crate::init::NodeInit;
use crate::network_server::NetworkServer;
use crate::roles::{AdminRole, IngressRole, WorkerRole};

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
    bifrost: BifrostService,
    metadata_server_role: Option<BoxedMetadataServer>,
    failure_detector: FailureDetector<Networking<GrpcConnector>>,
    admin_role: Option<AdminRole<GrpcConnector>>,
    worker_role: Option<WorkerRole>,
    ingress_role: Option<IngressRole<GrpcConnector>>,
    log_server: Option<LogServerService>,
    networking: Networking<GrpcConnector>,
    is_provisioned: bool,
    prometheus: Prometheus,
}

impl Node {
    pub async fn create(
        updateable_config: Live<Configuration>,
        prometheus: Prometheus,
    ) -> Result<Self, BuildError> {
        metric_definitions::describe_metrics();
        let mut server_builder = NetworkServerBuilder::default();
        let config = updateable_config.pinned();

        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        // We need to se the global metadata as soon as possible since the metadata store client's
        // auto update functionality won't be started if the global metadata is not set.
        let is_set = TaskCenter::try_set_global_metadata(metadata.clone());
        let tc = TaskCenter::current();
        debug_assert!(is_set, "Global metadata was already set");

        let is_provisioned =
            cluster_marker::validate_and_update_cluster_marker(config.common.cluster_name())?;

        // If MetadataServerKind::Local and Role::MetadataServer are configured,
        // we use an in-memory client, ignoring the rest of the client config.
        // Client kind defaults to MetadataClientKind::Replicated, so we turn a
        // blind eye to that, but reject other values (etcd, object-store) to
        // avoid confusion. This disparity will disappear once we remove the
        // local metadata server as an option.
        if !matches!(
            &config.common.metadata_client.kind,
            restate_types::config::MetadataClientKind::Replicated { .. }
        ) && config.has_role(Role::MetadataServer)
        {
            return Err(BuildError::InvalidConfiguration(anyhow::anyhow!(
                "Detected possible misconfiguration. This node runs a metadata \
                server but is configured to use the \"{}\" metadata client. If you \
                don't want to run a metadata server, remove the metadata-server role. If you \
                would like to use the Restate metadata store, then set \
                metadata-client.type = \"replicated\" or leave it unset",
                config.common.metadata_client.kind,
            )));
        };

        let (metadata_store_role, metadata_store_client) = if config.has_role(Role::MetadataServer)
        {
            restate_metadata_server::create_metadata_server_and_client(
                updateable_config.clone(),
                tc.health().metadata_server_status(),
                &mut server_builder,
            )
            .await
            .map(|(server, client)| (Some(server), client))?
        } else {
            let metadata_store_client =
                restate_metadata_providers::create_client(config.common.metadata_client.clone())
                    .await
                    .map_err(BuildError::MetadataStoreClient)?;
            (None, metadata_store_client)
        };

        let mut metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        let mut router_builder = MessageRouterBuilder::default();
        let networking = Networking::with_grpc_connector();
        metadata_manager.register_in_message_router(&mut router_builder);
        let replica_set_states = PartitionReplicaSetStates::default();

        let record_cache = RecordCache::new(
            Configuration::pinned()
                .bifrost
                .record_cache_memory_size
                .as_usize(),
        );

        // Setup Bifrost replicated-loglet
        let replicated_loglet_factory = restate_bifrost::providers::replicated_loglet::Factory::new(
            networking.clone(),
            record_cache.clone(),
            &mut router_builder,
        );

        let bifrost_svc = BifrostService::new(metadata_manager.writer());

        let bifrost_svc = bifrost_svc.enable_local_loglet(
            updateable_config
                .clone()
                .map(|config| &config.bifrost.local)
                .boxed(),
        );

        let bifrost_svc = bifrost_svc.with_factory(replicated_loglet_factory);

        #[cfg(feature = "memory-loglet")]
        let bifrost_svc = bifrost_svc.enable_in_memory_loglet();

        let bifrost = bifrost_svc.handle();

        let log_server = if config.has_role(Role::LogServer) {
            Some(
                LogServerService::create(
                    tc.health().log_server_status(),
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
                    tc.health().worker_status(),
                    replica_set_states.clone(),
                    &mut router_builder,
                    networking.clone(),
                    bifrost_svc.handle(),
                    metadata_manager.writer(),
                )
                .await?,
            )
        } else {
            None
        };

        // In v1.4.0 we start ingress role even if not explicitly configured. This allows softer
        // migration from v1.3.x to v1.4.0. In v1.5, ingress will be started _only_ if
        // http-ingress role is explicitly configured.
        let ingress_role = if config.has_role(Role::HttpIngress) || config.has_role(Role::Worker) {
            if !config.has_role(Role::HttpIngress) {
                info!(
                    "!!! This node has a `worker` role and no explicit `http-ingress` role. `http-ingress` will be started anyway in this version. In v1.5, running ingress will require the role `http-ingress` to be set."
                );
            }

            Some(IngressRole::create(
                updateable_config
                    .clone()
                    .map(|config| &config.ingress)
                    .boxed(),
                tc.health().ingress_status(),
                networking.clone(),
                metadata.updateable_schema(),
                metadata.updateable_partition_table(),
                PartitionRouting::new(replica_set_states.clone(), tc.clone()),
            ))
        } else {
            None
        };

        let admin_role = if config.has_role(Role::Admin) {
            Some(
                AdminRole::create(
                    tc.health().admin_status(),
                    bifrost.clone(),
                    updateable_config.clone(),
                    PartitionRouting::new(replica_set_states.clone(), tc),
                    metadata.updateable_partition_table(),
                    replica_set_states.clone(),
                    networking.clone(),
                    metadata,
                    metadata_manager.writer(),
                    &mut server_builder,
                    worker_role
                        .as_ref()
                        .map(|worker_role| worker_role.storage_query_context().clone()),
                )
                .await?,
            )
        } else {
            None
        };

        let failure_detector = FailureDetector::new(
            &updateable_config.pinned().common.gossip,
            networking.clone(),
            &mut router_builder,
            replica_set_states,
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
            bifrost: bifrost_svc,
            metadata_server_role: metadata_store_role,
            failure_detector,
            admin_role,
            ingress_role,
            worker_role,
            log_server,
            server_builder,
            networking,
            is_provisioned,
            prometheus,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        let config = self.updateable_config.pinned();

        let metadata_writer = self.metadata_manager.writer();
        let metadata = Metadata::current();

        // Start metadata manager
        spawn_metadata_manager(self.metadata_manager)?;

        // spawn the node rpc server first to enable connecting to the metadata store
        TaskCenter::spawn(TaskKind::RpcServer, "node-rpc-server", {
            let common_options = config.common.clone();
            let connection_manager = self.networking.connection_manager().clone();
            let metadata_writer = metadata_writer.clone();
            async move {
                NetworkServer::run(
                    connection_manager,
                    self.server_builder,
                    common_options,
                    metadata_writer,
                    self.prometheus,
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
                    let metadata_writer = metadata_writer.clone();
                    let common_opts = config.common.clone();
                    async move {
                        let response = provision_cluster_metadata(
                            &metadata_writer,
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

        let my_node_config = tokio::time::timeout(
            initialization_timeout,
            NodeInit::new(
                &metadata_writer,
                self.is_provisioned,
            )
            .init(),
        )
        .await
            .context("Giving up trying to initialize the node. Make sure that it can reach the metadata store and don't forget to provision the cluster on a fresh start")?
            .context("Failed initializing the node")?;

        self.failure_detector
            .start(self.updateable_config.clone().map(|c| &c.common.gossip))?;

        // Wait for initial metadata sync.
        //
        // Note that the sync can happen via adhoc peer-to-peer connection since failure
        // detector/gossip service has been started as well.
        let initial_fetch_dur = Duration::from_secs(15);
        let (logs_version, partition_table_version) = match futures::future::join(
            metadata.wait_for_version(MetadataKind::Logs, Version::MIN),
            metadata.wait_for_version(MetadataKind::PartitionTable, Version::MIN),
        )
        .log_slow_after(
            initial_fetch_dur,
            tracing::Level::INFO,
            "Initial fetch of global metadata",
        )
        .with_overdue(initial_fetch_dur * 2, tracing::Level::WARN)
        .await
        {
            (Ok(logs_version), Ok(partition_table_version)) => {
                (logs_version, partition_table_version)
            }
            _ => {
                anyhow::bail!(
                    "Failed to fetch the latest metadata when initializing the node, maybe server is shutting down"
                );
            }
        };

        // Bifrost expects metadata to be in sync, which it is by this point.
        self.bifrost.start().await?;

        let nodes_config = metadata.nodes_config_ref();
        info!(
            node_name = %my_node_config.name,
            roles = %my_node_config.roles,
            address = %my_node_config.address,
            location = %my_node_config.location,
            nodes_config_version = %metadata.nodes_config_version(),
            cluster_name = %nodes_config.cluster_name(),
            cluster_fingerprint =?nodes_config.cluster_fingerprint(),
            %partition_table_version,
            %logs_version,
            "My Node ID is {}", my_node_config.current_generation,
        );

        let my_node_id = metadata.my_node_id();
        debug_assert!(nodes_config.find_node_by_id(my_node_id).is_ok());

        migrate_logs_configuration_to_replicated_if_needed(
            &nodes_config,
            &metadata,
            &metadata_writer,
            &config,
        )
        .await?;

        if let Some(log_server) = self.log_server {
            log_server.start(metadata_writer).await?;
        }

        if let Some(ingress_role) = self.ingress_role {
            TaskCenter::spawn(TaskKind::IngressServer, "ingress-http", ingress_role.run())?;
        }

        if let Some(worker_role) = self.worker_role {
            worker_role.start()?;
        }

        if let Some(admin_role) = self.admin_role {
            TaskCenter::spawn(TaskKind::SystemBoot, "admin-init", admin_role.start())?;
        }

        let my_roles = my_node_config.roles;
        // Report that the node is running when all roles are ready
        let _ = TaskCenter::spawn(TaskKind::Disposable, "status-report", async move {
            let health = TaskCenter::with_current(|tc| tc.health().clone());
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
            health.node_status().update(NodeStatus::Alive);
            info!("Restate node roles [{}] were started", my_roles);
            Ok(())
        });

        Ok(())
    }

    pub fn bifrost(&self) -> restate_bifrost::Bifrost {
        self.bifrost.handle()
    }

    pub fn metadata_store_client(&self) -> MetadataStoreClient {
        self.metadata_manager
            .writer()
            .raw_metadata_store_client()
            .clone()
    }

    pub fn metadata_writer(&self) -> restate_core::MetadataWriter {
        self.metadata_manager.writer()
    }
}

#[derive(Clone, Debug, IntoProst)]
#[prost(target = "restate_types::protobuf::cluster::ClusterConfiguration")]
pub struct ClusterConfiguration {
    pub num_partitions: u16,
    pub partition_replication: PartitionReplication,
    #[prost(required)]
    pub bifrost_provider: ProviderConfiguration,
}

impl ClusterConfiguration {
    pub fn from_configuration(configuration: &Configuration) -> Self {
        ClusterConfiguration {
            num_partitions: configuration.common.default_num_partitions,
            partition_replication: configuration.common.default_replication.clone().into(),
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
    metadata_writer: &MetadataWriter,
    common_opts: &CommonOptions,
    cluster_configuration: &ClusterConfiguration,
) -> anyhow::Result<bool> {
    let (initial_nodes_configuration, initial_partition_table, initial_logs) =
        generate_initial_metadata(common_opts, cluster_configuration);

    let result = retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        metadata_writer
            .raw_metadata_store_client()
            .provision(&initial_nodes_configuration)
    })
    .await?;

    retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        write_initial_logs_dont_fail_if_it_exists(metadata_writer, initial_logs.clone())
    })
    .await
    .context("failed provisioning the initial logs")?;

    // NOTE:
    // The partition table metadata must be initialized only after the bifrost (logs) metadata.
    // Otherwise, the logs controller may begin creating logs with incorrect configurations
    // initialized by bifrost.
    let initial_partition_table = Arc::new(initial_partition_table);
    retry_on_retryable_error(common_opts.network_error_retry_policy.clone(), || {
        write_initial_value_dont_fail_if_it_exists(
            metadata_writer,
            Arc::clone(&initial_partition_table),
        )
    })
    .await
    .context("failed provisioning the initial partition table")?;

    Ok(result)
}

fn create_initial_nodes_configuration(common_opts: &CommonOptions) -> NodesConfiguration {
    let mut initial_nodes_configuration =
        NodesConfiguration::new(Version::MIN, common_opts.cluster_name().to_owned());
    let node_config = NodeConfig::builder()
        .name(common_opts.node_name().to_owned())
        .current_generation(
            common_opts
                .force_node_id
                .map(|force_node_id| force_node_id.with_generation(1))
                .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID),
        )
        .location(common_opts.location().clone())
        .address(common_opts.advertised_address.clone())
        .roles(common_opts.roles)
        .build();
    initial_nodes_configuration.upsert_node(node_config);
    initial_nodes_configuration
}

fn generate_initial_metadata(
    common_opts: &CommonOptions,
    cluster_configuration: &ClusterConfiguration,
) -> (NodesConfiguration, PartitionTable, Logs) {
    let mut initial_partition_table_builder = PartitionTableBuilder::default();
    initial_partition_table_builder
        .with_equally_sized_partitions(cluster_configuration.num_partitions)
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

async fn write_initial_value_dont_fail_if_it_exists<T: GlobalMetadata>(
    metadata_writer: &MetadataWriter,
    initial_value: Arc<T>,
) -> Result<(), WriteError> {
    match metadata_writer
        .global_metadata()
        .put(initial_value, Precondition::DoesNotExist)
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
    metadata_writer: &MetadataWriter,
    initial_value: Logs,
) -> Result<(), ReadWriteError> {
    let value = metadata_writer
        .global_metadata()
        .read_modify_write(|current| match current {
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

/// Update the logs configuration metadata on single nodes, if it is still using the local loglet
/// when the configured provider is replicated (default in 1.4.0+). This migrates older nodes
/// provisioned with default config from versions <1.4.0 to use the replicated loglet. Does not
/// update logs, we leave it to Bifrost auto-improvement to create the new segments.
async fn migrate_logs_configuration_to_replicated_if_needed(
    nodes_config: &NodesConfiguration,
    metadata: &Metadata,
    metadata_writer: &MetadataWriter,
    config: &Configuration,
) -> Result<(), anyhow::Error> {
    if nodes_config.len() != 1 {
        // Bail if this is a multi-node cluster.
        return Ok(());
    }

    if !config.has_role(Role::LogServer) {
        // This means that roles were explicitly set; we can not migrate to replicated.
        return Ok(());
    }

    let logs = metadata.logs_ref();
    let logs_configuration = logs.configuration();

    if matches!(config.bifrost.default_provider, ProviderKind::Replicated)
        && matches!(
            logs_configuration.default_provider.kind(),
            ProviderKind::Local
        )
    {
        info!("Migrating default loglet configuration to replicated");
        let result = metadata_writer
            .global_metadata()
            .read_modify_write(|current: Option<Arc<Logs>>| {
                let logs = current.expect("logs are initialized");
                let mut builder = logs.as_ref().clone().into_builder();

                // Why not ProviderConfiguration::from_configuration(config)? Using the default
                // replicated loglet provider configuration is a safer alternative as there might be
                // untested config changes that would fail later, while this is an appropriate
                // successor to the pre-1.4.0 default local loglet config for single nodes. Anything
                // more complex should be done as an explicit reconfiguration by the operator.
                let logs_configuration = LogsConfiguration {
                    default_provider: ProviderConfiguration::default(),
                };
                builder.set_configuration(logs_configuration);

                let Some(logs) = builder.build_if_modified() else {
                    return Err(anyhow::anyhow!("already up to date"));
                };

                Ok(logs)
            })
            .await;

        match result {
            Ok(_) => {}
            Err(err) => {
                warn!(
                    "Failed to update the default loglet configuration in metadata: {}",
                    err
                );
            }
        }
    }

    Ok(())
}
