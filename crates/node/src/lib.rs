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
mod network_server;
mod roles;

use std::sync::Arc;

use tracing::{debug, error, info, trace};

use codederror::CodedError;
use restate_bifrost::BifrostService;
use restate_core::metadata_store::{retry_on_network_error, ReadWriteError};
use restate_core::network::{
    GrpcConnector, MessageRouterBuilder, NetworkServerBuilder, Networking,
};
use restate_core::partitions::{spawn_partition_routing_refresher, PartitionRoutingRefresher};
use restate_core::{
    spawn_metadata_manager, MetadataBuilder, MetadataKind, MetadataManager, TargetVersion,
};
use restate_core::{task_center, TaskKind};
#[cfg(feature = "replicated-loglet")]
use restate_log_server::LogServerService;
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::{CommonOptions, Configuration};
use restate_types::errors::GenericError;
use restate_types::health::Health;
use restate_types::live::Live;
#[cfg(feature = "replicated-loglet")]
use restate_types::logs::RecordCache;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
use restate_types::protobuf::common::{
    AdminStatus, IngressStatus, LogServerStatus, MetadataServerStatus, NodeStatus, WorkerStatus,
};
use restate_types::Version;

use crate::cluster_marker::ClusterValidationError;
use crate::network_server::NetworkServer;
use crate::roles::{AdminRole, BaseRole, IngressRole, WorkerRole};

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
    MetadataStoreClient(GenericError),
}

pub struct Node {
    health: Health,
    server_builder: NetworkServerBuilder,
    updateable_config: Live<Configuration>,
    metadata_manager: MetadataManager,
    partition_routing_refresher: PartitionRoutingRefresher,
    metadata_store_client: MetadataStoreClient,
    bifrost: BifrostService,
    metadata_store_role: Option<LocalMetadataStoreService>,
    base_role: BaseRole,
    admin_role: Option<AdminRole<GrpcConnector>>,
    worker_role: Option<WorkerRole>,
    ingress_role: Option<IngressRole<GrpcConnector>>,
    #[cfg(feature = "replicated-loglet")]
    log_server: Option<LogServerService>,
    networking: Networking<GrpcConnector>,
}

impl Node {
    pub async fn create(updateable_config: Live<Configuration>) -> Result<Self, BuildError> {
        let tc = task_center();
        let health = Health::default();
        health.node_status().update(NodeStatus::StartingUp);
        let mut server_builder = NetworkServerBuilder::default();
        let config = updateable_config.pinned();

        cluster_marker::validate_and_update_cluster_marker(config.common.cluster_name())?;

        // todo(asoli) move local metadata store to use NetworkServer
        let metadata_store_role = if config.has_role(Role::MetadataStore) {
            Some(LocalMetadataStoreService::from_options(
                health.metadata_server_status(),
                updateable_config.clone().map(|c| &c.metadata_store).boxed(),
                updateable_config
                    .clone()
                    .map(|config| &config.metadata_store.rocksdb)
                    .boxed(),
            ))
        } else {
            None
        };

        let metadata_store_client = restate_metadata_store::local::create_client(
            config.common.metadata_store_client.clone(),
        )
        .await
        .map_err(BuildError::MetadataStoreClient)?;

        let mut router_builder = MessageRouterBuilder::default();
        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        let networking = Networking::new(metadata_builder.to_metadata(), config.networking.clone());
        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        metadata_manager.register_in_message_router(&mut router_builder);
        let partition_routing_refresher =
            PartitionRoutingRefresher::new(metadata_store_client.clone());

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
            tc.clone(),
            metadata_store_client.clone(),
            networking.clone(),
            record_cache.clone(),
            &mut router_builder,
        );
        let bifrost_svc = BifrostService::new(tc.clone(), metadata.clone())
            .enable_local_loglet(&updateable_config);

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
                    health.log_server_status(),
                    updateable_config.clone(),
                    tc.clone(),
                    metadata.clone(),
                    metadata_store_client.clone(),
                    record_cache,
                    &mut router_builder,
                )
                .await?,
            )
        } else {
            None
        };

        let worker_role = if config.has_role(Role::Worker) {
            Some(
                WorkerRole::create(
                    health.worker_status(),
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
                health.ingress_status(),
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
                    health.admin_status(),
                    tc.clone(),
                    bifrost.clone(),
                    updateable_config.clone(),
                    metadata,
                    partition_routing_refresher.partition_routing(),
                    networking.clone(),
                    metadata_manager.writer(),
                    &mut server_builder,
                    &mut router_builder,
                    metadata_store_client.clone(),
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
            health,
            updateable_config,
            metadata_manager,
            partition_routing_refresher,
            bifrost: bifrost_svc,
            metadata_store_role,
            metadata_store_client,
            base_role,
            admin_role,
            ingress_role,
            worker_role,
            #[cfg(feature = "replicated-loglet")]
            log_server,
            server_builder,
            networking,
        })
    }

    pub async fn start(mut self) -> Result<(), anyhow::Error> {
        let tc = task_center();

        let config = self.updateable_config.pinned();

        if let Some(metadata_store) = self.metadata_store_role {
            tc.spawn(
                TaskKind::MetadataStore,
                "local-metadata-store",
                None,
                async move {
                    metadata_store.run().await?;
                    Ok(())
                },
            )?;
        }

        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata().clone();
        let is_set = tc.try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        spawn_metadata_manager(&tc, self.metadata_manager)?;

        // Start partition routing information refresher
        spawn_partition_routing_refresher(&tc, self.partition_routing_refresher)?;

        let nodes_config =
            Self::upsert_node_config(&self.metadata_store_client, &config.common).await?;
        metadata_writer.update(Arc::new(nodes_config)).await?;

        if config.common.allow_bootstrap {
            // todo write bootstrap state
        }

        // fetch the latest schema information
        metadata
            .sync(MetadataKind::Schema, TargetVersion::Latest)
            .await?;

        let nodes_config = metadata.nodes_config_ref();

        // Find my node in nodes configuration.
        let my_node_config = nodes_config
            .find_node_by_name(config.common.node_name())
            .expect("node config should have been upserted");

        let my_node_id = my_node_config.current_generation;

        // Safety checks, same node (if set)?
        if config
            .common
            .force_node_id
            .is_some_and(|n| n != my_node_id.as_plain())
        {
            return Err(Error::SafetyCheck(
                format!(
                    "Node ID mismatch: configured node ID is {}, but the nodes configuration contains {}",
                    config.common.force_node_id.unwrap(),
                    my_node_id.as_plain()
                )))?;
        }

        // Same cluster?
        if config.common.cluster_name() != nodes_config.cluster_name() {
            return Err(Error::SafetyCheck(
                format!(
                    "Cluster name mismatch: configured cluster name is '{}', but the nodes configuration contains '{}'",
                    config.common.cluster_name(),
                    nodes_config.cluster_name()
                )))?;
        }

        // My Node ID is set
        metadata_writer.set_my_node_id(my_node_id);
        restate_tracing_instrumentation::set_global_node_id(my_node_id);

        info!(
            roles = %my_node_config.roles,
            address = %my_node_config.address,
            "My Node ID is {}", my_node_config.current_generation);

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

                tc.spawn_unmanaged(TaskKind::Disposable, "announce-node-at-admin-node", None, async move {
                    if let Err(err) = networking
                        .node_connection(admin_node_id)
                        .await
                    {
                        info!("Failed connecting to admin node '{admin_node_id}' and announcing myself. This can indicate network problems: {err}");
                    }
                })?;
            }
        }

        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in new tc scope to have access to metadata()
        tc.run_in_scope("bifrost-init", None, self.bifrost.start())
            .await?;

        #[cfg(feature = "replicated-loglet")]
        if let Some(log_server) = self.log_server {
            log_server
                .start(metadata_writer, &mut self.server_builder)
                .await?;
        }

        if let Some(admin_role) = self.admin_role {
            tc.spawn(TaskKind::SystemBoot, "admin-init", None, admin_role.start())?;
        }

        if let Some(worker_role) = self.worker_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "worker-init",
                None,
                worker_role.start(),
            )?;
        }

        if let Some(ingress_role) = self.ingress_role {
            tc.spawn_child(TaskKind::Ingress, "ingress-http", None, ingress_role.run())?;
        }

        tc.spawn(TaskKind::RpcServer, "node-rpc-server", None, {
            let health = self.health.clone();
            let common_options = config.common.clone();
            let connection_manager = self.networking.connection_manager().clone();
            async move {
                NetworkServer::run(
                    health,
                    connection_manager,
                    self.server_builder,
                    common_options,
                )
                .await?;
                Ok(())
            }
        })?;

        self.base_role.start()?;

        let my_roles = my_node_config.roles;
        // Report that the node is running when all roles are ready
        let _ = tc.spawn(TaskKind::Disposable, "status-report", None, async move {
            self.health
                .node_status()
                .wait_for_value(NodeStatus::Alive)
                .await;
            trace!("Node-to-node networking is ready");
            for role in my_roles {
                match role {
                    Role::Worker => {
                        self.health
                            .worker_status()
                            .wait_for_value(WorkerStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::Admin => {
                        self.health
                            .admin_status()
                            .wait_for_value(AdminStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::MetadataStore => {
                        self.health
                            .metadata_server_status()
                            .wait_for_value(MetadataServerStatus::Ready)
                            .await;
                        trace!("Metadata role is reporting ready");
                    }

                    Role::LogServer => {
                        self.health
                            .log_server_status()
                            .wait_for_value(LogServerStatus::Ready)
                            .await;
                        trace!("Log-server is reporting ready");
                    }
                    Role::HttpIngress => {
                        self.health
                            .ingress_status()
                            .wait_for_value(IngressStatus::Ready)
                            .await;
                        trace!("Ingress is reporting ready");
                    }
                }
            }
            info!("Restate server is ready");
            Ok(())
        });

        Ok(())
    }

    async fn upsert_node_config(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> Result<NodesConfiguration, Error> {
        retry_on_network_error(common_opts.network_error_retry_policy.clone(), || {
            let mut previous_node_generation = None;
            metadata_store_client.read_modify_write(NODES_CONFIG_KEY.clone(), move |nodes_config| {
                let mut nodes_config = if common_opts.allow_bootstrap {
                    debug!("allow-bootstrap is set to `true`, allowed to create initial NodesConfiguration!");
                    nodes_config.unwrap_or_else(|| {
                        NodesConfiguration::new(
                            Version::INVALID,
                            common_opts.cluster_name().to_owned(),
                        )
                    })
                } else {
                    nodes_config.ok_or(Error::MissingNodesConfiguration)?
                };

                // check whether we have registered before
                let node_config = nodes_config
                    .find_node_by_name(common_opts.node_name())
                    .cloned();

                let my_node_config = if let Some(mut node_config) = node_config {
                    assert_eq!(
                        common_opts.node_name(),
                        node_config.name,
                        "node name must match"
                    );

                    if let Some(previous_node_generation) = previous_node_generation {
                        if node_config
                            .current_generation
                            .is_newer_than(previous_node_generation)
                        {
                            // detected a concurrent registration of the same node
                            return Err(Error::ConcurrentNodeRegistration(
                                common_opts.node_name().to_owned(),
                            ));
                        }
                    } else {
                        // remember the previous node generation to detect concurrent modifications
                        previous_node_generation = Some(node_config.current_generation);
                    }

                    // update node_config
                    node_config.roles = common_opts.roles;
                    node_config.address = common_opts.advertised_address.clone();
                    node_config.current_generation.bump_generation();

                    node_config
                } else {
                    let plain_node_id = common_opts.force_node_id.unwrap_or_else(|| {
                        nodes_config
                            .max_plain_node_id()
                            .map(|n| n.next())
                            .unwrap_or_default()
                    });

                    assert!(
                        nodes_config.find_node_by_id(plain_node_id).is_err(),
                        "duplicate plain node id '{}'",
                        plain_node_id
                    );

                    let my_node_id = plain_node_id.with_generation(1);

                    NodeConfig::new(
                        common_opts.node_name().to_owned(),
                        my_node_id,
                        common_opts.advertised_address.clone(),
                        common_opts.roles,
                        LogServerConfig::default(),
                    )
                };

                nodes_config.upsert_node(my_node_config);
                nodes_config.increment_version();

                Ok(nodes_config)
            })
        })
            .await
            .map_err(|err| err.transpose())
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

#[cfg(not(feature = "replicated-loglet"))]
fn warn_if_log_store_left_artifacts(config: &Configuration) {
    if config.log_server.data_dir().exists() {
        tracing::warn!("Log server data directory '{}' exists, \
            but log-server is not implemented in this version of restate-server. \
            This may indicate that the log-server role was previously enabled and the data directory was not cleaned up. If this was created by v1.1.1 of restate-server, please remove this directory to avoid potential future conflicts.", config.log_server.data_dir().display());
    }
}
