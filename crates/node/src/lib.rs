// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use restate_types::errors::GenericError;
use tokio::sync::oneshot;

use codederror::CodedError;
use restate_bifrost::BifrostService;
use restate_core::network::GrpcConnector;
use restate_core::{task_center, TaskKind};
use restate_core::{Core, CoreBuilder};
#[cfg(feature = "replicated-loglet")]
use restate_log_server::LogServerService;
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::{NodeId, Version};
use tracing::{debug, error};

use crate::cluster_marker::ClusterValidationError;
use crate::network_server::{AdminDependencies, NetworkServer, WorkerDependencies};
use crate::roles::{AdminRole, WorkerRole};

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
    core: Core<GrpcConnector>,
    updateable_config: Live<Configuration>,
    bifrost: BifrostService,
    metadata_store_role: Option<LocalMetadataStoreService>,
    admin_role: Option<AdminRole<GrpcConnector>>,
    worker_role: Option<WorkerRole<GrpcConnector>>,
    #[cfg(feature = "replicated-loglet")]
    log_server: Option<LogServerService>,
    server: NetworkServer,
}

impl Node {
    pub async fn create(updateable_config: Live<Configuration>) -> Result<Self, BuildError> {
        let tc = task_center();
        let config = updateable_config.pinned();
        // ensure we have cluster admin role if bootstrapping.
        if config.common.allow_bootstrap {
            debug!("allow-bootstrap is set to `true`, bootstrapping is allowed!");
            if !config.has_role(Role::Admin) {
                return Err(BuildError::Bootstrap(format!(
                    "Node must include the 'admin' role when starting in bootstrap mode. Currently it has roles {}", config.roles()
                )));
            }

            if !config.has_role(Role::MetadataStore) {
                return Err(BuildError::Bootstrap(format!("Node must include the 'metadata-store' role when starting in bootstrap mode. Currently it has roles {}", config.roles())));
            }
        }

        cluster_marker::validate_and_update_cluster_marker(config.common.cluster_name())?;

        let metadata_store_role = if config.has_role(Role::MetadataStore) {
            Some(LocalMetadataStoreService::from_options(
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

        let mut core = CoreBuilder::with_metadata_store_client(metadata_store_client)
            .with_nodes_config(
                NodesConfiguration::new(Version::INVALID, config.common.cluster_name().to_owned()),
                config.common.node_name().to_owned(),
                config.common.force_node_id.map(NodeId::Plain),
                config.common.advertised_address.clone(),
                config.common.roles,
            )
            .with_grpc_networking(config.networking.clone())
            .with_tc(tc.clone())
            .set_num_partitions(config.common.bootstrap_num_partitions())
            .set_replication_strategy(config.admin.default_replication_strategy)
            .set_provider_kind(config.bifrost.default_provider)
            .set_allow_bootstrap(config.common.allow_bootstrap);

        let updating_schema_information = core.metadata.updateable_schema();

        // Setup bifrost
        // replicated-loglet
        #[cfg(feature = "replicated-loglet")]
        let replicated_loglet_factory = restate_bifrost::providers::replicated_loglet::Factory::new(
            tc.clone(),
            core.metadata_store_client.clone(),
            core.networking.clone(),
            &mut core.router_builder,
        );
        let bifrost_svc = BifrostService::new(tc.clone(), core.metadata.clone())
            .enable_local_loglet(&updateable_config);

        #[cfg(feature = "replicated-loglet")]
        let bifrost_svc = bifrost_svc.with_factory(replicated_loglet_factory);

        #[cfg(feature = "memory-loglet")]
        let bifrost_svc = bifrost_svc.enable_in_memory_loglet();

        #[cfg(not(feature = "replicated-loglet"))]
        warn_if_log_store_left_artifacts(&config);

        #[cfg(feature = "replicated-loglet")]
        let log_server = if config.has_role(Role::LogServer) {
            Some(
                LogServerService::create(
                    updateable_config.clone(),
                    tc.clone(),
                    core.metadata.clone(),
                    core.metadata_store_client.clone(),
                    &mut core.router_builder,
                )
                .await?,
            )
        } else {
            None
        };

        let admin_role = if config.has_role(Role::Admin) {
            let metadata_store_client = core.metadata_store_client.clone();
            Some(
                AdminRole::create(
                    tc.clone(),
                    updateable_config.clone(),
                    core.metadata.clone(),
                    core.networking.clone(),
                    core.metadata_manager.writer(),
                    &mut core.router_builder,
                    metadata_store_client,
                )
                .await?,
            )
        } else {
            None
        };

        let worker_role = if config.has_role(Role::Worker) {
            let networking = core.networking.clone();
            let metadata_store_client = core.metadata_store_client.clone();
            Some(
                WorkerRole::create(
                    core.metadata.clone(),
                    updateable_config.clone(),
                    &mut core.router_builder,
                    networking,
                    bifrost_svc.handle(),
                    metadata_store_client,
                    updating_schema_information,
                )
                .await?,
            )
        } else {
            None
        };

        let server = NetworkServer::new(
            core.networking.connection_manager().clone(),
            worker_role
                .as_ref()
                .map(|worker| WorkerDependencies::new(worker.storage_query_context().clone())),
            admin_role.as_ref().map(|cluster_controller| {
                AdminDependencies::new(
                    cluster_controller.cluster_controller_handle(),
                    core.metadata_store_client.clone(),
                    bifrost_svc.handle(),
                )
            }),
        );

        // Ensures that message router is updated after all services have registered themselves in
        // the builder.
        let core = core.build_router();

        Ok(Node {
            updateable_config,
            core,
            bifrost: bifrost_svc,
            metadata_store_role,
            admin_role,
            worker_role,
            #[cfg(feature = "replicated-loglet")]
            log_server,
            server,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
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

        let started_core = self
            .core
            .start(config.common.network_error_retry_policy.clone())
            .await?;

        let bifrost = self.bifrost.handle();

        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in new tc scope to have access to metadata()
        tc.run_in_scope("bifrost-init", None, self.bifrost.start())
            .await?;

        #[cfg(feature = "replicated-loglet")]
        if let Some(log_server) = self.log_server {
            tc.spawn(
                TaskKind::SystemBoot,
                "log-server-init",
                None,
                log_server.start(started_core.metadata_writer),
            )?;
        }
        #[cfg(not(feature = "replicated-loglet"))]
        drop(started_core);

        let all_partitions_started_rx = if let Some(admin_role) = self.admin_role {
            // todo: This is a temporary fix for https://github.com/restatedev/restate/issues/1651
            let (all_partitions_started_tx, all_partitions_started_rx) = oneshot::channel();
            tc.spawn(
                TaskKind::SystemBoot,
                "admin-init",
                None,
                admin_role.start(
                    config.common.allow_bootstrap,
                    bifrost.clone(),
                    all_partitions_started_tx,
                ),
            )?;

            all_partitions_started_rx
        } else {
            // We don't wait for all partitions being the leader if we are not co-located with the
            // admin role which should not be the normal deployment today.
            let (all_partitions_started_tx, all_partitions_started_rx) = oneshot::channel();
            let _ = all_partitions_started_tx.send(());
            all_partitions_started_rx
        };

        if let Some(worker_role) = self.worker_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "worker-init",
                None,
                worker_role.start(all_partitions_started_rx),
            )?;
        }

        tc.spawn(
            TaskKind::RpcServer,
            "node-rpc-server",
            None,
            self.server.run(config.common.clone()),
        )?;

        Ok(())
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
