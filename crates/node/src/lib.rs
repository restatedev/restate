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

use std::future::Future;
use std::time::Duration;
use tokio::sync::oneshot;

use codederror::CodedError;
#[cfg(feature = "replicated-loglet")]
use restate_log_server::LogServerService;
use restate_types::live::Live;
use tokio::time::Instant;
use tracing::{debug, error, info, trace};

use restate_bifrost::BifrostService;
use restate_core::metadata_store::{MetadataStoreClientError, ReadWriteError};
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::{
    spawn_metadata_manager, MetadataBuilder, MetadataKind, MetadataManager, TargetVersion,
};
use restate_core::{task_center, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::{CommonOptions, Configuration};
use restate_types::logs::metadata::{create_static_metadata, Logs};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::FixedPartitionTable;
use restate_types::retries::RetryPolicy;
use restate_types::Version;

use crate::cluster_marker::ClusterValidationError;
use crate::network_server::{AdminDependencies, NetworkServer, WorkerDependencies};
use crate::roles::{AdminRole, WorkerRole};

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
    #[cfg(feature = "replicated-loglet")]
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
}

pub struct Node {
    updateable_config: Live<Configuration>,
    metadata_manager: MetadataManager<Networking>,
    bifrost: BifrostService,
    metadata_store_role: Option<LocalMetadataStoreService>,
    admin_role: Option<AdminRole>,
    worker_role: Option<WorkerRole>,
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
            config.common.metadata_store_address.clone(),
        );

        let mut router_builder = MessageRouterBuilder::default();
        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        let networking = Networking::new(metadata_builder.to_metadata());
        let metadata_manager = MetadataManager::new(
            metadata_builder,
            networking.clone(),
            metadata_store_client.clone(),
        );
        metadata_manager.register_in_message_router(&mut router_builder);
        let updating_schema_information = metadata.updateable_schema();

        // Setup bifrost
        // replicated-loglet
        #[cfg(feature = "replicated-loglet")]
        let replicated_loglet_factory = restate_bifrost::loglets::replicated_loglet::Factory::new(
            updateable_config
                .clone()
                .map(|c| &c.bifrost.replicated_loglet)
                .boxed(),
            metadata_store_client.clone(),
            metadata.clone(),
            networking.clone(),
            &mut router_builder,
        );
        let bifrost_svc = BifrostService::new(tc.clone(), metadata.clone())
            .enable_local_loglet(&updateable_config);
        #[cfg(feature = "replicated-loglet")]
        let bifrost_svc = bifrost_svc.with_factory(replicated_loglet_factory);

        #[cfg(feature = "replicated-loglet")]
        let log_server = if config.has_role(Role::LogServer) {
            Some(
                LogServerService::create(
                    updateable_config.clone(),
                    tc.clone(),
                    metadata.clone(),
                    &mut router_builder,
                    networking.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let admin_role = if config.has_role(Role::Admin) {
            Some(AdminRole::new(
                tc.clone(),
                updateable_config.clone(),
                metadata.clone(),
                networking.clone(),
                metadata_manager.writer(),
                &mut router_builder,
                metadata_store_client.clone(),
            )?)
        } else {
            None
        };

        let worker_role = if config.has_role(Role::Worker) {
            Some(
                WorkerRole::create(
                    metadata,
                    updateable_config.clone(),
                    &mut router_builder,
                    networking.clone(),
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
            networking.connection_manager(),
            worker_role
                .as_ref()
                .map(|worker| WorkerDependencies::new(worker.storage_query_context().clone())),
            admin_role.as_ref().map(|cluster_controller| {
                AdminDependencies::new(
                    cluster_controller.cluster_controller_handle(),
                    restate_metadata_store::local::create_client(
                        config.common.metadata_store_address.clone(),
                    ),
                )
            }),
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

        let metadata_store_client = restate_metadata_store::local::create_client(
            config.common.metadata_store_address.clone(),
        );

        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata().clone();
        let is_set = tc.try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        spawn_metadata_manager(&tc, self.metadata_manager)?;

        let nodes_config = Self::upsert_node_config(&metadata_store_client, &config.common).await?;
        metadata_writer.update(nodes_config).await?;

        if config.common.allow_bootstrap {
            // only try to insert static configuration if in bootstrap mode
            let (partition_table, logs) =
                Self::fetch_or_insert_static_configuration(&metadata_store_client, &config).await?;

            metadata_writer.update(partition_table).await?;
            metadata_writer.update(logs).await?;
        } else {
            // otherwise, just sync the required metadata
            metadata
                .sync(MetadataKind::PartitionTable, TargetVersion::Latest)
                .await?;
            metadata
                .sync(MetadataKind::Logs, TargetVersion::Latest)
                .await?;

            // safety check until we can tolerate missing partition table and logs configuration
            if metadata.partition_table_version() == Version::INVALID
                || metadata.logs_version() == Version::INVALID
            {
                return Err(Error::SafetyCheck(
                    format!(
                        "Missing partition table or logs configuration for cluster '{}'. This indicates that the cluster bootstrap is incomplete. Please re-run with '--allow-bootstrap true'.",
                        config.common.cluster_name(),
                    )))?;
            }
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
        info!("My Node ID is {}", my_node_config.current_generation);

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
                log_server.start(),
            )?;
        }

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

    async fn fetch_or_insert_static_configuration(
        metadata_store_client: &MetadataStoreClient,
        options: &Configuration,
    ) -> Result<(FixedPartitionTable, Logs), Error> {
        let partition_table =
            Self::fetch_or_insert_partition_table(metadata_store_client, options).await?;
        let logs = Self::fetch_or_insert_logs_configuration(
            metadata_store_client,
            options,
            partition_table.num_partitions(),
        )
        .await?;

        // sanity check
        if partition_table.num_partitions()
            != u64::try_from(logs.num_logs()).expect("usize fits into u64")
        {
            return Err(Error::SafetyCheck(format!("The partition table (number partitions: {}) and logs configuration (number logs: {}) don't match. Please make sure that they are aligned.", partition_table.num_partitions(), logs.num_logs())))?;
        }

        Ok((partition_table, logs))
    }

    async fn fetch_or_insert_partition_table(
        metadata_store_client: &MetadataStoreClient,
        config: &Configuration,
    ) -> Result<FixedPartitionTable, Error> {
        Self::retry_on_network_error(|| {
            metadata_store_client.get_or_insert(PARTITION_TABLE_KEY.clone(), || {
                FixedPartitionTable::new(Version::MIN, config.common.bootstrap_num_partitions())
            })
        })
        .await
        .map_err(Into::into)
    }

    async fn fetch_or_insert_logs_configuration(
        metadata_store_client: &MetadataStoreClient,
        config: &Configuration,
        num_partitions: u64,
    ) -> Result<Logs, Error> {
        Self::retry_on_network_error(|| {
            metadata_store_client.get_or_insert(BIFROST_CONFIG_KEY.clone(), || {
                create_static_metadata(config.bifrost.default_provider, num_partitions)
            })
        })
        .await
        .map_err(Into::into)
    }

    async fn upsert_node_config(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> Result<NodesConfiguration, Error> {
        Self::retry_on_network_error(|| {
            let mut previous_node_generation = None;
            metadata_store_client.read_modify_write(NODES_CONFIG_KEY.clone(), move |nodes_config| {
                let mut nodes_config = if common_opts.allow_bootstrap {
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

    async fn retry_on_network_error<Fn, Fut, T, E>(action: Fn) -> Result<T, E>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: MetadataStoreClientError + std::fmt::Display,
    {
        // todo: Make upsert timeout configurable
        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(10),
            2.0,
            Some(15),
            Some(Duration::from_secs(5)),
        );
        let upsert_start = Instant::now();

        retry_policy
            .retry_if(action, |err: &E| {
                if err.is_network_error() {
                    if upsert_start.elapsed() < Duration::from_secs(5) {
                        trace!("could not connect to metadata store: {err}; retrying");
                    } else {
                        info!("could not connect to metadata store: {err}; retrying");
                    }
                    true
                } else {
                    false
                }
            })
            .await
    }
}
