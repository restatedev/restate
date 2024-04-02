// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod network_server;
mod options;
mod roles;

use axum::body::Bytes;
use bincode::error::{DecodeError, EncodeError};
pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
use restate_bifrost::BifrostService;
use restate_core::network::MessageRouterBuilder;
use restate_core::options::CommonOptions;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
use restate_network::Networking;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::time::Duration;

use codederror::CodedError;
use tokio::time::Instant;
use tracing::{debug, error, info, trace};

use restate_core::{spawn_metadata_manager, MetadataManager};
use restate_core::{task_center, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_metadata_store::{MetadataStoreClient, Operation, ReadModifyWriteError};
use restate_types::logs::metadata::{create_static_metadata, Logs};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::FixedPartitionTable;
use restate_types::retries::RetryPolicy;
use restate_types::Version;

use crate::network_server::{AdminDependencies, NetworkServer, WorkerDependencies};
use crate::roles::{AdminRole, WorkerRole};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("node failed to start due to failed safety check: {0}")]
    #[code(unknown)]
    SafetyCheck(String),
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
    #[error("building metadata store failed: {0}")]
    MetadataStore(
        #[from]
        #[code]
        restate_metadata_store::local::BuildError,
    ),
    #[error("node neither runs cluster controller nor its address has been configured")]
    #[code(unknown)]
    UnknownClusterController,
    #[error("cluster bootstrap failed: {0}")]
    #[code(unknown)]
    Bootstrap(String),
}

pub struct Node {
    common_opts: CommonOptions,
    options: Options,
    metadata_manager: MetadataManager<Networking>,
    bifrost: BifrostService,
    metadata_store_role: Option<LocalMetadataStoreService>,
    admin_role: Option<AdminRole>,
    worker_role: Option<WorkerRole>,
    server: NetworkServer,
}

impl Node {
    pub fn new(common_opts: CommonOptions, options: Options) -> Result<Self, BuildError> {
        let opts = options.clone();
        // ensure we have cluster admin role if bootstrapping.
        if *common_opts.allow_bootstrap() {
            info!("Bootstrapping cluster");
            if !common_opts.roles().contains(Role::Admin) {
                return Err(BuildError::Bootstrap(format!(
                    "Node must include the 'admin' role when starting in bootstrap mode. Currently it has roles {}", common_opts.roles()
                )));
            }

            if !common_opts.roles().contains(Role::MetadataStore) {
                return Err(BuildError::Bootstrap(format!("Node must include the 'metadata_store' role when starting in bootstrap mode. Currently it has roles {}", common_opts.roles())));
            }
        }

        let metadata_store_role = if common_opts.roles().contains(Role::MetadataStore) {
            Some(options.metadata_store.clone().build()?)
        } else {
            None
        };

        let metadata_store_client = restate_metadata_store::local::create_client(
            common_opts.metadata_store_address().clone(),
        );

        let mut router_builder = MessageRouterBuilder::default();
        let networking = Networking::default();
        let metadata_manager =
            MetadataManager::build(networking.clone(), metadata_store_client.clone());
        metadata_manager.register_in_message_router(&mut router_builder);

        let bifrost = options
            .bifrost
            .clone()
            .build(metadata_store_client.clone(), metadata_manager.writer());

        let admin_role = if common_opts.roles().contains(Role::Admin) {
            Some(AdminRole::new(
                options.admin.clone(),
                options.kafka.clone(),
                networking.clone(),
            )?)
        } else {
            None
        };

        let worker_role = if common_opts.roles().contains(Role::Worker) {
            Some(WorkerRole::new(
                options.worker.clone(),
                options.kafka.clone(),
                &mut router_builder,
                networking.clone(),
                bifrost.handle(),
                metadata_store_client,
            )?)
        } else {
            None
        };

        let server = NetworkServer::new(
            common_opts.clone(),
            networking.connection_manager(),
            worker_role.as_ref().map(|worker| {
                WorkerDependencies::new(
                    worker.rocksdb_storage().clone(),
                    worker.storage_query_context().clone(),
                    worker.schemas(),
                    worker.subscription_controller(),
                )
            }),
            admin_role.as_ref().map(|cluster_controller| {
                AdminDependencies::new(
                    cluster_controller.cluster_controller_handle(),
                    cluster_controller.schema_reader(),
                    restate_metadata_store::local::create_client(
                        common_opts.metadata_store_address().clone(),
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
            common_opts,
            options: opts,
            metadata_manager,
            bifrost,
            metadata_store_role,
            admin_role,
            worker_role,
            server,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        let tc = task_center();

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
            self.common_opts.metadata_store_address().clone(),
        );

        let nodes_config =
            Self::upsert_node_config(&metadata_store_client, &self.common_opts).await?;

        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata();
        let is_set = tc.try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        spawn_metadata_manager(&tc, self.metadata_manager)?;

        metadata_writer.update(nodes_config).await?;

        let (partition_table, logs) =
            Self::fetch_or_insert_static_configuration(&metadata_store_client, &self.options)
                .await?;

        // sanity check
        if partition_table.num_partitions()
            != u64::try_from(logs.logs.len()).expect("usize fits into u64")
        {
            return Err(Error::SafetyCheck(format!("The partition table (number partitions: {}) and logs configuration (number logs: {}) don't match. Please make sure that they are aligned.", partition_table.num_partitions(), logs.logs.len())))?;
        }

        metadata_writer.update(partition_table).await?;
        metadata_writer.update(logs).await?;

        let nodes_config = metadata.nodes_config();

        // Find my node in nodes configuration.
        let my_node_config = nodes_config
            .find_node_by_name(self.common_opts.node_name())
            .expect("node config should have been upserted");

        let my_node_id = my_node_config.current_generation;

        // Safety checks, same node (if set)?
        if self
            .common_opts
            .force_node_id()
            .is_some_and(|n| n != my_node_id.as_plain())
        {
            return Err(Error::SafetyCheck(
                format!(
                    "Node ID mismatch: configured node ID is {}, but the nodes configuration contains {}",
                    self.common_opts.force_node_id().unwrap(),
                    my_node_id.as_plain()
                    )))?;
        }

        // Same cluster?
        if self.common_opts.cluster_name() != nodes_config.cluster_name() {
            return Err(Error::SafetyCheck(
                format!(
                    "Cluster name mismatch: configured cluster name is '{}', but the nodes configuration contains '{}'",
                    self.common_opts.cluster_name(),
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

        if let Some(admin_role) = self.admin_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "admin-init",
                None,
                admin_role.start(*self.common_opts.allow_bootstrap(), bifrost.clone()),
            )?;
        }

        if let Some(worker_role) = self.worker_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "worker-init",
                None,
                worker_role.start(bifrost),
            )?;
        }

        tc.spawn(
            TaskKind::RpcServer,
            "node-rpc-server",
            None,
            self.server.run(),
        )?;

        Ok(())
    }

    async fn fetch_or_insert_static_configuration(
        metadata_store_client: &MetadataStoreClient,
        options: &Options,
    ) -> Result<(FixedPartitionTable, Logs), ReadModifyWriteError> {
        let partition_table =
            Self::fetch_or_insert_partition_table(metadata_store_client, options).await?;
        let logs = Self::fetch_or_insert_logs_configuration(
            metadata_store_client,
            options,
            partition_table.num_partitions(),
        )
        .await?;

        Ok((partition_table, logs))
    }

    async fn fetch_or_insert_partition_table(
        metadata_store_client: &MetadataStoreClient,
        options: &Options,
    ) -> Result<FixedPartitionTable, ReadModifyWriteError> {
        Self::retry_on_network_error(|| {
            metadata_store_client.read_modify_write(
                PARTITION_TABLE_KEY.clone(),
                |partition_table| {
                    if let Some(partition_table) = partition_table {
                        debug!("Retrieved partition table: {partition_table:?}");

                        Operation::Return(partition_table)
                    } else {
                        let partition_table =
                            FixedPartitionTable::new(Version::MIN, options.worker.partitions);
                        debug!("Initializing a new partition table: {partition_table:?}",);

                        Operation::Upsert(partition_table)
                    }
                },
            )
        })
        .await
    }

    async fn fetch_or_insert_logs_configuration(
        metadata_store_client: &MetadataStoreClient,
        options: &Options,
        num_partitions: u64,
    ) -> Result<Logs, ReadModifyWriteError> {
        Self::retry_on_network_error(|| {
            metadata_store_client.read_modify_write(BIFROST_CONFIG_KEY.clone(), |logs| {
                if let Some(logs) = logs {
                    debug!("Retrieved logs configuration: {logs:?}");

                    Operation::Return(logs)
                } else {
                    let logs =
                        create_static_metadata(options.bifrost.default_provider, num_partitions);

                    debug!("Initializing a new logs configuration: {logs:?}",);

                    Operation::Upsert(logs)
                }
            })
        })
        .await
    }

    async fn upsert_node_config(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> Result<NodesConfiguration, ReadModifyWriteError> {
        Self::retry_on_network_error(|| {
                    let mut previous_node_generation = None;
                    metadata_store_client.read_modify_write(
                        NODES_CONFIG_KEY.clone(),
                        move |nodes_config| {
                            let mut nodes_config = nodes_config.unwrap_or_else(|| {
                                NodesConfiguration::new(
                                    Version::INVALID,
                                    common_opts.cluster_name().clone(),
                                )
                            });

                            // check whether we have registered before
                            let node_config =
                                nodes_config.find_node_by_name(common_opts.node_name()).cloned();

                            let my_node_config = if let Some(mut node_config) = node_config {
                                assert_eq!(
                                    common_opts.node_name(), &node_config.name,
                                    "node name must match"
                                );

                                if let Some(previous_node_generation) = previous_node_generation {
                                    if node_config.current_generation.is_newer_than(previous_node_generation) {
                                        // detected a concurrent registration of the same node
                                        return Operation::Fail(format!("detected concurrent modification of the node_config for node '{}'; stepping down", common_opts.node_name()));
                                    }
                                } else {
                                    // remember the previous node generation to detect concurrent modifications
                                    previous_node_generation = Some(node_config.current_generation);
                                }

                                // update node_config
                                node_config.roles = *common_opts.roles();
                                node_config.address = common_opts.advertise_address().clone();
                                node_config.current_generation.bump_generation();

                                node_config
                            } else {
                                let plain_node_id = common_opts.force_node_id().unwrap_or_else(|| {
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
                                    common_opts.node_name().clone(),
                                    my_node_id,
                                    common_opts.advertise_address().clone(),
                                    *common_opts.roles(),
                                )
                            };

                            nodes_config.upsert_node(my_node_config);
                            nodes_config.increment_version();

                            Operation::Upsert(nodes_config)
                        },
                    )
                }).await
    }

    async fn retry_on_network_error<Fn, Fut, T>(action: Fn) -> Result<T, ReadModifyWriteError>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ReadModifyWriteError>>,
    {
        // todo: Make upsert timeout configurable
        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(10),
            2.0,
            15,
            Some(Duration::from_secs(5)),
        );
        let upsert_start = Instant::now();

        retry_policy
            .retry_if(action, |err: &ReadModifyWriteError| match err {
                ReadModifyWriteError::Network(err) => {
                    if upsert_start.elapsed() < Duration::from_secs(5) {
                        trace!("could not connect to metadata store: {err}; retrying");
                    } else {
                        info!("could not connect to metadata store: {err}; retrying");
                    }
                    true
                }
                _ => false,
            })
            .await
    }
}

/// Helper function for default encoding of values.
pub fn encode_default<T: Serialize>(value: T) -> Result<Bytes, EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map(Into::into)
        .map_err(Into::into)
}

pub fn decode_default<T: DeserializeOwned>(bytes: Bytes) -> Result<T, DecodeError> {
    bincode::serde::decode_from_slice(bytes.as_ref(), bincode::config::standard())
        .map(|(value, _)| value)
        .map_err(Into::into)
}
