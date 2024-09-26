// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod metadata;
pub mod metadata_store;
mod metric_definitions;
pub mod network;
mod task_center;
mod task_center_types;
pub mod worker_api;
use std::{
    future::Future,
    time::{Duration, Instant},
};

use enumset::EnumSet;
pub use error::*;

pub use metadata::{
    spawn_metadata_manager, Metadata, MetadataBuilder, MetadataKind, MetadataManager,
    MetadataWriter, SyncError, TargetVersion,
};
use metadata_store::{MetadataStoreClient, MetadataStoreClientError, ReadWriteError};
use network::{GrpcConnector, MessageRouterBuilder, Networking, TransportConnect};
use restate_types::{
    cluster_controller::{ReplicationStrategy, SchedulingPlan},
    config::NetworkingOptions,
    logs::metadata::{bootstrap_logs_metadata, Logs, ProviderKind},
    metadata_store::keys::{
        BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY, SCHEDULING_PLAN_KEY,
    },
    net::AdvertisedAddress,
    nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role},
    partition_table::PartitionTable,
    retries::RetryPolicy,
    GenerationalNodeId, NodeId, Version,
};
pub use task_center::*;
pub use task_center_types::*;

#[cfg(any(test, feature = "test-util"))]
mod test_env;

#[cfg(any(test, feature = "test-util"))]
pub use test_env::{NoOpMessageHandler, TestCoreEnv, TestCoreEnvBuilder};
use tracing::{info, trace};

pub struct CoreBuilder {
    metadata_store_client: MetadataStoreClient,
}

impl CoreBuilder {
    pub fn with_metadata_store_client(metadata_store_client: MetadataStoreClient) -> Self {
        CoreBuilder {
            metadata_store_client,
        }
    }

    pub fn with_nodes_config(
        self,
        nodes_config: NodesConfiguration,
        node_name: String,
        force_node_id: Option<NodeId>,
        advertise_address: AdvertisedAddress,
        roles: EnumSet<Role>,
    ) -> CoreBuilderWithNodesConfiguration {
        CoreBuilderWithNodesConfiguration {
            metadata_store_client: self.metadata_store_client,
            node_name,
            force_node_id,
            advertise_address,
            roles,
            nodes_config,
        }
    }
}

pub struct CoreBuilderWithNodesConfiguration {
    metadata_store_client: MetadataStoreClient,
    node_name: String,
    force_node_id: Option<NodeId>,
    advertise_address: AdvertisedAddress,
    roles: EnumSet<Role>,
    nodes_config: NodesConfiguration,
}

impl CoreBuilderWithNodesConfiguration {
    pub fn with_networking<T: TransportConnect>(
        self,
        networking: Networking<T>,
        metadata_builder: MetadataBuilder,
    ) -> CoreBuilderWithNetworking<T> {
        let mut router_builder = MessageRouterBuilder::default();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager = MetadataManager::new(
            metadata_builder,
            networking.clone(),
            self.metadata_store_client.clone(),
        );
        metadata_manager.register_in_message_router(&mut router_builder);

        CoreBuilderWithNetworking {
            node_name: self.node_name,
            force_node_id: self.force_node_id,
            advertise_address: self.advertise_address,
            roles: self.roles,
            nodes_config: self.nodes_config,
            metadata_manager,
            metadata_store_client: self.metadata_store_client,
            networking,
            metadata,
        }
    }

    pub fn with_grpc_networking(
        self,
        networking_options: NetworkingOptions,
    ) -> CoreBuilderWithNetworking<GrpcConnector> {
        let metadata_builder = MetadataBuilder::default();
        let networking = Networking::new(metadata_builder.to_metadata(), networking_options);

        self.with_networking(networking, metadata_builder)
    }
}

pub struct CoreBuilderWithNetworking<T> {
    metadata_store_client: MetadataStoreClient,
    node_name: String,
    force_node_id: Option<NodeId>,
    advertise_address: AdvertisedAddress,
    roles: EnumSet<Role>,
    nodes_config: NodesConfiguration,
    metadata_manager: MetadataManager<T>,
    metadata: Metadata,
    networking: Networking<T>,
}

impl<T> CoreBuilderWithNetworking<T> {
    pub fn with_tc(self, tc: TaskCenter) -> CoreBuilderFinal<T> {
        CoreBuilderFinal {
            tc,
            node_name: self.node_name,
            force_node_id: self.force_node_id,
            advertise_address: self.advertise_address,
            roles: self.roles,
            nodes_config: self.nodes_config,
            metadata_manager: self.metadata_manager,
            metadata_store_client: self.metadata_store_client,
            networking: self.networking,
            metadata: self.metadata,
            provider_kind: ProviderKind::Local,
            allow_bootstrap: false,
            partition_table: PartitionTable::with_equally_sized_partitions(Version::MIN, 10),
            replication_strategy: ReplicationStrategy::OnAllNodes,
            router_builder: MessageRouterBuilder::default(),
        }
    }
}

pub struct CoreBuilderFinal<T> {
    pub tc: TaskCenter,
    node_name: String,
    force_node_id: Option<NodeId>,
    advertise_address: AdvertisedAddress,
    roles: EnumSet<Role>,
    pub metadata_manager: MetadataManager<T>,
    pub metadata_store_client: MetadataStoreClient,
    pub metadata: Metadata,
    pub router_builder: MessageRouterBuilder,
    partition_table: PartitionTable,
    replication_strategy: ReplicationStrategy,
    pub nodes_config: NodesConfiguration,
    provider_kind: ProviderKind,
    pub networking: Networking<T>,
    allow_bootstrap: bool,
}

impl<T: TransportConnect> CoreBuilderFinal<T> {
    pub fn set_num_partitions(mut self, num_partitions: u16) -> Self {
        self.partition_table = PartitionTable::with_equally_sized_partitions(
            self.partition_table.version(),
            num_partitions,
        );
        self
    }

    pub fn set_replication_strategy(mut self, replication_strategy: ReplicationStrategy) -> Self {
        self.replication_strategy = replication_strategy;
        self
    }

    pub fn set_provider_kind(mut self, provider_kind: ProviderKind) -> Self {
        self.provider_kind = provider_kind;
        self
    }

    pub fn set_allow_bootstrap(mut self, allow_bootstrap: bool) -> Self {
        self.allow_bootstrap = allow_bootstrap;
        self
    }

    pub fn set_nodes_config(
        mut self,
        node_name: impl Into<String>,
        node_id: GenerationalNodeId,
        nodes_config: NodesConfiguration,
    ) -> Self {
        self.node_name = node_name.into();
        self.force_node_id = Some(NodeId::Generational(node_id));
        self.nodes_config = nodes_config;
        self
    }

    pub fn set_partition_table(mut self, partition_table: PartitionTable) -> Self {
        self.partition_table = partition_table;
        self
    }

    pub fn set_force_node_id(mut self, force_node_id: NodeId) -> Self {
        self.force_node_id = Some(force_node_id);
        self
    }

    pub fn build_router(self) -> Core<T> {
        let message_router = self.router_builder.build();
        self.networking
            .connection_manager()
            .set_message_router(message_router);
        Core {
            tc: self.tc,
            node_name: self.node_name,
            force_node_id: self.force_node_id,
            advertise_address: self.advertise_address,
            roles: self.roles,
            nodes_config: self.nodes_config,
            metadata_manager: self.metadata_manager,
            metadata_store_client: self.metadata_store_client,
            partition_table: self.partition_table,
            replication_strategy: self.replication_strategy,
            provider_kind: self.provider_kind,
            allow_bootstrap: self.allow_bootstrap,
            networking: self.networking,
        }
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
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

pub struct Core<T> {
    tc: TaskCenter,
    node_name: String,
    force_node_id: Option<NodeId>,
    advertise_address: AdvertisedAddress,
    roles: EnumSet<Role>,
    metadata_manager: MetadataManager<T>,
    metadata_store_client: MetadataStoreClient,
    partition_table: PartitionTable,
    replication_strategy: ReplicationStrategy,
    nodes_config: NodesConfiguration,
    provider_kind: ProviderKind,
    allow_bootstrap: bool,
    networking: Networking<T>,
}

impl<T: TransportConnect> Core<T> {
    pub async fn start(
        self,
        network_error_retry_policy: RetryPolicy,
    ) -> Result<StartedCore<T>, anyhow::Error> {
        let tc = self.tc;

        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata().clone();
        let is_set = tc.try_set_global_metadata(metadata.clone());

        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        let metadata_manager_task = spawn_metadata_manager(&tc, self.metadata_manager)?;

        let requested_node_id = self.force_node_id;
        let requested_cluster_name = self.nodes_config.cluster_name().to_owned();

        let node_name = self.node_name.clone();
        let nodes_config = Self::upsert_node_config(
            network_error_retry_policy.clone(),
            &self.metadata_store_client,
            self.allow_bootstrap,
            self.nodes_config,
            &self.node_name,
            self.force_node_id,
            self.advertise_address,
            self.roles,
        )
        .await?;
        metadata_writer.update(nodes_config).await?;

        if self.allow_bootstrap {
            // only try to insert static configuration if in bootstrap mode
            let (partition_table, logs) = Self::fetch_or_insert_initial_configuration(
                network_error_retry_policy,
                &self.metadata_store_client,
                self.partition_table,
                self.replication_strategy,
                self.provider_kind,
            )
            .await?;

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
                        requested_cluster_name,
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
            .find_node_by_name(node_name)
            .expect("node config should have been upserted");

        let my_node_id = my_node_config.current_generation;

        // Safety checks, same node (if set)?
        if let Some(requested_node_id) = requested_node_id {
            if requested_node_id.id() != my_node_id.as_plain() {
                return Err(Error::SafetyCheck(
                format!(
                    "Node ID mismatch: configured node ID is {}, but the nodes configuration contains {}",
                    requested_node_id.id(),
                    my_node_id.as_plain()
                    )))?;
            }
        }

        // Same cluster?
        if requested_cluster_name != nodes_config.cluster_name() {
            return Err(Error::SafetyCheck(
                format!(
                    "Cluster name mismatch: configured cluster name is '{}', but the nodes configuration contains '{}'",
                    requested_cluster_name,
                    nodes_config.cluster_name()
                    )))?;
        }

        // My Node ID is set
        metadata_writer.set_my_node_id(my_node_id);
        info!(
            roles = %my_node_config.roles,
            address = %my_node_config.address,
            "My Node ID is {}", my_node_config.current_generation);

        Ok(StartedCore {
            tc,
            metadata,
            metadata_manager_task,
            metadata_writer,
            networking: self.networking,
            metadata_store_client: self.metadata_store_client,
        })
    }

    async fn fetch_or_insert_initial_configuration(
        network_error_retry_policy: RetryPolicy,
        metadata_store_client: &MetadataStoreClient,
        partition_table: PartitionTable,
        replication_strategy: ReplicationStrategy,
        provider_kind: ProviderKind,
    ) -> Result<(PartitionTable, Logs), Error> {
        let partition_table = Self::fetch_or_insert_partition_table(
            network_error_retry_policy.clone(),
            metadata_store_client,
            partition_table,
        )
        .await?;
        Self::try_insert_initial_scheduling_plan(
            network_error_retry_policy.clone(),
            metadata_store_client,
            &partition_table,
            replication_strategy,
        )
        .await?;
        let logs = Self::fetch_or_insert_logs_configuration(
            network_error_retry_policy.clone(),
            metadata_store_client,
            provider_kind,
            partition_table.num_partitions(),
        )
        .await?;

        // sanity check
        if usize::from(partition_table.num_partitions()) != logs.num_logs() {
            return Err(Error::SafetyCheck(format!("The partition table (number partitions: {}) and logs configuration (number logs: {}) don't match. Please make sure that they are aligned.", partition_table.num_partitions(), logs.num_logs())))?;
        }

        Ok((partition_table, logs))
    }

    async fn fetch_or_insert_partition_table(
        network_error_retry_policy: RetryPolicy,
        metadata_store_client: &MetadataStoreClient,
        partition_table: PartitionTable,
    ) -> Result<PartitionTable, Error> {
        Self::retry_on_network_error(network_error_retry_policy.clone(), || {
            metadata_store_client
                .get_or_insert(PARTITION_TABLE_KEY.clone(), || partition_table.clone())
        })
        .await
        .map_err(Into::into)
    }

    /// Tries to insert an initial scheduling plan which is aligned with the given
    /// [`PartitionTable`]. If a scheduling plan already exists, then this method does nothing.
    async fn try_insert_initial_scheduling_plan(
        network_error_retry_policy: RetryPolicy,
        metadata_store_client: &MetadataStoreClient,
        partition_table: &PartitionTable,
        replication_strategy: ReplicationStrategy,
    ) -> Result<(), Error> {
        Self::retry_on_network_error(network_error_retry_policy.clone(), || {
            metadata_store_client.get_or_insert(SCHEDULING_PLAN_KEY.clone(), || {
                SchedulingPlan::from(partition_table, replication_strategy)
            })
        })
        .await
        .map_err(Into::into)
        .map(|_| ())
    }

    async fn fetch_or_insert_logs_configuration(
        network_error_retry_policy: RetryPolicy,
        metadata_store_client: &MetadataStoreClient,
        provider_kind: ProviderKind,
        num_partitions: u16,
    ) -> Result<Logs, Error> {
        Self::retry_on_network_error(network_error_retry_policy, || {
            metadata_store_client.get_or_insert(BIFROST_CONFIG_KEY.clone(), || {
                bootstrap_logs_metadata(provider_kind, num_partitions)
            })
        })
        .await
        .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_node_config(
        network_error_retry_policy: RetryPolicy,
        metadata_store_client: &MetadataStoreClient,
        allow_bootstrap: bool,
        initial_nodes_config: NodesConfiguration,
        node_name: &str,
        force_node_id: Option<NodeId>,
        advertised_address: AdvertisedAddress,
        roles: EnumSet<Role>,
    ) -> Result<NodesConfiguration, Error> {
        Self::retry_on_network_error(network_error_retry_policy.clone(), move || {
            let mut previous_node_generation = None;
            let initial_nodes_config = initial_nodes_config.clone();
            let advertised_address = advertised_address.clone();
            metadata_store_client.read_modify_write(NODES_CONFIG_KEY.clone(), move |nodes_config| {
                let mut nodes_config = if allow_bootstrap {
                    nodes_config.unwrap_or(initial_nodes_config.clone())
                } else {
                    nodes_config.ok_or(Error::MissingNodesConfiguration)?
                };

                // check whether we have registered before
                let node_config = nodes_config.find_node_by_name(node_name).cloned();

                let my_node_config = if let Some(mut node_config) = node_config {
                    assert_eq!(node_name, node_config.name, "node name must match");

                    if let Some(previous_node_generation) = previous_node_generation {
                        if node_config
                            .current_generation
                            .is_newer_than(previous_node_generation)
                        {
                            // detected a concurrent registration of the same node
                            return Err(Error::ConcurrentNodeRegistration(node_config.name));
                        }
                    } else {
                        // remember the previous node generation to detect concurrent modifications
                        previous_node_generation = Some(node_config.current_generation);
                    }

                    // update node_config
                    node_config.roles = roles;
                    node_config.address = advertised_address.clone();

                    if let Some(NodeId::Generational(force_node_id)) = force_node_id {
                        // Tests may set a requirement to a particular generation
                        assert_eq!(
                            force_node_id, node_config.current_generation,
                            "nodes config contained generational id '{}' for node '{}', but force_node_id was set to '{}'",
                            node_config.current_generation, node_name, force_node_id,
                        );

                        // Don't bump the generation if it was force set
                    } else {
                        node_config.current_generation.bump_generation();
                    }

                    node_config
                } else {
                    let node_id = match force_node_id {
                        None => nodes_config
                            .max_plain_node_id()
                            .map(|n| n.next())
                            .unwrap_or_default()
                            .with_generation(1),
                        Some(NodeId::Generational(g)) => g,
                        Some(NodeId::Plain(p)) => p.with_generation(1),
                    };

                    assert!(
                        nodes_config.find_node_by_id(node_id.as_plain()).is_err(),
                        "duplicate plain node id '{}'",
                        node_id.as_plain()
                    );

                    NodeConfig::new(
                        node_name.to_owned(),
                        node_id,
                        advertised_address.clone(),
                        roles,
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

    async fn retry_on_network_error<Fn, Fut, V, E, P>(retry_policy: P, action: Fn) -> Result<V, E>
    where
        P: Into<RetryPolicy>,
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: MetadataStoreClientError + std::fmt::Display,
    {
        let upsert_start = Instant::now();

        retry_policy
            .into()
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

pub struct StartedCore<T> {
    pub tc: TaskCenter,
    pub metadata: Metadata,
    pub metadata_writer: MetadataWriter,
    pub networking: Networking<T>,
    pub metadata_manager_task: TaskId,
    pub metadata_store_client: MetadataStoreClient,
}
