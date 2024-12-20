// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ProvisionCluster;
use anyhow::{anyhow, bail};
use bytestring::ByteString;
use futures::future::OptionFuture;
use futures::TryFutureExt;
use prost_dto::IntoProto;
use restate_core::metadata_store::{
    retry_on_network_error, MetadataStoreClient, MetadataStoreClientError, Precondition,
    ReadWriteError, WriteError,
};
use restate_core::{
    cancellation_watcher, Metadata, MetadataWriter, ShutdownError, SyncError, TargetVersion,
    TaskCenter, TaskKind,
};
use restate_metadata_store::local::ProvisionHandle;
use restate_types::config::{CommonOptions, Configuration};
use restate_types::errors::GenericError;
use restate_types::logs::metadata::{DefaultProvider, Logs, LogsConfiguration};
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY,
};
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration};
use restate_types::partition_table::{PartitionTable, PartitionTableBuilder, ReplicationStrategy};
use restate_types::retries::RetryPolicy;
use restate_types::storage::StorageEncode;
use restate_types::{GenerationalNodeId, Version, Versioned};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

#[derive(Debug, thiserror::Error)]
enum JoinError {
    #[error("missing nodes configuration")]
    MissingNodesConfiguration,
    #[error("detected a concurrent registration for node '{0}'")]
    ConcurrentNodeRegistration(String),
    #[error("failed writing to metadata store: {0}")]
    MetadataStore(#[from] ReadWriteError),
    #[error("trying to join wrong cluster; expected '{expected_cluster_name}', actual '{actual_cluster_name}'")]
    ClusterMismatch {
        expected_cluster_name: String,
        actual_cluster_name: String,
    },
}

#[derive(Debug, thiserror::Error)]
enum ProvisionMetadataError {
    #[error("already provisioned")]
    AlreadyProvisioned,
    #[error(transparent)]
    Other(GenericError),
    #[error(transparent)]
    Shutdown(ShutdownError),
}

impl ProvisionMetadataError {
    fn other(err: impl Into<GenericError>) -> Self {
        ProvisionMetadataError::Other(err.into())
    }
}

impl From<restate_metadata_store::local::ProvisionError> for ProvisionMetadataError {
    fn from(value: restate_metadata_store::local::ProvisionError) -> Self {
        match value {
            restate_metadata_store::local::ProvisionError::AlreadyProvisioned => {
                ProvisionMetadataError::AlreadyProvisioned
            }
            err => ProvisionMetadataError::Other(err.into()),
        }
    }
}

#[derive(Clone, Debug, IntoProto)]
#[proto(target = "restate_types::protobuf::cluster::ClusterConfiguration")]
pub struct ClusterConfiguration {
    #[into_proto(map = "num_partitions_to_u32")]
    pub num_partitions: NonZeroU16,
    #[proto(required)]
    pub replication_strategy: ReplicationStrategy,
    #[proto(required)]
    pub default_provider: DefaultProvider,
}

fn num_partitions_to_u32(num_partitions: NonZeroU16) -> u32 {
    u32::from(num_partitions.get())
}

#[derive(Clone, Debug, Default)]
pub struct ProvisionClusterRequest {
    dry_run: bool,
    num_partitions: Option<NonZeroU16>,
    placement_strategy: Option<ReplicationStrategy>,
    log_provider: Option<DefaultProvider>,
}

impl TryFrom<restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest>
    for ProvisionClusterRequest
{
    type Error = anyhow::Error;

    fn try_from(
        value: restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest,
    ) -> Result<Self, Self::Error> {
        let placement_strategy = value
            .placement_strategy
            .map(ReplicationStrategy::try_from)
            .transpose()?;
        let log_provider = value
            .log_provider
            .map(DefaultProvider::try_from)
            .transpose()?;

        let num_partitions = value
            .num_partitions
            .map(|num_partitions| {
                u16::try_from(num_partitions)
                    .map_err(Into::into)
                    .and_then(|num_partitions| {
                        NonZeroU16::new(num_partitions)
                            .ok_or(anyhow!("num_partitions must not be 0"))
                    })
            })
            .transpose()?;

        Ok(ProvisionClusterRequest {
            dry_run: value.dry_run,
            num_partitions,
            placement_strategy,
            log_provider,
        })
    }
}

#[derive(Clone, Debug)]
pub enum ProvisionClusterResponse {
    DryRun(ClusterConfiguration),
    NewlyProvisioned(ClusterConfiguration),
    AlreadyProvisioned,
}

enum HandleProvisionClusterResponse {
    DryRun {
        cluster_configuration: ClusterConfiguration,
    },
    Provisioned {
        cluster_configuration: ClusterConfiguration,
        nodes_configuration: NodesConfiguration,
    },
    AlreadyProvisioned,
}

pub struct NodeInit<'a> {
    provision_cluster_rx: Option<mpsc::Receiver<ProvisionCluster>>,
    // We are using an in-memory channel for provisioning a co-located metadata store to ensure
    // that it uses the local NodesConfiguration as initial value.
    metadata_store_provision_handle: Option<ProvisionHandle>,
    metadata_store_client: &'a MetadataStoreClient,
    metadata_writer: &'a MetadataWriter,
}

impl<'a> NodeInit<'a> {
    pub fn new(
        provision_cluster_rx: mpsc::Receiver<ProvisionCluster>,
        metadata_store_provision_handle: Option<ProvisionHandle>,
        metadata_store_client: &'a MetadataStoreClient,
        metadata_writer: &'a MetadataWriter,
    ) -> Self {
        Self {
            provision_cluster_rx: Some(provision_cluster_rx),
            metadata_store_provision_handle,
            metadata_store_client,
            metadata_writer,
        }
    }

    pub async fn init(mut self) -> anyhow::Result<()> {
        let config = Configuration::pinned().into_arc();

        let mut join_future = std::pin::pin!(Self::join_cluster(
            self.metadata_store_client,
            &config.common
        ));
        let mut cancellation = std::pin::pin!(cancellation_watcher());

        let nodes_configuration = loop {
            tokio::select! {
                _ = &mut cancellation => {
                    Err(ShutdownError)?;
                },
                result = &mut join_future => {
                    let nodes_configuration = result?;
                    info!("Successfully joined cluster");
                    break nodes_configuration;
                },
                Some(Some(provision_cluster)) = OptionFuture::from(self.provision_cluster_rx.as_mut().map(|rx| async { rx.recv().await })) => {
                    let (request, response_tx) = provision_cluster.into_inner();

                    match self.handle_provision_request(request, &config.common).await {
                        Ok(response) => {
                            match response {
                                HandleProvisionClusterResponse::DryRun{ cluster_configuration } => {
                                    // if receiver is no longer present, then caller is not interested in result
                                    let _ = response_tx.send(Ok(ProvisionClusterResponse::DryRun(cluster_configuration)));
                                }
                                HandleProvisionClusterResponse::Provisioned{ cluster_configuration, nodes_configuration } => {
                                    info!("Successfully provisioned cluster with {cluster_configuration:?}");
                                    // if receiver is no longer present, then caller is not interested in result
                                    let _ = response_tx.send(Ok(ProvisionClusterResponse::NewlyProvisioned(cluster_configuration)));
                                    break nodes_configuration;
                                }
                                HandleProvisionClusterResponse::AlreadyProvisioned => {
                                    // if receiver is no longer present, then caller is not interested in result
                                    let _ = response_tx.send(Ok(ProvisionClusterResponse::AlreadyProvisioned));
                                    self.mark_as_provisioned()
                                }
                            }
                        },
                        Err(err) => {
                            debug!("Failed processing cluster provision command: {err}");
                            let _ = response_tx.send(Err(err));
                        }
                    }
                },
            }
        };

        // Find my node in nodes configuration.
        let my_node_config = nodes_configuration
            .find_node_by_name(config.common.node_name())
            .expect("node config should have been upserted");

        let my_node_id = my_node_config.current_generation;

        // Safety checks, same node (if set)?
        if config
            .common
            .force_node_id
            .is_some_and(|n| n != my_node_id.as_plain())
        {
            bail!(
                format!(
                    "Node ID mismatch: configured node ID is {}, but the nodes configuration contains {}",
                    config.common.force_node_id.unwrap(),
                    my_node_id.as_plain()
                ));
        }

        // Same cluster?
        if config.common.cluster_name() != nodes_configuration.cluster_name() {
            bail!(
                format!(
                    "Cluster name mismatch: configured cluster name is '{}', but the nodes configuration contains '{}'",
                    config.common.cluster_name(),
                    nodes_configuration.cluster_name()
                ));
        }

        info!(
            roles = %my_node_config.roles,
            address = %my_node_config.address,
            "My Node ID is {}", my_node_config.current_generation);

        self.metadata_writer
            .update(Arc::new(nodes_configuration))
            .await?;

        // My Node ID is set
        self.metadata_writer.set_my_node_id(my_node_id);
        restate_tracing_instrumentation::set_global_node_id(my_node_id);

        self.mark_as_provisioned();

        self.sync_metadata().await;

        info!("Node initialization complete");

        Ok(())
    }

    async fn sync_metadata(&self) {
        // fetch the latest metadata
        let metadata = Metadata::current();

        let config = Configuration::pinned();

        let retry_policy = config.common.network_error_retry_policy.clone();

        if let Err(err) = retry_policy
            .retry_if(
                || async {
                    metadata
                        .sync(MetadataKind::Schema, TargetVersion::Latest)
                        .await?;
                    metadata
                        .sync(MetadataKind::PartitionTable, TargetVersion::Latest)
                        .await?;
                    metadata
                        .sync(MetadataKind::Logs, TargetVersion::Latest)
                        .await
                },
                |err| match err {
                    SyncError::MetadataStore(err) => err.is_network_error(),
                    SyncError::Shutdown(_) => false,
                },
            )
            .await
        {
            warn!("Failed to fetch the latest metadata when initializing the node: {err}");
        }
    }

    fn mark_as_provisioned(&mut self) {
        if let Some(mut provision_cluster_rx) = self.provision_cluster_rx.take() {
            // ignore if we are shutting down
            let _ = TaskCenter::spawn_child(
                TaskKind::Background,
                "provision-cluster-responder",
                async move {
                    let mut cancellation = std::pin::pin!(cancellation_watcher());

                    loop {
                        tokio::select! {
                            _ = &mut cancellation => {
                                break;
                            },
                            Some(provision_cluster_request) = provision_cluster_rx.recv() => {
                                let (_, response_tx) = provision_cluster_request.into_inner();
                                // if the receiver is gone then the caller is not interested in the result
                                let _ = response_tx.send(Ok(ProvisionClusterResponse::AlreadyProvisioned));
                            }
                        }
                    }

                    Ok(())
                },
            );
        }
    }

    async fn handle_provision_request(
        &self,
        request: ProvisionClusterRequest,
        common_opts: &CommonOptions,
    ) -> anyhow::Result<HandleProvisionClusterResponse> {
        trace!("Handle provision request: {request:?}");

        if common_opts
            .metadata_store_client
            .metadata_store_client
            .is_embedded()
            && self.metadata_store_provision_handle.is_none()
        {
            // todo forward internally to a node running the metadata store?
            bail!("Cannot provision cluster with embedded metadata store that is not running on this node. Please provision the cluster on a node that runs the metadata store.");
        }

        let dry_run = request.dry_run;

        let cluster_configuration = Self::resolve_cluster_configuration(common_opts, request);

        if dry_run {
            return Ok(HandleProvisionClusterResponse::DryRun {
                cluster_configuration,
            });
        }

        let nodes_configuration = match self
            .provision_metadata(common_opts, &cluster_configuration)
            .await
        {
            Ok(nodes_configuration) => nodes_configuration,
            Err(ProvisionMetadataError::AlreadyProvisioned) => {
                return Ok(HandleProvisionClusterResponse::AlreadyProvisioned);
            }
            err => err?,
        };

        Ok(HandleProvisionClusterResponse::Provisioned {
            nodes_configuration,
            cluster_configuration,
        })
    }

    async fn provision_metadata(
        &self,
        common_opts: &CommonOptions,
        cluster_configuration: &ClusterConfiguration,
    ) -> Result<NodesConfiguration, ProvisionMetadataError> {
        let (initial_nodes_configuration, initial_partition_table, initial_logs) =
            Self::generate_initial_metadata(common_opts, cluster_configuration);

        if let Some(metadata_store_handle) = self.metadata_store_provision_handle.as_ref() {
            metadata_store_handle
                .provision(
                    initial_nodes_configuration.clone(),
                    initial_partition_table,
                    initial_logs,
                )
                .await?;
            Ok(initial_nodes_configuration)
        } else {
            // we must be running with an external metadata store
            self.provision_external_metadata(
                &initial_nodes_configuration,
                &initial_partition_table,
                &initial_logs,
            )
            .await?;
            Ok(initial_nodes_configuration)
        }
    }

    fn resolve_cluster_configuration(
        common_opts: &CommonOptions,
        request: ProvisionClusterRequest,
    ) -> ClusterConfiguration {
        let num_partitions = request
            .num_partitions
            .unwrap_or(common_opts.bootstrap_num_partitions);
        let placement_strategy = request.placement_strategy.unwrap_or_default();
        let log_provider = request.log_provider.unwrap_or_default();

        ClusterConfiguration {
            num_partitions,
            replication_strategy: placement_strategy,
            default_provider: log_provider,
        }
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
            .set_replication_strategy(cluster_configuration.replication_strategy);
        let initial_partition_table = initial_partition_table_builder.build();

        let mut logs_builder = Logs::default().into_builder();
        logs_builder.set_configuration(LogsConfiguration::from(
            cluster_configuration.default_provider.clone(),
        ));
        let initial_logs = logs_builder.build();

        let mut initial_nodes_configuration =
            NodesConfiguration::new(Version::MIN, common_opts.cluster_name().to_owned());
        let node_config = NodeConfig::new(
            common_opts.node_name().to_owned(),
            common_opts
                .force_node_id
                .map(|force_node_id| force_node_id.with_generation(1))
                .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID),
            common_opts.advertised_address.clone(),
            common_opts.roles,
            LogServerConfig::default(),
        );
        initial_nodes_configuration.upsert_node(node_config);

        (
            initial_nodes_configuration,
            initial_partition_table,
            initial_logs,
        )
    }

    async fn join_cluster(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> anyhow::Result<NodesConfiguration> {
        info!(
            "Trying to join an existing cluster '{}'",
            common_opts.cluster_name()
        );

        // todo make configurable
        // Never give up trying to join the cluster. Users of this struct will set a timeout if
        // needed.
        let join_retry = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(5)),
        );

        let join_start = Instant::now();

        join_retry
            .retry_if(
                || Self::join_cluster_inner(metadata_store_client, common_opts),
                |err| {
                    if join_start.elapsed() < Duration::from_secs(5) {
                        trace!("Failed joining the cluster: {err}; retrying");
                    } else {
                        debug!("Failed joining cluster: {err}; retrying");
                    }
                    match err {
                        JoinError::MissingNodesConfiguration => true,
                        JoinError::ConcurrentNodeRegistration(_) => false,
                        JoinError::MetadataStore(err) => err.is_network_error(),
                        JoinError::ClusterMismatch { .. } => false,
                    }
                },
            )
            .await
            .map_err(Into::into)
    }

    async fn join_cluster_inner(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> Result<NodesConfiguration, JoinError> {
        let mut previous_node_generation = None;

        metadata_store_client
            .read_modify_write::<NodesConfiguration, _, _>(
                NODES_CONFIG_KEY.clone(),
                move |nodes_config| {
                    let mut nodes_config =
                        nodes_config.ok_or(JoinError::MissingNodesConfiguration)?;

                    // check that we are joining the right cluster
                    if nodes_config.cluster_name() != common_opts.cluster_name() {
                        return Err(JoinError::ClusterMismatch {
                            expected_cluster_name: common_opts.cluster_name().to_owned(),
                            actual_cluster_name: nodes_config.cluster_name().to_owned(),
                        });
                    }

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
                                return Err(JoinError::ConcurrentNodeRegistration(
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
                            "duplicate plain node id '{plain_node_id}'"
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
                },
            )
            .await
            .map_err(|err| err.transpose())
    }

    async fn provision_external_metadata(
        &self,
        initial_nodes_configuration: &NodesConfiguration,
        initial_partition_table: &PartitionTable,
        initial_logs: &Logs,
    ) -> Result<(), ProvisionMetadataError> {
        let retry_policy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();

        let write_metadata = std::pin::pin!(retry_on_network_error(retry_policy, || async {
            // todo replace with multi put once supported
            self.write_initial_partition_table(initial_partition_table)
                .await?;
            self.write_initial_logs(initial_logs).await?;
            self.write_initial_nodes_configuration(initial_nodes_configuration)
                .await
        })
        .map_err(|err| match err {
            WriteError::FailedPrecondition(_) => ProvisionMetadataError::AlreadyProvisioned,
            err => ProvisionMetadataError::other(err),
        }));

        tokio::select! {
            result = write_metadata => result,
            _ = cancellation_watcher() => {
                Err(ProvisionMetadataError::Shutdown(ShutdownError))
            }
        }
    }

    async fn write_initial_partition_table(
        &self,
        initial_partition_table: &PartitionTable,
    ) -> Result<(), WriteError> {
        // Don't overwrite the value if it exists because we might already be provisioned
        Self::write_initial_value_dont_fail_if_it_exists(
            self.metadata_store_client,
            PARTITION_TABLE_KEY.clone(),
            initial_partition_table,
        )
        .await
    }

    async fn write_initial_logs(&self, initial_logs: &Logs) -> Result<(), WriteError> {
        // Don't overwrite the value if it exists because we might already be provisioned
        Self::write_initial_value_dont_fail_if_it_exists(
            self.metadata_store_client,
            BIFROST_CONFIG_KEY.clone(),
            initial_logs,
        )
        .await
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

    async fn write_initial_nodes_configuration(
        &self,
        initial_nodes_configuration: &NodesConfiguration,
    ) -> Result<(), WriteError> {
        self.metadata_store_client
            .put(
                NODES_CONFIG_KEY.clone(),
                initial_nodes_configuration,
                Precondition::DoesNotExist,
            )
            .await
    }
}
