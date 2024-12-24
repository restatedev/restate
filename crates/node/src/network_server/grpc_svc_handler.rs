// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bytes::BytesMut;
use bytestring::ByteString;
use enumset::EnumSet;
use futures::stream::BoxStream;
use prost_dto::IntoProto;
use restate_core::metadata_store::{
    retry_on_network_error, MetadataStoreClient, Precondition, WriteError,
};
use restate_core::network::protobuf::core_node_svc::core_node_svc_server::CoreNodeSvc;
use restate_core::network::{ConnectionManager, ProtocolError, TransportConnect};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::NodeCtlSvc;
use restate_core::protobuf::node_ctl_svc::{
    GetMetadataRequest, GetMetadataResponse, IdentResponse, ProvisionClusterRequest,
    ProvisionClusterResponse,
};
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{task_center, Metadata, MetadataKind, TargetVersion};
use restate_types::config::{CommonOptions, Configuration};
use restate_types::health::Health;
use restate_types::logs::metadata::{Logs, LogsConfiguration, ProviderConfiguration};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, PARTITION_TABLE_KEY};
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
use restate_types::partition_table::{PartitionTable, PartitionTableBuilder, ReplicationStrategy};
use restate_types::protobuf::cluster::ClusterConfiguration as ProtoClusterConfiguration;
use restate_types::protobuf::node::Message;
use restate_types::storage::{StorageCodec, StorageEncode};
use restate_types::{GenerationalNodeId, Version, Versioned};
use std::num::NonZeroU16;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

pub struct NodeCtlSvcHandler {
    task_center: task_center::Handle,
    cluster_name: String,
    roles: EnumSet<Role>,
    health: Health,
    metadata_store_client: MetadataStoreClient,
}

impl NodeCtlSvcHandler {
    pub fn new(
        task_center: task_center::Handle,
        cluster_name: String,
        roles: EnumSet<Role>,
        health: Health,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        Self {
            task_center,
            cluster_name,
            roles,
            health,
            metadata_store_client,
        }
    }

    /// Provision the cluster metadata. Returns `true` if the cluster was newly provisioned. Returns
    /// `false` if the cluster is already provisioned.
    async fn provision_cluster_metadata(
        &self,
        common_opts: &CommonOptions,
        cluster_configuration: &ClusterConfiguration,
    ) -> anyhow::Result<bool> {
        let (initial_nodes_configuration, initial_partition_table, initial_logs) =
            Self::generate_initial_metadata(common_opts, cluster_configuration);

        let result = retry_on_network_error(common_opts.network_error_retry_policy.clone(), || {
            self.metadata_store_client
                .provision(&initial_nodes_configuration)
        })
        .await?;

        retry_on_network_error(common_opts.network_error_retry_policy.clone(), || {
            self.write_initial_value_dont_fail_if_it_exists(
                PARTITION_TABLE_KEY.clone(),
                &initial_partition_table,
            )
        })
        .await
        .context("failed provisioning the initial partition table")?;

        retry_on_network_error(common_opts.network_error_retry_policy.clone(), || {
            self.write_initial_value_dont_fail_if_it_exists(
                BIFROST_CONFIG_KEY.clone(),
                &initial_logs,
            )
        })
        .await
        .context("failed provisioning the initial logs configuration")?;

        Ok(result)
    }

    pub fn create_initial_nodes_configuration(common_opts: &CommonOptions) -> NodesConfiguration {
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
            .set_replication_strategy(cluster_configuration.replication_strategy);
        let initial_partition_table = initial_partition_table_builder.build();

        let mut logs_builder = Logs::default().into_builder();
        logs_builder.set_configuration(LogsConfiguration::from(
            cluster_configuration.default_provider.clone(),
        ));
        let initial_logs = logs_builder.build();

        let initial_nodes_configuration = Self::create_initial_nodes_configuration(common_opts);

        (
            initial_nodes_configuration,
            initial_partition_table,
            initial_logs,
        )
    }

    async fn write_initial_value_dont_fail_if_it_exists<T: Versioned + StorageEncode>(
        &self,
        key: ByteString,
        initial_value: &T,
    ) -> Result<(), WriteError> {
        match self
            .metadata_store_client
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

    fn resolve_cluster_configuration(
        config: &Configuration,
        request: ProvisionClusterRequest,
    ) -> anyhow::Result<ClusterConfiguration> {
        let num_partitions = request
            .num_partitions
            .map(|num_partitions| {
                u16::try_from(num_partitions)
                    .context("Restate only supports running up to 65535 partitions.")
                    .and_then(|num_partitions| {
                        NonZeroU16::try_from(num_partitions)
                            .context("The number of partitions needs to be > 0")
                    })
            })
            .transpose()?
            .unwrap_or(config.common.bootstrap_num_partitions);
        let replication_strategy = request
            .placement_strategy
            .map(ReplicationStrategy::try_from)
            .transpose()?
            .unwrap_or_default();
        let default_provider = request
            .log_provider
            .map(ProviderConfiguration::try_from)
            .unwrap_or_else(|| Ok(ProviderConfiguration::from_configuration(config)))?;

        Ok(ClusterConfiguration {
            num_partitions,
            replication_strategy,
            default_provider,
        })
    }
}

#[async_trait::async_trait]
impl NodeCtlSvc for NodeCtlSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        let node_status = self.health.current_node_status();
        let admin_status = self.health.current_admin_status();
        let worker_status = self.health.current_worker_status();
        let metadata_server_status = self.health.current_metadata_server_status();
        let log_server_status = self.health.current_log_server_status();
        let age_s = self.task_center.age().as_secs();
        let metadata = Metadata::current();
        Ok(Response::new(IdentResponse {
            status: node_status.into(),
            node_id: metadata.my_node_id_opt().map(Into::into),
            roles: self.roles.iter().map(|r| r.to_string()).collect(),
            cluster_name: self.cluster_name.clone(),
            age_s,
            admin_status: admin_status.into(),
            worker_status: worker_status.into(),
            metadata_server_status: metadata_server_status.into(),
            log_server_status: log_server_status.into(),
            nodes_config_version: metadata.nodes_config_version().into(),
            logs_version: metadata.logs_version().into(),
            schema_version: metadata.schema_version().into(),
            partition_table_version: metadata.partition_table_version().into(),
        }))
    }

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let request = request.into_inner();
        let metadata = Metadata::current();
        let kind = request
            .kind()
            .try_into()
            .map_err(|err: anyhow::Error| Status::invalid_argument(err.to_string()))?;
        if request.sync {
            metadata
                .sync(kind, TargetVersion::Latest)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        let mut encoded = BytesMut::new();
        match kind {
            MetadataKind::NodesConfiguration => {
                StorageCodec::encode(metadata.nodes_config_ref().as_ref(), &mut encoded)
                    .expect("We can always serialize");
            }
            MetadataKind::Schema => {
                StorageCodec::encode(metadata.schema_ref().as_ref(), &mut encoded)
                    .expect("We can always serialize");
            }
            MetadataKind::PartitionTable => {
                StorageCodec::encode(metadata.partition_table_ref().as_ref(), &mut encoded)
                    .expect("We can always serialize");
            }
            MetadataKind::Logs => {
                StorageCodec::encode(metadata.logs_ref().as_ref(), &mut encoded)
                    .expect("We can always serialize");
            }
        }
        Ok(Response::new(GetMetadataResponse {
            encoded: encoded.freeze(),
        }))
    }

    async fn provision_cluster(
        &self,
        request: Request<ProvisionClusterRequest>,
    ) -> Result<Response<ProvisionClusterResponse>, Status> {
        let request = request.into_inner();
        let config = Configuration::pinned();

        let dry_run = request.dry_run;
        let cluster_configuration = Self::resolve_cluster_configuration(&config, request)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        if dry_run {
            return Ok(Response::new(ProvisionClusterResponse::dry_run(
                ProtoClusterConfiguration::from(cluster_configuration),
            )));
        }

        let newly_provisioned = self
            .provision_cluster_metadata(&config.common, &cluster_configuration)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        if !newly_provisioned {
            return Err(Status::already_exists(
                "The cluster has already been provisioned",
            ));
        }

        Ok(Response::new(ProvisionClusterResponse::provisioned(
            ProtoClusterConfiguration::from(cluster_configuration),
        )))
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
    pub default_provider: ProviderConfiguration,
}

fn num_partitions_to_u32(num_partitions: NonZeroU16) -> u32 {
    u32::from(num_partitions.get())
}

pub struct CoreNodeSvcHandler<T> {
    connections: ConnectionManager<T>,
}

impl<T> CoreNodeSvcHandler<T> {
    pub fn new(connections: ConnectionManager<T>) -> Self {
        Self { connections }
    }
}

#[async_trait::async_trait]
impl<T> CoreNodeSvc for CoreNodeSvcHandler<T>
where
    T: TransportConnect,
{
    type CreateConnectionStream = BoxStream<'static, Result<Message, Status>>;

    // Status codes returned in different scenarios:
    // - DeadlineExceeded: No hello received within deadline
    // - InvalidArgument: Header should always be set or any other missing required part of the
    //                    handshake. This also happens if the client sent wrong message on handshake.
    // - AlreadyExists: A node with a newer generation has been observed already
    // - Cancelled: received an error from the client, or the client has dropped the stream during
    //              handshake.
    // - Unavailable: This node is shutting down
    async fn create_connection(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::CreateConnectionStream>, Status> {
        let incoming = request.into_inner();
        let transformed = incoming.map(|x| x.map_err(ProtocolError::from));
        let output_stream = self
            .connections
            .accept_incoming_connection(transformed)
            .await?;

        // For uniformity with outbound connections, we map all responses to Ok, we never rely on
        // sending tonic::Status errors explicitly. We use ConnectionControl frames to communicate
        // errors and/or drop the stream when necessary.
        Ok(Response::new(Box::pin(output_stream.map(Ok))))
    }
}
