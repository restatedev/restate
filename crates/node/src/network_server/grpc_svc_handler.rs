// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;

use anyhow::Context;
use bytes::BytesMut;
use enumset::EnumSet;
use futures::stream::BoxStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::protobuf::core_node_svc::core_node_svc_server::CoreNodeSvc;
use restate_core::network::{ConnectionManager, ProtocolError, TransportConnect};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::NodeCtlSvc;
use restate_core::protobuf::node_ctl_svc::{
    GetMetadataRequest, GetMetadataResponse, IdentResponse, ProvisionClusterRequest,
    ProvisionClusterResponse,
};
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{task_center, Metadata, MetadataKind, TargetVersion};
use restate_types::config::Configuration;
use restate_types::health::Health;
use restate_types::logs::metadata::ProviderConfiguration;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::ClusterConfiguration as ProtoClusterConfiguration;
use restate_types::protobuf::node::Message;
use restate_types::storage::StorageCodec;

use crate::{provision_cluster_metadata, ClusterConfiguration};

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
        let partition_replication = request.partition_replication.try_into()?;
        let bifrost_provider = request
            .log_provider
            .map(ProviderConfiguration::try_from)
            .unwrap_or_else(|| Ok(ProviderConfiguration::from_configuration(config)))?;

        Ok(ClusterConfiguration {
            num_partitions,
            partition_replication,
            bifrost_provider,
        })
    }
}

#[async_trait::async_trait]
impl NodeCtlSvc for NodeCtlSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        let node_status = self.health.current_node_status();
        let admin_status = self.health.current_admin_status();
        let worker_status = self.health.current_worker_status();
        let metadata_server_status = self.health.current_metadata_store_status();
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

        let newly_provisioned = provision_cluster_metadata(
            &self.metadata_store_client,
            &config.common,
            &cluster_configuration,
        )
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
