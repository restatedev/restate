// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max_by_key;

use anyhow::Context;
use bytes::BytesMut;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};
use tracing::debug;

use restate_metadata_server_grpc::grpc::new_metadata_server_client;
use restate_metadata_store::protobuf::metadata_proxy_svc::metadata_proxy_svc_server::{
    MetadataProxySvc, MetadataProxySvcServer,
};
use restate_metadata_store::protobuf::metadata_proxy_svc::{
    DeleteRequest, GetRequest, GetResponse, GetVersionResponse, PutRequest,
};

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::{NodeCtlSvc, NodeCtlSvcServer};
use restate_core::protobuf::node_ctl_svc::{
    ClusterHealthResponse, EmbeddedMetadataClusterHealth, GetMetadataRequest, GetMetadataResponse,
    IdentResponse, ProvisionClusterRequest, ProvisionClusterResponse,
};
use restate_core::{Identification, MetadataWriter};
use restate_core::{Metadata, MetadataKind};
use restate_metadata_store::{MetadataStoreClient, WriteError};
use restate_types::Version;
use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::errors::ConversionError;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration};
use restate_types::metadata::VersionedValue;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::ClusterConfiguration as ProtoClusterConfiguration;
use restate_types::replication::ReplicationProperty;
use restate_types::storage::StorageCodec;

use crate::{ClusterConfiguration, provision_cluster_metadata};

pub struct NodeCtlSvcHandler {
    metadata_writer: MetadataWriter,
}

impl NodeCtlSvcHandler {
    pub fn new(metadata_writer: MetadataWriter) -> Self {
        Self { metadata_writer }
    }

    pub fn into_server(self, config: &NetworkingOptions) -> NodeCtlSvcServer<Self> {
        let server = NodeCtlSvcServer::new(self)
            .max_decoding_message_size(config.max_message_size.as_usize())
            .max_encoding_message_size(config.max_message_size.as_usize())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        if config.disable_compression {
            server
        } else {
            // note: the order of those calls defines the priority
            // deflate/gzip has significantly higher CPU overhead according to our CPU profiling,
            // so we prefer zstd over gzip.
            server
                .send_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
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
            })
            .transpose()?
            .unwrap_or(config.common.default_num_partitions);

        let partition_replication: ReplicationProperty = request
            .partition_replication
            .map(TryInto::try_into)
            .transpose()?
            .unwrap_or_else(|| config.common.default_replication.clone());
        let log_provider = request
            .log_provider
            .map(|log_provider| log_provider.parse())
            .transpose()?
            .unwrap_or(config.bifrost.default_provider);
        let target_nodeset_size = request
            .target_nodeset_size
            .map(NodeSetSize::try_from)
            .transpose()?
            .unwrap_or(config.bifrost.replicated_loglet.default_nodeset_size);
        let log_replication = request
            .log_replication
            .map(ReplicationProperty::try_from)
            .transpose()?
            .unwrap_or_else(|| config.common.default_replication.clone());

        let provider_configuration =
            ProviderConfiguration::from((log_provider, log_replication, target_nodeset_size));

        Ok(ClusterConfiguration {
            num_partitions,
            partition_replication: partition_replication.into(),
            bifrost_provider: provider_configuration,
        })
    }
}

#[async_trait::async_trait]
impl NodeCtlSvc for NodeCtlSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        let identification = Identification::get();
        Ok(Response::new(IdentResponse::from(identification)))
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
            &self.metadata_writer,
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

    async fn cluster_health(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        if Metadata::with_current(|m| m.my_node_id_opt()).is_none() {
            return Err(Status::unavailable(
                "The cluster does not seem to be provisioned yet. Try again later.",
            ));
        }

        let nodes_configuration = Metadata::with_current(|m| m.nodes_config_ref());
        let cluster_name = nodes_configuration.cluster_name().to_string();

        let mut max_metadata_cluster_configuration = None;

        for (node_id, node_config) in nodes_configuration.iter_role(Role::MetadataServer) {
            let mut metadata_server_client = new_metadata_server_client(
                create_tonic_channel(
                    node_config.address.clone(),
                    &Configuration::pinned().networking,
                    DNSResolution::Gai,
                ),
                &Configuration::pinned().networking,
            );

            let response = match metadata_server_client.status(()).await {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    debug!("Failed retrieving metadata server status from '{node_id}': {err}");
                    continue;
                }
            };

            if let Some(configuration) = response.configuration {
                if let Some(max_configuration) = max_metadata_cluster_configuration {
                    max_metadata_cluster_configuration =
                        Some(max_by_key(max_configuration, configuration, |config| {
                            config.version.map(Version::from)
                        }))
                } else {
                    max_metadata_cluster_configuration = Some(configuration);
                }
            }
        }

        let metadata_cluster_health = max_metadata_cluster_configuration.map(|configuration| {
            let members = configuration
                .members
                .into_keys()
                .map(|plain_node_id| restate_types::protobuf::common::NodeId {
                    id: plain_node_id,
                    generation: None,
                })
                .collect();
            EmbeddedMetadataClusterHealth { members }
        });

        let cluster_state_response = ClusterHealthResponse {
            cluster_name,
            metadata_cluster_health,
        };

        Ok(Response::new(cluster_state_response))
    }
}

pub struct MetadataProxySvcHandler {
    metadata_store_client: MetadataStoreClient,
}

impl MetadataProxySvcHandler {
    pub fn new(metadata_store_client: MetadataStoreClient) -> Self {
        Self {
            metadata_store_client,
        }
    }

    pub fn into_server(self, config: &NetworkingOptions) -> MetadataProxySvcServer<Self> {
        let server = MetadataProxySvcServer::new(self)
            // note: the order of those calls defines the priority
            .max_decoding_message_size(config.max_message_size.as_usize())
            .max_encoding_message_size(config.max_message_size.as_usize())
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        if config.disable_compression {
            server
        } else {
            // note: the order of those calls defines the priority
            // deflate/gzip has significantly higher CPU overhead according to our CPU profiling,
            // so we prefer zstd over gzip.
            server
                .send_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
        }
    }
}

#[async_trait::async_trait]
impl MetadataProxySvc for MetadataProxySvcHandler {
    /// Get a versioned kv-pair
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .metadata_store_client
            .inner()
            .get(request.key.into())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = GetResponse {
            value: value.map(Into::into),
        };

        Ok(Response::new(response))
    }

    /// Get the current version for a kv-pair
    async fn get_version(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .metadata_store_client
            .inner()
            .get_version(request.key.into())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = GetVersionResponse {
            version: value.map(Into::into),
        };

        Ok(Response::new(response))
    }
    /// Puts the given kv-pair into the metadata store
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let value: VersionedValue = request
            .value
            .ok_or_else(|| Status::invalid_argument("value is required"))?
            .try_into()
            .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?;

        let precondition = request
            .precondition
            .ok_or_else(|| Status::invalid_argument("precondition is required"))?
            .try_into()
            .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?;

        self.metadata_store_client
            .inner()
            .put(request.key.into(), value, precondition)
            .await
            .map_err(|err| match err {
                WriteError::FailedPrecondition(msg) => Status::failed_precondition(msg),
                err => Status::internal(err.to_string()),
            })?;

        Ok(Response::new(()))
    }

    /// Deletes the given kv-pair
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let precondition = request
            .precondition
            .ok_or_else(|| Status::invalid_argument("precondition is required"))?
            .try_into()
            .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?;

        self.metadata_store_client
            .inner()
            .delete(request.key.into(), precondition)
            .await
            .map_err(|err| match err {
                WriteError::FailedPrecondition(msg) => Status::failed_precondition(msg),
                err => Status::internal(err.to_string()),
            })?;

        Ok(Response::new(()))
    }
}
