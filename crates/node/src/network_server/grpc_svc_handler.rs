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
use std::num::NonZeroU16;

use anyhow::Context;
use bytes::BytesMut;
use enumset::EnumSet;
use tonic::{Request, Response, Status};
use tracing::debug;

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::net_util::create_tonic_channel;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::NodeCtlSvc;
use restate_core::protobuf::node_ctl_svc::{
    ClusterHealthResponse, EmbeddedMetadataClusterHealth, GetMetadataRequest, GetMetadataResponse,
    IdentResponse, MetadataDeleteRequest, MetadataGetRequest, MetadataGetResponse,
    MetadataGetVersionResponse, MetadataPutRequest, ProvisionClusterRequest,
    ProvisionClusterResponse,
};
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{Metadata, MetadataKind, TargetVersion, TaskCenter, task_center};
use restate_metadata_server::WriteError;
use restate_metadata_server::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::errors::ConversionError;
use restate_types::logs::metadata::{NodeSetSize, ProviderConfiguration};
use restate_types::metadata::VersionedValue;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::ClusterConfiguration as ProtoClusterConfiguration;
use restate_types::replication::ReplicationProperty;
use restate_types::storage::StorageCodec;

use crate::{ClusterConfiguration, provision_cluster_metadata};

pub struct NodeCtlSvcHandler {
    task_center: task_center::Handle,
    cluster_name: String,
    roles: EnumSet<Role>,
    metadata_store_client: MetadataStoreClient,
}

impl NodeCtlSvcHandler {
    pub fn new(
        task_center: task_center::Handle,
        cluster_name: String,
        roles: EnumSet<Role>,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        Self {
            task_center,
            cluster_name,
            roles,
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
            .unwrap_or(config.common.default_num_partitions);
        let partition_replication = request.partition_replication.try_into()?;

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
            .unwrap_or_else(|| {
                config
                    .bifrost
                    .replicated_loglet
                    .default_log_replication
                    .clone()
            });

        let provider_configuration =
            ProviderConfiguration::from((log_provider, log_replication, target_nodeset_size));

        Ok(ClusterConfiguration {
            num_partitions,
            partition_replication,
            bifrost_provider: provider_configuration,
        })
    }
}

#[async_trait::async_trait]
impl NodeCtlSvc for NodeCtlSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        let (node_status, admin_status, worker_status, metadata_server_status, log_server_status) =
            TaskCenter::with_current(|tc| {
                let health = tc.health();
                (
                    health.current_node_status(),
                    health.current_admin_status(),
                    health.current_worker_status(),
                    health.current_metadata_store_status(),
                    health.current_log_server_status(),
                )
            });
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
            let mut metadata_server_client = MetadataServerSvcClient::new(create_tonic_channel(
                node_config.address.clone(),
                &Configuration::pinned().networking,
            ));

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

    /// Get a versioned kv-pair
    async fn metadata_get(
        &self,
        request: tonic::Request<MetadataGetRequest>,
    ) -> Result<Response<MetadataGetResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .metadata_store_client
            .inner()
            .get(request.key.into())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = MetadataGetResponse {
            value: value.map(Into::into),
        };

        Ok(Response::new(response))
    }

    /// Get the current version for a kv-pair
    async fn metadata_get_version(
        &self,
        request: Request<MetadataGetRequest>,
    ) -> Result<Response<MetadataGetVersionResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .metadata_store_client
            .inner()
            .get_version(request.key.into())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = MetadataGetVersionResponse {
            version: value.map(Into::into),
        };

        Ok(Response::new(response))
    }
    /// Puts the given kv-pair into the metadata store
    async fn metadata_put(
        &self,
        request: Request<MetadataPutRequest>,
    ) -> Result<Response<()>, Status> {
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
    async fn metadata_delete(
        &self,
        request: Request<MetadataDeleteRequest>,
    ) -> Result<Response<()>, Status> {
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
