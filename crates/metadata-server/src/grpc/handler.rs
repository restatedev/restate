// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use async_trait::async_trait;
use metrics::{counter, histogram};
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

use restate_metadata_server_grpc::grpc::metadata_server_svc_server::MetadataServerSvc;
use restate_metadata_server_grpc::grpc::metadata_server_svc_server::MetadataServerSvcServer;
use restate_metadata_server_grpc::grpc::{
    DeleteRequest, GetRequest, GetResponse, GetVersionResponse,
    ProvisionRequest as ProtoProvisionRequest, ProvisionResponse, PutRequest, RemoveNodeRequest,
    StatusResponse,
};
use restate_metadata_store::serialize_value;
use restate_types::config::Configuration;
use restate_types::errors::ConversionError;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::{
    MetadataServerConfig, MetadataServerState, NodeConfig, NodesConfiguration,
};
use restate_types::storage::StorageCodec;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::metric_definitions::{
    METADATA_SERVER_DELETE_DURATION, METADATA_SERVER_DELETE_TOTAL, METADATA_SERVER_GET_DURATION,
    METADATA_SERVER_GET_TOTAL, METADATA_SERVER_GET_VERSION_DURATION,
    METADATA_SERVER_GET_VERSION_TOTAL, METADATA_SERVER_PUT_DURATION, METADATA_SERVER_PUT_TOTAL,
    STATUS_COMPLETED, STATUS_FAILED,
};
use crate::{
    InvalidConfiguration, MetadataCommand, MetadataCommandSender, MetadataServerSummary,
    MetadataStoreRequest, ProvisionError, ProvisionRequest, ProvisionSender, RequestError,
    RequestSender, StatusWatch,
};

/// Ensures that the initial nodes configuration contains the current node and has the right
/// [`MetadataServerState`] set.
fn prepare_initial_nodes_configuration(
    configuration: &Configuration,
    nodes_configuration: &mut NodesConfiguration,
) -> Result<PlainNodeId, InvalidConfiguration> {
    let plain_node_id = if let Some(node_config) =
        nodes_configuration.find_node_by_name(configuration.common.node_name())
    {
        if let Some(force_node_id) = configuration.common.force_node_id {
            if force_node_id != node_config.current_generation.as_plain() {
                return Err(InvalidConfiguration(format!(
                    "nodes configuration has wrong plain node id; expected: {}, actual: {}",
                    force_node_id,
                    node_config.current_generation.as_plain()
                )));
            }
        }

        let restate_node_id = node_config.current_generation.as_plain();

        let mut node_config = node_config.clone();
        node_config.metadata_server_config.metadata_server_state = MetadataServerState::Member;

        nodes_configuration.upsert_node(node_config);

        restate_node_id
    } else {
        // give precedence to the force node id
        let current_generation = configuration
            .common
            .force_node_id
            .map(|node_id| node_id.with_generation(1))
            .unwrap_or_else(|| {
                nodes_configuration
                    .max_plain_node_id()
                    .map(|node_id| node_id.next().with_generation(1))
                    .unwrap_or(GenerationalNodeId::INITIAL_NODE_ID)
            });

        let metadata_server_config = MetadataServerConfig {
            metadata_server_state: MetadataServerState::Member,
        };

        let node_config = NodeConfig::builder()
            .name(configuration.common.node_name().to_owned())
            .current_generation(current_generation)
            .location(configuration.common.location().clone())
            .address(configuration.common.advertised_address.clone())
            .roles(configuration.common.roles)
            .metadata_server_config(metadata_server_config)
            .build();

        nodes_configuration.upsert_node(node_config);

        current_generation.as_plain()
    };

    Ok(plain_node_id)
}

/// Grpc svc handler for the metadata server.
#[derive(Debug)]
pub struct MetadataServerHandler {
    request_tx: RequestSender,
    provision_tx: Option<ProvisionSender>,
    status_watch: Option<StatusWatch>,
    command_tx: MetadataCommandSender,
}

impl MetadataServerHandler {
    pub fn new(
        request_tx: RequestSender,
        provision_tx: Option<ProvisionSender>,
        status_watch: Option<watch::Receiver<MetadataServerSummary>>,
        command_tx: MetadataCommandSender,
    ) -> Self {
        Self {
            request_tx,
            provision_tx,
            status_watch,
            command_tx,
        }
    }

    pub fn into_server(self) -> MetadataServerSvcServer<Self> {
        MetadataServerSvcServer::new(self)
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            // note: the order of those calls defines the priority
            // deflate/gzip has significantly higher CPU overhead according to our CPU profiling,
            // so we prefer zstd over gzip.
            .send_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
    }
}

#[async_trait]
impl MetadataServerSvc for MetadataServerHandler {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let start_time = Instant::now();

        let result = {
            let (result_tx, result_rx) = oneshot::channel();

            let request = request.into_inner();
            self.request_tx
                .send(MetadataStoreRequest::Get {
                    key: request.key.into(),
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))??;

            Ok(Response::new(GetResponse {
                value: result.map(Into::into),
            }))
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_SERVER_GET_DURATION).record(start_time.elapsed());
        counter!(METADATA_SERVER_GET_TOTAL, "status" => status).increment(1);

        result
    }

    async fn get_version(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        let start_time = Instant::now();
        let result = {
            let (result_tx, result_rx) = oneshot::channel();

            let request = request.into_inner();
            self.request_tx
                .send(MetadataStoreRequest::GetVersion {
                    key: request.key.into(),
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))??;

            Ok(Response::new(GetVersionResponse {
                version: result.map(Into::into),
            }))
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_SERVER_GET_VERSION_DURATION).record(start_time.elapsed());
        counter!(METADATA_SERVER_GET_VERSION_TOTAL, "status" => status).increment(1);

        result
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<()>, Status> {
        let start_time = Instant::now();
        let result = {
            let (result_tx, result_rx) = oneshot::channel();

            let request = request.into_inner();
            self.request_tx
                .send(MetadataStoreRequest::Put {
                    key: request.key.into(),
                    value: request
                        .value
                        .ok_or_else(|| Status::invalid_argument("missing value field"))?
                        .try_into()
                        .map_err(|err: ConversionError| {
                            Status::invalid_argument(err.to_string())
                        })?,
                    precondition: request
                        .precondition
                        .ok_or_else(|| Status::invalid_argument("missing precondition field"))?
                        .try_into()
                        .map_err(|err: ConversionError| {
                            Status::invalid_argument(err.to_string())
                        })?,
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))??;

            Ok(Response::new(()))
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_SERVER_PUT_DURATION).record(start_time.elapsed());
        counter!(METADATA_SERVER_PUT_TOTAL, "status" => status).increment(1);

        result
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<()>, Status> {
        let start_time = Instant::now();
        let result = {
            let (result_tx, result_rx) = oneshot::channel();

            let request = request.into_inner();
            self.request_tx
                .send(MetadataStoreRequest::Delete {
                    key: request.key.into(),
                    precondition: request
                        .precondition
                        .ok_or_else(|| Status::invalid_argument("missing precondition field"))?
                        .try_into()
                        .map_err(|err: ConversionError| {
                            Status::invalid_argument(err.to_string())
                        })?,
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))??;

            Ok(Response::new(()))
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_SERVER_DELETE_DURATION).record(start_time.elapsed());
        counter!(METADATA_SERVER_DELETE_TOTAL, "status" => status).increment(1);

        result
    }

    async fn provision(
        &self,
        request: Request<ProtoProvisionRequest>,
    ) -> Result<Response<ProvisionResponse>, Status> {
        if let Some(provision_tx) = self.provision_tx.as_ref() {
            let (result_tx, result_rx) = oneshot::channel();

            let mut request = request.into_inner();

            let nodes_configuration = StorageCodec::decode(&mut request.nodes_configuration)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

            provision_tx
                .send(ProvisionRequest {
                    nodes_configuration,
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            let newly_provisioned = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))??;

            Ok(Response::new(ProvisionResponse { newly_provisioned }))
        } else {
            // if there is no provision_tx configured, then the underlying metadata server does not
            // need a provision step.
            let mut request = request.into_inner();
            let mut nodes_configuration: NodesConfiguration =
                StorageCodec::decode(&mut request.nodes_configuration)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;

            // Make sure that the NodesConfiguration our node and has the right metadata server state set.
            prepare_initial_nodes_configuration(&Configuration::pinned(), &mut nodes_configuration)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

            let versioned_value = serialize_value(&nodes_configuration)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            let (result_tx, result_rx) = oneshot::channel();

            self.request_tx
                .send(MetadataStoreRequest::Put {
                    key: NODES_CONFIG_KEY.clone(),
                    value: versioned_value,
                    precondition: Precondition::DoesNotExist,
                    result_tx,
                })
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata server is shut down"))?;

            let newly_provisioned = match result {
                Ok(()) => true,
                Err(RequestError::FailedPrecondition(_)) => false,
                Err(err) => Err(err)?,
            };

            Ok(Response::new(ProvisionResponse { newly_provisioned }))
        }
    }

    async fn status(&self, _request: Request<()>) -> Result<Response<StatusResponse>, Status> {
        if let Some(status_watch) = &self.status_watch {
            let response = StatusResponse::from(status_watch.borrow().deref().clone());
            Ok(Response::new(response))
        } else {
            Err(Status::unimplemented(
                "metadata server does not support reporting its status",
            ))
        }
    }

    async fn add_node(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        let (node_added_tx, node_added_rx) = oneshot::channel();
        self.command_tx
            .send(MetadataCommand::AddNode(node_added_tx))
            .await
            .map_err(|_| Status::unavailable("metadata server is shut down"))?;

        node_added_rx
            .await
            .map_err(|_| Status::unavailable("metadata server is shut down"))?
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(()))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<()>, Status> {
        let (node_removed_tx, node_removed_rx) = oneshot::channel();
        let request = request.into_inner();
        self.command_tx
            .send(MetadataCommand::RemoveNode {
                plain_node_id: PlainNodeId::from(request.plain_node_id),
                created_at_millis: request.created_at_millis,
                response_tx: node_removed_tx,
            })
            .await
            .map_err(|_| Status::unavailable("metadata server is shut down"))?;

        node_removed_rx
            .await
            .map_err(|_| Status::unavailable("metadata server is shut down"))?
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(()))
    }
}

impl From<RequestError> for Status {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::FailedPrecondition(err) => Status::failed_precondition(err.to_string()),
            RequestError::Unavailable(err, known_leader) => {
                let mut status = Status::unavailable(err.to_string());

                if let Some(known_leader) = known_leader {
                    known_leader.add_to_status(&mut status);
                }

                status
            }
            err => Status::internal(err.to_string()),
        }
    }
}

impl From<ProvisionError> for Status {
    fn from(err: ProvisionError) -> Self {
        match err {
            ProvisionError::Internal(err) => Status::internal(err.to_string()),
        }
    }
}
