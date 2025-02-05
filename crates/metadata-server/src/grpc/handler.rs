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
use tonic::{Request, Response, Status};

use restate_core::metadata_store::{serialize_value, Precondition};
use restate_types::config::Configuration;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::StorageCodec;

use crate::grpc::metadata_server_svc_server::MetadataServerSvc;
use crate::grpc::pb_conversions::ConversionError;
use crate::grpc::{
    DeleteRequest, GetRequest, GetResponse, GetVersionResponse,
    ProvisionRequest as ProtoProvisionRequest, ProvisionResponse, PutRequest, StatusResponse,
};
use crate::metric_definitions::{
    METADATA_SERVER_DELETE_DURATION, METADATA_SERVER_DELETE_TOTAL, METADATA_SERVER_GET_DURATION,
    METADATA_SERVER_GET_TOTAL, METADATA_SERVER_GET_VERSION_DURATION,
    METADATA_SERVER_GET_VERSION_TOTAL, METADATA_SERVER_PUT_DURATION, METADATA_SERVER_PUT_TOTAL,
    STATUS_COMPLETED, STATUS_FAILED,
};
use crate::{
    prepare_initial_nodes_configuration, MetadataStoreRequest, MetadataStoreSummary,
    ProvisionError, ProvisionRequest, ProvisionSender, RequestError, RequestSender, StatusWatch,
};
/// Grpc svc handler for the metadata store.
#[derive(Debug)]
pub struct MetadataStoreHandler {
    request_tx: RequestSender,
    provision_tx: Option<ProvisionSender>,
    status_watch: Option<StatusWatch>,
}

impl MetadataStoreHandler {
    pub fn new(
        request_tx: RequestSender,
        provision_tx: Option<ProvisionSender>,
        status_watch: Option<watch::Receiver<MetadataStoreSummary>>,
    ) -> Self {
        Self {
            request_tx,
            provision_tx,
            status_watch,
        }
    }
}

#[async_trait]
impl MetadataServerSvc for MetadataStoreHandler {
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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))??;

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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))??;

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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))??;

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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))??;

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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            let newly_provisioned = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))??;

            Ok(Response::new(ProvisionResponse { newly_provisioned }))
        } else {
            // if there is no provision_tx configured, then the underlying metadata store does not
            // need a provision step.
            let mut request = request.into_inner();
            let mut nodes_configuration: NodesConfiguration =
                StorageCodec::decode(&mut request.nodes_configuration)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;

            // Make sure that the NodesConfiguration our node and has the right metadata store state set.
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
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

            let result = result_rx
                .await
                .map_err(|_| Status::unavailable("metadata store is shut down"))?;

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
                "metadata store does not support reporting its status",
            ))
        }
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
