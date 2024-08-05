// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::pb_conversions::ConversionError;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvc;
use crate::grpc_svc::{DeleteRequest, GetRequest, GetResponse, GetVersionResponse, PutRequest};
use crate::{Error, MetadataStoreRequest, RequestSender};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

/// Grpc svc handler for the metadata store.
#[derive(Debug)]
pub struct MetadataStoreHandler {
    request_tx: RequestSender,
}

impl MetadataStoreHandler {
    pub fn new(request_tx: RequestSender) -> Self {
        Self { request_tx }
    }
}

#[async_trait]
impl MetadataStoreSvc for MetadataStoreHandler {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
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
    }

    async fn get_version(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
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
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<()>, Status> {
        let (result_tx, result_rx) = oneshot::channel();

        let request = request.into_inner();
        self.request_tx
            .send(MetadataStoreRequest::Put {
                key: request.key.into(),
                value: request
                    .value
                    .ok_or_else(|| Status::invalid_argument("missing value field"))?
                    .try_into()
                    .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?,
                precondition: request
                    .precondition
                    .ok_or_else(|| Status::invalid_argument("missing precondition field"))?
                    .try_into()
                    .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?,
                result_tx,
            })
            .await
            .map_err(|_| Status::unavailable("metadata store is shut down"))?;

        result_rx
            .await
            .map_err(|_| Status::unavailable("metadata store is shut down"))??;

        Ok(Response::new(()))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<()>, Status> {
        let (result_tx, result_rx) = oneshot::channel();

        let request = request.into_inner();
        self.request_tx
            .send(MetadataStoreRequest::Delete {
                key: request.key.into(),
                precondition: request
                    .precondition
                    .ok_or_else(|| Status::invalid_argument("missing precondition field"))?
                    .try_into()
                    .map_err(|err: ConversionError| Status::invalid_argument(err.to_string()))?,
                result_tx,
            })
            .await
            .map_err(|_| Status::unavailable("metadata store is shut down"))?;

        result_rx
            .await
            .map_err(|_| Status::unavailable("metadata store is shut down"))??;

        Ok(Response::new(()))
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::FailedPrecondition(msg) => Status::failed_precondition(msg),
            err => Status::internal(err.to_string()),
        }
    }
}
