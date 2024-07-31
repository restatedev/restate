// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use restate_core::network::protobuf::node_svc::node_svc_server::NodeSvc;
use restate_core::network::protobuf::node_svc::IdentResponse;
use restate_core::network::protobuf::node_svc::{StorageQueryRequest, StorageQueryResponse};
use restate_core::network::ConnectionManager;
use restate_core::network::ProtocolError;
use restate_core::{metadata, TaskCenter};
use restate_types::protobuf::common::NodeStatus;
use restate_types::protobuf::node::Message;
use std::str::FromStr;
use tokio_stream::StreamExt;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::{Code, Request, Response, Status, Streaming};

use crate::network_server::WorkerDependencies;

pub struct NodeSvcHandler {
    task_center: TaskCenter,
    worker: Option<WorkerDependencies>,
    connections: ConnectionManager,
}

impl NodeSvcHandler {
    pub fn new(
        task_center: TaskCenter,
        worker: Option<WorkerDependencies>,
        connections: ConnectionManager,
    ) -> Self {
        Self {
            task_center,
            worker,
            connections,
        }
    }
}

#[async_trait::async_trait]
impl NodeSvc for NodeSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        // STUB IMPLEMENTATION
        self.task_center.run_in_scope_sync("get_ident", None, || {
            Ok(Response::new(IdentResponse {
                status: NodeStatus::Alive.into(),
                node_id: Some(metadata().my_node_id().into()),
            }))
        })
    }

    type QueryStorageStream = BoxStream<'static, Result<StorageQueryResponse, Status>>;

    async fn query_storage(
        &self,
        request: Request<StorageQueryRequest>,
    ) -> Result<Response<Self::QueryStorageStream>, Status> {
        let Some(ref worker) = self.worker else {
            return Err(Status::failed_precondition("Not a worker node"));
        };
        let query = request.into_inner().query;

        let record_stream = self
            .task_center
            .run_in_scope("query-storage", None, async move {
                worker.query_context.execute(&query).await.map_err(|err| {
                    Status::internal(format!("failed executing the query '{}': {}", query, err))
                })
            })
            .await?;

        let schema = record_stream.schema();
        let response_stream =
            FlightDataEncoderBuilder::new()
                // CLI is expecting schema information
                .with_schema(schema)
                .build(record_stream.map_err(|err| {
                    FlightError::from(datafusion::arrow::error::ArrowError::from(err))
                }))
                .map_ok(|flight_data| StorageQueryResponse {
                    header: flight_data.data_header,
                    data: flight_data.data_body,
                })
                .map_err(flight_error_to_tonic_status);
        Ok(Response::new(Box::pin(response_stream)))
    }

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
            .task_center
            .run_in_scope(
                "accept-connection",
                None,
                self.connections.accept_incoming_connection(transformed),
            )
            .await?;

        Ok(Response::new(output_stream))
    }
}

// todo: Remove once arrow-flight supports tonic 0.12
fn flight_error_to_tonic_status(err: FlightError) -> Status {
    match err {
        FlightError::Arrow(e) => Status::internal(e.to_string()),
        FlightError::NotYetImplemented(e) => Status::internal(e),
        FlightError::Tonic(status) => tonic_status_010_to_012(status),
        FlightError::ProtocolError(e) => Status::internal(e),
        FlightError::DecodeError(e) => Status::internal(e),
        FlightError::ExternalError(e) => Status::internal(e.to_string()),
    }
}

// todo: Remove once arrow-flight works with tonic 0.12
fn tonic_status_010_to_012(status: tonic_0_11::Status) -> Status {
    let code = Code::from(status.code() as i32);
    let message = status.message().to_owned();
    let details = Bytes::copy_from_slice(status.details());
    let metadata = tonic_metadata_map_010_to_012(status.metadata());
    Status::with_details_and_metadata(code, message, details, metadata)
}

// todo: Remove once arrow-flight works with tonic 0.12
fn tonic_metadata_map_010_to_012(metadata_map: &tonic_0_11::metadata::MetadataMap) -> MetadataMap {
    let mut resulting_metadata_map = MetadataMap::with_capacity(metadata_map.len());
    for key_value in metadata_map.iter() {
        match key_value {
            tonic_0_11::metadata::KeyAndValueRef::Ascii(key, value) => {
                // ignore metadata map entries if conversion fails
                if let Ok(value) = MetadataValue::from_str(value.to_str().unwrap_or("")) {
                    if let Ok(key) = MetadataKey::from_str(key.as_str()) {
                        resulting_metadata_map.insert(key, value);
                    }
                }
            }
            tonic_0_11::metadata::KeyAndValueRef::Binary(key, value) => {
                if let Ok(key) = MetadataKey::from_bytes(key.as_ref()) {
                    let value = MetadataValue::from_bytes(value.as_ref());
                    resulting_metadata_map.insert_bin(key, value);
                }
            }
        }
    }

    resulting_metadata_map
}
