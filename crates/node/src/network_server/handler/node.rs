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
use futures::stream::BoxStream;
use futures::TryStreamExt;
use restate_core::{metadata, TaskCenter};
use restate_network::error::ProtocolError;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::network_server::WorkerDependencies;
use restate_network::ConnectionManager;
use restate_node_protocol::node::Message;
use restate_node_services::node_svc::node_svc_server::NodeSvc;
use restate_node_services::node_svc::{IdentResponse, NodeStatus};
use restate_node_services::node_svc::{StorageQueryRequest, StorageQueryResponse};

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
                .map_err(Status::from);
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
