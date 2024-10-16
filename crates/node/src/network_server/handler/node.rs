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
use enumset::EnumSet;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use restate_core::network::protobuf::node_svc::node_svc_server::NodeSvc;
use restate_core::network::protobuf::node_svc::IdentResponse;
use restate_core::network::protobuf::node_svc::{StorageQueryRequest, StorageQueryResponse};
use restate_core::network::ProtocolError;
use restate_core::network::{ConnectionManager, GrpcConnector};
use restate_core::{metadata, TaskCenter};
use restate_types::health::Health;
use restate_types::nodes_config::Role;
use restate_types::protobuf::node::Message;

use crate::network_server::WorkerDependencies;

pub struct NodeSvcHandler {
    task_center: TaskCenter,
    cluster_name: String,
    roles: EnumSet<Role>,
    health: Health,
    worker: Option<WorkerDependencies>,
    connections: ConnectionManager<GrpcConnector>,
}

impl NodeSvcHandler {
    pub fn new(
        task_center: TaskCenter,
        cluster_name: String,
        roles: EnumSet<Role>,
        health: Health,
        worker: Option<WorkerDependencies>,
        connections: ConnectionManager<GrpcConnector>,
    ) -> Self {
        Self {
            task_center,
            cluster_name,
            roles,
            health,
            worker,
            connections,
        }
    }
}

#[async_trait::async_trait]
impl NodeSvc for NodeSvcHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        let node_status = self.health.current_node_status();
        let admin_status = self.health.current_admin_status();
        let worker_status = self.health.current_worker_status();
        let metadata_server_status = self.health.current_metadata_server_status();
        let log_server_status = self.health.current_log_server_status();
        let age_s = self.task_center.age().as_secs();
        self.task_center.run_in_scope_sync("get_ident", None, || {
            let metadata = metadata();
            Ok(Response::new(IdentResponse {
                status: node_status.into(),
                node_id: Some(metadata.my_node_id().into()),
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

        // For uniformity with outbound connections, we map all responses to Ok, we never rely on
        // sending tonic::Status errors explicitly. We use ConnectionControl frames to communicate
        // errors and/or drop the stream when necessary.
        Ok(Response::new(Box::pin(output_stream.map(Ok))))
    }
}

// todo: Remove once arrow-flight supports tonic 0.12
fn flight_error_to_tonic_status(err: FlightError) -> Status {
    match err {
        FlightError::Arrow(e) => Status::internal(e.to_string()),
        FlightError::NotYetImplemented(e) => Status::internal(e),
        FlightError::Tonic(status) => status,
        FlightError::ProtocolError(e) => Status::internal(e),
        FlightError::DecodeError(e) => Status::internal(e),
        FlightError::ExternalError(e) => Status::internal(e.to_string()),
    }
}
