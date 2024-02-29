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
use restate_node_services::node::node_svc_server::NodeSvc;
use restate_node_services::node::{
    IdentResponse, NodeStatus, StateMutationRequest, StorageQueryRequest, StorageQueryResponse,
    TerminationRequest, UpdateSchemaRequest,
};
use restate_schema_impl::SchemasUpdateCommand;
use restate_worker_api::Handle;
use tonic::{Request, Response, Status};

use crate::network_server::WorkerDependencies;

pub struct NodeSvcHandler {
    task_center: TaskCenter,
    worker: Option<WorkerDependencies>,
}

impl NodeSvcHandler {
    pub fn new(task_center: TaskCenter, worker: Option<WorkerDependencies>) -> Self {
        Self {
            task_center,
            worker,
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

    async fn terminate_invocation(
        &self,
        request: Request<TerminationRequest>,
    ) -> Result<Response<()>, Status> {
        let Some(ref worker) = self.worker else {
            return Err(Status::failed_precondition("Not a worker node"));
        };

        let (invocation_termination, _) = bincode::serde::decode_from_slice(
            &request.into_inner().invocation_termination,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        worker
            .worker_cmd_tx
            .terminate_invocation(invocation_termination)
            .await
            .map_err(|_| Status::unavailable("worker shut down"))?;

        Ok(Response::new(()))
    }

    async fn mutate_state(
        &self,
        request: Request<StateMutationRequest>,
    ) -> Result<Response<()>, Status> {
        let Some(ref worker) = self.worker else {
            return Err(Status::failed_precondition("Not a worker node"));
        };

        let (state_mutation, _) = bincode::serde::decode_from_slice(
            &request.into_inner().state_mutation,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        worker
            .worker_cmd_tx
            .external_state_mutation(state_mutation)
            .await
            .map_err(|_| Status::unavailable("worker shut down"))?;

        Ok(Response::new(()))
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

    async fn update_schemas(
        &self,
        request: Request<UpdateSchemaRequest>,
    ) -> Result<Response<()>, Status> {
        let Some(ref worker) = self.worker else {
            return Err(Status::failed_precondition("Not a worker node"));
        };

        let (schema_updates, _) =
            bincode::serde::decode_from_slice::<Vec<SchemasUpdateCommand>, _>(
                &request.into_inner().schema_bin,
                bincode::config::standard(),
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        self.task_center
            .run_in_scope("update-schema", None, async move {
                crate::roles::update_schemas(
                    &worker.schemas,
                    worker.subscription_controller.as_ref(),
                    schema_updates,
                )
                .await
                .map_err(|err| {
                    Status::internal(format!("failed updating the schema information: {err}"))
                })
            })
            .await?;

        Ok(Response::new(()))
    }
}
