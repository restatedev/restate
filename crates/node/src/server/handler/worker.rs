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
use restate_node_services::worker::worker_svc_server::WorkerSvc;
use restate_node_services::worker::{
    StateMutationRequest, StorageQueryRequest, StorageQueryResponse, TerminationRequest,
    UpdateSchemaRequest,
};
use restate_schema_impl::{Schemas, SchemasUpdateCommand};
use restate_storage_query_datafusion::context::QueryContext;
use restate_worker::{SubscriptionControllerHandle, WorkerCommandSender};
use restate_worker_api::Handle;
use tonic::{Request, Response, Status};

pub struct WorkerHandler {
    worker_cmd_tx: WorkerCommandSender,
    query_context: QueryContext,
    schemas: Schemas,
    subscription_controller: Option<SubscriptionControllerHandle>,
}

impl WorkerHandler {
    pub fn new(
        worker_cmd_tx: WorkerCommandSender,
        query_context: QueryContext,
        schemas: Schemas,
        subscription_controller: Option<SubscriptionControllerHandle>,
    ) -> Self {
        Self {
            worker_cmd_tx,
            query_context,
            schemas,
            subscription_controller,
        }
    }
}

#[async_trait::async_trait]
impl WorkerSvc for WorkerHandler {
    async fn terminate_invocation(
        &self,
        request: Request<TerminationRequest>,
    ) -> Result<Response<()>, Status> {
        let (invocation_termination, _) = bincode::serde::decode_from_slice(
            &request.into_inner().invocation_termination,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        self.worker_cmd_tx
            .terminate_invocation(invocation_termination)
            .await
            .map_err(|_| Status::unavailable("worker shut down"))?;

        Ok(Response::new(()))
    }

    async fn mutate_state(
        &self,
        request: Request<StateMutationRequest>,
    ) -> Result<Response<()>, Status> {
        let (state_mutation, _) = bincode::serde::decode_from_slice(
            &request.into_inner().state_mutation,
            bincode::config::standard(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

        self.worker_cmd_tx
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
        let query = request.into_inner().query;

        let record_stream = self.query_context.execute(&query).await.map_err(|err| {
            Status::internal(format!("failed executing the query '{}': {}", query, err))
        })?;

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
        let (schema_updates, _) =
            bincode::serde::decode_from_slice::<Vec<SchemasUpdateCommand>, _>(
                &request.into_inner().schema_bin,
                bincode::config::standard(),
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        crate::roles::update_schemas(
            &self.schemas,
            self.subscription_controller.as_ref(),
            schema_updates,
        )
        .await
        .map_err(|err| {
            Status::internal(format!("failed updating the schema information: {err}"))
        })?;

        Ok(Response::new(()))
    }
}
