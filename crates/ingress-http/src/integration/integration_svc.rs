// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Experimental gRPC (tonic) integration API.
//!
//! Exposes the bidirectional-streaming `dev.restate.ingress.integration.IntegrationSvc/Ingest`
//! RPC (see `protobuf/integration_svc.proto`). This is a **scaffold**: the handler
//! parses the incoming `Start`/`Settings`/`Invocation` stream and replies with an
//! `Ack`, but does not yet write records to the log. Real ingestion (WAL `Envelope` +
//! producer/offset deduplication via `restate_ingestion_client::IngestionClient`)
//! is a follow-up.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;

/// Generated protobuf bindings for `dev.restate.ingress.integration`
/// (see `protobuf/integration_svc.proto`).
pub mod proto {
    tonic::include_proto!("dev.restate.ingress.integration");
}

use proto::integration_svc_server::{IntegrationSvc, IntegrationSvcServer};
use proto::{
    Request as IntegrationRequest, Response as IntegrationResponse, Settings, WindowUpdate,
    request, response,
};

/// Build the tonic integration server.
pub(crate) fn integration_server<Schemas>(
    schemas: Live<Schemas>,
) -> IntegrationSvcServer<IntegrationService<Schemas>>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    IntegrationSvcServer::new(IntegrationService::new(schemas))
}

/// Scaffold implementation of the `IntegrationSvc` gRPC service.
#[derive(Clone)]
pub(crate) struct IntegrationService<Schemas> {
    schemas: Live<Schemas>,
}

impl<Schemas> IntegrationService<Schemas> {
    fn new(schemas: Live<Schemas>) -> Self {
        Self { schemas }
    }
}

#[tonic::async_trait]
impl<Schemas> IntegrationSvc for IntegrationService<Schemas>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    type IngestStream = BoxStream<'static, Result<IntegrationResponse, Status>>;

    async fn ingest(
        &self,
        request: Request<Streaming<IntegrationRequest>>,
    ) -> Result<Response<Self::IngestStream>, Status> {
        // Snapshot the schema once so the stream owns a `'static` resolver rather
        // than borrowing `&self` across `.await` points.
        let schemas = self.schemas.snapshot();

        let responses = futures::stream::unfold(
            State {
                requests: request.into_inner(),
                _schemas: schemas,
                settings: None,
            },
            |mut state| async move {
                loop {
                    let message = match state.requests.next().await {
                        None => return None,
                        Some(Ok(message)) => message,
                        Some(Err(status)) => return Some((Err(status), state)),
                    };

                    match message.payload {
                        // `Start` opens the stream and may carry initial settings.
                        Some(request::Payload::Start(start)) => {
                            state.settings = start.settings;
                        }
                        // `Settings` establishes the defaults for subsequent invocations;
                        // its fields are replaced wholesale (not merged) and it emits no ack.
                        Some(request::Payload::Settings(settings)) => {
                            state.settings = Some(settings);
                        }
                        Some(request::Payload::Invocation(invocation)) => {
                            return Some((Ok(ack(invocation.offset)), state));
                        }
                        // Empty request payload: nothing to do.
                        None => {}
                    }
                }
            },
        );

        Ok(Response::new(responses.boxed()))
    }
}

/// Streaming state carried across the inbound `Request` stream.
struct State<Schemas> {
    requests: Streaming<IntegrationRequest>,
    _schemas: Arc<Schemas>,
    settings: Option<Settings>,
}

fn ack(offset: u64) -> IntegrationResponse {
    IntegrationResponse {
        last_committed: Some(offset),
        response: Some(response::Response::Ack(WindowUpdate::default())),
    }
}
