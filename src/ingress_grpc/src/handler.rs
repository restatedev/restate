use super::options::JsonOptions;
use super::protocol::{BoxBody, Protocol};
use super::*;

use std::sync::Arc;
use std::task::Poll;

use crate::reflection::ServerReflectionService;
use futures::future::{ok, BoxFuture};
use futures::{FutureExt, TryFutureExt};
use http::{Request, Response, StatusCode};
use http_body::Body;
use hyper::Body as HyperBody;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use prost::Message;
use restate_pb::grpc::health;
use restate_pb::grpc::reflection::server_reflection_server::ServerReflectionServer;
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::key::KeyExtractor;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_types::identifiers::{IngressId, InvocationId};
use restate_types::invocation::{ServiceInvocationResponseSink, SpanRelation};
use tokio::sync::Semaphore;
use tonic_web::{GrpcWebLayer, GrpcWebService};
use tower::{BoxError, Layer, Service};
use tracing::{debug, info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct Handler<Schemas, ProtoSymbols>
where
    ProtoSymbols: ProtoSymbolResolver + Clone + Send + Sync + 'static,
{
    ingress_id: IngressId,
    json: JsonOptions,
    schemas: Schemas,
    reflection_server:
        GrpcWebService<ServerReflectionServer<ServerReflectionService<ProtoSymbols>>>,
    dispatcher_command_sender: DispatcherCommandSender,
    global_concurrency_semaphore: Arc<Semaphore>,
}

impl<Schemas, ProtoSymbols> Clone for Handler<Schemas, ProtoSymbols>
where
    Schemas: Clone,
    ProtoSymbols: ProtoSymbolResolver + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            ingress_id: self.ingress_id,
            json: self.json.clone(),
            schemas: self.schemas.clone(),
            reflection_server: self.reflection_server.clone(),
            dispatcher_command_sender: self.dispatcher_command_sender.clone(),
            global_concurrency_semaphore: self.global_concurrency_semaphore.clone(),
        }
    }
}

impl<Schemas> Handler<Schemas, Schemas>
where
    Schemas: ProtoSymbolResolver + Clone + Send + Sync + 'static,
{
    pub fn new(
        ingress_id: IngressId,
        json: JsonOptions,
        schemas: Schemas,
        dispatcher_command_sender: DispatcherCommandSender,
        global_concurrency_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            ingress_id,
            json,
            schemas: schemas.clone(),
            reflection_server: GrpcWebLayer::new().layer(ServerReflectionServer::new(
                ServerReflectionService(schemas),
            )),
            dispatcher_command_sender,
            global_concurrency_semaphore,
        }
    }
}

// TODO When porting to hyper 1.0 https://github.com/restatedev/restate/issues/96
//  replace this impl with hyper::Service impl
impl<Schemas, JsonDecoder, JsonEncoder> Service<Request<HyperBody>> for Handler<Schemas, Schemas>
where
    JsonDecoder: Send,
    JsonEncoder: Send,
    Schemas: JsonMapperResolver<JsonToProtobufMapper = JsonDecoder, ProtobufToJsonMapper = JsonEncoder>
        + KeyExtractor
        + ServiceMetadataResolver
        + ProtoSymbolResolver
        + Clone
        + Send
        + Sync
        + 'static,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response<BoxBody>, BoxError>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Don't add any logic to this method, as it will go away with Hyper 1.0
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<HyperBody>) -> Self::Future {
        // Don't depend on &mut self, as hyper::Service will replace this with an immutable borrow!

        // Discover the protocol
        let protocol = if let Some(p) = Protocol::pick_protocol(req.method(), req.headers()) {
            p
        } else {
            return ok(encode_http_status_code(StatusCode::UNSUPPORTED_MEDIA_TYPE)).boxed();
        };

        // Acquire the semaphore permit to check if we have available quota
        let permit = if let Ok(p) = self
            .global_concurrency_semaphore
            .clone()
            .try_acquire_owned()
        {
            p
        } else {
            warn!("No available quota to process the request");
            return ok(
                protocol.encode_grpc_status(Status::resource_exhausted("Resource exhausted"))
            )
            .boxed();
        };

        // Parse service_name and method_name
        let mut path_parts: Vec<&str> = req.uri().path().split('/').collect();
        if path_parts.len() != 3 {
            // Let's immediately reply with a status code invalid argument
            debug!(
                "Cannot parse the request path '{}' into a valid GRPC/Connect request path. \
                Allowed format is '/Service-Name/Method-Name'",
                req.uri().path()
            );
            return ok(
                protocol.encode_grpc_status(Status::invalid_argument(format!(
                    "Request path {} invalid",
                    req.uri().path()
                ))),
            )
            .boxed();
        }
        let method_name = path_parts.remove(2).to_string();
        let service_name = path_parts.remove(1).to_string();

        // Check if the service is public
        match self.schemas.is_service_public(&service_name) {
            None => {
                return ok(protocol.encode_grpc_status(Status::not_found(format!(
                    "Service {} not found",
                    service_name
                ))))
                .boxed();
            }
            Some(false) => {
                return ok(
                    protocol.encode_grpc_status(Status::permission_denied(format!(
                        "Service {} is not accessible",
                        service_name
                    ))),
                )
                .boxed();
            }
            _ => {}
        };

        // --- Special Restate services
        // Reflections
        if restate_pb::REFLECTION_SERVICE_NAME == service_name {
            if matches!(protocol, Protocol::Connect) {
                // Can't process reflection requests with connect
                return ok(encode_http_status_code(StatusCode::UNSUPPORTED_MEDIA_TYPE)).boxed();
            }
            return self
                .reflection_server
                .call(req)
                .map(|result| {
                    result.map(|response| response.map(|b| b.map_err(Into::into).boxed_unsync()))
                })
                .map_err(BoxError::from)
                .boxed();
        }

        // Encapsulate in this closure the remaining part of the processing
        let ingress_id = self.ingress_id;
        let schemas = self.schemas.clone();
        let dispatcher_command_sender = self.dispatcher_command_sender.clone();
        let ingress_request_handler = move |ingress_request: IngressRequest| {
            let (req_headers, req_payload) = ingress_request;

            // Create the ingress span and attach it to the next async block.
            // This span is committed once the async block terminates, recording the execution time of the invocation.
            // Another span is created later by the ServiceInvocationFactory, for the ServiceInvocation itself,
            // which is used by the Restate components to correctly link to a single parent span
            // to commit intermediate results of the processing.
            let ingress_span = info_span!(
                "ingress_service_invocation",
                rpc.system = "grpc",
                rpc.service = %req_headers.service_name,
                rpc.method = %req_headers.method_name
            );
            // Attach this ingress_span to the parent parsed from the headers, if any.
            span_relation(req_headers.tracing_context.span().span_context())
                .attach_to_span(&ingress_span);
            info!(parent: &ingress_span, "Processing ingress request");
            trace!(parent: &ingress_span, rpc.request = ?req_payload);

            // We need the context to link it to the service invocation span
            let ingress_span_context = ingress_span.context().span().span_context().clone();

            async move {
                let mut service_name = req_headers.service_name;
                let mut method_name = req_headers.method_name;
                let mut req_payload = req_payload;

                // --- Health built-in service
                if restate_pb::HEALTH_SERVICE_NAME == service_name {
                    if method_name == "Watch" {
                        return Err(Status::unimplemented(
                            "Watch unimplemented"
                        ));
                    }
                    if method_name == "Check" {
                        let health_check_req = health::HealthCheckRequest::decode(req_payload)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?;
                        if health_check_req.service.is_empty() {
                            // If unspecified, then just return ok
                            return Ok(
                                health::HealthCheckResponse {
                                    status: health::health_check_response::ServingStatus::Serving.into()
                                }.encode_to_vec().into()
                            )
                        }
                        return match schemas.is_service_public(&health_check_req.service) {
                            None => {
                                Err(Status::not_found(format!(
                                    "Service {} not found",
                                    health_check_req.service
                                )))
                            }
                            Some(true) => {
                                Ok(
                                    health::HealthCheckResponse {
                                        status: health::health_check_response::ServingStatus::Serving.into()
                                    }.encode_to_vec().into()
                                )
                            }
                            Some(false) => {
                                Ok(
                                    health::HealthCheckResponse {
                                        status: health::health_check_response::ServingStatus::NotServing.into()
                                    }.encode_to_vec().into()
                                )
                            }
                        }
                    }
                    // This should not really happen because the method existence is checked before
                    return Err(Status::not_found("Not found"))
                }

                let mut response_sink = Some(ServiceInvocationResponseSink::Ingress(ingress_id));
                let mut wait_response = true;

                // --- Ingress built-in service
                if is_ingress_invoke(&service_name, &method_name) {
                    let invoke_request = restate_pb::restate::services::InvokeRequest::decode(req_payload)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;

                    service_name = invoke_request.service;
                    method_name = invoke_request.method;
                    req_payload = invoke_request.argument;
                    response_sink = None;
                    wait_response = false;
                }

                // Extract the key
                let key = schemas
                    .extract(&service_name, &method_name, req_payload.clone())
                    .map_err(|err| match err {
                        restate_schema_api::key::KeyExtractorError::NotFound => {
                            Status::not_found(format!(
                                "Service method {}/{} not found",
                                service_name,
                                method_name
                            ))
                        }
                        err => Status::internal(err.to_string())
                    })?;

                let service_invocation = ServiceInvocation::new(
                    ServiceInvocationId::new(service_name, key, InvocationId::now_v7()),
                    method_name.into(),
                    req_payload,
                    response_sink,
                    SpanRelation::Parent(ingress_span_context),
                );

                // Ingress built-in service just sends a fire and forget and closes
                if !wait_response {
                    let sid = service_invocation.id.to_string();

                    if dispatcher_command_sender.send(Command::fire_and_forget(
                        service_invocation
                    )).is_err() {
                        debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
                        return Err(Status::unavailable("Unavailable"));
                    }
                    return Ok(
                        restate_pb::restate::services::InvokeResponse {
                            sid,
                        }.encode_to_vec().into()
                    )
                }

                // Send the service invocation
                let (service_invocation_command, response_rx) =
                    Command::prepare(service_invocation);
                if dispatcher_command_sender.send(service_invocation_command).is_err() {
                    debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
                    return Err(Status::unavailable("Unavailable"));
                }

                // Wait on response
                return match response_rx.await {
                    Ok(Ok(response_payload)) => {
                        trace!(rpc.response = ?response_payload, "Complete external gRPC request successfully");
                        Ok(response_payload)
                    }
                    Ok(Err(error)) => {
                        let status: Status = error.into();
                        info!(rpc.grpc.status_code = ?status.code(), rpc.grpc.status_message = ?status.message(), "Complete external gRPC request with a failure");
                        Err(status)
                    }
                    Err(_) => {
                        warn!("Response channel was closed");
                        Err(Status::unavailable("Unavailable"))
                    }
                }
            }.instrument(ingress_span)
        };

        // Let the protocol handle the request
        let result_fut = protocol.handle_request(
            service_name,
            method_name,
            self.schemas.clone(),
            self.json.clone(),
            req,
            ingress_request_handler,
        );
        async {
            let result = result_fut.await;

            // We hold the semaphore permit up to the end of the request processing
            drop(permit);

            result
        }
        .boxed()
    }
}

fn span_relation(request_span: &SpanContext) -> SpanRelation {
    if request_span.is_valid() {
        SpanRelation::Parent(request_span.clone())
    } else {
        SpanRelation::None
    }
}

fn is_ingress_invoke(service_name: &str, method_name: &str) -> bool {
    "dev.restate.Ingress" == service_name && "Invoke" == method_name
}

pub(crate) fn encode_http_status_code(status_code: StatusCode) -> Response<BoxBody> {
    // In case we need to encode an http status, we just write it without considering the protocol.
    let mut res = Response::new(hyper::Body::empty().map_err(Into::into).boxed_unsync());
    *res.status_mut() = status_code;
    res
}
