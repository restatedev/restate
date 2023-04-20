use super::protocol::{BoxBody, Protocol};
use super::reflection::{ServerReflection, ServerReflectionServer};
use super::*;

use std::sync::Arc;
use std::task::Poll;

use futures::future::{ok, BoxFuture};
use futures::{FutureExt, TryFutureExt};
use http::{Request, Response};
use http_body::Body;
use hyper::Body as HyperBody;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use restate_common::types::{IngressId, ServiceInvocationResponseSink, SpanRelation};
use restate_service_metadata::MethodDescriptorRegistry;
use tokio::sync::Semaphore;
use tonic::server::NamedService;
use tonic_web::{GrpcWebLayer, GrpcWebService};
use tower::{BoxError, Layer, Service};
use tracing::{debug, info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct Handler<InvocationFactory, MethodRegistry, ReflectionService>
where
    ReflectionService: ServerReflection,
{
    ingress_id: IngressId,
    invocation_factory: InvocationFactory,
    method_registry: MethodRegistry,
    reflection_server: GrpcWebService<ServerReflectionServer<ReflectionService>>,
    dispatcher_command_sender: DispatcherCommandSender,
    global_concurrency_semaphore: Arc<Semaphore>,
}

impl<InvocationFactory, MethodRegistry, ReflectionService> Clone
    for Handler<InvocationFactory, MethodRegistry, ReflectionService>
where
    InvocationFactory: Clone,
    MethodRegistry: Clone,
    ReflectionService: ServerReflection,
{
    fn clone(&self) -> Self {
        Self {
            ingress_id: self.ingress_id,
            invocation_factory: self.invocation_factory.clone(),
            method_registry: self.method_registry.clone(),
            reflection_server: self.reflection_server.clone(),
            dispatcher_command_sender: self.dispatcher_command_sender.clone(),
            global_concurrency_semaphore: self.global_concurrency_semaphore.clone(),
        }
    }
}

impl<InvocationFactory, MethodRegistry, ReflectionService>
    Handler<InvocationFactory, MethodRegistry, ReflectionService>
where
    ReflectionService: ServerReflection,
{
    pub fn new(
        ingress_id: IngressId,
        invocation_factory: InvocationFactory,
        method_registry: MethodRegistry,
        reflection_service: ReflectionService,
        dispatcher_command_sender: DispatcherCommandSender,
        global_concurrency_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            ingress_id,
            invocation_factory,
            method_registry,
            reflection_server: GrpcWebLayer::new()
                .layer(ServerReflectionServer::new(reflection_service)),
            dispatcher_command_sender,
            global_concurrency_semaphore,
        }
    }
}

// TODO When porting to hyper 1.0 https://github.com/restatedev/restate/issues/96
//  replace this impl with hyper::Service impl
impl<InvocationFactory, MethodRegistry, ReflectionService> Service<Request<HyperBody>>
    for Handler<InvocationFactory, MethodRegistry, ReflectionService>
where
    InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
    MethodRegistry: MethodDescriptorRegistry,
    ReflectionService: ServerReflection,
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
        let protocol = Protocol::pick_protocol(req.headers());

        // Acquire the semaphore permit to check if we have available quota
        let permit = if let Ok(p) = self
            .global_concurrency_semaphore
            .clone()
            .try_acquire_owned()
        {
            p
        } else {
            warn!("No available quota to process the request");
            return ok(protocol.encode_status(Status::resource_exhausted("Resource exhausted")))
                .boxed();
        };

        // Parse service_name and method_name
        let mut path_parts: Vec<&str> = req.uri().path().split('/').collect();
        if path_parts.len() != 3 {
            // Let's immediately reply with a status code not found
            debug!(
                "Cannot parse the request path '{}' into a valid GRPC/Connect request path. \
                Allowed format is '/Service-Name/Method-Name'",
                req.uri().path()
            );
            return ok(protocol.encode_status(Status::not_found(format!(
                "Request path {} invalid",
                req.uri().path()
            ))))
            .boxed();
        }
        let method_name = path_parts.remove(2).to_string();
        let service_name = path_parts.remove(1).to_string();

        if ServerReflectionServer::<ReflectionService>::NAME == service_name {
            return self
                .reflection_server
                .call(req)
                .map(|result| {
                    result.map(|response| response.map(|b| b.map_err(Into::into).boxed_unsync()))
                })
                .map_err(BoxError::from)
                .boxed();
        }

        // Find the service method descriptor
        let descriptor = if let Some(desc) = self
            .method_registry
            .resolve_method_descriptor(&service_name, &method_name)
        {
            desc
        } else {
            debug!("{}/{} not found", service_name, method_name);
            return ok(protocol.encode_status(Status::not_found(format!(
                "{service_name}/{method_name} not found"
            ))))
            .boxed();
        };

        // Encapsulate in this closure the remaining part of the processing
        let ingress_id = self.ingress_id;
        let invocation_factory = self.invocation_factory.clone();
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
                // Create the service_invocation
                let (service_invocation, service_invocation_span) = match invocation_factory.create(
                    &req_headers.service_name,
                    &req_headers.method_name,
                    req_payload,
                    Some(ServiceInvocationResponseSink::Ingress(ingress_id)),
                    SpanRelation::Parent(ingress_span_context)
                ) {
                    Ok(i) => i,
                    Err(e) => {
                        warn!("Cannot create service invocation: {:?}", e);
                        let status = match e {
                            err @ ServiceInvocationFactoryError::UnknownServiceMethod { .. } => {
                                Status::not_found(err.to_string())
                            }
                            err @ ServiceInvocationFactoryError::KeyExtraction(_) => Status::internal(err.to_string())
                        };
                        return Err(status);
                    }
                };

                // Be aware that between this enter and the drop later there must not be any .await
                // https://docs.rs/tracing/latest/tracing/struct.Span.html#in-asynchronous-code
                let enter_service_invocation_span = service_invocation_span.enter();

                // More trace info
                trace!(restate.invocation.request_headers = ?req_headers);

                // Send the service invocation
                let (service_invocation_command, response_rx) =
                    Command::prepare(service_invocation);
                if dispatcher_command_sender.send(service_invocation_command).is_err() {
                    debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
                    return Err(Status::unavailable("Unavailable"));
                }

                // Drop the service invocation span to commit it
                drop(enter_service_invocation_span);

                // Wait on response
                return match response_rx.await {
                    Ok(Ok(response_payload)) => {
                        trace!(rpc.response = ?response_payload, "Complete external gRPC request successfully");
                        Ok(response_payload)
                    }
                    Ok(Err(error)) => {
                        let status: Status = error;
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
            descriptor,
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
