use super::protocol::{BoxBody, Protocol};
use super::response_dispatcher::IngressResponseRequester;
use super::*;

use std::sync::Arc;
use std::task::Poll;

use common::types::{
    IngressId, ServiceInvocation, ServiceInvocationFactory, ServiceInvocationResponseSink,
    SpanRelation,
};
use futures::future::{ok, BoxFuture};
use futures::FutureExt;
use http::{Request, Response};
use hyper::Body as HyperBody;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use tokio::sync::{mpsc, Semaphore};
use tower::{BoxError, Service};
use tracing::{debug, info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Clone)]
pub struct Handler<InvocationFactory, MethodRegistry> {
    ingress_id: IngressId,
    invocation_factory: InvocationFactory,
    method_registry: MethodRegistry,
    response_requester: IngressResponseRequester,
    invocation_sender: mpsc::Sender<ServiceInvocation>,
    global_concurrency_semaphore: Arc<Semaphore>,
}

impl<InvocationFactory, MethodRegistry> Handler<InvocationFactory, MethodRegistry> {
    pub fn new(
        ingress_id: IngressId,
        invocation_factory: InvocationFactory,
        method_registry: MethodRegistry,
        response_requester: IngressResponseRequester,
        invocation_sender: mpsc::Sender<ServiceInvocation>,
        global_concurrency_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            ingress_id,
            invocation_factory,
            method_registry,
            response_requester,
            invocation_sender,
            global_concurrency_semaphore,
        }
    }
}

// TODO When porting to hyper 1.0 https://github.com/restatedev/restate/issues/96
//  replace this impl with hyper::Service impl
impl<InvocationFactory, MethodRegistry> Service<Request<HyperBody>>
    for Handler<InvocationFactory, MethodRegistry>
where
    InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
    MethodRegistry: MethodDescriptorRegistry,
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
        let ingress_id = self.ingress_id.clone();
        let invocation_factory = self.invocation_factory.clone();
        let invocation_sender = self.invocation_sender.clone();
        let response_requester = self.response_requester.clone();
        let ingress_request_handler = move |ingress_request: IngressRequest| {
            let (req_headers, req_payload) = ingress_request;

            // Create the span and attach it to the next async block
            let span = info_span!(
                "Ingress invocation",
                rpc.system = "grpc",
                rpc.service = %req_headers.service_name,
                rpc.method = %req_headers.method_name
            );
            trace!(parent: &span, rpc.request = ?req_payload);

            async move {
                // Create the service_invocation
                let service_invocation = match invocation_factory.create(
                    &req_headers.service_name,
                    &req_headers.method_name,
                    req_payload,
                    ServiceInvocationResponseSink::Ingress(ingress_id),
                    SpanRelation::from_parent(create_parent_span_context(
                        req_headers.tracing_context.span().span_context(),
                    )),
                ) {
                    Ok(i) => i,
                    Err(e) => {
                        warn!("Cannot create service invocation: {:?}", e);
                        return Err(Status::internal(e.to_string()));
                    }
                };
                info!(restate.invocation.id = %service_invocation.id);
                trace!(restate.invocation.request_headers = ?req_headers);

                // Register to the ResponseDispatcherLoop
                let (response_registration_cmd, response_rx) =
                    Command::prepare(service_invocation.id.clone());
                if response_requester
                    .send(response_registration_cmd)
                    .is_err()
                {
                    warn!("Cannot register invocation to response dispatcher loop");
                    return Err(Status::unavailable("Unavailable"));
                }

                // Send the service invocation
                if invocation_sender.send(service_invocation).await.is_err() {
                    warn!("Cannot send the invocation to the network component");
                    return Err(Status::unavailable("Unavailable"));
                }

                // Wait on response
                return match response_rx.await {
                    Ok(Ok(response_payload)) => {
                        trace!(rpc.response = ?response_payload, "Complete external gRPC request successfully");
                        Ok(response_payload)
                    }
                    Ok(Err(status)) => {
                        info!(rpc.grpc.status_code = ?status.code(), rpc.grpc.status_message = ?status.message(), "Complete external gRPC request with a failure");
                        Err(status)
                    }
                    Err(_) => {
                        warn!("Response channel was closed");
                        Err(Status::unavailable("Unavailable"))
                    }
                }
            }.instrument(span)
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

fn create_parent_span_context(request_span: &SpanContext) -> SpanContext {
    if request_span.is_valid() {
        request_span.clone()
    } else {
        tracing::Span::current()
            .context()
            .span()
            .span_context()
            .clone()
    }
}
