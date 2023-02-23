use super::protocol::{BoxBody, Protocol};
use super::*;

use std::sync::Arc;
use std::task::Poll;

use common::types::{IngressId, ServiceInvocation, ServiceInvocationFactory};
use futures::future::{ok, BoxFuture};
use futures::FutureExt;
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::combinators::UnsyncBoxBody;
use hyper::Body as HyperBody;
use tokio::sync::{mpsc, Semaphore};
use tower::{BoxError, Service};
use tracing::{debug, warn};

#[derive(Clone)]
pub struct Handler<InvocationFactory, MethodRegistry> {
    ingress_id: IngressId,
    invocation_factory: InvocationFactory,
    method_registry: MethodRegistry,
    response_requester: IngressResponseRequester,
    invocation_sender: mpsc::Sender<ServiceInvocation>,
    global_concurrency_semaphore: Arc<Semaphore>,
}

// TODO When porting to hyper 1.0 https://github.com/restatedev/restate/issues/96
//  replace this impl with hyper::Service impl
impl<InvocationFactory, MethodRegistry> Service<Request<HyperBody>>
    for Handler<InvocationFactory, MethodRegistry>
where
    InvocationFactory: ServiceInvocationFactory + Clone,
    MethodRegistry: MethodDescriptorRegistry + Clone,
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
        let method_name = path_parts.remove(2);
        let service_name = path_parts.remove(1);

        // We hold the semaphore permit up to the end of the request processing
        drop(permit);

        unimplemented!()
    }
}
