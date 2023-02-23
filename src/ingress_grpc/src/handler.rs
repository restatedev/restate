use std::sync::Arc;
use super::*;

use std::task::Poll;

use futures::future::BoxFuture;
use http::{Request, Response};
use http_body::combinators::UnsyncBoxBody;
use tokio::sync::{mpsc, Semaphore};
use tower::{BoxError, Service};
use hyper::Body as HyperBody;
use common::types::{IngressId, ServiceInvocation, ServiceInvocationFactory};

#[derive(Clone)]
pub struct Handler<InvocationFactory, MethodRegistry> {
    ingress_id: IngressId,
    invocation_factory: InvocationFactory,
    method_registry: MethodRegistry,
    response_requester: IngressResponseRequester,
    invocation_sender: mpsc::Sender<ServiceInvocation>,
    global_concurrency_semaphore: Arc<Semaphore>
}

type BoxBody = UnsyncBoxBody<Bytes, BoxError>;

// TODO When porting to hyper 1.0 https://github.com/restatedev/restate/issues/96
//  replace this impl with hyper::Service impl
impl<InvocationFactory, MethodRegistry> Service<Request<HyperBody>> for Handler<InvocationFactory, MethodRegistry> where
    InvocationFactory: ServiceInvocationFactory + Clone,
    MethodRegistry: MethodDescriptorRegistry + Clone
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
        unimplemented!()
    }
}