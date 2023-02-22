use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes};
use futures::future::{ok, BoxFuture};
use futures::ready;
use http::{Request, Response};
use ingress_common::{
    IngressError, IngressRequest, IngressRequestHeaders, IngressResponse, IngressResult,
};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use pin_project::pin_project;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::{Grpc, UnaryService};
use tonic::Status;
use tonic_web::GrpcWebService;
use tower::{BoxError, Layer, Service};
use tracing::debug;

/// This layer converts a grpc or grpc-web request/response in a Restate ingress request/response internally using tonic.
#[derive(Clone)]
pub(super) struct GrpcLayer {}

impl<S> Layer<S> for GrpcLayer
where
    S: Service<IngressRequest, Response = IngressResponse, Error = IngressError>
        + Send
        + Clone
        + 'static,
    S::Future: Send,
{
    type Service = GrpcWebService<GrpcIngressService<S>>;

    fn layer(&self, inner: S) -> Self::Service {
        tonic_web::GrpcWebLayer::new().layer(GrpcIngressService { inner: Some(inner) })
    }
}

#[derive(Clone)]
pub(crate) struct GrpcIngressService<S> {
    inner: Option<S>,
}

impl<S> Service<Request<hyper::Body>> for GrpcIngressService<S>
where
    S: Service<IngressRequest, Response = IngressResponse, Error = IngressError> + Send + 'static,
    S::Future: Send,
{
    type Response = Response<tonic::body::BoxBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response<tonic::body::BoxBody>, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .as_mut()
            .expect("Invoked poll_ready after consuming the service")
            .poll_ready(cx)
            .map_err(|e| BoxError::from(e.message()))
    }

    fn call(&mut self, request: Request<hyper::Body>) -> Self::Future {
        let inner = self
            .inner
            .take()
            .expect("This service cannot be used twice");

        debug!(?request, "Receiving request");

        // Parse service_name and method_name
        let mut path_parts: Vec<&str> = request.uri().path().split('/').collect();
        if path_parts.len() != 3 {
            // Let's immediately reply with a status code not found
            debug!(
                "Cannot parse the request path '{}' into a valid GRPC request path. \
                Allowed format is '/Service-Name/Method-Name'",
                request.uri().path()
            );
            return Box::pin(ok(Status::not_found(format!(
                "Request path {} invalid",
                request.uri().path()
            ))
            .to_http()));
        }
        let method_name = path_parts.remove(2).to_string();
        let service_name = path_parts.remove(1).to_string();

        // Unfortunately we can't get rid of this Box, because unary is an async fn
        Box::pin(async move {
            Ok(Grpc::new(NoopCodec)
                .unary(
                    HandlerAdapter {
                        service_name: Some(service_name),
                        method_name: Some(method_name),
                        inner,
                    },
                    request,
                )
                .await)
        })
    }
}

// --- Adapters for tonic

struct HandlerAdapter<S> {
    service_name: Option<String>,
    method_name: Option<String>,
    inner: S,
}

impl<S> UnaryService<Bytes> for HandlerAdapter<S>
where
    S: Service<IngressRequest, Response = IngressResponse, Error = IngressError> + 'static,
    S::Future: Send,
{
    type Response = Bytes;
    type Future = AdapterResponseFuture<S::Future>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        // Extract tracing context and payload
        let (metadata, _, payload) = request.into_parts();
        let headers = metadata.into_headers();
        let tracing_context =
            TraceContextPropagator::new().extract(&opentelemetry_http::HeaderExtractor(&headers));

        let request = (
            IngressRequestHeaders::new(
                self.service_name.take().unwrap(),
                self.method_name.take().unwrap(),
                tracing_context,
            ),
            payload,
        );

        AdapterResponseFuture(self.inner.call(request))
    }
}

#[pin_project]
struct AdapterResponseFuture<F>(#[pin] F);

impl<F> Future for AdapterResponseFuture<F>
where
    F: Future<Output = IngressResult>,
{
    type Output = Result<tonic::Response<Bytes>, Status>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        Poll::Ready(ready!(this.0.poll(cx)).map(tonic::Response::new))
    }
}

// --- Noop codec to skip encode/decode in tonic

struct NoopCodec;

impl Codec for NoopCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        NoopCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        NoopCodec
    }
}

impl Encoder for NoopCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for NoopCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}
