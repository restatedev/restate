use super::req::*;
use super::res::*;
use super::*;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use std::task::{ready, Context, Poll};

use ingress_common::{
    IngressError, IngressRequest, IngressRequestHeaders, IngressResponse, IngressResult,
};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use pin_project::pin_project;
use prost::Message;
use tonic::Status;
use tower::{Layer, Service, ServiceBuilder};

/// This layer decodes a connect protocol request to Ingress types.
pub(super) struct ConnectLayer<Registry> {
    descriptor_registry: Registry,
}

impl<Registry> ConnectLayer<Registry> {
    // TODO remove once we wire up everything
    #[allow(dead_code)]
    pub(crate) fn new(descriptor_registry: Registry) -> Self {
        Self {
            descriptor_registry,
        }
    }
}

impl<S, Registry> Layer<S> for ConnectLayer<Registry>
where
    S: Service<IngressRequest, Response = IngressResponse, Error = IngressError> + Send + 'static,
    S::Future: Send,
    Registry: MethodDescriptorRegistry + Clone,
{
    type Service =
        ConnectRequestService<Registry, ConnectResponseService<ConnectAdapterService<S>>>;

    fn layer(&self, inner: S) -> Self::Service {
        ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(self.descriptor_registry.clone()))
            .layer(ConnectResponseLayer::default())
            .layer_fn(|s| ConnectAdapterService { inner: Some(s)} )
            .check_service::<S, hyper::Request<hyper::Body>, hyper::Response<hyper::Body>, tower::BoxError>()
            .service(inner)
    }
}

pub(crate) struct ConnectAdapterService<S> {
    inner: Option<S>,
}

impl<S> Service<ConnectRequest> for ConnectAdapterService<S>
where
    S: Service<IngressRequest, Response = IngressResponse, Error = IngressError> + Send + 'static,
{
    type Response = ConnectResponse;
    type Error = Status;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().unwrap().poll_ready(cx)
    }

    fn call(&mut self, request: ConnectRequest) -> Self::Future {
        // Extract tracing context and payload
        let method_desc = request.method_descriptor();
        let service_name = method_desc.parent_service().full_name().to_string();
        let method_name = method_desc.name().to_string();

        let (headers, payload, response_builder) = request.into_inner();

        let tracing_context =
            TraceContextPropagator::new().extract(&opentelemetry_http::HeaderExtractor(&headers));

        let ingress_request_headers =
            IngressRequestHeaders::new(service_name, method_name, tracing_context);
        let payload = Bytes::from(payload.encode_to_vec());

        ResponseFuture {
            response_builder: Some(response_builder),
            fut: self
                .inner
                .take()
                .unwrap()
                .call((ingress_request_headers, payload)),
        }
    }
}

#[pin_project]
pub(crate) struct ResponseFuture<F> {
    response_builder: Option<ConnectResponseBuilder>,
    #[pin]
    fut: F,
}

const _: () = {
    fn assert_send<T: Send + ?Sized>() {}

    fn assert_all<T: Send>() {
        assert_send::<ResponseFuture<T>>();
    }
};

impl<F> Future for ResponseFuture<F>
where
    F: Future<Output = IngressResult>,
{
    type Output = Result<ConnectResponse, Status>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let payload = match ready!(this.fut.poll(cx)) {
            Ok(payload) => payload,
            Err(err) => return Poll::Ready(Err(err)),
        };

        let response = match this
            .response_builder
            .take()
            .unwrap()
            .encoded_message(payload)
        {
            Ok(grpc_res) => grpc_res,
            Err(err) => {
                return Poll::Ready(Err(Status::internal(format!(
                    "The response payload cannot be decoded: {err}",
                ))))
            }
        };

        Poll::Ready(Ok(response))
    }
}

#[cfg(test)]
mod tests {
    use super::mocks::*;
    use super::*;

    use futures::future::{ok, Ready};

    use http::header::CONTENT_TYPE;
    use http::{Method, Request, StatusCode};
    use hyper::body::HttpBody;
    use serde_json::json;
    use test_utils::{assert_eq, test};
    use tower::ServiceExt;

    // Mock protobufs

    fn greeter_service_fn(ingress_req: IngressRequest) -> Ready<IngressResult> {
        let person = pb::GreetingRequest::decode(ingress_req.1).unwrap().person;
        ok(pb::GreetingResponse {
            greeting: format!("Hello {person}"),
        }
        .encode_to_vec()
        .into())
    }

    #[test(tokio::test)]
    async fn layers_works() {
        let svc = ServiceBuilder::new()
            .layer(ConnectLayer::new(test_descriptor_registry()))
            .service_fn(greeter_service_fn);

        let mut res = svc
            .oneshot(
                Request::builder()
                    .uri("http://localhost/greeter.Greeter/Greet")
                    .method(Method::POST)
                    .header(CONTENT_TYPE, "application/json")
                    .body(
                        json!({
                            "person": "Francesco"
                        })
                        .to_string()
                        .into(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = res.data().await.unwrap().unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            json_body.get("greeting").unwrap().as_str().unwrap(),
            "Hello Francesco"
        );
    }
}
