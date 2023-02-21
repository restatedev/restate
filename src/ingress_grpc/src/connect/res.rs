use super::content_type::*;
use super::req::ConnectRequest;
use super::utils::*;
use super::*;

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Buf;
use http::header::{HeaderName, CONTENT_TYPE};
use http::Response as HttpResponse;
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Body;
use pin_project::pin_project;
use prost_reflect::DynamicMessage;
use tower::{BoxError, Layer, Service};
use tracing::warn;

#[derive(Debug)]
pub struct ConnectResponse {
    method_descriptor: MethodDescriptor,
    headers: HeaderMap,
    content_type: ConnectContentType,
    payload: DynamicMessage,
}

impl ConnectResponse {
    pub(super) fn new(
        method_descriptor: MethodDescriptor,
        headers: HeaderMap,
        content_type: ConnectContentType,
        payload: DynamicMessage,
    ) -> Self {
        Self {
            method_descriptor,
            headers,
            content_type,
            payload,
        }
    }

    pub fn into_inner(self) -> (MethodDescriptor, ConnectContentType, HeaderMap, DynamicMessage) {
        (
            self.method_descriptor,
            self.content_type,
            self.headers,
            self.payload,
        )
    }
}

pub struct ConnectResponseBuilder {
    pub(crate) method_descriptor: MethodDescriptor,
    pub(crate) headers: HeaderMap,
    pub(crate) content_type: ConnectContentType,
}

impl ConnectResponseBuilder {
    pub fn content_type(mut self, content_type: ConnectContentType) -> ConnectResponseBuilder {
        self.content_type = content_type;
        self
    }

    pub fn header<K: Into<HeaderName>, V: Into<HeaderValue>>(
        mut self,
        key: K,
        value: V,
    ) -> ConnectResponseBuilder {
        self.headers.insert(key.into(), value.into());
        ConnectResponseBuilder {
            method_descriptor: self.method_descriptor,
            headers: self.headers,
            content_type: self.content_type,
        }
    }

    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    pub fn dynamic_message(self, payload: DynamicMessage) -> ConnectResponse {
        ConnectResponse::new(
            self.method_descriptor,
            self.headers,
            self.content_type,
            payload,
        )
    }

    pub fn reflect_message<T: prost_reflect::ReflectMessage>(self, payload: &T) -> ConnectResponse {
        self.dynamic_message(payload.transcode_to_dynamic())
    }

    pub fn message<T: prost::Message>(
        self,
        payload: &T,
    ) -> Result<ConnectResponse, prost::DecodeError> {
        let mut dynamic_msg = DynamicMessage::new(self.method_descriptor.output());
        dynamic_msg.transcode_from(payload)?;

        Ok(self.dynamic_message(dynamic_msg))
    }

    pub fn encoded_message<B: Buf>(
        self,
        payload: B,
    ) -> Result<ConnectResponse, prost::DecodeError> {
        let dynamic_msg = DynamicMessage::decode(self.method_descriptor.output(), payload)?;
        Ok(self.dynamic_message(dynamic_msg))
    }
}

#[derive(Clone, Default)]
pub struct ConnectResponseLayer {}

impl<S> Layer<S> for ConnectResponseLayer {
    type Service = ConnectResponseService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectResponseService { inner: Some(inner) }
    }
}

// A middleware that converts the HTTP request to GRPC request
pub struct ConnectResponseService<S> {
    inner: Option<S>,
}

impl<S> Service<ConnectRequest> for ConnectResponseService<S>
where
    S: Service<ConnectRequest, Response = ConnectResponse, Error = tonic::Status>,
{
    type Response = HttpResponse<Body>;
    type Error = BoxError;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().unwrap().poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: ConnectRequest) -> Self::Future {
        ResponseFuture {
            method_name: request.method_name().to_string(),
            inner_fut: self.inner.take().unwrap().call(request),
        }
    }
}

#[pin_project]
pub struct ResponseFuture<F> {
    method_name: String,
    #[pin]
    inner_fut: F,
}

// Assert ResponseFuture is Send
const _: () = {
    fn assert_send<T: Send>() {}

    // RFC 2056
    fn assert_all<T: Send>() {
        assert_send::<ResponseFuture<T>>();
    }
};

fn grpc_error_to_response(method_name: String, grpc_error: tonic::Status) -> HttpResponse<Body> {
    warn!(
        "Error when executing the method {}: {}",
        method_name, grpc_error
    );
    status_response(grpc_error)
}

impl<F> Future for ResponseFuture<F>
where
    F: Future<Output = Result<ConnectResponse, tonic::Status>>,
{
    type Output = Result<HttpResponse<Body>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = futures::ready!(this.inner_fut.poll(cx));

        let (method_desc, content_type, grpc_headers, dynamic_message) = match res {
            Ok(r) => r.into_inner(),
            Err(e) => {
                return Poll::Ready(Ok(grpc_error_to_response(mem::take(this.method_name), e)))
            }
        };

        let (content_type_header, body) = match write_message(content_type, dynamic_message) {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Cannot serialize the body for invocation {}: {}",
                    method_desc.full_name(),
                    e
                );
                return Poll::Ready(Ok(internal_server_error()));
            }
        };

        let mut res_builder = HttpResponse::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, content_type_header);

        // Append all the headers from the grpc response
        res_builder
            .headers_mut()
            .unwrap()
            .extend(grpc_headers.into_iter());

        Poll::Ready(Ok(res_builder.body(body).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{greeter_greet_method_descriptor, pb};
    use super::*;

    use http::header::HeaderName;
    use http::{HeaderValue, StatusCode};
    use hyper::body::HttpBody;
    use test_utils::{assert_eq, test};
    use tonic::Code;
    use tower::{ServiceBuilder, ServiceExt};

    #[test(tokio::test)]
    async fn pass_through_headers_and_payload() {
        let svc = ServiceBuilder::new()
            .layer(ConnectResponseLayer::default())
            .service_fn(|grpc_req: ConnectRequest| async move {
                Ok(grpc_req
                    .response()
                    .header(
                        HeaderName::from_static("x-my-header"),
                        HeaderValue::from_static("my-value"),
                    )
                    .message(&pb::GreetingResponse {
                        greeting: "Hello Francesco".to_string(),
                    })
                    .unwrap())
            });

        let mut res = svc
            .oneshot(ConnectRequest::mock(greeter_greet_method_descriptor()))
            .await
            .unwrap();

        let body = res.data().await.unwrap().unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get("x-my-header").unwrap(), "my-value");
        assert_eq!(
            json_body.get("greeting").unwrap().as_str().unwrap(),
            "Hello Francesco"
        );
    }

    #[test(tokio::test)]
    async fn pass_through_grpc_error() {
        let svc = ServiceBuilder::new()
            .layer(ConnectResponseLayer::default())
            .service_fn(|_: ConnectRequest| async move {
                Err(tonic::Status::unimplemented("My status"))
            });

        let res = svc
            .oneshot(ConnectRequest::mock(greeter_greet_method_descriptor()))
            .await
            .unwrap();

        assert_eq!(res.status(), code_response(Code::Unimplemented).status());
    }
}
