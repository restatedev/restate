use super::content_type::*;
use super::res::ConnectResponseBuilder;
use super::utils::*;
use super::*;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::future::{ok, BoxFuture, Either, Ready};
use futures::ready;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::request::Parts;
use http::Request as HttpRequest;
use http::Response as HttpResponse;
use http::{HeaderMap, Method};
use hyper::Body;
use pin_project::pin_project;
use prost_reflect::{DynamicMessage, MethodDescriptor};
use tonic::Status;
use tower::{BoxError, Layer, Service};
use tracing::warn;

// --- Request

#[derive(Debug)]
pub struct ConnectRequest {
    method_descriptor: MethodDescriptor,
    headers: HeaderMap,
    content_type: ConnectContentType,
    payload: DynamicMessage,
}

impl ConnectRequest {
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

    /// Returns the full name of the method
    #[inline]
    pub(super) fn method_name(&self) -> &str {
        self.method_descriptor.full_name()
    }

    #[inline]
    pub(super) fn method_descriptor(&self) -> &MethodDescriptor {
        &self.method_descriptor
    }

    pub fn into_inner(self) -> (HeaderMap, DynamicMessage, ConnectResponseBuilder) {
        (
            self.headers,
            self.payload,
            ConnectResponseBuilder {
                method_descriptor: self.method_descriptor,
                headers: Default::default(),
                content_type: self.content_type,
            },
        )
    }
}

pub struct ConnectRequestLayer<Registry> {
    method_registry: Registry,
}

impl<Registry> ConnectRequestLayer<Registry> {
    pub fn new(method_registry: Registry) -> Self {
        Self { method_registry }
    }
}

impl<S, Registry: Clone> Layer<S> for ConnectRequestLayer<Registry> {
    type Service = ConnectRequestService<Registry, S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectRequestService {
            method_registry: self.method_registry.clone(),
            inner: Some(inner),
        }
    }
}

// A middleware that converts the HTTP request to GRPC request
#[derive(Clone)]
pub struct ConnectRequestService<Registry, S> {
    method_registry: Registry,

    inner: Option<S>,
}

fn create_connect_request(
    method_descriptor: MethodDescriptor,
    parts: Parts,
    content_type: ConnectContentType,
    body: impl Buf + Sized,
) -> Result<ConnectRequest, HttpResponse<Body>> {
    let msg = match read_message(content_type, method_descriptor.input(), body) {
        Ok(msg) => msg,
        Err(e) => {
            warn!(
                "Error when parsing request {}: {}",
                method_descriptor.full_name(),
                e
            );
            return Err(status_response(Status::invalid_argument(format!(
                "Error when parsing request {}: {}",
                method_descriptor.full_name(),
                e
            ))));
        }
    };
    Ok(ConnectRequest::new(
        method_descriptor,
        parts.headers,
        content_type,
        msg,
    ))
}

#[inline]
fn is_get_allowed(desc: &MethodDescriptor) -> bool {
    return desc.input().full_name() == "google.protobuf.Empty";
}

#[inline]
fn is_content_encoding_identity(parts: &Parts) -> bool {
    return parts
        .headers
        .get(CONTENT_ENCODING)
        .and_then(|hv| hv.to_str().ok().map(|s| s.contains("identity")))
        .unwrap_or(true);
}

impl<Registry, S> Service<HttpRequest<Body>> for ConnectRequestService<Registry, S>
where
    S: Service<ConnectRequest, Response = HttpResponse<Body>, Error = BoxError> + 'static,
    Registry: MethodDescriptorRegistry,
{
    type Response = HttpResponse<Body>;
    type Error = S::Error;
    type Future = Either<Ready<Result<HttpResponse<Body>, S::Error>>, ResponseFuture<Bytes, S>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.as_mut().unwrap().poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        let (mut parts, body) = request.into_parts();

        // Check content encoding
        if !is_content_encoding_identity(&parts) {
            return Either::Left(ok(not_implemented()));
        }

        // Parse service_name and method_name
        let mut path_parts: Vec<&str> = parts.uri.path().split('/').collect();
        if path_parts.len() != 3 {
            return Either::Left(ok(not_found()));
        }
        let method_name = path_parts.remove(2);
        let service_name = path_parts.remove(1);

        // Figure out the method descriptor
        let desc = match self
            .method_registry
            .resolve_method_descriptor(service_name, method_name)
        {
            Some(desc) => desc,
            None => return Either::Left(ok(not_found())),
        };

        if desc.is_client_streaming() || desc.is_server_streaming() {
            return Either::Left(ok(not_implemented()));
        }

        if parts.method == Method::GET && is_get_allowed(&desc) {
            let payload = DynamicMessage::new(desc.input());
            let connect_req =
                ConnectRequest::new(desc, parts.headers, ConnectContentType::Json, payload);

            Either::Right(ResponseFuture::NextFut {
                future: self.inner.take().unwrap().call(connect_req),
            })
        } else if parts.method == Method::POST {
            // Read content type
            let content_type = match parts
                .headers
                .remove(CONTENT_TYPE)
                .and_then(|hv| resolve_content_type(&hv))
            {
                Some(ct) => ct,
                None => return Either::Left(ok(unsupported_media_type())),
            };

            Either::Right(ResponseFuture::BodyFut {
                method_descriptor: Some(desc),
                parts: Some(parts),
                content_type: Some(content_type),
                future: Box::pin(hyper::body::to_bytes(body)),
                svc: self.inner.take().unwrap(),
            })
        } else {
            Either::Left(ok(method_not_allowed()))
        }
    }
}

#[pin_project(project = ResponseFutureProj)]
pub enum ResponseFuture<B, S>
where
    S: Service<ConnectRequest>,
{
    BodyFut {
        method_descriptor: Option<MethodDescriptor>,
        parts: Option<Parts>,
        content_type: Option<ConnectContentType>,

        // TODO when this is released https://github.com/hyperium/http-body/pull/70
        //  we can remove this box
        #[pin]
        future: BoxFuture<'static, Result<B, hyper::Error>>,
        svc: S,
    },
    NextFut {
        #[pin]
        future: S::Future,
    },
}

// Assert ResponseFuture can be Send
const _: () = {
    fn assert_send<T: Send + ?Sized>() {}

    // RFC 2056
    fn assert_all<B: Send, T: Service<ConnectRequest, Future = dyn Send> + Send>() {
        assert_send::<ResponseFuture<B, T>>();
    }
};

impl<B, S> Future for ResponseFuture<B, S>
where
    B: Buf + Sized,
    S: Service<ConnectRequest, Response = HttpResponse<Body>, Error = BoxError> + 'static,
{
    type Output = Result<HttpResponse<Body>, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().project() {
                ResponseFutureProj::NextFut { future } => return future.poll(cx),
                ResponseFutureProj::BodyFut {
                    method_descriptor,
                    parts,
                    content_type,
                    future,
                    svc,
                } => {
                    let body = match ready!(future.poll(cx)) {
                        Ok(b) => b,
                        Err(e) => return Poll::Ready(Err(BoxError::from(e))),
                    };

                    let method_descriptor = method_descriptor.take().expect(
                        "Looks like this future has been invoked after returning Poll::Ready",
                    );
                    let parts = parts.take().expect(
                        "Looks like this future has been invoked after returning Poll::Ready",
                    );
                    let content_type = content_type.take().expect(
                        "Looks like this future has been invoked after returning Poll::Ready",
                    );
                    let grpc_req = match create_connect_request(
                        method_descriptor,
                        parts,
                        content_type,
                        body,
                    ) {
                        Ok(grpc_req) => grpc_req,
                        Err(res) => {
                            return Poll::Ready(Ok(res));
                        }
                    };

                    let f = svc.call(grpc_req);
                    self.set(ResponseFuture::NextFut { future: f });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::{pb, test_descriptor_registry};
    use super::*;

    use http::{HeaderValue, StatusCode};
    use prost::Message;
    use serde_json::json;
    use test_utils::{assert_eq, test};
    use tower::{ServiceBuilder, ServiceExt};

    impl ConnectRequest {
        pub(crate) fn mock(method_descriptor: MethodDescriptor) -> ConnectRequest {
            let input_desc = method_descriptor.input();
            ConnectRequest {
                method_descriptor,
                headers: Default::default(),
                content_type: ConnectContentType::Json,
                payload: DynamicMessage::new(input_desc),
            }
        }
    }

    #[test(tokio::test)]
    async fn invoke_greeter_json_and_header_propagation() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|connect_req: ConnectRequest| async move {
                assert_eq!(connect_req.method_name(), "greeter.Greeter.Greet");

                let (mut headers, payload, _) = connect_req.into_inner();
                assert_eq!(
                    headers.remove("x-my-header").unwrap(),
                    HeaderValue::from_static("my-value")
                );
                assert_eq!(
                    payload.transcode_to::<pb::GreetingRequest>().unwrap(),
                    pb::GreetingRequest {
                        person: "Francesco".to_string()
                    }
                );

                Ok(HttpResponse::builder()
                    .status(StatusCode::from_u16(299).unwrap())
                    .body(Body::empty())
                    .unwrap())
            });

        let res = svc
            .oneshot(
                HttpRequest::builder()
                    .uri("http://localhost/greeter.Greeter/Greet")
                    .method(Method::POST)
                    .header(CONTENT_TYPE, "application/json")
                    .header("x-my-header", "my-value")
                    .body(json!({"person": "Francesco"}).to_string().into())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status().as_u16(), 299_u16);
    }

    #[test(tokio::test)]
    async fn invoke_greeter_get_count() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|connect_req: ConnectRequest| async move {
                assert_eq!(connect_req.method_name(), "greeter.Greeter.GetCount");

                let (mut headers, payload, _) = connect_req.into_inner();
                assert_eq!(
                    headers.remove("x-my-header").unwrap(),
                    HeaderValue::from_static("my-value")
                );
                payload
                    .transcode_to::<()>() // google.protobuf.Empty
                    .unwrap();

                Ok(HttpResponse::builder()
                    .status(StatusCode::from_u16(299).unwrap())
                    .body(Body::empty())
                    .unwrap())
            });

        let res = svc
            .oneshot(
                HttpRequest::get("http://localhost/greeter.Greeter/GetCount")
                    .header("x-my-header", "my-value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status().as_u16(), 299_u16);
    }

    #[test(tokio::test)]
    async fn invoke_greeter_protobuf() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|connect_req: ConnectRequest| async move {
                assert_eq!(connect_req.method_name(), "greeter.Greeter.Greet");
                assert_eq!(
                    connect_req
                        .payload
                        .transcode_to::<pb::GreetingRequest>()
                        .unwrap(),
                    pb::GreetingRequest {
                        person: "Francesco".to_string()
                    }
                );

                Ok(HttpResponse::builder()
                    .status(StatusCode::from_u16(299).unwrap())
                    .body(Body::empty())
                    .unwrap())
            });

        let res = svc
            .oneshot(
                HttpRequest::builder()
                    .uri("http://localhost/greeter.Greeter/Greet")
                    .method(Method::POST)
                    .header(CONTENT_TYPE, "application/protobuf")
                    .body(
                        pb::GreetingRequest {
                            person: "Francesco".to_string(),
                        }
                        .encode_to_vec()
                        .into(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status().as_u16(), 299_u16);
    }

    #[test(tokio::test)]
    async fn invoke_wrong_http_method() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|_: ConnectRequest| async move { unreachable!() });

        let res = svc
            .oneshot(
                HttpRequest::builder()
                    .uri("http://localhost/greeter.Greeter/Greet")
                    .method(Method::PUT)
                    .header(CONTENT_TYPE, "application/json")
                    .body(json!({"person": "Francesco"}).to_string().into())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test(tokio::test)]
    async fn invoke_unknown_content_type() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|_: ConnectRequest| async move { unreachable!() });

        let res = svc
            .oneshot(
                HttpRequest::builder()
                    .uri("http://localhost/greeter.Greeter/Greet")
                    .method(Method::POST)
                    .header(CONTENT_TYPE, "application/yaml")
                    .body("person: Francesco".to_string().into())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test(tokio::test)]
    async fn invoke_streaming_method() {
        let svc = ServiceBuilder::new()
            .layer(ConnectRequestLayer::new(test_descriptor_registry()))
            .service_fn(|_: ConnectRequest| async move { unreachable!() });

        let res = svc
            .oneshot(
                HttpRequest::builder()
                    .uri("http://localhost/greeter.Greeter/GreetStream")
                    .method(Method::POST)
                    .header(CONTENT_TYPE, "application/json")
                    .body(json!({"person": "Francesco"}).to_string().into())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    }

    macro_rules! test_404 {
            ($test_name:ident, $uri:literal) => {
                #[test(tokio::test)]
                async fn $test_name() {
                    let svc = ServiceBuilder::new()
                        .layer(ConnectRequestLayer::new(test_descriptor_registry()))
                        .service_fn(|_: ConnectRequest| async move {
                            unreachable!()
                        });

                    let res = svc.oneshot(HttpRequest::builder()
                        .uri($uri)
                        .method(Method::POST)
                        .header(CONTENT_TYPE, "application/json")
                        .body(json!({
                            "person": "Francesco"
                        }).to_string().into())
                        .unwrap()
                    ).await.unwrap();

                    assert_eq!(res.status(), StatusCode::NOT_FOUND);
                }
            };
        }

    test_404!(
        invoke_another_greeter_fails,
        "http://localhost/greeter.AnotherGreeter/Greet"
    );
    test_404!(
        invoke_unknown_service_fails,
        "http://localhost/Unknown/Greet"
    );
    test_404!(
        invoke_unknown_method_fails,
        "http://localhost/greeter.Greeter/Unknown"
    );
    test_404!(
        invoke_missing_method_fails,
        "http://localhost/greeter.Greeter"
    );
    test_404!(invoke_missing_service_fails, "http://localhost/");
}
