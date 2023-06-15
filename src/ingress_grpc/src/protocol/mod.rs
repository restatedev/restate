mod connect_adapter;
mod tonic_adapter;
mod tower_utils;

use super::options::JsonOptions;
use super::*;

use std::future::Future;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use prost::Message;
use prost_reflect::MethodDescriptor;
use restate_service_metadata::MethodDescriptorRegistry;
use tonic::server::Grpc;
use tonic::Status;
use tower::{BoxError, Layer, Service};
use tower_utils::service_fn_once;
use tracing::debug;

pub(crate) enum Protocol {
    // Use tonic (gRPC or gRPC-Web)
    Tonic,
    // Use Connect
    Connect,
}

impl Protocol {
    pub(crate) fn pick_protocol(headers: &HeaderMap<HeaderValue>) -> Option<Self> {
        let content_type = headers.get(http::header::CONTENT_TYPE);
        match content_type {
            Some(ct)
                if ct.as_bytes().starts_with(b"application/json")
                    || ct.as_bytes().starts_with(b"application/proto")
                    || ct.as_bytes().starts_with(b"application/protobuf") =>
            {
                Some(Protocol::Connect)
            }
            Some(ct) if ct.as_bytes().starts_with(b"application/grpc") => Some(Protocol::Tonic),
            _ => None,
        }
    }

    pub(crate) fn encode_status(&self, status: Status) -> Response<BoxBody> {
        match self {
            Protocol::Tonic => status.to_http().map(to_box_body),
            Protocol::Connect => connect_adapter::status::status_response(status).map(to_box_body),
        }
    }

    pub(crate) async fn handle_request<MethodRegistry, Handler, HandlerFut>(
        self,
        service_name: String,
        method_name: String,
        method_registry: MethodRegistry,
        json: JsonOptions,
        req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        MethodRegistry: MethodDescriptorRegistry,
        Handler: FnOnce(IngressRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = Result<IngressResponse, Status>> + Send,
    {
        // Find the service method descriptor
        let descriptor = if let Some(desc) =
            method_registry.resolve_method_descriptor(&service_name, &method_name)
        {
            desc
        } else {
            debug!("{}/{} not found", service_name, method_name);
            return Ok(self.encode_status(Status::not_found(format!(
                "{service_name}/{method_name} not found"
            ))));
        };

        // Extract tracing context if any
        let tracing_context = TraceContextPropagator::new()
            .extract(&opentelemetry_http::HeaderExtractor(req.headers()));

        // Create Ingress request headers
        let ingress_request_headers =
            IngressRequestHeaders::new(service_name, method_name, tracing_context);

        match self {
            Protocol::Tonic => {
                Self::handle_tonic_request(ingress_request_headers, req, handler_fn).await
            }
            Protocol::Connect => Ok(Self::handle_connect_request(
                ingress_request_headers,
                descriptor,
                method_registry,
                json,
                req,
                handler_fn,
            )
            .await),
        }
    }

    async fn handle_tonic_request<Handler, HandlerFut>(
        ingress_request_headers: IngressRequestHeaders,
        req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        Handler: FnOnce(IngressRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = Result<IngressResponse, Status>> + Send,
    {
        // Why FnOnce and service_fn_once are safe here?
        //
        // The reason is that the interface of Grpc::unary() is probably incorrect,
        // because it gets the ownership of the service, rather than a &self mut borrow.
        //
        // There is no reason to get the ownership, as the service could be reused.
        // There is also no reason for which Grpc::unary() should invoke twice Service::call() within
        // its code (you can verify this point by looking inside the Grpc::unary() implementation).
        //
        // Hence we can safely provide a service which after the first Service::call()
        // is consumed and it cannot be reused anymore.

        let mut s = tonic_web::GrpcWebLayer::new().layer(service_fn_once(move |hyper_req| async {
            Ok::<_, Status>(
                Grpc::new(tonic_adapter::NoopCodec)
                    .unary(
                        tonic_adapter::TonicUnaryServiceAdapter::new(
                            ingress_request_headers,
                            handler_fn,
                        ),
                        hyper_req,
                    )
                    .await,
            )
        }));

        s.call(req)
            .await
            .map_err(BoxError::from)
            .map(|res| res.map(to_box_body))
    }

    async fn handle_connect_request<MethodRegistry, Handler, HandlerFut>(
        ingress_request_headers: IngressRequestHeaders,
        descriptor: MethodDescriptor,
        method_registry: MethodRegistry,
        json: JsonOptions,
        req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Response<BoxBody>
    where
        MethodRegistry: MethodDescriptorRegistry,
        Handler: FnOnce(IngressRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = Result<IngressResponse, Status>> + Send,
    {
        let (content_type, request_message) = match connect_adapter::decode_request(
            req,
            &descriptor,
            method_registry,
            json.to_deserialize_options(),
        )
        .await
        {
            Ok(req) => req,
            Err(error_res) => return error_res.map(to_box_body),
        };

        let ingress_request_body = Bytes::from(request_message.encode_to_vec());
        let response = match handler_fn((ingress_request_headers, ingress_request_body)).await {
            Ok(ingress_response_body) => ingress_response_body,
            Err(error) => return connect_adapter::status::status_response(error).map(to_box_body),
        };

        connect_adapter::encode_response(
            response,
            &descriptor,
            json.to_serialize_options(),
            content_type,
        )
        .map(to_box_body)
    }
}

// TODO use https://docs.rs/http-body-util/0.1.0-rc.2/http_body_util/enum.Either.html when released
pub type BoxBody = UnsyncBoxBody<Bytes, BoxError>;

fn to_box_body<B, BE>(body: B) -> UnsyncBoxBody<Bytes, BoxError>
where
    BE: Into<BoxError> + 'static,
    B: Body<Data = Bytes, Error = BE> + Sized + Send + 'static,
{
    body.map_err(Into::into).boxed_unsync()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::mocks::*;
    use futures::future::{ok, Ready};
    use http::header::CONTENT_TYPE;
    use http::{Method, Request, StatusCode};
    use hyper::body::HttpBody;
    use restate_test_util::{assert_eq, test};
    use serde_json::json;

    fn greeter_service_fn(ingress_req: IngressRequest) -> Ready<Result<IngressResponse, Status>> {
        let person = mocks::pb::GreetingRequest::decode(ingress_req.1)
            .unwrap()
            .person;
        ok(mocks::pb::GreetingResponse {
            greeting: format!("Hello {person}"),
        }
        .encode_to_vec()
        .into())
    }

    #[test(tokio::test)]
    async fn handle_connect_request_works() {
        let request = Request::builder()
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
            .unwrap();

        let mut res = Protocol::handle_connect_request(
            IngressRequestHeaders::new(
                "greeter.Greeter".to_string(),
                "Greet".to_string(),
                Context::default(),
            ),
            greeter_greet_method_descriptor(),
            test_descriptor_registry(),
            JsonOptions::default(),
            request,
            greeter_service_fn,
        )
        .await;

        let body = res.data().await.unwrap().unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            json_body.get("greeting").unwrap().as_str().unwrap(),
            "Hello Francesco"
        );
    }
}
