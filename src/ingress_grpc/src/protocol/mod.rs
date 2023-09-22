// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod connect_adapter;
mod tonic_adapter;
mod tower_utils;

use super::options::JsonOptions;
use super::*;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response};
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use restate_schema_api::json::JsonMapperResolver;
use std::future::Future;
use tonic::server::Grpc;
use tonic::Status;
use tower::{BoxError, Layer, Service};
use tower_utils::service_fn_once;

pub(crate) enum Protocol {
    // Use tonic (gRPC or gRPC-Web)
    Tonic,
    // Use Connect
    Connect,
}

impl Protocol {
    pub(crate) fn pick_protocol(method: &Method, headers: &HeaderMap<HeaderValue>) -> Option<Self> {
        let content_type = headers.get(http::header::CONTENT_TYPE);
        match (method, content_type) {
            (&Method::POST, Some(ct))
                if ct.as_bytes().starts_with(b"application/json")
                    || ct.as_bytes().starts_with(b"application/proto")
                    || ct.as_bytes().starts_with(b"application/protobuf") =>
            {
                Some(Protocol::Connect)
            }
            (&Method::GET, None) => {
                // We accept requests without payload with connect, we'll just convert them to empty request objects.
                Some(Protocol::Connect)
            }
            (&Method::POST, Some(ct)) if ct.as_bytes().starts_with(b"application/grpc") => {
                Some(Protocol::Tonic)
            }
            _ => None,
        }
    }

    pub(crate) fn encode_grpc_status(&self, status: Status) -> Response<BoxBody> {
        match self {
            Protocol::Tonic => status.to_http().map(to_box_body),
            Protocol::Connect => connect_adapter::status::status_response(status).map(to_box_body),
        }
    }

    pub(crate) async fn handle_request<MapperResolver, Handler, HandlerFut>(
        self,
        service_name: String,
        method_name: String,
        mapper_resolver: MapperResolver,
        json: JsonOptions,
        req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        MapperResolver: JsonMapperResolver,
        Handler: FnOnce(HandlerRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = HandlerResult> + Send,
    {
        // Extract tracing context if any
        let tracing_context = TraceContextPropagator::new()
            .extract(&opentelemetry_http::HeaderExtractor(req.headers()));

        // Create Ingress request headers
        let ingress_request_headers = IngressRequestHeaders::new(
            service_name,
            method_name,
            tracing_context,
            MetadataMap::from_headers(req.headers().clone()),
        );

        match self {
            Protocol::Tonic => {
                Self::handle_tonic_request(ingress_request_headers, req, handler_fn).await
            }
            Protocol::Connect => Ok(Self::handle_connect_request(
                ingress_request_headers,
                mapper_resolver,
                json,
                req,
                handler_fn,
            )
            .await
            .map(to_box_body)),
        }
    }

    async fn handle_tonic_request<Handler, HandlerFut>(
        ingress_request_headers: IngressRequestHeaders,
        req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        Handler: FnOnce(HandlerRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = HandlerResult> + Send,
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

    async fn handle_connect_request<MapperResolver, Handler, HandlerFut>(
        ingress_request_headers: IngressRequestHeaders,
        mapper_resolver: MapperResolver,
        json: JsonOptions,
        mut req: Request<hyper::Body>,
        handler_fn: Handler,
    ) -> Response<hyper::Body>
    where
        MapperResolver: JsonMapperResolver,
        Handler: FnOnce(HandlerRequest) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = HandlerResult> + Send,
    {
        let content_type = match connect_adapter::verify_headers_and_infer_body_type(&mut req) {
            Ok(c) => c,
            Err(res) => return res,
        };

        let (decoder, encoder) = match content_type
            .infer_encoder_and_decoder(&ingress_request_headers, mapper_resolver)
        {
            Ok(c) => c,
            Err(res) => return res,
        };

        let ingress_request_body = match decoder.decode(req, &json.to_deserialize_options()).await {
            Ok(c) => c,
            Err(res) => return res,
        };
        let handler_response =
            match handler_fn((ingress_request_headers, ingress_request_body)).await {
                Ok(ingress_response_body) => ingress_response_body,
                Err(error) => return connect_adapter::status::status_response(error),
            };

        // Add headers
        let mut response =
            encoder.encode_response(handler_response.body, &json.to_serialize_options());
        response
            .headers_mut()
            .extend(handler_response.metadata.into_headers());
        response
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

    use futures::future::{ok, Ready};
    use http::header::CONTENT_TYPE;
    use http::{Method, Request, StatusCode};
    use hyper::body::HttpBody;
    use prost::Message;
    use restate_test_util::{assert, assert_eq, test};
    use serde_json::json;

    fn greeter_service_fn(ingress_req: HandlerRequest) -> Ready<HandlerResult> {
        let person = restate_pb::mocks::greeter::GreetingRequest::decode(ingress_req.1)
            .unwrap()
            .person;
        ok(HandlerResponse::from_message(
            restate_pb::mocks::greeter::GreetingResponse {
                greeting: format!("Hello {person}"),
            },
        ))
    }

    #[test(tokio::test)]
    async fn handle_json_connect_request() {
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
                MetadataMap::default(),
            ),
            mocks::test_schemas(),
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

    #[test(tokio::test)]
    async fn handle_connect_request_with_empty_body() {
        let request = Request::builder()
            .uri("http://localhost/greeter.Greeter/Greet")
            .method(Method::GET)
            .body(hyper::Body::empty())
            .unwrap();

        assert!(let Some(Protocol::Connect) = Protocol::pick_protocol(request.method(), request.headers()));

        let mut res = Protocol::handle_connect_request(
            IngressRequestHeaders::new(
                "greeter.Greeter".to_string(),
                "Greet".to_string(),
                Context::default(),
                MetadataMap::default(),
            ),
            mocks::test_schemas(),
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
            "Hello "
        );
    }

    #[test(tokio::test)]
    async fn handle_protobuf_connect_request() {
        let request = Request::builder()
            .uri("http://localhost/greeter.Greeter/Greet")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/protobuf")
            .body(
                restate_pb::mocks::greeter::GreetingRequest {
                    person: "Francesco".to_string(),
                }
                .encode_to_vec()
                .into(),
            )
            .unwrap();

        let mut res = Protocol::handle_connect_request(
            IngressRequestHeaders::new(
                "greeter.Greeter".to_string(),
                "Greet".to_string(),
                Context::default(),
                MetadataMap::default(),
            ),
            mocks::test_schemas(),
            JsonOptions::default(),
            request,
            greeter_service_fn,
        )
        .await;

        let body = res.data().await.unwrap().unwrap();
        let pb_body: restate_pb::mocks::greeter::GreetingResponse = Message::decode(body).unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(pb_body.greeting.as_str(), "Hello Francesco");
    }
}
