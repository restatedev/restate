// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::component_handler::*;
use super::health::HealthResponse;
use super::mocks::*;
use super::ConnectInfo;
use super::Handler;

use bytes::Bytes;
use bytestring::ByteString;
use http::StatusCode;
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use restate_core::TestCoreEnv;
use restate_ingress_dispatcher::mocks::MockDispatcher;
use restate_ingress_dispatcher::{IngressCorrelationId, IngressDispatcherRequest};
use restate_schema_api::component::{ComponentType, HandlerType};
use restate_schema_api::invocation_target::{
    InputContentType, InputRules, InputValidationRule, InvocationTargetMetadata,
    OutputContentTypeRule, OutputRules,
};
use restate_test_util::{assert, assert_eq};
use restate_types::identifiers::IdempotencyId;
use restate_types::invocation::Header;
use restate_types::invocation::{Idempotency, ResponseResult};
use std::time::Duration;
use tokio::sync::mpsc;
use tower::ServiceExt;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn call_service() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::POST)
        .header("content-type", "application/json")
        .header("Connection", "Close")
        .header("my-header", "my-value")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _, response_tx, headers) =
            ingress_req.expect_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        assert_eq!(method_name, "greet");
        assert_eq!(
            headers,
            vec![
                Header::new("content-type", "application/json"),
                Header::new("my-header", "my-value")
            ]
        );

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(
                ResponseResult::Success(
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn call_service_with_get() {
    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::GET)
        .body(Empty::<Bytes>::default())
        .unwrap();

    let response = handle_with_schemas(
        req,
        MockSchemas::default().with_component_and_target(
            "greeter.Greeter",
            "greet",
            InvocationTargetMetadata {
                input_rules: InputRules {
                    input_validation_rules: vec![InputValidationRule::NoBodyAndContentType],
                },
                ..InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared)
            },
        ),
        |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, _, response_tx, _) =
                ingress_req.expect_invocation();
            assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            assert_eq!(method_name, "greet");

            assert!(argument.is_empty());

            response_tx
                .send(
                    ResponseResult::Success(
                        serde_json::to_vec(&GreetingResponse {
                            greeting: "Igal".to_string(),
                        })
                        .unwrap()
                        .into(),
                    )
                    .into(),
                )
                .unwrap();
        },
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn call_virtual_object() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.GreeterObject/my-key/greet")
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _, response_tx, _) = ingress_req.expect_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
        assert_eq!(fid.service_id.key, &"my-key");
        assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(
                ResponseResult::Success(
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn send_service() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet/send")
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _) = ingress_req.expect_background_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");
    })
    .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[tokio::test]
#[traced_test]
async fn send_with_delay_service() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet/send?delay=PT1M")
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, execution_time) =
            ingress_req.expect_background_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        assert_eq!(method_name, "greet");
        assert!(execution_time.is_some());

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");
    })
    .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[tokio::test]
#[traced_test]
async fn send_virtual_object() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.GreeterObject/my-key/greet/send")
        .method(Method::POST)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _) = ingress_req.expect_background_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
        assert_eq!(fid.service_id.key, &"my-key");
        assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");
    })
    .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[tokio::test]
#[traced_test]
async fn idempotency_key_parsing() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::POST)
        .header("content-type", "application/json")
        .header(IDEMPOTENCY_KEY, "123456")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        let correlation_id = ingress_req.correlation_id.clone();

        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, idempotency, response_tx, _) =
            ingress_req.expect_invocation();
        assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        assert_eq!(
            correlation_id,
            IngressCorrelationId::IdempotencyId(IdempotencyId::combine(
                fid.service_id.clone(),
                ByteString::from_static("greet"),
                ByteString::from_static("123456")
            ))
        );
        assert_eq!(
            idempotency,
            Some(Idempotency {
                key: ByteString::from_static("123456"),
                retention: Duration::from_secs(60 * 60 * 24)
            })
        );

        response_tx
            .send(
                ResponseResult::Success(
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn bad_path_service() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[traced_test]
async fn bad_path_virtual_object() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[traced_test]
async fn unknown_component() {
    let response = handle(
        hyper::Request::get(
            "http://localhost/whatevernotexistingservice/whatevernotexistinghandler",
        )
        .body(Empty::<Bytes>::default())
        .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[traced_test]
async fn unknown_handler() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/whatevernotexistinghandler")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[traced_test]
async fn private_service() {
    let response = handle_with_schemas(
        hyper::Request::get("http://localhost/greeter.GreeterPrivate/greet")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockSchemas::default().with_component_and_target(
            "greeter.GreeterPrivate",
            "greet",
            InvocationTargetMetadata {
                public: false,
                ..InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared)
            },
        ),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[traced_test]
async fn invalid_input() {
    let response = handle_with_schemas(
        hyper::Request::get("http://localhost/greeter.Greeter/greet")
            .header("content-type", "application/cbor")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockSchemas::default().with_component_and_target(
            "greeter.Greeter",
            "greet",
            InvocationTargetMetadata {
                input_rules: InputRules {
                    input_validation_rules: vec![InputValidationRule::ContentType {
                        content_type: InputContentType::MimeTypeAndSubtype(
                            "application".into(),
                            "json".into(),
                        ),
                    }],
                },
                ..InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared)
            },
        ),
        request_handler_not_reached,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[traced_test]
async fn set_custom_content_type_on_response() {
    let mock_schemas = MockSchemas::default().with_component_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata {
            output_rules: OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: http_old::HeaderValue::from_static("application/cbor"),
                    set_content_type_if_empty: false,
                    has_json_schema: false,
                },
            },
            ..InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared)
        },
    );
    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::POST)
        .body(Empty::<Bytes>::default())
        .unwrap();

    // Case when the response is empty
    let response = handle_with_schemas(
        req.clone(),
        mock_schemas.clone(),
        expect_invocation_and_reply_with_empty,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("content-type").is_none());

    // Case when the response is non-empty
    let response = handle_with_schemas(
        req.clone(),
        mock_schemas,
        expect_invocation_and_reply_with_non_empty,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/cbor"
    );
}

#[tokio::test]
#[traced_test]
async fn set_custom_content_type_on_empty_response() {
    let mock_schemas = MockSchemas::default().with_component_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata {
            output_rules: OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: http_old::HeaderValue::from_static("application/protobuf"),
                    set_content_type_if_empty: true,
                    has_json_schema: false,
                },
            },
            ..InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared)
        },
    );
    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::POST)
        .body(Empty::<Bytes>::default())
        .unwrap();

    // Case when the response is empty
    let response = handle_with_schemas(
        req.clone(),
        mock_schemas.clone(),
        expect_invocation_and_reply_with_empty,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/protobuf"
    );

    // Case when the response is non-empty
    let response = handle_with_schemas(
        req.clone(),
        mock_schemas,
        expect_invocation_and_reply_with_non_empty,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/protobuf"
    );
}

#[tokio::test]
#[traced_test]
async fn health() {
    let req = hyper::Request::builder()
        .uri("http://localhost/restate/health")
        .method(Method::GET)
        .body(Empty::<Bytes>::default())
        .unwrap();

    let response = handle(req, |_| {
        panic!("This code should not be reached in this test");
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: HealthResponse = serde_json::from_slice(&response_bytes).unwrap();
}

fn request_handler_not_reached(_req: IngressDispatcherRequest) {
    panic!("This code should not be reached in this test");
}

fn expect_invocation_and_reply_with_empty(req: IngressDispatcherRequest) {
    let (_, _, _, _, _, response_tx, _) = req.expect_invocation();
    response_tx
        .send(ResponseResult::Success(Bytes::new()).into())
        .unwrap();
}

fn expect_invocation_and_reply_with_non_empty(req: IngressDispatcherRequest) {
    let (_, _, _, _, _, response_tx, _) = req.expect_invocation();
    response_tx
        .send(ResponseResult::Success(Bytes::from_static(b"123")).into())
        .unwrap();
}

pub async fn handle_with_schemas<B: http_body::Body + Send + 'static>(
    mut req: Request<B>,
    schemas: MockSchemas,
    f: impl FnOnce(IngressDispatcherRequest) + Send + 'static,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
    let (ingress_request_tx, mut ingress_request_rx) = mpsc::unbounded_channel();
    let dispatcher = MockDispatcher::new(ingress_request_tx);

    req.extensions_mut()
        .insert(ConnectInfo::new("0.0.0.0:0".parse().unwrap()));
    req.extensions_mut().insert(opentelemetry::Context::new());

    let handler_fut = node_env.tc.run_in_scope(
        "ingress",
        None,
        Handler::new(schemas, dispatcher).oneshot(req),
    );

    // Mock the service invocation receiver
    tokio::spawn(async move {
        f(ingress_request_rx.recv().await.unwrap());
    });

    handler_fut.await.unwrap()
}

pub async fn handle<B: http_body::Body + Send + 'static>(
    req: Request<B>,
    f: impl FnOnce(IngressDispatcherRequest) + Send + 'static,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    handle_with_schemas(req, mock_schemas(), f).await
}
