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
use http::StatusCode;
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use restate_core::TestCoreEnv;
use restate_ingress_dispatcher::mocks::MockDispatcher;
use restate_ingress_dispatcher::IdempotencyMode;
use restate_ingress_dispatcher::IngressRequest;
use restate_types::invocation::Header;
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
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        restate_test_util::assert_eq!(method_name, "greet");
        restate_test_util::assert_eq!(headers, vec![Header::new("my-header", "my-value")]);

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(
                Ok(serde_json::to_vec(&GreetingResponse {
                    greeting: "Igal".to_string(),
                })
                .unwrap()
                .into())
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    restate_test_util::assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn call_service_with_get() {
    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::GET)
        .body(Empty::<Bytes>::default())
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _, response_tx, _) = ingress_req.expect_invocation();
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        restate_test_util::assert_eq!(method_name, "greet");

        assert!(argument.is_empty());

        response_tx
            .send(
                Ok(serde_json::to_vec(&GreetingResponse {
                    greeting: "Igal".to_string(),
                })
                .unwrap()
                .into())
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    restate_test_util::assert_eq!(response_value.greeting, "Igal");
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
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, _, response_tx, _) = ingress_req.expect_invocation();
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
        restate_test_util::assert_eq!(fid.service_id.key, &"my-key");
        restate_test_util::assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(
                Ok(serde_json::to_vec(&GreetingResponse {
                    greeting: "Igal".to_string(),
                })
                .unwrap()
                .into())
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    restate_test_util::assert_eq!(response_value.greeting, "Igal");
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
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _) = ingress_req.expect_background_invocation();
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        restate_test_util::assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        restate_test_util::assert_eq!(&greeting_req.person, "Francesco");
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
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _) = ingress_req.expect_background_invocation();
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
        restate_test_util::assert_eq!(fid.service_id.key, &"my-key");
        restate_test_util::assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        restate_test_util::assert_eq!(&greeting_req.person, "Francesco");
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
        .header(IDEMPOTENCY_KEY, "123456")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, |ingress_req| {
        // Get the function invocation and assert on it
        let (fid, method_name, argument, _, idempotency_mode, response_tx, _) =
            ingress_req.expect_invocation();
        restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
        restate_test_util::assert_eq!(method_name, "greet");

        let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
        restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

        restate_test_util::assert_eq!(
            idempotency_mode,
            IdempotencyMode::key(Bytes::from_static(b"123456"), None)
        );

        response_tx
            .send(
                Ok(serde_json::to_vec(&GreetingResponse {
                    greeting: "Igal".to_string(),
                })
                .unwrap()
                .into())
                .into(),
            )
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    restate_test_util::assert_eq!(response_value.greeting, "Igal");
}

#[tokio::test]
#[traced_test]
async fn bad_path_service() {
    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.Greeter")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;

    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;

    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
}

#[tokio::test]
#[traced_test]
async fn bad_path_virtual_object() {
    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;

    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;

    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
}

#[tokio::test]
#[traced_test]
async fn unknown_component() {
    test_handle_with_status_code_error_response(
        hyper::Request::get(
            "http://localhost/whatevernotexistingservice/whatevernotexistinghandler",
        )
        .body(Empty::<Bytes>::default())
        .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
}

#[tokio::test]
#[traced_test]
async fn unknown_handler() {
    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.Greeter/whatevernotexistinghandler")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
}

#[tokio::test]
#[traced_test]
async fn private_service() {
    test_handle_with_status_code_error_response(
        hyper::Request::get("http://localhost/greeter.GreeterPrivate/greet")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
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

pub async fn test_handle_with_status_code_error_response<B: http_body::Body + Send + 'static>(
    req: Request<B>,
    expected_status_code: StatusCode,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    let response = handle(req, |_| {
        panic!("This code should not be reached in this test");
    })
    .await;

    assert_eq!(response.status(), expected_status_code);

    response
}

pub async fn handle<B: http_body::Body + Send + 'static>(
    mut req: Request<B>,
    f: impl FnOnce(IngressRequest) + Send + 'static,
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
        Handler::new(mock_component_resolver(), dispatcher).oneshot(req),
    );

    // Mock the service invocation receiver
    tokio::spawn(async move {
        f(ingress_request_rx.recv().await.unwrap());
    });

    handler_fut.await.unwrap()
}
