// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::health::HealthResponse;
use super::mocks::*;
use super::service_handler::*;
use super::ConnectInfo;
use super::Handler;
use restate_ingress_dispatcher::{IngressInvocationResponse, SubmittedInvocationNotification};
use std::collections::HashMap;

use crate::handler::responses::X_RESTATE_ID;
use bytes::Bytes;
use bytestring::ByteString;
use googletest::prelude::*;
use http::StatusCode;
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use restate_core::TestCoreEnv;
use restate_ingress_dispatcher::mocks::MockDispatcher;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::invocation_target::{
    InputContentType, InputRules, InputValidationRule, InvocationTargetMetadata,
    OutputContentTypeRule, OutputRules,
};
use restate_test_util::{assert, assert_eq};
use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
use restate_types::ingress::{IngressResponseResult, InvocationResponse};
use restate_types::invocation::{
    Header, InvocationQuery, InvocationTarget, InvocationTargetType, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
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
        let (service_invocation, _, response_tx) = ingress_req.expect_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");
        assert_eq!(
            service_invocation.headers,
            vec![
                Header::new("content-type", "application/json"),
                Header::new("my-header", "my-value")
            ]
        );

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(InvocationId::mock_random()),
                result: IngressResponseResult::Success(
                    service_invocation.invocation_target,
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (parts, response_body) = response.into_parts();
    assert!(parts.headers.contains_key(X_RESTATE_ID));
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
        MockSchemas::default().with_service_and_target(
            "greeter.Greeter",
            "greet",
            InvocationTargetMetadata {
                input_rules: InputRules {
                    input_validation_rules: vec![InputValidationRule::NoBodyAndContentType],
                },
                ..InvocationTargetMetadata::mock(InvocationTargetType::Service)
            },
        ),
        |ingress_req| {
            // Get the function invocation and assert on it
            let (service_invocation, _, response_tx) = ingress_req.expect_invocation();
            assert_eq!(
                service_invocation.invocation_target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

            assert!(service_invocation.argument.is_empty());

            response_tx
                .send(IngressInvocationResponse {
                    idempotency_expiry_time: None,
                    invocation_id: Some(InvocationId::mock_random()),
                    result: IngressResponseResult::Success(
                        service_invocation.invocation_target,
                        serde_json::to_vec(&GreetingResponse {
                            greeting: "Igal".to_string(),
                        })
                        .unwrap()
                        .into(),
                    ),
                })
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
        let (service_invocation, _, response_tx) = ingress_req.expect_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.GreeterObject"
        );
        assert_eq!(
            service_invocation.invocation_target.key().unwrap(),
            &"my-key"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(InvocationId::mock_random()),
                result: IngressResponseResult::Success(
                    service_invocation.invocation_target,
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
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
        let service_invocation = ingress_req.expect_one_way_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
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
        let service_invocation = ingress_req.expect_one_way_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");
        assert!(service_invocation.execution_time.is_some());

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
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
        let service_invocation = ingress_req.expect_one_way_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.GreeterObject"
        );
        assert_eq!(
            service_invocation.invocation_target.key().unwrap(),
            &"my-key"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
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
        // Get the function invocation and assert on it
        let (service_invocation, _, response_tx) = ingress_req.expect_invocation();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        assert_eq!(
            service_invocation.idempotency_key,
            Some(ByteString::from_static("123456"))
        );
        assert_eq!(
            service_invocation.completion_retention_time,
            Some(Duration::from_secs(60 * 60 * 24))
        );

        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(InvocationId::mock_random()),
                result: IngressResponseResult::Success(
                    service_invocation.invocation_target,
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
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
async fn idempotency_key_and_send() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet/send")
        .method(Method::POST)
        .header("content-type", "application/json")
        .header(IDEMPOTENCY_KEY, "123456")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let response = handle(req, move |ingress_req| {
        // Get the function invocation and assert on it
        let (service_invocation, notification_tx) =
            ingress_req.expect_one_way_invocation_with_submit_notification();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        assert_eq!(
            service_invocation.idempotency_key,
            Some(ByteString::from_static("123456"))
        );
        assert_eq!(
            service_invocation.completion_retention_time,
            Some(Duration::from_secs(60 * 60 * 24))
        );
        assert_that!(
            service_invocation.submit_notification_sink,
            some(anything())
        );

        notification_tx
            .send(SubmittedInvocationNotification {
                invocation_id: service_invocation.invocation_id,
            })
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[tokio::test]
#[traced_test]
async fn idempotency_key_and_send_with_different_invocation_id() {
    let greeting_req = GreetingRequest {
        person: "Francesco".to_string(),
    };

    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet/send")
        .method(Method::POST)
        .header("content-type", "application/json")
        .header(IDEMPOTENCY_KEY, "123456")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&greeting_req).unwrap(),
        )))
        .unwrap();

    let expected_invocation_id = InvocationId::mock_random();

    let response = handle(req, move |ingress_req| {
        // Get the function invocation and assert on it
        let (service_invocation, notification_tx) =
            ingress_req.expect_one_way_invocation_with_submit_notification();
        assert_eq!(
            service_invocation.invocation_target.service_name(),
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

        let greeting_req: GreetingRequest =
            serde_json::from_slice(&service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        assert_eq!(
            service_invocation.idempotency_key,
            Some(ByteString::from_static("123456"))
        );
        assert_eq!(
            service_invocation.completion_retention_time,
            Some(Duration::from_secs(60 * 60 * 24))
        );
        assert_that!(
            service_invocation.submit_notification_sink,
            some(anything())
        );

        notification_tx
            .send(SubmittedInvocationNotification {
                invocation_id: expected_invocation_id,
            })
            .unwrap();
    })
    .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let send_response: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(send_response.invocation_id, expected_invocation_id);
}

#[tokio::test]
#[traced_test]
async fn attach_with_invocation_id() {
    let invocation_id = InvocationId::mock_random();

    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata::mock(InvocationTargetType::Service),
    );

    let req = hyper::Request::builder()
        .uri(format!(
            "http://localhost/restate/invocation/{}/attach",
            invocation_id
        ))
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = handle_with_schemas(req, mock_schemas, move |ingress_req| {
        let (actual_invocation_query, _, response_tx) = ingress_req.expect_attach();
        assert_eq!(
            InvocationQuery::Invocation(invocation_id),
            actual_invocation_query
        );
        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(invocation_id),
                result: IngressResponseResult::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
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
async fn attach_with_idempotency_id_to_unkeyed_service() {
    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata::mock(InvocationTargetType::Service),
    );
    let invocation_id = InvocationId::mock_random();

    let req = hyper::Request::builder()
        .uri("http://localhost/restate/invocation/greeter.Greeter/greet/myid/attach")
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = handle_with_schemas(req, mock_schemas, move |ingress_req| {
        let (actual_invocation_query, _, response_tx) = ingress_req.expect_attach();
        assert_eq!(
            InvocationQuery::IdempotencyId(IdempotencyId::new(
                "greeter.Greeter".into(),
                None,
                "greet".into(),
                "myid".into()
            )),
            actual_invocation_query
        );
        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(invocation_id),
                result: IngressResponseResult::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
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
async fn attach_with_idempotency_id_to_keyed_service() {
    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata::mock(InvocationTargetType::VirtualObject(
            VirtualObjectHandlerType::Exclusive,
        )),
    );
    let invocation_id = InvocationId::mock_random();

    let req = hyper::Request::builder()
        .uri("http://localhost/restate/invocation/greeter.Greeter/mygreet/greet/myid/attach")
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = handle_with_schemas(req, mock_schemas, move |ingress_req| {
        let (actual_invocation_query, _, response_tx) = ingress_req.expect_attach();
        assert_eq!(
            InvocationQuery::IdempotencyId(IdempotencyId::new(
                "greeter.Greeter".into(),
                Some("mygreet".into()),
                "greet".into(),
                "myid".into()
            )),
            actual_invocation_query
        );
        response_tx
            .send(IngressInvocationResponse {
                idempotency_expiry_time: None,
                invocation_id: Some(invocation_id),
                result: IngressResponseResult::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })
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
async fn get_output_with_invocation_id() {
    let invocation_id = InvocationId::mock_random();

    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata::mock(InvocationTargetType::Service),
    );

    let req = hyper::Request::builder()
        .uri(format!(
            "http://localhost/restate/invocation/{}/output",
            invocation_id
        ))
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = handle_with_schemas_and_storage_reader(
        req,
        mock_schemas,
        MockStorageReader(HashMap::from([(
            InvocationQuery::Invocation(invocation_id),
            InvocationResponse {
                request_id: Default::default(),
                invocation_id: Some(invocation_id),
                response: IngressResponseResult::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            },
        )])),
        move |_| panic!("This should not be called"),
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
async fn get_output_with_workflow_key() {
    let service_id = ServiceId::new("MyWorkflow", "my-key");

    let mock_schemas = MockSchemas::default().with_service_and_target(
        &service_id.service_name,
        "run",
        InvocationTargetMetadata::mock(InvocationTargetType::Workflow(
            WorkflowHandlerType::Workflow,
        )),
    );

    let req = hyper::Request::builder()
        .uri(format!(
            "http://localhost/restate/workflow/{}/{}/output",
            service_id.service_name, service_id.key
        ))
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = handle_with_schemas_and_storage_reader(
        req,
        mock_schemas,
        MockStorageReader(HashMap::from([(
            InvocationQuery::Workflow(service_id.clone()),
            InvocationResponse {
                request_id: Default::default(),
                invocation_id: None,
                response: IngressResponseResult::Success(
                    InvocationTarget::workflow(
                        service_id.service_name,
                        service_id.key,
                        "run",
                        WorkflowHandlerType::Workflow,
                    ),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            },
        )])),
        move |_| panic!("This should not be called"),
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
async fn unknown_service() {
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
        MockSchemas::default().with_service_and_target(
            "greeter.GreeterPrivate",
            "greet",
            InvocationTargetMetadata {
                public: false,
                ..InvocationTargetMetadata::mock(InvocationTargetType::Service)
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
        MockSchemas::default().with_service_and_target(
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
                ..InvocationTargetMetadata::mock(InvocationTargetType::Service)
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
    let mock_schemas = MockSchemas::default().with_service_and_target(
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
            ..InvocationTargetMetadata::mock(InvocationTargetType::Service)
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
    let mock_schemas = MockSchemas::default().with_service_and_target(
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
            ..InvocationTargetMetadata::mock(InvocationTargetType::Service)
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
    let (service_invocation, _, response_tx) = req.expect_invocation();
    response_tx
        .send(IngressInvocationResponse {
            idempotency_expiry_time: None,
            invocation_id: Some(InvocationId::mock_random()),
            result: IngressResponseResult::Success(
                service_invocation.invocation_target,
                Bytes::new(),
            ),
        })
        .unwrap();
}

fn expect_invocation_and_reply_with_non_empty(req: IngressDispatcherRequest) {
    let (service_invocation, _, response_tx) = req.expect_invocation();
    response_tx
        .send(IngressInvocationResponse {
            idempotency_expiry_time: None,
            invocation_id: Some(InvocationId::mock_random()),
            result: IngressResponseResult::Success(
                service_invocation.invocation_target,
                Bytes::from_static(b"123"),
            ),
        })
        .unwrap();
}

pub async fn handle_with_schemas_and_storage_reader<B: http_body::Body + Send + 'static>(
    mut req: Request<B>,
    schemas: MockSchemas,
    invocation_storage_reader: MockStorageReader,
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
        Handler::new(schemas, dispatcher, invocation_storage_reader).oneshot(req),
    );

    // Mock the service invocation receiver
    tokio::spawn(async move {
        f(ingress_request_rx.recv().await.unwrap());
    });

    handler_fut.await.unwrap()
}

pub async fn handle_with_schemas<B: http_body::Body + Send + 'static>(
    req: Request<B>,
    schemas: MockSchemas,
    f: impl FnOnce(IngressDispatcherRequest) + Send + 'static,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    handle_with_schemas_and_storage_reader(req, schemas, MockStorageReader::default(), f).await
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
