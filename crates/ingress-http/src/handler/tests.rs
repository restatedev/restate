// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::FutureExt;
use http::StatusCode;
use http::{HeaderValue, Method, Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use tower::ServiceExt;
use tracing_test::traced_test;

use restate_core::TestCoreEnv;
use restate_test_util::{assert, assert_eq};
use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId, WithInvocationId};
use restate_types::invocation::client::{
    AttachInvocationResponse, GetInvocationOutputResponse, InvocationOutput,
    InvocationOutputResponse, SubmittedInvocationNotification,
};
use restate_types::invocation::{
    InvocationQuery, InvocationTarget, InvocationTargetType, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::live::Live;
use restate_types::net::address::SocketAddress;
use restate_types::schema::invocation_target::{
    InputContentType, InputRules, InputValidationRule, InvocationTargetMetadata,
    OutputContentTypeRule, OutputRules,
};

use super::ConnectInfo;
use super::Handler;
use super::health::HealthResponse;
use super::mocks::*;
use super::service_handler::*;
use crate::MockRequestDispatcher;
use crate::handler::responses::X_RESTATE_ID;

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            Box::pin(ready(Ok(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_request.invocation_id()),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (parts, response_body) = response.into_parts();
    assert!(parts.headers.contains_key(X_RESTATE_ID));
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
#[traced_test]
async fn call_service_with_get() {
    let req = hyper::Request::builder()
        .uri("http://localhost/greeter.Greeter/greet")
        .method(Method::GET)
        .body(Empty::<Bytes>::default())
        .unwrap();

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            assert!(invocation_request.body.is_empty());

            ready(Ok(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(InvocationId::mock_random()),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    invocation_request.header.target.clone(),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            }))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(
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
        mock_dispatcher,
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.GreeterObject"
            );
            assert_eq!(invocation_request.header.target.key().unwrap(), &"my-key");
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            Box::pin(ready(Ok(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(InvocationId::mock_random()),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    invocation_request.header.target.clone(),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_send()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            ready(Ok(SubmittedInvocationNotification {
                request_id: Default::default(),
                execution_time: None,
                is_new_invocation: true,
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_send()
        .return_once(|invocation_request| {
            // Get the function invocation and assert on it
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");
            assert!(invocation_request.header.execution_time.is_some());

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            ready(Ok(SubmittedInvocationNotification {
                request_id: Default::default(),
                execution_time: None,
                is_new_invocation: true,
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_send()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.GreeterObject"
            );
            assert_eq!(invocation_request.header.target.key().unwrap(), &"my-key");
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            ready(Ok(SubmittedInvocationNotification {
                request_id: Default::default(),
                execution_time: None,
                is_new_invocation: true,
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            assert_eq!(
                invocation_request.header.idempotency_key,
                Some(ByteString::from_static("123456"))
            );
            assert_eq!(
                invocation_request.header.completion_retention_duration(),
                Duration::from_secs(60 * 60 * 24)
            );

            ready(Ok(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(InvocationId::mock_random()),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    invocation_request.header.target.clone(),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_send()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            assert_eq!(
                invocation_request.header.idempotency_key,
                Some(ByteString::from_static("123456"))
            );
            assert_eq!(
                invocation_request.header.completion_retention_duration(),
                Duration::from_secs(60 * 60 * 24)
            );

            ready(Ok(SubmittedInvocationNotification {
                request_id: Default::default(),
                execution_time: None,
                is_new_invocation: true,
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
}

#[restate_core::test]
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

    let expected_invocation_id = InvocationId::generate(
        &InvocationTarget::service("greeter.Greeter", "greet"),
        Some(&ByteString::from_static("123456")),
    );

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_send()
        .return_once(|invocation_request| {
            assert_eq!(
                invocation_request.header.target.service_name(),
                "greeter.Greeter"
            );
            assert_eq!(invocation_request.header.target.handler_name(), "greet");

            let greeting_req: GreetingRequest =
                serde_json::from_slice(&invocation_request.body).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            assert_eq!(
                invocation_request.header.idempotency_key,
                Some(ByteString::from_static("123456"))
            );
            assert_eq!(
                invocation_request.header.completion_retention_duration(),
                Duration::from_secs(60 * 60 * 24)
            );

            ready(Ok(SubmittedInvocationNotification {
                request_id: Default::default(),
                execution_time: None,
                is_new_invocation: true,
            }))
            .boxed()
        });

    let response = handle(req, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let send_response: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(send_response.invocation_id, expected_invocation_id);
}

#[restate_core::test]
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
            "http://localhost/restate/invocation/{invocation_id}/attach"
        ))
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_attach_invocation()
        .return_once(move |actual_invocation_query| {
            assert_eq!(
                InvocationQuery::Invocation(invocation_id),
                actual_invocation_query
            );

            ready(Ok(AttachInvocationResponse::Ready(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_id),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(req, mock_schemas, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_attach_invocation()
        .return_once(move |actual_invocation_query| {
            assert_eq!(
                InvocationQuery::IdempotencyId(IdempotencyId::new(
                    "greeter.Greeter".into(),
                    None,
                    "greet".into(),
                    "myid".into()
                )),
                actual_invocation_query
            );

            ready(Ok(AttachInvocationResponse::Ready(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_id),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(req, mock_schemas, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_attach_invocation()
        .return_once(move |actual_invocation_query| {
            assert_eq!(
                InvocationQuery::IdempotencyId(IdempotencyId::new(
                    "greeter.Greeter".into(),
                    Some("mygreet".into()),
                    "greet".into(),
                    "myid".into()
                )),
                actual_invocation_query
            );

            ready(Ok(AttachInvocationResponse::Ready(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_id),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    InvocationTarget::virtual_object(
                        "greeter.Greeter",
                        "mygreet",
                        "greet",
                        VirtualObjectHandlerType::Exclusive,
                    ),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(req, mock_schemas, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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
            "http://localhost/restate/invocation/{invocation_id}/output"
        ))
        .method(Method::GET)
        .header("content-type", "application/json")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_get_invocation_output()
        .return_once(move |actual_invocation_query| {
            assert_eq!(
                InvocationQuery::Invocation(invocation_id),
                actual_invocation_query
            );

            ready(Ok(GetInvocationOutputResponse::Ready(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_id),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    InvocationTarget::service("greeter.Greeter", "greet"),
                    serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into(),
                ),
            })))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(req, mock_schemas, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
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

    let mut mock_dispatcher = MockRequestDispatcher::default();
    mock_dispatcher
        .expect_get_invocation_output()
        .return_once(|actual_invocation_query| {
            assert_eq!(
                InvocationQuery::Workflow(service_id.clone()),
                actual_invocation_query
            );

            ready(Ok(GetInvocationOutputResponse::Ready(InvocationOutput {
                request_id: Default::default(),
                invocation_id: None,
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
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
            })))
            .boxed()
        });

    let response = handle_with_schemas_and_dispatcher(req, mock_schemas, mock_dispatcher).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response_value.greeting, "Igal");
}

#[restate_core::test]
#[traced_test]
async fn bad_path_service() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[restate_core::test]
#[traced_test]
async fn bad_path_virtual_object() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/sendbla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = handle(
        hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/send/bla")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[restate_core::test]
#[traced_test]
async fn unknown_service() {
    let response = handle(
        hyper::Request::get(
            "http://localhost/whatevernotexistingservice/whatevernotexistinghandler",
        )
        .body(Empty::<Bytes>::default())
        .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[restate_core::test]
#[traced_test]
async fn unknown_handler() {
    let response = handle(
        hyper::Request::get("http://localhost/greeter.Greeter/whatevernotexistinghandler")
            .body(Empty::<Bytes>::default())
            .unwrap(),
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[restate_core::test]
#[traced_test]
async fn private_service() {
    let response = handle_with_schemas_and_dispatcher(
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
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[restate_core::test]
#[traced_test]
async fn invalid_input() {
    let response = handle_with_schemas_and_dispatcher(
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
        MockRequestDispatcher::default(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[restate_core::test]
#[traced_test]
async fn set_custom_content_type_on_response() {
    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata {
            output_rules: OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: HeaderValue::from_static("application/cbor"),
                    set_content_type_if_empty: false,
                    has_json_schema: false,
                },
                json_schema: None,
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
    let response = handle_with_schemas_and_dispatcher(
        req.clone(),
        mock_schemas.clone(),
        expect_invocation_and_reply_with_empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("content-type").is_none());

    // Case when the response is non-empty
    let response = handle_with_schemas_and_dispatcher(
        req.clone(),
        mock_schemas,
        expect_invocation_and_reply_with_non_empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/cbor"
    );
}

#[restate_core::test]
#[traced_test]
async fn set_custom_content_type_on_empty_response() {
    let mock_schemas = MockSchemas::default().with_service_and_target(
        "greeter.Greeter",
        "greet",
        InvocationTargetMetadata {
            output_rules: OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: HeaderValue::from_static("application/protobuf"),
                    set_content_type_if_empty: true,
                    has_json_schema: false,
                },
                json_schema: None,
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
    let response = handle_with_schemas_and_dispatcher(
        req.clone(),
        mock_schemas.clone(),
        expect_invocation_and_reply_with_empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/protobuf"
    );

    // Case when the response is non-empty
    let response = handle_with_schemas_and_dispatcher(
        req.clone(),
        mock_schemas,
        expect_invocation_and_reply_with_non_empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/protobuf"
    );
}

#[restate_core::test]
#[traced_test]
async fn health() {
    let req = hyper::Request::builder()
        .uri("http://localhost/restate/health")
        .method(Method::GET)
        .body(Empty::<Bytes>::default())
        .unwrap();

    let response = handle(req, MockRequestDispatcher::default()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let (_, response_body) = response.into_parts();
    let response_bytes = response_body.collect().await.unwrap().to_bytes();
    let _: HealthResponse = serde_json::from_slice(&response_bytes).unwrap();
}

fn expect_invocation_and_reply_with_empty() -> MockRequestDispatcher {
    let mut mock_dispatcher = MockRequestDispatcher::new();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            ready(Ok(InvocationOutput {
                request_id: Default::default(),
                completion_expiry_time: None,
                invocation_id: Some(invocation_request.invocation_id()),
                response: InvocationOutputResponse::Success(
                    invocation_request.header.target.clone(),
                    Bytes::new(),
                ),
            }))
            .boxed()
        });

    mock_dispatcher
}

fn expect_invocation_and_reply_with_non_empty() -> MockRequestDispatcher {
    let mut mock_dispatcher = MockRequestDispatcher::new();
    mock_dispatcher
        .expect_call()
        .return_once(|invocation_request| {
            ready(Ok(InvocationOutput {
                request_id: Default::default(),
                invocation_id: Some(invocation_request.invocation_id()),
                completion_expiry_time: None,
                response: InvocationOutputResponse::Success(
                    invocation_request.header.target.clone(),
                    Bytes::from_static(b"123"),
                ),
            }))
            .boxed()
        });

    mock_dispatcher
}

pub async fn handle_with_schemas_and_dispatcher<B: http_body::Body + Send + 'static>(
    mut req: Request<B>,
    schemas: MockSchemas,
    dispatcher: MockRequestDispatcher,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;

    req.extensions_mut()
        .insert(ConnectInfo::new(SocketAddress::Anonymous));
    req.extensions_mut().insert(opentelemetry::Context::new());

    let handler_fut = Handler::new(Live::from_value(schemas), Arc::new(dispatcher)).oneshot(req);

    handler_fut.await.unwrap()
}

pub async fn handle<B: http_body::Body + Send + 'static>(
    req: Request<B>,
    mock_request_dispatcher: MockRequestDispatcher,
) -> Response<Full<Bytes>>
where
    <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    <B as http_body::Body>::Data: Send + Sync + 'static,
{
    handle_with_schemas_and_dispatcher(req, mock_schemas(), mock_request_dispatcher).await
}
