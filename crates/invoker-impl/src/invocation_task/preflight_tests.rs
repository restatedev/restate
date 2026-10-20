// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preflight attempts against a fake relay: an h2c server speaking the preflight protocol.

use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use http::{Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use restate_memory::LocalMemoryPool;
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol_v4::message_codec::{Decoder, Encoder, Message, proto};
use restate_service_protocol_v4::preflight;
use restate_types::LimitKey;
use restate_types::config::{
    Configuration, ServiceClientOptions, SingleEndpointMode, set_current_config,
};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};
use restate_types::journal_v2::raw::RawCommandSpecificMetadata;
use restate_types::live::Live;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_util_string::RestateString;
use restate_worker_api::invoker::invocation_reader::InvocationReader;

use super::{InvocationTask, InvocationTaskOutputInner};
use crate::error::{InvokerError, RequestedErrorBehavior};
use crate::test_util::{EmptyStorageReader, PinnedStorageReader};

const PREFLIGHT_CONTENT_TYPE: &str = "application/vnd.restate.invocation.preflight.v1";

/// What the fake relay answers to the preflight request.
#[derive(Clone)]
enum RelayAnswer {
    /// The preflight response, then these frames, then EndMessage.
    Preflight(proto::PreflightResponseMessage, Vec<Message>),
    Error(proto::ErrorMessage),
    Status(StatusCode),
}

/// What the fake relay saw.
#[derive(Debug, Default, PartialEq)]
struct Observed {
    path: String,
    content_type: String,
    preflight_request: Option<proto::PreflightRequestMessage>,
    /// A StartMessage arrived after the preflight, decoded in the negotiated version.
    start_received: bool,
    /// The invoker closed the request body after the StartMessage (request/response).
    request_body_closed: bool,
}

/// What the invocation task reported to the invoker.
#[derive(Debug, PartialEq)]
enum Output {
    Pinned(PinnedDeployment, bool),
    ServerHeader(String),
    Closed,
    Failed(&'static str, RequestedErrorBehavior),
    /// A call or send: callee target, completion and journal retention.
    Call(InvocationTarget, Duration, Duration),
    Other,
}

type RelayBody = StreamBody<UnboundedReceiverStream<Result<Frame<Bytes>, Infallible>>>;

async fn next_message(body: &mut Incoming, decoder: &mut Decoder) -> Option<Message> {
    loop {
        if let Some((_, message)) = decoder.consume_next().unwrap() {
            return Some(message);
        }
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    decoder.push(data)
                }
            }
            _ => return None,
        }
    }
}

async fn relay_handler(
    req: Request<Incoming>,
    answer: RelayAnswer,
    observed_tx: oneshot::Sender<Observed>,
) -> Result<Response<RelayBody>, Infallible> {
    let (tx, rx) = mpsc::unbounded_channel();
    let body = StreamBody::new(UnboundedReceiverStream::new(rx));
    let mut observed = Observed {
        path: req.uri().path().to_owned(),
        content_type: req.headers()[http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .to_owned(),
        ..Default::default()
    };

    if let RelayAnswer::Status(status) = answer {
        let _ = observed_tx.send(observed);
        return Ok(Response::builder().status(status).body(body).unwrap());
    }

    tokio::spawn(async move {
        let mut request_body = req.into_body();
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V7,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );
        if let Some(Message::PreflightRequest(request)) =
            next_message(&mut request_body, &mut decoder).await
        {
            observed.preflight_request = Some(request);
        }

        match answer {
            RelayAnswer::Preflight(response, frames) => {
                let version =
                    ServiceProtocolVersion::try_from(response.protocol_version as i32).unwrap();
                let request_response =
                    response.protocol_type == i32::from(preflight::ProtocolType::RequestResponse);
                let _ = tx.send(Ok(Frame::data(
                    Encoder::new(version).encode(Message::PreflightResponse(response)),
                )));

                decoder.set_service_protocol_version(version);
                observed.start_received = matches!(
                    next_message(&mut request_body, &mut decoder).await,
                    Some(Message::Start(_))
                );
                if request_response {
                    observed.request_body_closed = next_message(&mut request_body, &mut decoder)
                        .await
                        .is_none();
                }
                for frame in frames {
                    let _ = tx.send(Ok(Frame::data(Encoder::new(version).encode(frame))));
                }
                let _ = tx.send(Ok(Frame::data(
                    Encoder::new(version).encode(Message::End(proto::EndMessage {})),
                )));
            }
            RelayAnswer::Error(error) => {
                let _ = tx.send(Ok(Frame::data(
                    Encoder::new(ServiceProtocolVersion::V7).encode(Message::Error(error)),
                )));
            }
            RelayAnswer::Status(_) => unreachable!(),
        }
        let _ = observed_tx.send(observed);
    });

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, PREFLIGHT_CONTENT_TYPE)
        .body(body)
        .unwrap())
}

/// Starts a fake relay serving a single preflight request, and configures the invoker to use
/// it as the single endpoint in preflight mode.
async fn start_fake_relay(answer: RelayAnswer) -> oneshot::Receiver<Observed> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (observed_tx, observed_rx) = oneshot::channel();
    let observed_tx = Arc::new(Mutex::new(Some(observed_tx)));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |req| {
                    let observed_tx = observed_tx.lock().unwrap().take().unwrap();
                    relay_handler(req, answer.clone(), observed_tx)
                }),
            )
            .await;
    });

    let mut config = Configuration::default();
    config.worker.invoker.single_endpoint_mode = SingleEndpointMode::Preflight;
    config.worker.invoker.single_endpoint_address =
        Box::new(format!("http://{address}/_/preflight/").parse().unwrap());
    set_current_config(config);

    observed_rx
}

/// Runs one attempt of `invocation_target` against the fake relay.
async fn run_attempt<IR: InvocationReader + Clone>(
    invocation_target: InvocationTarget,
    invocation_id: InvocationId,
    reader: IR,
) -> Vec<Output> {
    let (invoker_tx, mut invoker_rx) = mpsc::unbounded_channel();
    let (_notifications_tx, notifications_rx) = mpsc::unbounded_channel();
    let task = InvocationTask::new(
        ServiceClient::from_options(&ServiceClientOptions::default(), AssumeRoleCacheMode::None)
            .unwrap(),
        invocation_id,
        0,
        invocation_target,
        Duration::from_secs(60),
        Duration::from_secs(60),
        1024,
        NonZeroUsize::MAX,
        NonZeroUsize::MAX,
        0,
        Live::from_value(crate::tests::MockSchemas::default()),
        invoker_tx,
        notifications_rx,
        None,
        LimitKey::None,
        None,
        100,
    );
    task.run(reader, LocalMemoryPool::unlimited()).await;

    let mut outputs = vec![];
    while let Ok(output) = invoker_rx.try_recv() {
        outputs.push(match output.inner {
            InvocationTaskOutputInner::PinnedDeployment(pin, has_changed) => {
                Output::Pinned(pin, has_changed)
            }
            InvocationTaskOutputInner::ServerHeaderReceived(header) => Output::ServerHeader(header),
            InvocationTaskOutputInner::Closed => Output::Closed,
            InvocationTaskOutputInner::NewCommand { command, .. } => {
                match command.command_specific_metadata() {
                    RawCommandSpecificMetadata::CallOrSend(call) => Output::Call(
                        call.invocation_target.clone(),
                        call.completion_retention_duration,
                        call.journal_retention_duration,
                    ),
                    RawCommandSpecificMetadata::None => Output::Other,
                }
            }
            InvocationTaskOutputInner::Failed(err, _) => Output::Failed(
                match err {
                    InvokerError::SdkV2(_) => "SdkV2",
                    InvokerError::RateLimited { .. } => "RateLimited",
                    InvokerError::BadPreflightResponse(_) => "BadPreflightResponse",
                    _ => "other",
                },
                err.requested_error_behavior(),
            ),
            _ => Output::Other,
        });
    }
    outputs
}

fn preflight_response(
    deployment_id: DeploymentId,
    protocol_version: u32,
    protocol_type: preflight::ProtocolType,
) -> RelayAnswer {
    RelayAnswer::Preflight(
        proto::PreflightResponseMessage {
            deployment_id: deployment_id.to_string(),
            protocol_version,
            protocol_type: protocol_type.into(),
            sdk_version: Some("restate-sdk-test/1.0".to_owned()),
        },
        vec![],
    )
}

#[restate_core::test]
async fn first_attempt_negotiates_and_pins_the_deployment() {
    let deployment_id = DeploymentId::new();
    let observed = start_fake_relay(preflight_response(
        deployment_id,
        6,
        preflight::ProtocolType::BidiStream,
    ))
    .await;
    let target = InvocationTarget::service("greeter", "greet");
    let invocation_id = InvocationId::mock_generate(&target);

    let outputs = run_attempt(target, invocation_id, EmptyStorageReader).await;

    assert_eq!(
        (observed.await.unwrap(), outputs),
        (
            Observed {
                path: "/_/preflight/invoke/greeter/greet".to_owned(),
                content_type: PREFLIGHT_CONTENT_TYPE.to_owned(),
                preflight_request: Some(proto::PreflightRequestMessage {
                    service_name: "greeter".to_owned(),
                    handler_name: "greet".to_owned(),
                    invocation_id: invocation_id.to_string(),
                    min_protocol_version: 5,
                    max_protocol_version: 7,
                    ..Default::default()
                }),
                start_received: true,
                request_body_closed: false,
            },
            vec![
                Output::Pinned(
                    PinnedDeployment::new(deployment_id, ServiceProtocolVersion::V6),
                    true
                ),
                Output::ServerHeader("restate-sdk-test/1.0".to_owned()),
                Output::Closed,
            ]
        )
    );
}

#[restate_core::test]
async fn pinned_attempt_sends_the_pin_and_runs_request_response() {
    let deployment_id = DeploymentId::new();
    let pin = PinnedDeployment::new(deployment_id, ServiceProtocolVersion::V5);
    let observed = start_fake_relay(preflight_response(
        deployment_id,
        5,
        preflight::ProtocolType::RequestResponse,
    ))
    .await;
    let target = InvocationTarget::virtual_object(
        "counter",
        "my-key",
        "add",
        VirtualObjectHandlerType::Exclusive,
    )
    .with_scope(Some(restate_types::Scope::try_new("tenant").unwrap()));
    let invocation_id = InvocationId::mock_generate(&target);

    let outputs = run_attempt(target, invocation_id, PinnedStorageReader(pin.clone())).await;

    assert_eq!(
        (observed.await.unwrap(), outputs),
        (
            Observed {
                path: "/_/preflight/invoke/counter/add".to_owned(),
                content_type: PREFLIGHT_CONTENT_TYPE.to_owned(),
                preflight_request: Some(proto::PreflightRequestMessage {
                    service_name: "counter".to_owned(),
                    handler_name: "add".to_owned(),
                    key: "my-key".to_owned(),
                    scope: Some("tenant".to_owned()),
                    invocation_id: invocation_id.to_string(),
                    min_protocol_version: 5,
                    max_protocol_version: 7,
                    pinned_deployment_id: Some(deployment_id.to_string()),
                    pinned_protocol_version: Some(5),
                    ..Default::default()
                }),
                start_received: true,
                request_body_closed: true,
            },
            vec![
                Output::Pinned(pin, false),
                Output::ServerHeader("restate-sdk-test/1.0".to_owned()),
                Output::Closed,
            ]
        )
    );
}

#[restate_core::test]
async fn relay_error_message_is_handled_like_an_sdk_error() {
    let _observed = start_fake_relay(RelayAnswer::Error(proto::ErrorMessage {
        code: 503,
        message: "relay: registry unavailable".to_owned(),
        behavior: proto::ErrorBehavior::Retry.into(),
        next_retry_delay: Some(1000),
        ..Default::default()
    }))
    .await;
    let target = InvocationTarget::service("greeter", "greet");

    let outputs = run_attempt(
        target.clone(),
        InvocationId::mock_generate(&target),
        EmptyStorageReader,
    )
    .await;

    assert_eq!(
        outputs,
        vec![Output::Failed(
            "SdkV2",
            RequestedErrorBehavior::RetryWithIntervalOverride(Duration::from_secs(1))
        )]
    );
}

#[restate_core::test]
async fn relay_refusing_the_request_is_a_non_2xx_response() {
    let _observed = start_fake_relay(RelayAnswer::Status(StatusCode::SERVICE_UNAVAILABLE)).await;
    let target = InvocationTarget::service("greeter", "greet");

    let outputs = run_attempt(
        target.clone(),
        InvocationId::mock_generate(&target),
        EmptyStorageReader,
    )
    .await;

    assert_eq!(
        outputs,
        vec![Output::Failed("RateLimited", RequestedErrorBehavior::Retry)]
    );
}

#[restate_core::test]
async fn relay_ignoring_the_pin_fails_the_attempt() {
    let observed = start_fake_relay(preflight_response(
        DeploymentId::new(),
        5,
        preflight::ProtocolType::BidiStream,
    ))
    .await;
    let target = InvocationTarget::service("greeter", "greet");
    let pin = PinnedDeployment::new(DeploymentId::new(), ServiceProtocolVersion::V5);

    let outputs = run_attempt(
        target.clone(),
        InvocationId::mock_generate(&target),
        PinnedStorageReader(pin),
    )
    .await;
    drop(observed);

    assert_eq!(
        outputs,
        vec![Output::Failed(
            "BadPreflightResponse",
            RequestedErrorBehavior::Retry
        )]
    );
}

#[restate_core::test]
async fn call_takes_the_relay_fields_appended_to_the_sdk_bytes() {
    // The SDK's command, then the relay-only fields appended by the relay: the last occurrence
    // of a field wins, the SDK's bytes are untouched.
    let mut call = BytesMut::new();
    call.put(
        proto::OneWayCallCommandMessage {
            service_name: "checkout".to_owned(),
            handler_name: "run".to_owned(),
            idempotency_key: Some("run-1".to_owned()),
            scope: Some("sdk-scope".to_owned()),
            relay_target_type: proto::RelayTargetType::RelayTargetVirtualObjectShared.into(),
            ..Default::default()
        }
        .encode_to_vec()
        .as_slice(),
    );
    call.put(
        proto::OneWayCallCommandMessage {
            relay_target_type: proto::RelayTargetType::RelayTargetService.into(),
            relay_effective_scope: Some("tenant".to_owned()),
            relay_completion_retention_millis: Some(60_000),
            relay_journal_retention_millis: Some(0),
            ..Default::default()
        }
        .encode_to_vec()
        .as_slice(),
    );
    let RelayAnswer::Preflight(response, _) =
        preflight_response(DeploymentId::new(), 7, preflight::ProtocolType::BidiStream)
    else {
        unreachable!()
    };
    let _observed = start_fake_relay(RelayAnswer::Preflight(
        response,
        vec![Message::OneWayCallCommand(call.freeze())],
    ))
    .await;
    let target = InvocationTarget::service("cron", "wake");

    let outputs = run_attempt(
        target.clone(),
        InvocationId::mock_generate(&target),
        EmptyStorageReader,
    )
    .await;

    assert_eq!(
        outputs
            .into_iter()
            .find(|output| matches!(output, Output::Call(..))),
        Some(Output::Call(
            InvocationTarget::service("checkout", "run")
                .with_scope(Some(restate_types::Scope::try_new("tenant").unwrap())),
            Duration::from_secs(60),
            Duration::ZERO
        ))
    );
}
