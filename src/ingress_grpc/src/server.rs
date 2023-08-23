// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::options::JsonOptions;
use super::*;

use crate::dispatcher::DispatcherCommandSender;
use codederror::CodedError;
use futures::FutureExt;
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::key::KeyExtractor;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_types::identifiers::IngressId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

pub type StartSignal = oneshot::Receiver<SocketAddr>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressServerError {
    #[error(
        "failed binding to address '{address}' specified in 'worker.ingress_grpc.bind_address'"
    )]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running ingress grpc server: {0}")]
    #[code(unknown)]
    Running(#[from] hyper::Error),
}

pub struct HyperServerIngress<Schemas> {
    listening_addr: SocketAddr,
    concurrency_limit: usize,

    // Parameters to build the layers
    ingress_id: IngressId,
    json: JsonOptions,
    schemas: Schemas,
    dispatcher_command_sender: DispatcherCommandSender,

    // Signals
    start_signal_tx: oneshot::Sender<SocketAddr>,
}

impl<Schemas, JsonDecoder, JsonEncoder> HyperServerIngress<Schemas>
where
    Schemas: JsonMapperResolver<JsonToProtobufMapper = JsonDecoder, ProtobufToJsonMapper = JsonEncoder>
        + ServiceMetadataResolver
        + KeyExtractor
        + ProtoSymbolResolver
        + Clone
        + Send
        + Sync
        + 'static,
    JsonDecoder: Send,
    JsonEncoder: Send,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        listening_addr: SocketAddr,
        concurrency_limit: usize,
        json: JsonOptions,
        ingress_id: IngressId,
        schemas: Schemas,
        dispatcher_command_sender: DispatcherCommandSender,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            json,
            ingress_id,
            schemas,
            dispatcher_command_sender,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), IngressServerError> {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            ingress_id,
            json,
            schemas,
            dispatcher_command_sender,
            start_signal_tx,
        } = self;

        let server_builder = hyper::Server::try_bind(&listening_addr).map_err(|err| {
            IngressServerError::Binding {
                address: listening_addr,
                source: err,
            }
        })?;

        let global_concurrency_limit_semaphore = Arc::new(Semaphore::new(concurrency_limit));

        let make_svc = Shared::new(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .service(handler::Handler::new(
                    ingress_id,
                    json,
                    schemas,
                    dispatcher_command_sender,
                    global_concurrency_limit_semaphore,
                )),
        );

        let server = server_builder.serve(make_svc);

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Ingress gRPC/gRPC-web/Connect listening"
        );

        // future completion does not affect endpoint
        let _ = start_signal_tx.send(server.local_addr());

        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(IngressServerError::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::dispatcher::InvocationOrResponse;
    use crate::mocks::*;
    use bytes::Bytes;
    use drain::Signal;
    use http::header::CONTENT_TYPE;
    use http::StatusCode;
    use hyper::Body;
    use prost::Message;
    use restate_service_protocol::awakeable_id::AwakeableIdentifier;
    use restate_test_util::{assert_eq, let_assert, test};
    use restate_types::identifiers::{FullInvocationId, InvocationId};
    use restate_types::invocation::MaybeFullInvocationId;
    use serde_json::json;
    use std::net::SocketAddr;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    #[test(tokio::test)]
    async fn test_http_connect_call() {
        let (address, cmd_fut, handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            response_tx
                .send(Ok(restate_pb::mocks::greeter::GreetingResponse {
                    greeting: "Igal".to_string(),
                }
                .encode_to_vec()
                .into()))
                .unwrap();
            service_invocation
        });

        // Send the request
        let json_payload = json!({"person": "Francesco"});
        let http_response = hyper::Client::new()
            .request(
                hyper::Request::post(format!("http://{address}/greeter.Greeter/Greet"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&json_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);

        // Get the function invocation and assert on it
        let_assert!(InvocationOrResponse::Invocation(mut service_invocation) = cmd_fut.await.unwrap());
        assert_eq!(
            service_invocation.fid.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            restate_pb::mocks::greeter::GreetingRequest::decode(&mut service_invocation.argument)
                .unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        // Read the http_response_future
        let (_, response_body) = http_response.into_parts();
        let response_bytes = hyper::body::to_bytes(response_body).await.unwrap();
        let response_json_value: serde_json::Value =
            serde_json::from_slice(&response_bytes).unwrap();
        assert_eq!(
            response_json_value
                .get("greeting")
                .unwrap()
                .as_str()
                .unwrap(),
            "Igal"
        );

        handle.close().await;
    }

    #[test(tokio::test)]
    async fn test_ingress_service_http_connect_call() {
        let (address, cmd_fut, handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            assert!(response_tx.is_closed());
            service_invocation
        });

        // Send the request
        let json_payload = json!({
            "service": "greeter.Greeter",
            "method": "Greet",
            "argument": {
                "person": "Francesco"
            }
        });
        let http_response = hyper::Client::new()
            .request(
                hyper::Request::post(format!("http://{address}/dev.restate.Ingress/Invoke"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&json_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);

        // Get the function invocation and assert on it
        let_assert!(InvocationOrResponse::Invocation(mut service_invocation) = cmd_fut.await.unwrap());
        assert_eq!(
            service_invocation.fid.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            restate_pb::mocks::greeter::GreetingRequest::decode(&mut service_invocation.argument)
                .unwrap();
        assert_eq!(&greeting_req.person, "Francesco");
        assert!(service_invocation.response_sink.is_none());

        // Read the http_response_future
        let (_, response_body) = http_response.into_parts();
        let response_bytes = hyper::body::to_bytes(response_body).await.unwrap();
        let response_json_value: serde_json::Value =
            serde_json::from_slice(&response_bytes).unwrap();
        let id: InvocationId = response_json_value
            .get("id")
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(id, InvocationId::from(service_invocation.fid));

        handle.close().await;
    }

    #[test(tokio::test)]
    async fn test_awakeables_service_http_connect_call() {
        let (address, cmd_fut, handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            assert!(response_tx.is_closed());
            service_invocation
        });

        let fid = FullInvocationId::mock_random();
        let awakeable_id = AwakeableIdentifier::new(fid.clone().into(), 2).encode();

        // Send the request
        let json_payload = json!({
            "id": awakeable_id,
            "json_result": {
                "my_result": false
            }
        });
        let http_response = hyper::Client::new()
            .request(
                hyper::Request::post(format!("http://{address}/dev.restate.Awakeables/Resolve"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&json_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);

        // Get the function invocation and assert on it
        let_assert!(InvocationOrResponse::Response(invocation_response) = cmd_fut.await.unwrap());
        assert_eq!(
            invocation_response.id,
            MaybeFullInvocationId::Partial(fid.into())
        );
        assert_eq!(invocation_response.entry_index, 2);
        assert_eq!(
            invocation_response.result,
            ResponseResult::Success(Bytes::from(
                json!({
                    "my_result": false
                })
                .to_string()
            ))
        );

        handle.close().await;
    }

    #[test(tokio::test)]
    async fn test_grpc_call() {
        let expected_greeting_response = restate_pb::mocks::greeter::GreetingResponse {
            greeting: "Igal".to_string(),
        };
        let encoded_greeting_response = Bytes::from(expected_greeting_response.encode_to_vec());

        let (address, cmd_fut, handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            response_tx.send(Ok(encoded_greeting_response)).unwrap();
            service_invocation
        });

        let mut client = restate_pb::mocks::greeter::greeter_client::GreeterClient::connect(
            format!("http://{address}"),
        )
        .await
        .unwrap();

        let response = client
            .greet(restate_pb::mocks::greeter::GreetingRequest {
                person: "Francesco".to_string(),
            })
            .await
            .unwrap();

        let_assert!(InvocationOrResponse::Invocation(mut service_invocation) = cmd_fut.await.unwrap());
        assert_eq!(
            service_invocation.fid.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            restate_pb::mocks::greeter::GreetingRequest::decode(&mut service_invocation.argument)
                .unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        // Read the http_response_future
        let greeting_res = response.into_inner();
        assert_eq!(greeting_res, expected_greeting_response);

        handle.close().await;
    }

    async fn bootstrap_test() -> (
        SocketAddr,
        JoinHandle<Option<Command<InvocationOrResponse, IngressResult>>>,
        TestHandle,
    ) {
        let (drain, watch) = drain::channel();
        let (dispatcher_command_tx, mut dispatcher_command_rx) = mpsc::unbounded_channel();

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            JsonOptions::default(),
            IngressId("0.0.0.0:0".parse().unwrap()),
            test_schemas(),
            dispatcher_command_tx,
        );
        let ingress_handle = tokio::spawn(ingress.run(watch));

        // Mock the service invocation receiver
        let cmd_fut = tokio::spawn(async move { dispatcher_command_rx.recv().await });

        // Wait server to start
        let address = start_signal.await.unwrap();

        (address, cmd_fut, TestHandle(drain, ingress_handle))
    }

    struct TestHandle(Signal, JoinHandle<Result<(), IngressServerError>>);

    impl TestHandle {
        async fn close(self) {
            self.0.drain().await;
            self.1.await.unwrap().unwrap();
        }
    }
}
