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

use codederror::CodedError;
use hyper::server::conn::AddrStream;
use hyper::service::make_service_fn;
use hyper::service::service_fn;
use restate_core::cancellation_watcher;
use restate_ingress_dispatcher::IngressRequestSender;
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::key::KeyExtractor;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tower::Service;
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
    json: JsonOptions,
    schemas: Schemas,
    request_tx: IngressRequestSender,

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
        schemas: Schemas,
        request_tx: IngressRequestSender,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            json,
            schemas,
            request_tx,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            json,
            schemas,
            request_tx,
            start_signal_tx,
        } = self;

        let server_builder = hyper::Server::try_bind(&listening_addr).map_err(|err| {
            IngressServerError::Binding {
                address: listening_addr,
                source: err,
            }
        })?;

        let global_concurrency_limit_semaphore = Arc::new(Semaphore::new(concurrency_limit));

        let service_builder = ServiceBuilder::new()
            .layer(CorsLayer::very_permissive())
            .service(handler::Handler::new(
                json,
                schemas,
                request_tx,
                global_concurrency_limit_semaphore,
            ));

        let make_svc = make_service_fn(|socket: &AddrStream| {
            // Extract remote address information and add to request extensions.
            let connect_info = ConnectInfo::new(socket);
            let mut inner_svc = service_builder.clone();
            let outer_svc = service_fn(move |mut req: http::Request<hyper::Body>| {
                req.extensions_mut().insert(connect_info);
                inner_svc.call(req)
            });

            async { Ok::<_, Infallible>(outer_svc) }
        });

        let server = server_builder.serve(make_svc);

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Ingress gRPC/gRPC-web/Connect listening"
        );

        // future completion does not affect endpoint
        let _ = start_signal_tx.send(server.local_addr());

        Ok(server
            .with_graceful_shutdown(cancellation_watcher())
            .await
            .map_err(IngressServerError::Running)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use bytes::Bytes;
    use http::header::CONTENT_TYPE;
    use http::StatusCode;
    use hyper::Body;
    use prost::Message;
    use restate_core::TestCoreEnv;
    use restate_core::{TaskCenter, TaskKind};
    use serde_json::json;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tonic::metadata::{AsciiMetadataValue, MetadataKey};
    use tonic::Request;

    use restate_ingress_dispatcher::{IdempotencyMode, IngressRequest};
    use restate_test_util::assert_eq;

    use crate::mocks::*;

    #[test(tokio::test)]
    async fn test_http_connect_call() {
        let (address, input, handle) = bootstrap_test().await;
        let process_fut = tokio::spawn(async move {
            // Get the function invocation and assert on it
            let (fid, method_name, mut argument, _, _, response_tx) =
                input.await.unwrap().unwrap().expect_invocation();
            assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            assert_eq!(method_name, "Greet");
            let greeting_req =
                restate_pb::mocks::greeter::GreetingRequest::decode(&mut argument).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            response_tx
                .send(
                    Ok(restate_pb::mocks::greeter::GreetingResponse {
                        greeting: "Igal".to_string(),
                    }
                    .encode_to_vec()
                    .into())
                    .into(),
                )
                .unwrap();
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

        // check that the input processing has completed
        process_fut.await.unwrap();

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
    async fn test_grpc_call() {
        let expected_greeting_response = restate_pb::mocks::greeter::GreetingResponse {
            greeting: "Igal".to_string(),
        };
        let encoded_greeting_response = Bytes::from(expected_greeting_response.encode_to_vec());

        let (address, input, handle) = bootstrap_test().await;
        let process_fut = tokio::spawn(async move {
            let (fid, method_name, mut argument, _, _, response_tx) =
                input.await.unwrap().unwrap().expect_invocation();
            assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            assert_eq!(method_name, "Greet");
            let greeting_req =
                restate_pb::mocks::greeter::GreetingRequest::decode(&mut argument).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");
            response_tx
                .send(Ok(encoded_greeting_response).into())
                .unwrap();
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

        // Check that the input processing completed
        process_fut.await.unwrap();

        // Read the http_response_future
        let greeting_res = response.into_inner();
        assert_eq!(greeting_res, expected_greeting_response);

        handle.close().await;
    }

    #[test(tokio::test)]
    async fn idempotency_key_parsing() {
        let expected_greeting_response = restate_pb::mocks::greeter::GreetingResponse {
            greeting: "Igal".to_string(),
        };
        let encoded_greeting_response = Bytes::from(expected_greeting_response.encode_to_vec());

        let (address, input, handle) = bootstrap_test().await;
        let process_fut = tokio::spawn(async move {
            let (fid, method_name, mut argument, _, idempotency_mode, response_tx) =
                input.await.unwrap().unwrap().expect_invocation();
            assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            assert_eq!(method_name, "Greet");
            let greeting_req =
                restate_pb::mocks::greeter::GreetingRequest::decode(&mut argument).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");
            assert_eq!(
                idempotency_mode,
                IdempotencyMode::key(Bytes::from_static(b"123456"), None)
            );
            response_tx
                .send(Ok(encoded_greeting_response).into())
                .unwrap();
        });

        let mut client = restate_pb::mocks::greeter::greeter_client::GreeterClient::connect(
            format!("http://{address}"),
        )
        .await
        .unwrap();

        let mut request = Request::new(restate_pb::mocks::greeter::GreetingRequest {
            person: "Francesco".to_string(),
        });
        request.metadata_mut().insert(
            MetadataKey::from_static("idempotency-key"),
            AsciiMetadataValue::from_static("123456"),
        );

        let response = client.greet(request).await.unwrap();

        // Check that the input processing completed
        process_fut.await.unwrap();

        // Read the http_response_future
        let greeting_res = response.into_inner();
        assert_eq!(greeting_res, expected_greeting_response);

        handle.close().await;
    }

    async fn bootstrap_test() -> (SocketAddr, JoinHandle<Option<IngressRequest>>, TestHandle) {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;
        let (ingress_request_tx, mut ingress_request_rx) = mpsc::unbounded_channel();

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            JsonOptions::default(),
            test_schemas(),
            ingress_request_tx,
        );
        tc.spawn(TaskKind::SystemService, "ingress", None, ingress.run())
            .unwrap();

        // Mock the service invocation receiver
        let input = tokio::spawn(async move { ingress_request_rx.recv().await });

        // Wait server to start
        let address = start_signal.await.unwrap();

        (address, input, TestHandle(tc))
    }

    struct TestHandle(TaskCenter);

    impl TestHandle {
        async fn close(self) {
            self.0.cancel_tasks(None, None).await;
        }
    }
}
