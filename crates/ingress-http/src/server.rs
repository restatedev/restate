// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::handler::Handler;
use codederror::CodedError;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use restate_core::{cancellation_watcher, task_center, TaskKind};
use restate_types::config::IngressOptions;
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tower::{ServiceBuilder, ServiceExt};
use tower_http::cors::CorsLayer;
use tower_http::normalize_path::NormalizePathLayer;
use tracing::{info, warn};

pub type StartSignal = oneshot::Receiver<SocketAddr>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressServerError {
    #[error(
        "failed binding to address '{address}' specified in 'worker.ingress_http.bind_address'"
    )]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("error while running ingress http server: {0}")]
    #[code(unknown)]
    Running(#[from] hyper::Error),
}

pub struct HyperServerIngress<Schemas, Dispatcher> {
    listening_addr: SocketAddr,
    concurrency_limit: usize,

    // Parameters to build the layers
    schemas: Live<Schemas>,
    dispatcher: Dispatcher,

    // Signals
    start_signal_tx: oneshot::Sender<SocketAddr>,
}

impl<Schemas, Dispatcher> HyperServerIngress<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub fn from_options(
        ingress_options: &IngressOptions,
        dispatcher: Dispatcher,
        schemas: Live<Schemas>,
    ) -> HyperServerIngress<Schemas, Dispatcher> {
        crate::metric_definitions::describe_metrics();
        let (hyper_ingress_server, _) = HyperServerIngress::new(
            ingress_options.bind_address,
            ingress_options.concurrent_api_requests_limit(),
            schemas,
            dispatcher,
        );

        hyper_ingress_server
    }
}

impl<Schemas, Dispatcher> HyperServerIngress<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        listening_addr: SocketAddr,
        concurrency_limit: usize,
        schemas: Live<Schemas>,
        dispatcher: Dispatcher,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            schemas,
            dispatcher,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            schemas,
            dispatcher,
            start_signal_tx,
        } = self;

        // We create a TcpListener and bind it
        let listener =
            TcpListener::bind(listening_addr)
                .await
                .map_err(|err| IngressServerError::Binding {
                    address: listening_addr,
                    source: err,
                })?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| IngressServerError::Binding {
                address: listening_addr,
                source: err,
            })?;

        // Prepare the handler
        let service = ServiceBuilder::new()
            .layer(NormalizePathLayer::trim_trailing_slash())
            .layer(layers::load_shed::LoadShedLayer::new(concurrency_limit))
            .layer(CorsLayer::very_permissive())
            .layer(layers::tracing_context_extractor::HttpTraceContextExtractorLayer)
            .service(Handler::new(schemas, dispatcher));

        info!(
            net.host.addr = %local_addr.ip(),
            net.host.port = %local_addr.port(),
            "Ingress HTTP listening"
        );

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // Send start signal
        let _ = start_signal_tx.send(local_addr);

        // We start a loop to continuously accept incoming connections
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, remote_peer) = res?;
                    Self::handle_connection(stream, remote_peer, service.clone())?;
                }
                  _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn handle_connection<T, F>(
        stream: TcpStream,
        remote_peer: SocketAddr,
        handler: T,
    ) -> anyhow::Result<()>
    where
        F: Send,
        T: tower::Service<
                Request<Incoming>,
                Response = Response<Full<Bytes>>,
                Error = Infallible,
                Future = F,
            > + Clone
            + Send
            + 'static,
    {
        let connect_info = ConnectInfo::new(remote_peer);
        let io = TokioIo::new(stream);
        let handler = hyper_util::service::TowerToHyperService::new(handler.map_request(
            move |mut req: Request<Incoming>| {
                req.extensions_mut().insert(connect_info);
                req
            },
        ));

        // Spawn a tokio task to serve the connection
        task_center().spawn(TaskKind::Ingress, "ingress", None, async move {
            let shutdown = cancellation_watcher();
            let auto_connection = auto::Builder::new(TaskCenterExecutor);
            let serve_connection_fut = auto_connection.serve_connection(io, handler);

            tokio::select! {
                res = serve_connection_fut => {
                    if let Err(err) = res {
                        warn!("Error when serving the connection: {:?}", err);
                    }
                }
                _ = shutdown => {}
            }
            Ok(())
        })?;

        Ok(())
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct TaskCenterExecutor;

impl<Fut> hyper::rt::Executor<Fut> for TaskCenterExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        let _ = task_center().spawn(TaskKind::Ingress, "ingress", None, async {
            fut.await;
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::mocks::*;
    use super::*;

    use http_body_util::BodyExt;
    use http_body_util::Full;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv};
    use restate_test_util::assert_eq;
    use restate_types::invocation::InvocationTarget;
    use restate_types::net::partition_processor::IngressResponseResult;
    use serde::{Deserialize, Serialize};
    use std::future::ready;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tracing_test::traced_test;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct GreetingRequest {
        pub person: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct GreetingResponse {
        pub greeting: String,
    }

    #[tokio::test]
    #[traced_test]
    async fn test_http_post() {
        let mut mock_dispatcher = MockRequestDispatcher::default();
        mock_dispatcher
            .expect_append_invocation_and_wait_output()
            .once()
            .return_once(|service_invocation| {
                assert_eq!(
                    service_invocation.invocation_target.service_name(),
                    "greeter.Greeter"
                );
                assert_eq!(service_invocation.invocation_target.handler_name(), "greet");

                let greeting_req: GreetingRequest =
                    serde_json::from_slice(&service_invocation.argument).unwrap();
                assert_eq!(&greeting_req.person, "Francesco");

                Box::pin(ready(Ok(InvocationOutput {
                    request_id: Default::default(),
                    invocation_id: Some(service_invocation.invocation_id),
                    completion_expiry_time: None,
                    response: IngressResponseResult::Success(
                        InvocationTarget::service("greeter.Greeter", "greet"),
                        serde_json::to_vec(&GreetingResponse {
                            greeting: "Igal".to_string(),
                        })
                        .unwrap()
                        .into(),
                    ),
                })))
            });

        let (address, handle) = bootstrap_test(mock_dispatcher).await;

        // Send the request
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build_http::<Full<Bytes>>();
        let http_response = client
            .request(
                http::Request::post(format!("http://{address}/greeter.Greeter/greet"))
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Full::new(
                        serde_json::to_vec(&GreetingRequest {
                            person: "Francesco".to_string(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Read the http_response_future
        assert_eq!(http_response.status(), http::StatusCode::OK);
        let (_, response_body) = http_response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");

        handle.close().await;
    }

    async fn bootstrap_test(
        mock_request_dispatcher: MockRequestDispatcher,
    ) -> (SocketAddr, TestHandle) {
        let node_env = TestCoreEnv::create_with_single_node(1, 1).await;

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            Live::from_value(mock_schemas()),
            Arc::new(mock_request_dispatcher),
        );
        node_env
            .tc
            .spawn(TaskKind::SystemService, "ingress", None, ingress.run())
            .unwrap();

        // Wait server to start
        let address = start_signal.await.unwrap();

        (address, TestHandle(node_env.tc))
    }

    struct TestHandle(TaskCenter);

    impl TestHandle {
        async fn close(self) {
            self.0.cancel_tasks(None, None).await;
        }
    }
}
