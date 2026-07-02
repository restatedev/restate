// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use codederror::CodedError;
use http::{Request, Response};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use metrics::counter;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::{ServiceBuilder, ServiceExt};
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::normalize_path::NormalizePathLayer;
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, info, info_span, instrument};

use restate_core::network::hyper_error_status;
use restate_core::{TaskCenter, TaskCenterFutureExt, cancellation_token, task_center};
use restate_types::config::IngressOptions;
use restate_types::errors::GenericError;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::net::address::{HttpIngressPort, ListenerPort, SocketAddress};
use restate_types::net::listener::Listeners;
use restate_types::protobuf::common::IngressStatus;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_util_time::DurationExt;

use super::*;
use crate::handler::Handler;
use crate::metric_definitions::{HTTP_CONNECTION_CREATED, HTTP_CONNECTION_DROPPED};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressServerError {
    #[error("error while running ingress http server: {0}")]
    #[code(unknown)]
    Running(#[from] hyper::Error),
}

pub struct HyperServerIngress<Schemas, Dispatcher> {
    listeners: Listeners<HttpIngressPort>,
    concurrency_limit: usize,
    request_size_limit: usize,
    http2_max_concurrent_streams: Option<NonZeroU32>,

    // Parameters to build the layers
    schemas: Live<Schemas>,
    dispatcher: Dispatcher,

    health: HealthStatus<IngressStatus>,
}

impl<Schemas, Dispatcher> HyperServerIngress<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub fn from_options(
        ingress_options: &IngressOptions,
        listeners: Listeners<HttpIngressPort>,
        dispatcher: Dispatcher,
        schemas: Live<Schemas>,
        health: HealthStatus<IngressStatus>,
    ) -> HyperServerIngress<Schemas, Dispatcher> {
        crate::metric_definitions::describe_metrics();
        HyperServerIngress::new(
            listeners,
            ingress_options.concurrent_api_requests_limit(),
            ingress_options.request_size_limit().get(),
            ingress_options.http2_max_concurrent_streams(),
            schemas,
            dispatcher,
            health,
        )
    }
}

impl<Schemas, Dispatcher> HyperServerIngress<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        listeners: Listeners<HttpIngressPort>,
        concurrency_limit: usize,
        request_size_limit: usize,
        http2_max_concurrent_streams: Option<NonZeroU32>,
        schemas: Live<Schemas>,
        dispatcher: Dispatcher,
        health: HealthStatus<IngressStatus>,
    ) -> Self {
        health.update(IngressStatus::StartingUp);

        Self {
            listeners,
            concurrency_limit,
            request_size_limit,
            http2_max_concurrent_streams,
            schemas,
            dispatcher,
            health,
        }
    }

    #[instrument(
        level = "error",
        name = "server",
        skip_all,
        fields(server_name = %HttpIngressPort::NAME, uds.path = tracing::field::Empty, server.address = tracing::field::Empty, server.port = tracing::field::Empty)
    )]
    pub async fn run(self) -> anyhow::Result<()> {
        let HyperServerIngress {
            mut listeners,
            concurrency_limit,
            request_size_limit,
            http2_max_concurrent_streams,
            schemas,
            dispatcher,
            health,
        } = self;

        // Prepare the handler
        let service = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &Request<_>| {
                        info_span!(
                            target: "restate_ingress_http::api",
                            "ingress-http-request",
                            http.version = ?request.version(),
                            http.request.method = %request.method(),
                            url.path = request.uri().path(),
                            url.query = request.uri().query().unwrap_or_default(),
                            url.scheme = request.uri().scheme_str().unwrap_or("http")
                        )
                    })
                    // Just log on response
                    .on_request(())
                    .on_eos(())
                    .on_body_chunk(())
                    .on_response(
                        move |response: &Response<_>, latency: Duration, span: &Span| {
                            debug!(
                                name: "access-log",
                                target: "restate_ingress_http::api",
                                parent: span,
                                { http.response.status_code = response.status().as_u16(), http.response.latency = %latency.friendly().to_seconds_span() },
                                "Replied"
                            )
                        },
                    )
                    .on_failure(
                        move |error: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                            match error {
                                ServerErrorsFailureClass::StatusCode(_) => {
                                    // No need to log it, on_response will log it already
                                }
                                ServerErrorsFailureClass::Error(error_string) => {
                                    debug!(
                                        name: "access-log",
                                        target: "restate_ingress_http::api",
                                        parent: span,
                                        { error.type = error_string, http.response.latency = %latency.friendly().to_seconds_span() },
                                        "Failed processing"
                                    )
                                }
                            }
                        },
                    ),
            )
            .layer(NormalizePathLayer::trim_trailing_slash())
            .layer(RequestBodyLimitLayer::new(request_size_limit))
            .layer(CorsLayer::very_permissive())
            .layer(layers::load_shed::LoadShedLayer::new(concurrency_limit))
            .layer(layers::tracing_context_extractor::HttpTraceContextExtractorLayer)
            .service(Handler::new(schemas, dispatcher));

        // todo(azmy): `CorsLayer` should sit above `RequestBodyLimitLayer` so CORS is applied
        // as early as possible. This is currently blocked because `CorsLayer` requires the
        // response body to implement `Default`, which `RequestBodyLimitLayer`'s body does not.
        // Tracked upstream in https://github.com/tower-rs/tower-http/pull/679  once merged,
        // move `CorsLayer` above `RequestBodyLimitLayer`.

        let shutdown = cancellation_token();

        if let Some(uds_path) = listeners.uds_address() {
            Span::current().record("uds.path", uds_path.display().to_string());
        }
        if let Some(socket_addr) = listeners.tcp_address() {
            Span::current().record("server.address", socket_addr.ip().to_string());
            Span::current().record("server.port", socket_addr.port());
        }
        info!("Ingress HTTP listening");
        health.update(IngressStatus::Ready);

        let mut inflight = TaskTracker::default();
        let force_shutdown = CancellationToken::new();

        // UDS
        loop {
            tokio::select! {
                res = listeners.accept() => {
                    let (stream, peer_addr) = res?;
                    match stream {
                        Either::Left(tcp_stream) => {
                            Self::handle_connection(
                                tcp_stream,
                                peer_addr,
                                service.clone(),
                                http2_max_concurrent_streams,
                                shutdown.child_token(),
                                force_shutdown.child_token(),
                                &mut inflight,
                            )?;
                        }
                        Either::Right(unix_stream) => {
                            Self::handle_connection(
                                unix_stream,
                                peer_addr,
                                service.clone(),
                                http2_max_concurrent_streams,
                                shutdown.child_token(),
                                force_shutdown.child_token(),
                                &mut inflight,
                            )?;
                        }

                    }
                }
                  _ = shutdown.cancelled() => {
                      info!("HTTP ingress shutdown requested");
                      drop(listeners);
                      inflight.close();
                      break;
                }
            }
        }

        // drain in-flight requests (give them some time to finish)
        match tokio::time::timeout(Duration::from_secs(5), inflight.wait()).await {
            Ok(()) => {
                info!("All in-flight HTTP ingress connections drained");
            }
            Err(_) => {
                info!(
                    in_flight_tasks = inflight.len(),
                    "HTTP Ingress drain timeout elapsed; cancelling in-flight work",
                );
                // The force token wraps every tracked future, so this drops remaining
                // connection/request work instead of just stopping the wait.
                force_shutdown.cancel();
                inflight.wait().await;
            }
        }
        return Ok(());
    }

    fn handle_connection<S, T, F, B>(
        stream: S,
        remote_peer: SocketAddress,
        handler: T,
        http2_max_concurrent_streams: Option<NonZeroU32>,
        drain: CancellationToken,
        force_shutdown: CancellationToken,
        inflight: &mut TaskTracker,
    ) -> anyhow::Result<()>
    where
        S: AsyncWrite + AsyncRead + Unpin + Send + 'static,
        F: Send,
        B: http_body::Body + Send + 'static,
        <B as http_body::Body>::Data: Send + 'static,
        <B as http_body::Body>::Error: Into<GenericError>,
        T: tower::Service<
                Request<Incoming>,
                Response = Response<B>,
                Error = Infallible,
                Future = F,
            > + Clone
            + Send
            + 'static,
    {
        let connect_info = ConnectInfo::new(remote_peer);
        counter!(HTTP_CONNECTION_CREATED).increment(1);

        let io = TokioIo::new(stream);
        let handler = hyper_util::service::TowerToHyperService::new(handler.map_request(
            move |mut req: Request<Incoming>| {
                req.extensions_mut().insert(connect_info.clone());
                req
            },
        ));

        let tc_executor = TaskCenterExecutor::new(
            TaskCenter::current(),
            inflight.clone(),
            force_shutdown.clone(),
        );

        // Spawn a tokio task to serve the connection
        inflight.spawn(async move {
            let mut auto_connection = auto::Builder::new(tc_executor);
            auto_connection.http2().adaptive_window(true);

            if let Some(max_concurrent_streams) = http2_max_concurrent_streams {
                auto_connection
                    .http2()
                    .max_concurrent_streams(max_concurrent_streams.get());
            }
            let mut serve_connection_fut =
                std::pin::pin!(auto_connection.serve_connection(io, handler));

            let mut draining = false;
            loop {
                tokio::select! {
                    res = &mut serve_connection_fut => {
                        match res {
                            Ok(()) => {
                                counter!(HTTP_CONNECTION_DROPPED, "status" => "success").increment(1);
                                break;
                            },
                            Err(err) => {
                                if let Some(hyper_error) = err.downcast_ref::<hyper::Error>() {
                                    let status = hyper_error_status(hyper_error);
                                    counter!(HTTP_CONNECTION_DROPPED, "status" => status).increment(1);
                                    debug!("Connection dropped: status={status}, err={hyper_error:?}");
                                } else {
                                    counter!(HTTP_CONNECTION_DROPPED, "status" => "server-error").increment(1);
                                    debug!("Error when serving the connection: {:?}", err);
                                }
                                break;
                            }
                        }
                    }
                    _ = drain.cancelled(), if !draining => {
                        // Ask clients to not send any more requests on this connection
                        serve_connection_fut.as_mut().graceful_shutdown();
                        draining = true;
                    }
                    _ = force_shutdown.cancelled() => { break; }
                }
            }
        }.in_current_tc());

        Ok(())
    }
}

#[derive(Clone)]
struct TaskCenterExecutor {
    tc: task_center::Handle,
    inflight: TaskTracker,
    force_shutdown: CancellationToken,
}

impl TaskCenterExecutor {
    fn new(
        tc: task_center::Handle,
        inflight: TaskTracker,
        force_shutdown: CancellationToken,
    ) -> Self {
        Self {
            tc,
            inflight,
            force_shutdown,
        }
    }
}

impl<Fut> hyper::rt::Executor<Fut> for TaskCenterExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        let tc = self.tc.clone();
        let force_shutdown = self.force_shutdown.clone();
        self.inflight.spawn(async move {
            tokio::select! {
                _ = force_shutdown.cancelled() => {}
                _ = fut.in_tc(&tc) => {}
            }
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
    use restate_core::TestCoreEnv;
    use restate_core::{TaskCenter, TaskKind};
    use restate_hyper_uds::UnixSocketConnector;
    use restate_test_util::assert_eq;
    use restate_types::health::Health;
    use restate_types::identifiers::WithInvocationId;
    use restate_types::invocation::InvocationTarget;
    use restate_types::invocation::client::InvocationOutputResponse;
    use serde::{Deserialize, Serialize};
    use std::future::ready;
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

    #[restate_core::test]
    #[traced_test]
    async fn http_post() {
        let mut mock_dispatcher = MockRequestDispatcher::default();
        mock_dispatcher
            .expect_call()
            .once()
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

        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("ingress.sock");
        bootstrap_test(
            Listeners::new_unix_listener(socket_path.clone()).unwrap(),
            mock_dispatcher,
        )
        .await;

        // Send the request
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build::<_, Full<Bytes>>(UnixSocketConnector::new(socket_path));

        let http_response = client
            .request(
                http::Request::post("http://localhost/greeter.Greeter/greet")
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
    }

    async fn bootstrap_test(
        listeners: Listeners<HttpIngressPort>,
        mock_request_dispatcher: MockRequestDispatcher,
    ) {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let health = Health::default();

        // Create the ingress and start it
        let ingress = HyperServerIngress::new(
            listeners,
            Semaphore::MAX_PERMITS,
            10 * 1024 * 1024, // 10MB
            None,
            Live::from_value(mock_schemas()),
            Arc::new(mock_request_dispatcher),
            health.ingress_status(),
        );
        TaskCenter::spawn(TaskKind::SystemService, "ingress", ingress.run()).unwrap();
    }
}
