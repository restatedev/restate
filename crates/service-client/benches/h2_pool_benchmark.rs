// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::BoxFuture;
use http::{Request, Uri};
use http_body_util::BodyExt;
use hyper_util::client::legacy::connect::{Connected, Connection};
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tower::Service;

use restate_service_client::pool::PoolBuilder;
use restate_service_client::pool::test_util::{ServerConfig, TestConnector, run_echo_server};
use restate_types::errors::GenericError;

const MAX_CONCURRENT_STREAMS: u32 = 100;

// ---------------------------------------------------------------------------
// Raw h2 helpers
// ---------------------------------------------------------------------------

/// Creates a raw h2 client `SendRequest` over an in-memory duplex, with the
/// echo server on the other end and the connection driver spawned.
async fn make_raw_h2() -> h2::client::SendRequest<Bytes> {
    let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
    let config = Arc::new(ServerConfig {
        max_concurrent_streams: MAX_CONCURRENT_STREAMS,
    });
    tokio::spawn(run_echo_server(server_stream, config));

    let (send_request, connection) = h2::client::Builder::new()
        .initial_max_send_streams(MAX_CONCURRENT_STREAMS as usize)
        .handshake::<_, Bytes>(client_stream)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    send_request
}

/// Send a request with body via raw h2 and drain the echoed response.
async fn raw_h2_request(send_request: &h2::client::SendRequest<Bytes>, payload: Bytes) {
    let mut ready = send_request.clone().ready().await.unwrap();
    let req = Request::builder()
        .uri("http://bench-host:80")
        .body(())
        .unwrap();
    let (resp_fut, mut send_stream) = ready.send_request(req, payload.is_empty()).unwrap();

    // Send the body
    send_stream.reserve_capacity(payload.len());
    let _ = futures::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await;
    if !payload.is_empty() {
        send_stream.send_data(payload, true).unwrap();
    }

    // Drain response
    let resp = resp_fut.await.unwrap();
    let mut body = resp.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        body.flow_control().release_capacity(chunk.len()).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Shared in-memory H2 echo server infrastructure
// ---------------------------------------------------------------------------

/// A wrapper around `DuplexStream` that implements hyper_util's `Connection`
/// trait (plus hyper's `Read`/`Write`) for use with the legacy client.
struct DuplexConnection(DuplexStream);

impl Connection for DuplexConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl hyper::rt::Read for DuplexConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let n = {
            let mut tbuf = ReadBuf::uninit(unsafe { buf.as_mut() });
            match Pin::new(&mut self.0).poll_read(cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };
        unsafe { buf.advance(n) };
        Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for DuplexConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// A connector wrapping `TestConnector` that returns `DuplexConnection`,
/// which implements the traits required by hyper_util's legacy client.
#[derive(Clone)]
struct HyperUtilConnector {
    inner: TestConnector,
}

impl HyperUtilConnector {
    fn new(max_concurrent_streams: u32) -> Self {
        Self {
            inner: TestConnector::new(max_concurrent_streams),
        }
    }
}

impl Service<Uri> for HyperUtilConnector {
    type Response = DuplexConnection;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let fut = self.inner.call(req);
        Box::pin(async move { fut.await.map(DuplexConnection) })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_custom_pool(max_connections: usize) -> restate_service_client::pool::Pool<TestConnector> {
    PoolBuilder::default()
        .max_connections(NonZeroUsize::new(max_connections).unwrap())
        .initial_max_send_streams(NonZeroU32::new(MAX_CONCURRENT_STREAMS).unwrap())
        .build(TestConnector::new(MAX_CONCURRENT_STREAMS))
}

type BoxBody = http_body_util::combinators::BoxBody<Bytes, GenericError>;

fn make_hyper_util_client() -> hyper_util::client::legacy::Client<HyperUtilConnector, BoxBody> {
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::default())
        .timer(hyper_util::rt::TokioTimer::default())
        .http2_only(true)
        .build(HyperUtilConnector::new(MAX_CONCURRENT_STREAMS))
}

fn empty_request(uri: &str) -> Request<http_body_util::Empty<Bytes>> {
    Request::builder()
        .uri(uri)
        .body(http_body_util::Empty::<Bytes>::new())
        .unwrap()
}

fn body_request(uri: &str, payload: Bytes) -> Request<http_body_util::Full<Bytes>> {
    Request::builder()
        .uri(uri)
        .body(http_body_util::Full::new(payload))
        .unwrap()
}

fn boxed_empty_request(uri: &str) -> Request<BoxBody> {
    Request::builder()
        .uri(uri)
        .body(
            http_body_util::Empty::<Bytes>::new()
                .map_err(|e| e.into())
                .boxed(),
        )
        .unwrap()
}

fn boxed_body_request(uri: &str, payload: Bytes) -> Request<BoxBody> {
    Request::builder()
        .uri(uri)
        .body(
            http_body_util::Full::new(payload)
                .map_err(|e| e.into())
                .boxed(),
        )
        .unwrap()
}

fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    }
    options
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_sequential_requests(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("sequential");
    group
        .sample_size(50)
        .measurement_time(Duration::from_secs(10));

    group.bench_function("custom-pool", |b| {
        // Warm up: establish H2 connection
        let pool = rt.block_on(async {
            let pool = make_custom_pool(1);
            let resp = pool
                .request(empty_request("http://bench-host:80"))
                .await
                .unwrap();
            drop(resp.into_body().collect().await);
            pool
        });

        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            async move {
                let resp = pool
                    .request(empty_request("http://bench-host:80"))
                    .await
                    .unwrap();
                resp.into_body().collect().await.unwrap();
            }
        });
    });

    group.bench_function("hyper-util-legacy", |b| {
        let client = make_hyper_util_client();
        // Warm up
        rt.block_on(async {
            let resp = client
                .request(boxed_empty_request("http://bench-host:80"))
                .await
                .unwrap();
            drop(resp.into_body().collect().await);
        });

        b.to_async(&rt).iter(|| {
            let client = client.clone();
            async move {
                let resp = client
                    .request(boxed_empty_request("http://bench-host:80"))
                    .await
                    .unwrap();
                resp.into_body().collect().await.unwrap();
            }
        });
    });

    group.bench_function("h2-raw", |b| {
        let send_request = rt.block_on(make_raw_h2());
        // Warm up
        rt.block_on(raw_h2_request(&send_request, Bytes::default()));

        b.to_async(&rt).iter(|| {
            let send_request = send_request.clone();
            async move {
                raw_h2_request(&send_request, Bytes::default()).await;
            }
        });
    });

    group.finish();
}

fn bench_concurrent_requests(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    for concurrency in [10, 50] {
        let mut group = c.benchmark_group("concurrent");
        group
            .sample_size(30)
            .measurement_time(Duration::from_secs(15))
            .throughput(Throughput::Elements(concurrency as u64));

        group.bench_with_input(
            BenchmarkId::new("custom-pool", concurrency),
            &concurrency,
            |b, &n| {
                // Warm up
                let pool = rt.block_on(async {
                    let pool = make_custom_pool(1);
                    let resp = pool
                        .request(empty_request("http://bench-host:80"))
                        .await
                        .unwrap();
                    drop(resp.into_body().collect().await);
                    pool
                });

                b.to_async(&rt).iter(|| {
                    let pool = pool.clone();
                    async move {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let pool = pool.clone();
                            set.spawn(async move {
                                let resp = pool
                                    .request(empty_request("http://bench-host:80"))
                                    .await
                                    .unwrap();
                                resp.into_body().collect().await.unwrap();
                            });
                        }
                        set.join_all().await;
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("hyper-util-legacy", concurrency),
            &concurrency,
            |b, &n| {
                let client = make_hyper_util_client();
                // Warm up
                rt.block_on(async {
                    let resp = client
                        .request(boxed_empty_request("http://bench-host:80"))
                        .await
                        .unwrap();
                    drop(resp.into_body().collect().await);
                });

                b.to_async(&rt).iter(|| {
                    let client = client.clone();
                    async move {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let client = client.clone();
                            set.spawn(async move {
                                let resp = client
                                    .request(boxed_empty_request("http://bench-host:80"))
                                    .await
                                    .unwrap();
                                resp.into_body().collect().await.unwrap();
                            });
                        }
                        set.join_all().await;
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("h2-raw", concurrency),
            &concurrency,
            |b, &n| {
                let send_request = rt.block_on(make_raw_h2());
                // Warm up
                rt.block_on(raw_h2_request(&send_request, Bytes::default()));

                b.to_async(&rt).iter(|| {
                    let send_request = send_request.clone();
                    async move {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let send_request = send_request.clone();
                            set.spawn(async move {
                                raw_h2_request(&send_request, Bytes::default()).await;
                            });
                        }
                        set.join_all().await;
                    }
                });
            },
        );

        group.finish();
    }
}

fn bench_body_throughput(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    for (label, size) in [("1KB", 1024), ("64KB", 64 * 1024)] {
        let payload = Bytes::from(vec![0xABu8; size]);
        let mut group = c.benchmark_group(format!("body-{label}"));
        group
            .sample_size(30)
            .measurement_time(Duration::from_secs(10))
            .throughput(Throughput::Bytes(size as u64));

        group.bench_function("custom-pool", |b| {
            let payload = payload.clone();
            // Warm up
            let pool = rt.block_on(async {
                let pool = make_custom_pool(1);
                let resp = pool
                    .request(body_request("http://bench-host:80", payload.clone()))
                    .await
                    .unwrap();
                resp.into_body().collect().await.unwrap();
                pool
            });

            b.to_async(&rt).iter(|| {
                let pool = pool.clone();
                let payload = payload.clone();
                async move {
                    let resp = pool
                        .request(body_request("http://bench-host:80", payload))
                        .await
                        .unwrap();
                    resp.into_body().collect().await.unwrap();
                }
            });
        });

        group.bench_function("hyper-util-legacy", |b| {
            let client = make_hyper_util_client();
            let payload = payload.clone();
            // Warm up
            rt.block_on(async {
                let resp = client
                    .request(boxed_body_request("http://bench-host:80", payload.clone()))
                    .await
                    .unwrap();
                resp.into_body().collect().await.unwrap();
            });

            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let payload = payload.clone();
                async move {
                    let resp = client
                        .request(boxed_body_request("http://bench-host:80", payload))
                        .await
                        .unwrap();
                    resp.into_body().collect().await.unwrap();
                }
            });
        });

        group.bench_function("h2-raw", |b| {
            let send_request = rt.block_on(make_raw_h2());
            let payload = payload.clone();
            // Warm up
            rt.block_on(raw_h2_request(&send_request, payload.clone()));

            b.to_async(&rt).iter(|| {
                let send_request = send_request.clone();
                let payload = payload.clone();
                async move {
                    raw_h2_request(&send_request, payload).await;
                }
            });
        });

        group.finish();
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(flamegraph_options()))));
    targets = bench_sequential_requests, bench_concurrent_requests, bench_body_throughput
);
criterion_main!(benches);
