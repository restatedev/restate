use crate::counter::counter_client::CounterClient;
use crate::counter::CounterAddRequest;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use drain::Signal;
use futures_util::TryFutureExt;
use hyper::header::CONTENT_TYPE;
use hyper::{Body, StatusCode, Uri};
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;
use restate::{Application, ApplicationError, Configuration};
use restate_common::retry_policy::RetryPolicy;
use std::future;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

// This benchmark requires the counter.Counter service running on localhost:8080 to work
fn throughput_benchmark(criterion: &mut Criterion) {
    let config = Configuration::default();
    let (_rt, signal, app_handle) = spawn_restate(config);

    let current_thread_rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current thread runtime must build");

    discover_endpoint(
        &current_thread_rt,
        Uri::from_static("http://localhost:8080"),
    );

    let counter_client = current_thread_rt.block_on(async {
        counter::counter_client::CounterClient::connect("http://localhost:9090")
            .await
            .expect("should be able to connect to Restate gRPC ingress")
    });

    let num_requests = 1;
    let mut group = criterion.benchmark_group("throughput_measurements");
    group
        .throughput(Throughput::Elements(num_requests))
        .bench_function("sequential_input", |bencher| {
            bencher
                .to_async(&current_thread_rt)
                .iter(|| send_sequential_counter_requests(counter_client.clone(), num_requests))
        });

    current_thread_rt.block_on(async move {
        signal.drain().await;
        app_handle
            .await
            .expect("restate should not panic")
            .expect("restate should not fail");
    });
}

async fn send_sequential_counter_requests(
    mut counter_client: CounterClient<tonic::transport::Channel>,
    num_requests: u64,
) {
    for _ in 0..num_requests {
        counter_client
            .get_and_add(CounterAddRequest {
                counter_name: "single".into(),
                value: 10,
            })
            .await
            .expect("counter.Counter::get_and_add should not fail");
    }
}

fn discover_endpoint(current_thread_rt: &Runtime, address: Uri) {
    let discovery_result = current_thread_rt.block_on(async {
        let result = RetryPolicy::fixed_delay(Duration::from_millis(200), 50)
            .retry_operation(|| {
                hyper::Client::new()
                    .request(
                        hyper::Request::post("http://localhost:8081/endpoint/discover")
                            .header(CONTENT_TYPE, "application/json")
                            .body(Body::from(format!(r#"{{"uri": "{}"}}"#, address)))
                            .expect("building discovery request should not fail"),
                    )
                    .map_err(anyhow::Error::from)
                    .and_then(|response| {
                        if response.status() != StatusCode::OK {
                            future::ready(Err(anyhow::anyhow!("Discovery was unsuccessful.")))
                        } else {
                            future::ready(Ok(response))
                        }
                    })
            })
            .await;

        result
    });

    assert_eq!(
        discovery_result
            .expect("Discovery must be successful")
            .status(),
        StatusCode::OK
    );
}

fn spawn_restate(
    config: Configuration,
) -> (Runtime, Signal, JoinHandle<Result<(), ApplicationError>>) {
    let rt = config
        .tokio_runtime
        .build()
        .expect("Tokio runtime must build");
    let app = Application::new(config.meta, config.worker).expect("Application must build");
    let (signal, drain) = drain::channel();
    let app_future = app.run(drain);
    let app_handle = rt.spawn(app_future);

    (rt, signal, app_handle)
}

fn flamegraph_options<'a>() -> Options<'a> {
    let mut options = Options::default();
    // ignore different thread origins to merge traces
    options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    options
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(flamegraph_options()))));
    targets = throughput_benchmark
);
criterion_main!(benches);
