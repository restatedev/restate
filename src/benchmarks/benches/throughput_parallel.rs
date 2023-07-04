//! This benchmark requires the [counter.Counter service](https://github.com/restatedev/e2e/blob/a500164a31d58c0ee65ae77a7f99a8a2ef1825cb/services/node-services/src/counter.ts)
//! running on localhost:8080 in order to run.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use hyper::Uri;
use pprof::criterion::{Output, PProfProfiler};
use rand::distributions::{Alphanumeric, DistString};
use restate_benchmarks::counter::counter_client::CounterClient;
use restate_benchmarks::counter::CounterAddRequest;
use tokio::runtime::Builder;
use tonic::transport::Channel;

fn throughput_benchmark(criterion: &mut Criterion) {
    let config = restate_benchmarks::restate_configuration();
    let (_rt, signal, app_handle) = restate_benchmarks::spawn_restate(config);

    let current_thread_rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current thread runtime must build");

    restate_benchmarks::discover_endpoint(
        &current_thread_rt,
        Uri::from_static("http://localhost:8080"),
    );

    let counter_client = current_thread_rt.block_on(async {
        CounterClient::connect("http://localhost:9090")
            .await
            .expect("should be able to connect to Restate gRPC ingress")
    });

    let num_requests = 2000;
    let num_parallel_requests = 100;
    let mut group = criterion.benchmark_group("throughput");
    group
        .sample_size(40)
        .throughput(Throughput::Elements(
            u64::try_from(num_requests).expect("usize to u64 conversion should work"),
        ))
        .bench_function("parallel", |bencher| {
            bencher.to_async(&current_thread_rt).iter(|| {
                send_parallel_counter_requests(
                    counter_client.clone(),
                    num_requests,
                    num_parallel_requests,
                )
            })
        });

    current_thread_rt.block_on(async move {
        signal.drain().await;
        app_handle
            .await
            .expect("restate should not panic")
            .expect("restate should not fail");
    });
}

async fn send_parallel_counter_requests(
    counter_client: CounterClient<Channel>,
    num_requests: usize,
    num_parallel_requests: usize,
) {
    let mut pending_requests = FuturesUnordered::new();
    let mut completed_requests = 0;
    let mut issued_requests = 0;

    while completed_requests < num_requests {
        if pending_requests.len() < num_parallel_requests && issued_requests < num_requests {
            let mut client = counter_client.clone();
            let counter_name = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
            pending_requests.push(async move {
                client
                    .get_and_add(CounterAddRequest {
                        counter_name,
                        value: 10,
                    })
                    .await
            });
            issued_requests += 1;
        } else {
            let _ = pending_requests
                .next()
                .await
                .expect("pending requests should not be empty");
            completed_requests += 1;
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(restate_benchmarks::flamegraph_options()))));
    targets = throughput_benchmark
);
criterion_main!(benches);
