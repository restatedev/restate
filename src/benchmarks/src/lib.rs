//! Utilities for benchmarking the Restate runtime
use drain::Signal;
use futures_util::{future, TryFutureExt};
use hyper::header::CONTENT_TYPE;
use hyper::{Body, StatusCode, Uri};
use pprof::flamegraph::Options;
use restate::config::{
    ConfigurationBuilder, MetaOptionsBuilder, RocksdbOptionsBuilder, WorkerOptionsBuilder,
};
use restate::{Application, ApplicationError, Configuration};
use restate_types::retries::RetryPolicy;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

pub fn discover_endpoint(current_thread_rt: &Runtime, address: Uri) {
    let discovery_payload = serde_json::json!({"uri": address.to_string()}).to_string();
    let discovery_result = current_thread_rt.block_on(async {
        let result = RetryPolicy::fixed_delay(Duration::from_millis(200), 50)
            .retry_operation(|| {
                hyper::Client::new()
                    .request(
                        hyper::Request::post("http://localhost:8081/endpoints")
                            .header(CONTENT_TYPE, "application/json")
                            .body(Body::from(discovery_payload.clone()))
                            .expect("building discovery request should not fail"),
                    )
                    .map_err(anyhow::Error::from)
                    .and_then(|response| {
                        if response.status().is_success() {
                            future::ready(Ok(response))
                        } else {
                            future::ready(Err(anyhow::anyhow!("Discovery was unsuccessful.")))
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

pub fn spawn_restate(
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

pub fn flamegraph_options<'a>() -> Options<'a> {
    let mut options = Options::default();
    // ignore different thread origins to merge traces
    options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    options
}

pub fn restate_configuration() -> Configuration {
    let meta_options = MetaOptionsBuilder::default()
        .storage_path(
            tempfile::tempdir()
                .expect("tempdir failed")
                .into_path()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .build()
        .expect("building meta options should work");

    let rocksdb_options = RocksdbOptionsBuilder::default()
        .path(
            tempfile::tempdir()
                .expect("tempdir failed")
                .into_path()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .build()
        .expect("building rocksdb options should work");

    let worker_options = WorkerOptionsBuilder::default()
        .partitions(10)
        .storage_rocksdb(rocksdb_options)
        .build()
        .expect("building worker options should work");

    let config = ConfigurationBuilder::default()
        .worker(worker_options)
        .meta(meta_options)
        .build()
        .expect("building the configuration should work");

    config
}
