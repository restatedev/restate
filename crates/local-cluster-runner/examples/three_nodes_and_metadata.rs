use std::num::NonZeroU16;
use std::time::Duration;

use enumset::enum_set;
use futures::{FutureExt, StreamExt};
use regex::Regex;
use tokio::spawn;
use tracing::{error, info};

use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
    shutdown,
};
use restate_types::logs::metadata::ProviderKind::Replicated;
use restate_types::{
    config::{Configuration, LogFormat},
    nodes_config::Role,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let mut base_config = Configuration::default();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_filter = "warn,restate=debug".to_string();
    base_config.common.bootstrap_num_partitions = NonZeroU16::new(4).unwrap();
    base_config.bifrost.default_provider = Replicated;

    let nodes = Node::new_test_nodes_with_metadata(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Worker | Role::LogServer),
        3,
    );

    let shutdown_signal = shutdown();
    tokio::pin!(shutdown_signal);

    // Create the log stream upfront to avoid startup race condition
    let mut admin_ready = nodes[0].lines(Regex::new("Restate server is ready").unwrap());

    let admin_startup_timeout = spawn(async {
        tokio::time::sleep(Duration::from_secs(5)).await;
    })
    .fuse();
    tokio::pin!(admin_startup_timeout);

    let cluster = Cluster::builder()
        .cluster_name("test-cluster")
        .nodes(nodes)
        .build();

    let mut cluster = cluster.start().await.unwrap();
    let mut ready = false;

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                break;
            }
            _ = &mut admin_startup_timeout => {
                if !ready {
                    error!("timeout waiting for admin node to start up");
                    break;
                }
            }
            line = admin_ready.next() => match line
            {
                None => {
                    error!("admin node exited");
                    break;
                }
                Some(line) => {
                    info!("admin node ready: {line}");
                    ready = true;
                }
            }
        }
    }

    info!("cluster shutting down");
    cluster
        .graceful_shutdown(Duration::from_secs(5))
        .await
        .expect("cluster to shut down");
}
