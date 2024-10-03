use std::time::Duration;

use enumset::enum_set;
use futures::StreamExt;
use regex::Regex;
use tracing::{error, info};

use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, Node},
    shutdown,
};
use restate_types::{
    config::{Configuration, LogFormat},
    nodes_config::Role,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let mut base_config = Configuration::default();
    base_config.common.log_format = LogFormat::Compact;

    let nodes = Node::new_test_nodes_with_metadata(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Admin | Role::Worker),
        2,
    );

    let cluster = Cluster::builder()
        .cluster_name("test-cluster")
        .nodes(nodes)
        .build();

    // start capturing signals
    let shutdown_fut = shutdown();

    let mut cluster = cluster.start().await.unwrap();

    match cluster.nodes[0]
        .lines(Regex::new("Server listening").unwrap())
        .next()
        .await
    {
        None => {
            error!("metadata node exited early");
            std::process::exit(1)
        }
        Some(line) => {
            info!("matched metadata logline: {line}")
        }
    };

    shutdown_fut.await;

    cluster
        .graceful_shutdown(Duration::from_secs(5))
        .await
        .expect("cluster to shut down");
}
