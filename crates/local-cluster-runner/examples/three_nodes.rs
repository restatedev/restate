// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::pin::pin;
use std::time::Duration;

use enumset::enum_set;
use futures::never::Never;
use tracing::{error, info};

use restate_local_cluster_runner::cluster::StartedCluster;
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
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let mut base_config = Configuration::default();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_filter = "warn,restate=debug".to_string();
    base_config.common.default_num_partitions = NonZeroU16::new(4).unwrap();
    base_config.bifrost.default_provider = Replicated;

    let nodes = Node::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        enum_set!(Role::Worker | Role::LogServer),
        3,
        true,
    );

    let mut shutdown_signal = pin!(shutdown());

    let cluster = Cluster::builder()
        .cluster_name("test-cluster")
        .nodes(nodes)
        .build();

    let mut cluster = tokio::select! {
        _ = &mut shutdown_signal => {
            info!("Shutting down before the cluster started.");
            return Ok(());
        },
        started = cluster.start() => {
            started?
        }
    };

    let result = tokio::select! {
        _ = &mut shutdown_signal => {
            Ok(())
        },
        result = run_cluster(&cluster) => {
            result.map(|_never| ())
        }
    };

    if let Err(err) = result {
        error!("Error running cluster: {err}")
    }

    info!("cluster shutting down");
    cluster
        .graceful_shutdown(Duration::from_secs(5))
        .await
        .expect("cluster to shut down");

    Ok(())
}

async fn run_cluster(cluster: &StartedCluster) -> anyhow::Result<Never> {
    cluster.wait_healthy(Duration::from_secs(5)).await?;
    futures::future::pending().await
}
