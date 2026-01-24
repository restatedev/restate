// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;
use std::time::Duration;

use futures::never::Never;
use tracing::{error, info};

use restate_core::TaskCenterBuilder;
use restate_local_cluster_runner::cluster::StartedCluster;
use restate_local_cluster_runner::{
    cluster::Cluster,
    node::{BinarySource, NodeSpec},
    shutdown,
};
use restate_types::config::{Configuration, LogFormat};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::logs::metadata::ProviderKind::Replicated;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let mut base_config = Configuration::new_random_ports();
    base_config.common.log_format = LogFormat::Compact;
    base_config.common.log_filter = "warn,restate=debug".to_string();
    base_config.common.default_num_partitions = 4;
    base_config.bifrost.default_provider = Replicated;

    let config = ConfigLoaderBuilder::default()
        .load_env(true)
        .custom_default(base_config)
        .build()?
        .load_once()?;

    let tc = TaskCenterBuilder::default()
        .options(config.common.clone())
        .build()?;

    tc.block_on(async move {
        let roles = *config.roles();
        let auto_provision = config.common.auto_provision;

        let nodes =
            NodeSpec::new_test_nodes(config, BinarySource::CargoTest, roles, 3, auto_provision);

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
    })
    .expect("panicked!")
}

async fn run_cluster(cluster: &StartedCluster) -> anyhow::Result<Never> {
    cluster.wait_healthy(Duration::from_secs(10)).await?;
    futures::future::pending().await
}
