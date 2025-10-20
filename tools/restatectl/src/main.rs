// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use cling::prelude::*;
use crossterm::execute;
use restatectl::CliApp;
use rustls::crypto::aws_lc_rs;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<CliApp> {
    // Explicitly install a crypto provider in order to avoid that using tls panics because the
    // workspace-hack activates the aws-lc-rs as well as ring rustls feature. Activating both
    // features will cause panics because of https://github.com/rustls/rustls/issues/1877.
    aws_lc_rs::default_provider()
        .install_default()
        .expect("no other default crypto provider being installed");

    let _ = ctrlc::set_handler(move || {
        // Showing cursor again if it was hidden by dialoguer.
        let mut stdout = std::io::stdout();
        let _ = execute!(
            stdout,
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
            crossterm::style::ResetColor
        );

        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr);
        let _ = writeln!(stderr, "Ctrl-C pressed, aborting...");

        std::process::exit(1);
    });

    Cling::parse_and_run().await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::{ffi::OsString, num::NonZeroU8};

    use cling::Cling;
    use futures::StreamExt;

    use restate_futures_util::overdue::OverdueLoggingExt;
    use restate_local_cluster_runner::{
        cluster::Cluster,
        node::{BinarySource, NodeSpec},
    };
    use restate_types::{
        config::Configuration, logs::metadata::ProviderKind, replication::ReplicationProperty,
    };
    use tracing::info;

    #[test_log::test(tokio::test)]
    async fn restatectl_smoke_test() -> googletest::Result<()> {
        let mut config = Configuration::new_unix_sockets();
        config.common.default_num_partitions = 1.try_into()?;
        config.common.auto_provision = true;
        config.bifrost.default_provider = ProviderKind::Replicated;
        config.common.default_replication =
            ReplicationProperty::new(NonZeroU8::new(1).expect("non-zero"));

        let roles = *config.roles();

        let mut cluster = Cluster::builder()
            .temp_base_dir("restatectl_smoke_test")
            .nodes(NodeSpec::new_test_nodes(
                config,
                BinarySource::CargoTest,
                roles,
                1,
                true,
            ))
            .build()
            .start()
            .await?;
        // registering the search pattern as early as possible since we might miss it if it was
        // logged too quickly.
        let mut node = cluster.nodes[0].lines("Partition [0-9]+ started".parse()?);

        cluster
            .wait_healthy(Duration::from_secs(30))
            .log_slow_after(
                Duration::from_secs(10),
                tracing::Level::INFO,
                "Cluster is not healthy yet",
            )
            .with_overdue(Duration::from_secs(20), tracing::Level::WARN)
            .await?;
        {
            // wait for the node to report that the partition has started
            node.next()
                .log_slow_after(
                    Duration::from_secs(10),
                    tracing::Level::INFO,
                    "Node didn't start PPs yet",
                )
                .with_overdue(Duration::from_secs(20), tracing::Level::WARN)
                .await;

            drop(node);
        }

        let node_address = cluster.nodes[0].advertised_address().to_string();

        info!("Running restatectl....");

        let os_args: Vec<_> = vec![
            "restatectl",
            "status",
            "--extra",
            "--address",
            &node_address,
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let restatectl = Cling::<restatectl::CliApp>::try_parse_from(os_args)?;
        let result = restatectl.run().await;
        assert!(result.is_success());

        cluster.graceful_shutdown(Duration::from_secs(5)).await?;

        Ok(())
    }
}
