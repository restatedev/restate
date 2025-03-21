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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<CliApp> {
    restatectl::lambda::handle_lambda_if_needed().await;

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

    use restate_local_cluster_runner::{
        cluster::Cluster,
        node::{BinarySource, Node},
    };
    use restate_test_util::let_assert;
    use restate_types::config::MetadataClientKind;
    use restate_types::{
        config::Configuration, logs::metadata::ProviderKind, net::AdvertisedAddress,
        replication::ReplicationProperty,
    };

    #[tokio::test]
    async fn restatectl_smoke_test() -> googletest::Result<()> {
        let mut config = Configuration::default();
        config.common.default_num_partitions = 1.try_into()?;
        config.common.auto_provision = true;
        config.bifrost.default_provider = ProviderKind::Replicated;
        config.bifrost.replicated_loglet.default_log_replication =
            ReplicationProperty::new(NonZeroU8::new(1).expect("non-zero"));
        let roles = *config.roles();

        let mut cluster = Cluster::builder()
            .temp_base_dir()
            .nodes(Node::new_test_nodes(
                config,
                BinarySource::CargoTest,
                roles,
                1,
                true,
            ))
            .build()
            .start()
            .await?;

        cluster.wait_healthy(Duration::from_secs(10)).await?;
        {
            let mut node = cluster.nodes[0].lines("Partition [0-9]+ started".parse()?);
            tokio::time::timeout(Duration::from_secs(10), node.next()).await?;
        }

        let_assert!(
            MetadataClientKind::Replicated { addresses } =
                &cluster.nodes[0].config().common.metadata_client.kind
        );
        let_assert!(AdvertisedAddress::Uds(sock_path) = &addresses[0]);
        let node_address = format!("unix:{}", cluster.base_dir().join(sock_path).display());

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
        // Note that we can only run this once per test as restatectl will initialize the global tracing subscriber:
        let result = restatectl.run().await;
        assert!(result.is_success());

        cluster.graceful_shutdown(Duration::from_secs(5)).await?;

        Ok(())
    }
}
