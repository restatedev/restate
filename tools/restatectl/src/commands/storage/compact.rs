// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;
use tokio::task::JoinSet;
use tracing::info;

use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::node_ctl_svc::{TriggerCompactionRequest, new_node_ctl_client};
use restate_types::PlainNodeId;
use restate_types::net::address::{AdvertisedAddress, FabricPort};
use restate_types::protobuf::common::DatabaseKind;

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "compact")]
pub struct CompactOpts {
    /// Database type(s) to compact: partition-store, log-server,
    /// metadata-server, local-loglet. Defaults to all databases.
    #[arg(long, short = 'd', value_delimiter = ',', value_parser = parse_database_kind)]
    database: Vec<DatabaseKind>,

    /// Target specific nodes by node ID (e.g. N1,N2). Defaults to all nodes in the cluster.
    #[arg(long, short = 'n', value_delimiter = ',')]
    node: Vec<PlainNodeId>,
}

fn parse_database_kind(s: &str) -> Result<DatabaseKind, String> {
    match s.to_lowercase().as_str() {
        "partition-store" => Ok(DatabaseKind::PartitionStore),
        "log-server" => Ok(DatabaseKind::LogServer),
        "metadata-server" => Ok(DatabaseKind::MetadataServer),
        "local-loglet" => Ok(DatabaseKind::LocalLoglet),
        _ => Err(format!(
            "Unknown database type '{}'. Valid options: partition-store, log-server, metadata-server, local-loglet",
            s
        )),
    }
}

async fn compact(connection: &ConnectionInfo, opts: &CompactOpts) -> anyhow::Result<()> {
    // An empty list means "compact all" at the server side.
    let database_kinds: Vec<i32> = opts.database.iter().map(|k| *k as i32).collect();

    // Resolve target node addresses from the cluster configuration.
    let nodes_config = connection.get_nodes_configuration().await?;
    let addresses: Vec<AdvertisedAddress<FabricPort>> = if opts.node.is_empty() {
        nodes_config
            .iter()
            .map(|(_, node)| node.address.clone())
            .collect()
    } else {
        opts.node
            .iter()
            .map(|node_id| {
                nodes_config
                    .find_node_by_id(*node_id)
                    .map(|n| n.address.clone())
                    .map_err(|_| {
                        anyhow::anyhow!("Node {} not found in cluster configuration", node_id)
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };

    if addresses.is_empty() {
        anyhow::bail!("No nodes available to compact");
    }

    c_println!("Triggering compaction on {} node(s)...", addresses.len());

    // Spawn compaction tasks in parallel across nodes.
    let mut tasks = JoinSet::new();
    for address in addresses {
        let db_kinds = database_kinds.clone();
        tasks.spawn(async move {
            let result = trigger_compaction_on_node(&address, db_kinds).await;
            (address, result)
        });
    }

    // Collect results
    let mut results_table = Table::new_styled();
    results_table.set_styled_header(vec!["NODE", "DATABASE", "STATUS", "COLUMN FAMILIES"]);

    let mut total_success = 0;
    let mut total_failed = 0;

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((address, Ok(response))) => {
                for db_result in response.results {
                    let status = if db_result.success {
                        total_success += 1;
                        "OK".to_string()
                    } else {
                        total_failed += 1;
                        format!("FAILED: {}", db_result.error.unwrap_or_default())
                    };
                    results_table.add_row(vec![
                        Cell::new(address.to_string()),
                        Cell::new(&db_result.db_name),
                        Cell::new(status),
                        Cell::new(db_result.column_families_compacted.to_string()),
                    ]);
                }
            }
            Ok((address, Err(err))) => {
                total_failed += 1;
                results_table.add_row(vec![
                    Cell::new(address.to_string()),
                    Cell::new("-"),
                    Cell::new(format!("ERROR: {}", err)),
                    Cell::new("-"),
                ]);
            }
            Err(err) => {
                total_failed += 1;
                info!("Task join error: {}", err);
            }
        }
    }

    c_println!("{}", results_table);
    c_println!(
        "Compaction complete: {} succeeded, {} failed",
        total_success,
        total_failed
    );

    Ok(())
}

async fn trigger_compaction_on_node(
    address: &AdvertisedAddress<FabricPort>,
    databases: Vec<i32>,
) -> Result<restate_core::protobuf::node_ctl_svc::TriggerCompactionResponse, anyhow::Error> {
    let channel = grpc_channel(address.clone());
    let mut client = new_node_ctl_client(channel, &CliContext::get().network);

    let timeout = CliContext::get().request_timeout();
    let request = TriggerCompactionRequest { databases };
    let response = tokio::time::timeout(timeout, client.trigger_compaction(request))
        .await
        .map_err(|_| anyhow::anyhow!("Compaction timed out after {:?}", timeout))?
        .map_err(|e| anyhow::anyhow!("gRPC error: {}", e))?;

    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_kind() {
        assert_eq!(
            parse_database_kind("partition-store"),
            Ok(DatabaseKind::PartitionStore)
        );
        assert_eq!(
            parse_database_kind("PARTITION-STORE"),
            Ok(DatabaseKind::PartitionStore)
        );
        assert_eq!(
            parse_database_kind("log-server"),
            Ok(DatabaseKind::LogServer)
        );
        assert_eq!(
            parse_database_kind("LOG-SERVER"),
            Ok(DatabaseKind::LogServer)
        );
        assert_eq!(
            parse_database_kind("metadata-server"),
            Ok(DatabaseKind::MetadataServer)
        );
        assert_eq!(
            parse_database_kind("local-loglet"),
            Ok(DatabaseKind::LocalLoglet)
        );
        // Invalid values should return an error
        assert!(parse_database_kind("invalid").is_err());
        assert!(parse_database_kind("all").is_err());
        assert!(parse_database_kind("").is_err());
        assert!(parse_database_kind("db").is_err());
    }
}
