// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use cling::prelude::*;
use tokio::task::JoinSet;
use tracing::info;

use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::node_ctl_svc::{
    DatabaseKind, TriggerCompactionRequest, new_node_ctl_client,
};
use restate_types::net::address::{AdvertisedAddress, FabricPort};

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

const COMPACTION_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "compact")]
pub struct CompactOpts {
    /// Database type(s) to compact: all, partition-store, log-server,
    /// metadata-server, local-loglet. Defaults to all.
    #[arg(long, short = 'd', value_delimiter = ',')]
    database: Vec<String>,

    /// Target specific node addresses (default: all nodes from cluster)
    #[arg(long, short = 'n', value_delimiter = ',')]
    node: Vec<AdvertisedAddress<FabricPort>>,
}

fn parse_database_kind(s: &str) -> Option<DatabaseKind> {
    match s.to_lowercase().as_str() {
        "all" => Some(DatabaseKind::All),
        "partition-store" => Some(DatabaseKind::PartitionStore),
        "log-server" => Some(DatabaseKind::LogServer),
        "metadata-server" => Some(DatabaseKind::MetadataServer),
        "local-loglet" => Some(DatabaseKind::LocalLoglet),
        _ => None,
    }
}

async fn compact(connection: &ConnectionInfo, opts: &CompactOpts) -> anyhow::Result<()> {
    // Parse database kinds
    let database_kinds: Vec<i32> = if opts.database.is_empty() {
        vec![DatabaseKind::All as i32]
    } else {
        opts.database
            .iter()
            .filter_map(|s| parse_database_kind(s))
            .map(|k| k as i32)
            .collect()
    };

    if database_kinds.is_empty() {
        anyhow::bail!(
            "Invalid database type(s). Valid options: all, partition-store, log-server, metadata-server, local-loglet"
        );
    }

    // Get target addresses
    let addresses: Vec<AdvertisedAddress<FabricPort>> = if opts.node.is_empty() {
        let nodes_config = connection.get_nodes_configuration().await?;
        nodes_config
            .iter()
            .map(|(_, node)| node.address.clone())
            .collect()
    } else {
        opts.node.clone()
    };

    if addresses.is_empty() {
        anyhow::bail!("No nodes available to compact");
    }

    c_println!("Triggering compaction on {} node(s)...", addresses.len());

    // Spawn compaction tasks in parallel
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

    let request = TriggerCompactionRequest { databases };
    let response = tokio::time::timeout(COMPACTION_TIMEOUT, client.trigger_compaction(request))
        .await
        .map_err(|_| anyhow::anyhow!("Compaction timed out after {:?}", COMPACTION_TIMEOUT))?
        .map_err(|e| anyhow::anyhow!("gRPC error: {}", e))?;

    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_kind_all() {
        assert_eq!(parse_database_kind("all"), Some(DatabaseKind::All));
        assert_eq!(parse_database_kind("ALL"), Some(DatabaseKind::All));
        assert_eq!(parse_database_kind("All"), Some(DatabaseKind::All));
    }

    #[test]
    fn test_parse_database_kind_partition_store() {
        assert_eq!(
            parse_database_kind("partition-store"),
            Some(DatabaseKind::PartitionStore)
        );
        assert_eq!(
            parse_database_kind("PARTITION-STORE"),
            Some(DatabaseKind::PartitionStore)
        );
    }

    #[test]
    fn test_parse_database_kind_log_server() {
        assert_eq!(
            parse_database_kind("log-server"),
            Some(DatabaseKind::LogServer)
        );
        assert_eq!(
            parse_database_kind("LOG-SERVER"),
            Some(DatabaseKind::LogServer)
        );
    }

    #[test]
    fn test_parse_database_kind_metadata_server() {
        assert_eq!(
            parse_database_kind("metadata-server"),
            Some(DatabaseKind::MetadataServer)
        );
    }

    #[test]
    fn test_parse_database_kind_local_loglet() {
        assert_eq!(
            parse_database_kind("local-loglet"),
            Some(DatabaseKind::LocalLoglet)
        );
    }

    #[test]
    fn test_parse_database_kind_invalid() {
        assert_eq!(parse_database_kind("invalid"), None);
        assert_eq!(parse_database_kind(""), None);
        assert_eq!(parse_database_kind("db"), None);
    }
}
