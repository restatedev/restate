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
use tonic::Code;
use tracing::info;

use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_eprintln, c_println};
use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::node_ctl_svc::{
    ManualCompactionOptions as ProtoManualCompactionOptions, TriggerCompactionRequest,
    TriggerCompactionResponse, new_node_ctl_client,
};
use restate_types::PlainNodeId;
use restate_types::net::address::{AdvertisedAddress, FabricPort};
use restate_types::protobuf::common::DatabaseKind;
use restate_types::rocksdb::{BottommostLevelCompaction, ManualCompactionOptions};

use crate::connection::ConnectionInfo;

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

    /// Maximum time to wait for compaction results. Accepted compactions continue after timeout.
    #[arg(long, default_value = "30m", value_parser = humantime::parse_duration)]
    timeout: Duration,

    /// Controls whether RocksDB rewrites files already in the bottommost level.
    ///
    /// Forcing bottommost-level compaction can reclaim space after large deletions, but causes
    /// additional I/O and write amplification.
    ///
    /// See https://github.com/facebook/rocksdb/wiki/Manual-Compaction#compactrange.
    #[arg(long, value_enum, default_value = "if-have-compaction-filter")]
    bottommost_level_compaction: BottommostLevelCompaction,

    /// Refit compacted files to the minimum level capable of holding the data.
    ///
    /// Use after compaction substantially reduces the data size and the current level is no longer
    /// appropriate for the resulting files.
    ///
    /// See https://github.com/facebook/rocksdb/wiki/Manual-Compaction#compactrange.
    #[arg(long)]
    recalculate_level: bool,
}

impl From<&CompactOpts> for ProtoManualCompactionOptions {
    fn from(value: &CompactOpts) -> Self {
        ManualCompactionOptions {
            bottommost_level_compaction: value.bottommost_level_compaction,
            recalculate_level: value.recalculate_level,
        }
        .into()
    }
}

#[derive(Debug, thiserror::Error)]
enum TriggerCompactionError {
    #[error("compaction request timed out after {0:?}")]
    Timeout(Duration),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
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
    let compaction_options = ProtoManualCompactionOptions::from(opts);

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
        let timeout = opts.timeout;
        tasks.spawn(async move {
            let result =
                trigger_compaction_on_node(&address, db_kinds, compaction_options, timeout).await;
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
                if let TriggerCompactionError::Timeout(timeout) = &err {
                    c_eprintln!(
                        "Compaction request to {address} timed out after {}. Any compaction request accepted by the node continues in the background; check the node logs before retrying.",
                        humantime::format_duration(*timeout)
                    );
                }
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
        "Compaction results: {} succeeded, {} failed",
        total_success,
        total_failed
    );

    if total_failed > 0 {
        anyhow::bail!("{total_failed} compaction operation(s) failed");
    }
    Ok(())
}

async fn trigger_compaction_on_node(
    address: &AdvertisedAddress<FabricPort>,
    databases: Vec<i32>,
    options: ProtoManualCompactionOptions,
    timeout: Duration,
) -> Result<TriggerCompactionResponse, TriggerCompactionError> {
    let mut network = CliContext::get().network.clone();
    network.request_timeout = timeout
        .as_millis()
        .try_into()
        .map_err(|_| anyhow::anyhow!("compaction timeout is too large"))?;
    let channel = create_tonic_channel(address.clone(), &network, DNSResolution::Gai);
    let mut client = new_node_ctl_client(channel, &network);

    let request = TriggerCompactionRequest {
        databases,
        options: Some(options),
    };
    let response = client.trigger_compaction(request).await.map_err(|err| {
        if err.code() == Code::DeadlineExceeded {
            TriggerCompactionError::Timeout(timeout)
        } else {
            TriggerCompactionError::Other(anyhow::anyhow!("gRPC error: {err}"))
        }
    })?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use restate_core::protobuf::node_ctl_svc::BottommostLevelCompaction as ProtoBottommostLevelCompaction;

    use super::*;

    #[test]
    fn parse_database_kind() {
        assert_eq!(
            super::parse_database_kind("partition-store"),
            Ok(DatabaseKind::PartitionStore)
        );
        assert_eq!(
            super::parse_database_kind("PARTITION-STORE"),
            Ok(DatabaseKind::PartitionStore)
        );
        assert_eq!(
            super::parse_database_kind("log-server"),
            Ok(DatabaseKind::LogServer)
        );
        assert_eq!(
            super::parse_database_kind("LOG-SERVER"),
            Ok(DatabaseKind::LogServer)
        );
        assert_eq!(
            super::parse_database_kind("metadata-server"),
            Ok(DatabaseKind::MetadataServer)
        );
        assert_eq!(
            super::parse_database_kind("local-loglet"),
            Ok(DatabaseKind::LocalLoglet)
        );
        // Invalid values should return an error
        assert!(super::parse_database_kind("invalid").is_err());
        assert!(super::parse_database_kind("all").is_err());
        assert!(super::parse_database_kind("").is_err());
        assert!(super::parse_database_kind("db").is_err());
    }

    #[test]
    fn compaction_options_are_mapped() {
        let opts = CompactOpts {
            database: Vec::new(),
            node: Vec::new(),
            timeout: Duration::from_secs(30 * 60),
            bottommost_level_compaction: BottommostLevelCompaction::ForceOptimized,
            recalculate_level: true,
        };
        let requested = ProtoManualCompactionOptions::from(&opts);
        assert_eq!(
            requested.bottommost_level_compaction,
            ProtoBottommostLevelCompaction::ForceOptimized as i32
        );
        assert!(requested.recalculate_level);
    }
}
