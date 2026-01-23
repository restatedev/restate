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

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use itertools::Itertools;
use tonic::{Code, IntoRequest};
use tracing::{error, warn};

use restate_cli_util::_comfy_table::{Cell, Color, Row, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{ClusterStateRequest, new_cluster_ctrl_client};
use restate_metadata_server_grpc::grpc::new_metadata_server_client;
use restate_time_util::DurationExt;
use restate_types::health::MetadataServerStatus;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::{AliveNode, RunMode};
use restate_types::{GenerationalNodeId, NodeId};

use crate::commands::log::list_logs::{ListLogsOpts, list_logs};
use crate::commands::metadata_server::list_servers::{ListMetadataServers, list_metadata_servers};
use crate::commands::node::list_nodes::{ListNodesOpts, list_nodes, list_nodes_lite};
use crate::commands::partition::list::{ListPartitionsOpts, list_partitions};
use crate::connection::{ConnectionInfo, ConnectionInfoError};
use crate::util::grpc_channel;

use super::log::deserialize_replicated_log_params;
use super::metadata_server::list_servers::render_metadata_server_status;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_status")]
pub struct ClusterStatusOpts {
    /// Display additional status information
    #[arg(long)]
    extra: bool,
}

async fn cluster_status(
    connection: &ConnectionInfo,
    status_opts: &ClusterStatusOpts,
) -> anyhow::Result<()> {
    if !status_opts.extra {
        return match connection.get_nodes_configuration().await {
            Ok(nodes_config) => compact_cluster_status(nodes_config, connection).await,
            Err(ConnectionInfoError::MetadataValueNotAvailable { contacted_nodes }) => {
                warn!(
                    "Could not read nodes configuration from cluster, using GetIdent responses to render basic list"
                );

                list_nodes_lite(&contacted_nodes, &ListNodesOpts { extra: false });

                // short-circuit if called as part of `restatectl status`
                Err(ConnectionInfoError::ClusterNotProvisioned.into())
            }
            Err(err) => Err(err.into()),
        };
    }

    list_nodes(
        connection,
        &ListNodesOpts {
            extra: status_opts.extra,
        },
    )
    .await?;
    c_println!();

    list_logs(connection, &ListLogsOpts {}).await?;
    c_println!();

    list_partitions(connection, &ListPartitionsOpts::default()).await?;
    c_println!();

    list_metadata_servers(connection, &ListMetadataServers {}).await?;

    Ok(())
}

async fn compact_cluster_status(
    nodes_config: NodesConfiguration,
    connection: &ConnectionInfo,
) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;

    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state
        .context("no cluster state returned")?;

    let mut table = Table::new_styled();
    table.set_styled_header(NodeRow::header());
    for (node_id, node_config) in nodes_config.iter().sorted_by_key(|(id, _)| *id) {
        let mut row = NodeRow::default();
        row.with_name(node_config.name.clone())
            .with_roles(node_config.roles.iter());

        let Some(node_state) = cluster_state.nodes.get(&u32::from(node_id)) else {
            continue;
        };

        let node_state = node_state.state.as_ref().context("missing node state")?;

        match node_state {
            State::Alive(alive) => {
                row.with_id(
                    GenerationalNodeId::from(
                        alive.generational_node_id.context("node id is missing")?,
                    ),
                    Color::Green,
                )
                .with_uptime(Duration::from_secs(alive.uptime_s));

                alive_node_status(&mut row, alive, node_config, &logs).await?;
            }
            State::Dead(_dead) => {
                row.with_id(node_id, Color::Red);
            }
        };

        table.add_row(row);
    }

    c_println!("Node Configuration ({})", nodes_config.version());
    c_println!("{}", table);
    Ok(())
}

async fn alive_node_status(
    row: &mut NodeRow,
    alive_node: &AliveNode,
    node_config: &NodeConfig,
    logs: &Logs,
) -> anyhow::Result<()> {
    let node_id: GenerationalNodeId = alive_node
        .generational_node_id
        .context("node id is missing")?
        .into();

    let counter = alive_node.partitions.values().fold(
        PartitionCounter::default(),
        |mut counter, partition_status| {
            let effective =
                RunMode::try_from(partition_status.effective_mode).expect("valid effective mode");
            let planned =
                RunMode::try_from(partition_status.planned_mode).expect("valid planned mode");

            match (effective, planned) {
                (RunMode::Leader, RunMode::Leader) => counter.leaders += 1,
                (RunMode::Follower, RunMode::Follower) => counter.followers += 1,
                (RunMode::Leader, RunMode::Follower) => counter.downgrading += 1,
                (RunMode::Follower, RunMode::Leader) => counter.upgrading += 1,
                (_, _) => {
                    // unknown state!
                }
            };

            counter
        },
    );

    row.with_partitions(counter);

    let mut counter = LogsCounter::default();

    for (log_id, chain) in logs.iter() {
        let tail = chain.tail();
        let Some(replicated_loglet_params) = deserialize_replicated_log_params(&tail) else {
            continue;
        };

        if replicated_loglet_params.nodeset.contains(node_id) {
            counter.nodesets += 1;
        }

        if replicated_loglet_params.sequencer != node_id {
            // sequencer for this log is not running here
            continue;
        }

        // check if we also running the partition
        counter.sequencers += 1;
        let Some(partition_status) = alive_node.partitions.get(&u32::from(*log_id)) else {
            // partition is not running here
            counter.optimal_placement = false;
            continue;
        };

        let effective =
            RunMode::try_from(partition_status.effective_mode).expect("valid effective mode");
        let planned = RunMode::try_from(partition_status.planned_mode).expect("valid planned mode");

        // or partition is not (or not becoming) the leader
        if effective != RunMode::Leader && planned != RunMode::Leader {
            counter.optimal_placement = false;
        }
    }

    row.with_logs(counter);

    if node_config.has_role(Role::MetadataServer) {
        let metadata_channel = grpc_channel(node_config.address.clone());
        let metadata_store_status =
            new_metadata_server_client(metadata_channel, &CliContext::get().network)
                .status(().into_request())
                .await;

        let metadata_cell = match metadata_store_status {
            Ok(response) => {
                let status = MetadataServerStatus::try_from(response.into_inner().status)
                    .unwrap_or_default();
                render_metadata_server_status(status)
            }
            Err(err) if err.code() == Code::Unimplemented => Cell::new("Local").fg(Color::Yellow),
            Err(err) => {
                error!(
                    "failed to get metadata status from node {}: {}",
                    node_config.current_generation, err,
                );
                Cell::new("ERR!").fg(Color::Red)
            }
        };

        row.with_metadata(metadata_cell);
    }

    Ok(())
}

#[derive(Default)]
struct PartitionCounter {
    leaders: u16,
    followers: u16,
    upgrading: u16,
    downgrading: u16,
}

impl PartitionCounter {
    fn leaders(&self) -> String {
        use std::fmt::Write;

        let mut buf = String::with_capacity(5);
        write!(buf, "{}", self.leaders).expect("must succeed");
        if self.upgrading > 0 {
            write!(buf, "+{}", self.upgrading).expect("must succeed");
        }

        buf
    }

    fn followers(&self) -> String {
        use std::fmt::Write;

        let mut buf = String::with_capacity(5);
        write!(buf, "{}", self.followers).expect("must succeed");
        if self.downgrading > 0 {
            write!(buf, "+{}", self.downgrading).expect("must succeed");
        }

        buf
    }
}

struct LogsCounter {
    sequencers: usize,
    nodesets: usize,
    optimal_placement: bool,
}

impl Default for LogsCounter {
    fn default() -> Self {
        Self {
            sequencers: 0,
            nodesets: 0,
            optimal_placement: true,
        }
    }
}

#[derive(Default)]
struct NodeRow {
    id: Option<Cell>,
    name: Option<Cell>,
    uptime: Option<Cell>,
    roles: Option<Cell>,
    metadata: Option<Cell>,
    leaders: Option<Cell>,
    followers: Option<Cell>,
    nodesets: Option<Cell>,
    sequencers: Option<Cell>,
}

impl NodeRow {
    fn header() -> Vec<&'static str> {
        vec![
            "NODE-ID",
            "NAME",
            "UPTIME",
            "METADATA",
            "LEADER",
            "FOLLOWER",
            "NODESET-MEMBER",
            "SEQUENCER",
            "ROLES",
        ]
    }

    fn with_id<I: Into<NodeId>>(&mut self, node_id: I, color: Color) -> &mut Self {
        self.id = Some(Cell::new(node_id.into()).fg(color));
        self
    }

    fn with_uptime(&mut self, uptime: Duration) -> &mut Self {
        if uptime.as_secs() == 0 {
            self.uptime = Some(Cell::new("n/a"));
        } else {
            self.uptime = Some(Cell::new(uptime.friendly()));
        }
        self
    }

    fn with_name(&mut self, name: String) -> &mut Self {
        self.name = Some(Cell::new(name));
        self
    }

    fn with_roles(&mut self, roles: impl Iterator<Item = Role>) -> &mut Self {
        self.roles = Some(Cell::new(roles.map(|r| r.to_string()).sorted().join(" | ")));
        self
    }

    fn with_partitions(&mut self, counter: PartitionCounter) -> &mut Self {
        self.leaders = Some(Cell::new(counter.leaders()));
        self.followers = Some(Cell::new(counter.followers()));
        self
    }

    fn with_logs(&mut self, counter: LogsCounter) -> &mut Self {
        self.nodesets = Some(Cell::new(counter.nodesets));

        let mut cell = Cell::new(counter.sequencers).fg(Color::Yellow);
        if counter.optimal_placement {
            cell = cell.fg(Color::Green);
        }
        self.sequencers = Some(cell);
        self
    }

    fn with_metadata(&mut self, metadata: Cell) -> &mut Self {
        self.metadata = Some(metadata);
        self
    }
}

// helper macro to unwrap cells
macro_rules! unwrap {
    ($cell:expr) => {
        $cell.unwrap_or_else(|| Cell::new(""))
    };
    ($cell:expr, $default:expr) => {
        $cell.unwrap_or_else(|| Cell::new($default))
    };
    ($cell:expr, $default:expr => $color:expr) => {
        $cell.unwrap_or_else(|| Cell::new($default).fg($color))
    };
}

impl From<NodeRow> for Row {
    fn from(value: NodeRow) -> Self {
        let row = vec![
            value.id.expect("must be set"),
            unwrap!(value.name),
            unwrap!(value.uptime, "offline" => Color::Red),
            unwrap!(value.metadata),
            unwrap!(value.leaders),
            unwrap!(value.followers),
            unwrap!(value.nodesets),
            unwrap!(value.sequencers),
            unwrap!(value.roles),
        ];
        row.into()
    }
}
