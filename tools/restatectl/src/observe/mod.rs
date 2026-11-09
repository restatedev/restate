// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A point-in-time view of a cluster for `status`, `partitions list` and `logs list`.
//!
//! It combines the metadata (nodes, logs, partition table), the cluster controller's view of
//! nodes and partition processors, each node's own report, and the cluster's SQL tables. Only the
//! nodes configuration is required; anything else that can't be read becomes a warning and the
//! views go without it.

pub mod dump;
mod health;
mod model;
mod placement;
mod sql;
mod summary;
#[cfg(test)]
pub(crate) mod tests;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::join_all;
use tonic::transport::Channel;
use tonic::{Code, IntoRequest};

use restate_cli_util::CliContext;
use restate_core::protobuf::cluster_ctrl_svc::{ClusterStateRequest, new_cluster_ctrl_client};
use restate_core::protobuf::node_ctl_svc::{IdentResponse, new_node_ctl_client};
use restate_metadata_server_grpc::grpc::{StatusResponse, new_metadata_server_client};
use restate_types::PlainNodeId;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::cluster::ClusterState;
use restate_types::protobuf::cluster::node_state::State;

use crate::connection::{ConnectionInfo, ConnectionInfoError};
use crate::util::grpc_channel;

pub use health::{Finding, Severity, Subject, analyze, mode_label, short_duration};
pub use model::*;
pub use placement::*;
pub use summary::*;

/// How long to wait for each node's own report
const NODE_TIMEOUT: Duration = Duration::from_secs(2);

/// What the cluster returned for its status, as it returned it: the views are built from it, and
/// a dump keeps it.
#[derive(Clone)]
pub struct Snapshot {
    pub nodes_config: NodesConfiguration,
    pub partition_table: Option<PartitionTable>,
    pub logs: Option<Logs>,
    pub idents: HashMap<PlainNodeId, IdentResponse>,
    pub metadata_servers: BTreeMap<PlainNodeId, Result<StatusResponse, String>>,
    /// the SQL tables that change slowly
    pub tables: sql::Tables,
    /// whether nodes were asked for their own report; not in single-address mode
    pub nodes_probed: bool,
    /// the control address dialed for each node's report
    pub dialed: HashMap<PlainNodeId, String>,
    pub warnings: Vec<String>,
    /// what changes by the second
    pub sample: Sample,
}

/// The part of the cluster's status that changes by the second, from which rates are computed:
/// the partition processors' and nodes' state, log tails and read streams.
#[derive(Clone)]
pub struct Sample {
    pub captured_at: DateTime<Utc>,
    pub cluster_state: Option<ClusterState>,
    pub tables: sql::Tables,
    pub warnings: Vec<String>,
}

impl Sample {
    /// Reads a sample from the first admin node that answers, and returns that node's channel.
    pub async fn fetch(connection: &ConnectionInfo) -> (Self, Option<Channel>) {
        let captured_at = Utc::now();
        let admin = connection
            .try_each(Some(Role::Admin), |channel| async move {
                let state = new_cluster_ctrl_client(channel.clone(), &CliContext::get().network)
                    .get_cluster_state(ClusterStateRequest::default())
                    .await?
                    .into_inner()
                    .cluster_state;
                Ok::<_, tonic::Status>((channel, state))
            })
            .await;
        match admin {
            Ok((channel, cluster_state)) => {
                let tables = sql::fetch(&channel, sql::SAMPLED).await;
                let sample = Self {
                    captured_at,
                    cluster_state,
                    tables,
                    warnings: Vec::new(),
                };
                (sample, Some(channel))
            }
            Err(err) => {
                let sample = Self {
                    captured_at,
                    cluster_state: None,
                    tables: sql::Tables::new(),
                    warnings: vec![format!("cluster state unavailable: {err}")],
                };
                (sample, None)
            }
        }
    }
}

impl Snapshot {
    /// Reads the cluster's status, and the SQL tables in `more_tables` besides those the views
    /// use.
    pub async fn fetch(
        connection: &ConnectionInfo,
        more_tables: &[&str],
    ) -> Result<Self, ConnectionInfoError> {
        let nodes_config = connection.get_nodes_configuration().await?;
        let mut warnings = Vec::new();

        let (logs, partition_table, (sample, admin)) = tokio::join!(
            connection.get_logs(),
            connection.get_partition_table(),
            Sample::fetch(connection),
        );
        let logs = logs
            .inspect_err(|err| warnings.push(format!("logs metadata unavailable: {err}")))
            .ok();
        let partition_table = partition_table
            .inspect_err(|err| warnings.push(format!("partition table unavailable: {err}")))
            .ok();
        let cluster_state = sample.cluster_state.as_ref();
        // Ask every node that isn't known to be dead for its own report, unless restatectl may
        // only talk to the one address it was given.
        let nodes_probed = !connection.is_single_address_mode();
        let dialed = if nodes_probed {
            nodes_config
                .iter()
                .map(|(id, config)| {
                    (
                        id,
                        connection
                            .ctrl_address_for(&nodes_config, config)
                            .to_string(),
                    )
                })
                .collect()
        } else {
            HashMap::new()
        };
        let probes = nodes_config
            .iter()
            .filter(|_| nodes_probed)
            .filter(|(id, _)| {
                cluster_state.is_none_or(|state| {
                    !matches!(
                        state
                            .nodes
                            .get(&u32::from(*id))
                            .and_then(|n| n.state.as_ref()),
                        Some(State::Dead(_))
                    )
                })
            })
            .map(|(node_id, config)| {
                let channel = grpc_channel(
                    connection
                        .ctrl_address_for(&nodes_config, config)
                        .into_owned(),
                );
                let is_metadata_server = config.has_role(Role::MetadataServer);
                async move {
                    let ident_channel = channel.clone();
                    let ident = async move {
                        let mut client =
                            new_node_ctl_client(ident_channel, &CliContext::get().network);
                        tokio::time::timeout(NODE_TIMEOUT, client.get_ident(())).await
                    };
                    let metadata = async {
                        if !is_metadata_server {
                            return None;
                        }
                        let status = tokio::time::timeout(
                            NODE_TIMEOUT,
                            new_metadata_server_client(channel, &CliContext::get().network)
                                .status(().into_request()),
                        )
                        .await;
                        Some(match status {
                            Ok(Ok(response)) => Ok(response.into_inner()),
                            Ok(Err(status)) if status.code() == Code::Unimplemented => {
                                Err("not a replicated metadata server".to_owned())
                            }
                            Ok(Err(status)) => Err(status.message().to_owned()),
                            Err(_) => Err("timed out".to_owned()),
                        })
                    };
                    let (ident, metadata) = tokio::join!(ident, metadata);
                    (node_id, ident.ok().and_then(Result::ok), metadata)
                }
            });

        let tables = async {
            let tables: Vec<&str> = [sql::SETTLED, more_tables].concat();
            match &admin {
                Some(channel) => sql::fetch(channel, &tables).await,
                None => sql::Tables::new(),
            }
        };

        let (probes, tables) = tokio::join!(join_all(probes), tables);

        let mut idents = HashMap::new();
        let mut metadata_servers = BTreeMap::new();
        for (node_id, ident, metadata) in probes {
            if let Some(ident) = ident {
                idents.insert(node_id, ident.into_inner());
            }
            if let Some(metadata) = metadata {
                metadata_servers.insert(node_id, metadata);
            }
        }

        Ok(Snapshot {
            nodes_config,
            partition_table,
            logs,
            idents,
            metadata_servers,
            tables,
            nodes_probed,
            dialed,
            warnings,
            sample,
        })
    }
}

impl ClusterView {
    pub async fn fetch(connection: &ConnectionInfo) -> Result<Self, ConnectionInfoError> {
        Ok(ClusterView::build(Snapshot::fetch(connection, &[]).await?))
    }
}
