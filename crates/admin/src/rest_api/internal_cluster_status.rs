// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use axum::Json;
use axum::extract::Query;
use axum::http::StatusCode;
use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tonic::Code;

use restate_core::Metadata;
use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::cluster_ctrl_svc::{ClusterStateRequest, new_cluster_ctrl_client};
use restate_core::protobuf::node_ctl_svc::{IdentResponse, new_node_ctl_client};
use restate_metadata_server_grpc::grpc::{
    StatusResponse as MetadataServerStatusResponse, new_metadata_server_client,
};
use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::logs::metadata::{Logs, ProviderKind};
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::protobuf::cluster::{
    AliveNode, ClusterState, ReplayStatus, RunMode, node_state,
};
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::ReplicationProperty;
use restate_types::{GenerationalNodeId, PlainNodeId, Versioned};

use super::internal_cluster_common::{LogsProviderView, logs_provider_view};
use crate::rest_api::error::GenericRestError;

#[derive(Debug, Default, Deserialize)]
pub struct InternalClusterStatusQuery {
    /// Include expanded sections for nodes/logs/partitions/metadata-servers.
    #[serde(default)]
    extra: bool,
}

const OPTIONAL_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

type NodeRuntimeInfoMap = HashMap<PlainNodeId, Result<IdentResponse, String>>;
type MetadataServerStatusMap =
    HashMap<PlainNodeId, Result<MetadataServerStatusResponse, tonic::Status>>;

#[derive(Debug, Clone, Serialize)]
pub struct InternalClusterStatusResponse {
    /// Version of the nodes configuration used to build this response.
    pub node_configuration_version: u32,
    /// Cluster nodes. Extra-only fields are included when `extra=true`.
    pub nodes: Vec<NodeStatus>,
    /// Expanded logs metadata information. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<LogsSection>,
    /// Expanded partition-processor information. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<PartitionsSection>,
    /// Expanded metadata-role node status. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_servers: Option<MetadataServersSection>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeLiveness {
    Alive,
    Dead,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    /// Node identifier including generation.
    pub node_id: String,
    /// Human-readable node name from nodes configuration.
    pub name: String,
    /// Enabled roles for this node.
    pub roles: Vec<String>,
    /// Static and cluster-state derived state for this node.
    pub state: NodeStateView,
    /// Probe-derived and role-specific statuses for this node.
    pub status: NodeStatusView,
    /// Compact partition leadership counters for this node.
    pub partitions: NodePartitionSummary,
    /// Compact log placement counters for this node.
    pub logs: NodeLogSummary,
    /// Uptime in seconds, if reported by cluster state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_seconds: Option<u64>,
    /// Seconds since process start according to the direct node status probe. Present only when
    /// `extra=true` and the probe succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_age_seconds: Option<u64>,
    /// Node generation from nodes configuration. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u32>,
    /// Advertised address used by cluster internals. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Node locality label. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Metadata versions observed by this node. Present only when `extra=true` and the direct node
    /// status probe succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versions: Option<NodeObservedVersions>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStateView {
    /// Whether this node is currently alive in cluster state.
    pub liveness: NodeLiveness,
    /// Log-server storage state from nodes configuration. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// Worker placement state from nodes configuration. Present only when `extra=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NodeStatusView {
    /// Node-level metadata status. Without `extra` this comes from the metadata status probe;
    /// with `extra` it comes from the direct node status probe to match `restatectl nodes
    /// list --extra`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// High-level node RPC status from the direct node status probe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Admin role status from the direct node status probe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin: Option<String>,
    /// Worker role status from the direct node status probe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// Log-server role status from the direct node status probe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_server: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodePartitionSummary {
    /// Number of partitions where this node is currently leader.
    pub leader: u16,
    /// Number of partitions where this node is currently follower.
    pub follower: u16,
    /// Number of partitions transitioning follower -> leader.
    pub upgrading: u16,
    /// Number of partitions transitioning leader -> follower.
    pub downgrading: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeLogSummary {
    /// Number of writable log tails where this node is in the nodeset.
    pub nodeset_member_count: usize,
    /// Number of writable log tails where this node is the sequencer.
    pub sequencer_count: usize,
    /// Whether sequencer placement appears optimal for this node.
    pub sequencer_placement_optimal: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeObservedVersions {
    /// Nodes configuration version observed by this node.
    pub nodes_config: u32,
    /// Logs metadata version observed by this node.
    pub logs: u32,
    /// Schema version observed by this node.
    pub schema: u32,
    /// Partition-table version observed by this node.
    pub partition_table: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogsSection {
    /// Version of logs metadata.
    pub version: u32,
    /// Default provider used when creating new logs.
    pub default_provider: LogsProviderView,
    /// One entry per log id.
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    /// Log identifier.
    pub log_id: u32,
    /// Number of segments in this log chain.
    pub segment_count: usize,
    /// Base LSN of the tail segment.
    pub from_lsn: u64,
    /// Tail segment provider kind.
    pub provider_kind: String,
    /// Replicated loglet id (for replicated tails).
    pub loglet_id: Option<String>,
    /// Replication property (for replicated tails).
    pub replication_property: Option<ReplicationProperty>,
    /// Sequencer node id (for replicated tails).
    pub sequencer: Option<String>,
    /// Nodeset members (for replicated tails).
    pub nodeset: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionsSection {
    /// Nodes-config version attached to cluster state.
    pub node_configuration_version: u32,
    /// Partition-table version attached to cluster state.
    pub partition_table_version: u32,
    /// Partition-processor entries across alive nodes.
    pub processors: Vec<PartitionProcessorEntry>,
    /// Nodes currently marked dead.
    pub dead_nodes: Vec<DeadNodeRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionProcessorEntry {
    /// Partition identifier.
    pub partition_id: u32,
    /// Host node for this processor instance (with generation).
    pub host_node: String,
    /// Planned run mode.
    pub planned_mode: String,
    /// Effective run mode.
    pub effective_mode: String,
    /// Replay status.
    pub replay_status: String,
    /// Last observed leader epoch.
    pub observed_leader_epoch: Option<u64>,
    /// Last observed leader node (plain id + generation if present).
    pub observed_leader_node: Option<String>,
    /// Last applied log LSN.
    pub last_applied_lsn: Option<u64>,
    /// Last durable LSN.
    pub durable_lsn: Option<u64>,
    /// Last archived LSN.
    pub last_archived_lsn: Option<u64>,
    /// Target tail LSN if catching up.
    pub target_tail_lsn: Option<u64>,
    /// Computed lag `(target_tail - 1) - last_applied`.
    pub lsn_lag: Option<u64>,
    /// Last processor status update timestamp in unix milliseconds.
    pub updated_at_unix_millis: Option<i64>,
    /// Whether the processor currently sees itself as leader.
    pub sees_itself_as_leader: bool,
    /// Whether this processor's observed epoch is older than peers for the same partition.
    pub leadership_epoch_outdated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeadNodeRow {
    /// Plain node id.
    pub node_id: String,
    /// Last time this node was seen alive in unix milliseconds.
    pub last_seen_alive_unix_millis: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetadataServersSection {
    /// Reachable metadata-role nodes with status from metadata-server `status()`.
    pub servers: Vec<MetadataServerEntry>,
    /// Metadata-role nodes for which the metadata-server status RPC failed.
    pub unreachable_nodes: Vec<UnreachableNodeRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetadataServerEntry {
    /// Plain node id.
    pub node_id: String,
    /// Metadata server state.
    pub status: String,
    /// Metadata cluster configuration version on this node.
    pub configuration_version: Option<u32>,
    /// Current metadata leader plain node id.
    pub leader: Option<String>,
    /// Metadata cluster members.
    pub members: Vec<String>,
    /// Last raft log index applied.
    pub raft_applied: u64,
    /// Last raft log index committed.
    pub raft_committed: u64,
    /// Current raft term.
    pub raft_term: u64,
    /// Raft log length computed as `(last_index + 1) - first_index`.
    pub raft_log_length: u64,
    /// Last snapshot index.
    pub snapshot_index: u64,
    /// Snapshot size in bytes.
    pub snapshot_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnreachableNodeRow {
    /// Plain node id.
    pub node_id: String,
    /// Error message returned when querying metadata server status.
    pub error: String,
}

pub async fn get_internal_cluster_status(
    Query(query): Query<InternalClusterStatusQuery>,
) -> Result<Json<InternalClusterStatusResponse>, GenericRestError> {
    let (networking, nodes_config, logs, my_node_id) = gather_local_snapshots()?;
    let cluster_state = fetch_cluster_state(&networking, &nodes_config, my_node_id).await?;
    let alive_nodes = alive_nodes(&cluster_state);

    let (metadata_server_statuses, node_runtime_info) = if query.extra {
        tokio::join!(
            fetch_metadata_server_status(&networking, &nodes_config, &alive_nodes, true),
            fetch_node_runtime_info(&networking, &nodes_config),
        )
    } else {
        (
            fetch_metadata_server_status(&networking, &nodes_config, &alive_nodes, false).await,
            NodeRuntimeInfoMap::new(),
        )
    };

    Ok(Json(InternalClusterStatusResponse {
        node_configuration_version: nodes_config.version().into(),
        nodes: build_nodes(
            &nodes_config,
            &logs,
            &cluster_state,
            &metadata_server_statuses,
            &node_runtime_info,
            query.extra,
        ),
        logs: query.extra.then(|| build_logs_section(&logs)),
        partitions: query
            .extra
            .then(|| build_partitions_section(&cluster_state)),
        metadata_servers: query
            .extra
            .then(|| build_metadata_servers_section(&metadata_server_statuses)),
    }))
}

fn gather_local_snapshots() -> Result<
    (
        NetworkingOptions,
        std::sync::Arc<NodesConfiguration>,
        std::sync::Arc<Logs>,
        GenerationalNodeId,
    ),
    GenericRestError,
> {
    let networking = Configuration::pinned().networking.clone();

    let nodes_config = Metadata::with_current(|m| m.nodes_config_snapshot());
    let logs = Metadata::with_current(|m| m.logs_snapshot());
    let my_node_id = Metadata::with_current(|m| m.my_node_id_opt()).ok_or_else(|| {
        GenericRestError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "The cluster does not seem to be provisioned yet. Try again later.",
        )
    })?;

    Ok((networking, nodes_config, logs, my_node_id))
}

async fn fetch_cluster_state(
    networking: &NetworkingOptions,
    nodes_config: &NodesConfiguration,
    my_node_id: GenerationalNodeId,
) -> Result<ClusterState, GenericRestError> {
    let node = nodes_config.find_node_by_id(my_node_id).map_err(|_| {
        GenericRestError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Could not resolve this node from cluster metadata.",
        )
    })?;

    let channel = create_tonic_channel(
        node.ctrl_address().into_owned(),
        networking,
        DNSResolution::Gai,
    );
    let mut client = new_cluster_ctrl_client(channel, networking);

    let cluster_state = client
        .get_cluster_state(ClusterStateRequest::default())
        .await
        .map_err(|err| {
            GenericRestError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch cluster state: {}", err.message()),
            )
        })?
        .into_inner()
        .cluster_state
        .ok_or_else(|| {
            GenericRestError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Cluster state response did not contain a payload.",
            )
        })?;

    Ok(cluster_state)
}

fn alive_nodes(cluster_state: &ClusterState) -> HashSet<PlainNodeId> {
    cluster_state
        .nodes
        .iter()
        .filter_map(|(node_id, node_state)| {
            matches!(node_state.state.as_ref(), Some(node_state::State::Alive(_)))
                .then_some(PlainNodeId::from(*node_id))
        })
        .collect()
}

async fn fetch_node_runtime_info(
    networking: &NetworkingOptions,
    nodes_config: &NodesConfiguration,
) -> NodeRuntimeInfoMap {
    let futures = nodes_config.iter().map(|(plain_node_id, node)| {
        let address = node.ctrl_address().into_owned();
        let networking = networking.clone();
        async move {
            let result = timeout(OPTIONAL_PROBE_TIMEOUT, async move {
                let channel = create_tonic_channel(address, &networking, DNSResolution::Gai);
                new_node_ctl_client(channel, &networking)
                    .get_ident(())
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|err| err.message().to_owned())
            })
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "Timed out calling GetIdent after {}s",
                    OPTIONAL_PROBE_TIMEOUT.as_secs()
                ))
            });

            (plain_node_id, result)
        }
    });

    join_all(futures)
        .await
        .into_iter()
        .collect::<NodeRuntimeInfoMap>()
}

async fn fetch_metadata_server_status(
    networking: &NetworkingOptions,
    nodes_config: &NodesConfiguration,
    alive_nodes: &HashSet<PlainNodeId>,
    include_dead_nodes: bool,
) -> MetadataServerStatusMap {
    let futures = nodes_config
        .iter_role(Role::MetadataServer)
        .filter(|(plain_node_id, _)| include_dead_nodes || alive_nodes.contains(plain_node_id))
        .map(|(plain_node_id, node)| {
            let address = node.address.clone();
            let networking = networking.clone();
            async move {
                let channel = create_tonic_channel(address, &networking, DNSResolution::Gai);
                let result = new_metadata_server_client(channel, &networking)
                    .status(())
                    .await
                    .map(|response| response.into_inner());

                (plain_node_id, result)
            }
        });

    join_all(futures)
        .await
        .into_iter()
        .collect::<MetadataServerStatusMap>()
}

fn build_nodes(
    nodes_config: &NodesConfiguration,
    logs: &Logs,
    cluster_state: &ClusterState,
    metadata_statuses: &MetadataServerStatusMap,
    node_runtime_info: &NodeRuntimeInfoMap,
    include_extra: bool,
) -> Vec<NodeStatus> {
    nodes_config
        .iter()
        .sorted_by_key(|(node_id, _)| *node_id)
        .filter_map(|(plain_node_id, node_config)| {
            let roles = node_config
                .roles
                .iter()
                .map(|r| r.to_string())
                .sorted()
                .collect::<Vec<_>>();

            let node_state = cluster_state.nodes.get(&u32::from(plain_node_id))?;

            match node_state.state.as_ref() {
                Some(node_state::State::Alive(alive)) => {
                    let node_id = alive
                        .generational_node_id
                        .map(GenerationalNodeId::from)
                        .unwrap_or(node_config.current_generation);

                    let (
                        leader_partitions,
                        follower_partitions,
                        upgrading_partitions,
                        downgrading_partitions,
                    ) = partition_counters(alive);
                    let (nodeset_member_count, sequencer_count, sequencer_placement_optimal) =
                        log_counters(logs, alive, node_id);

                    let (process_age_seconds, mut status, versions) =
                        node_runtime_details(node_runtime_info.get(&plain_node_id), include_extra);
                    if !include_extra && node_config.has_role(Role::MetadataServer) {
                        status.metadata = Some(compact_metadata_status(
                            metadata_statuses.get(&plain_node_id),
                        ));
                    }

                    Some(NodeStatus {
                        node_id: node_id.to_string(),
                        name: node_config.name.clone(),
                        roles,
                        state: NodeStateView {
                            liveness: NodeLiveness::Alive,
                            storage: include_extra.then(|| {
                                normalize_display_enum(node_config.log_server_config.storage_state)
                            }),
                            worker: include_extra.then(|| {
                                normalize_display_enum(node_config.worker_config.worker_state)
                            }),
                        },
                        status,
                        partitions: NodePartitionSummary {
                            leader: leader_partitions,
                            follower: follower_partitions,
                            upgrading: upgrading_partitions,
                            downgrading: downgrading_partitions,
                        },
                        logs: NodeLogSummary {
                            nodeset_member_count,
                            sequencer_count,
                            sequencer_placement_optimal,
                        },
                        uptime_seconds: Some(alive.uptime_s),
                        process_age_seconds,
                        generation: include_extra.then_some(node_id.generation()),
                        address: include_extra.then_some(node_config.address.to_string()),
                        location: include_extra
                            .then(|| string_or_none(node_config.location.to_string()))
                            .flatten(),
                        versions,
                    })
                }
                Some(node_state::State::Dead(_)) => {
                    let (process_age_seconds, status, versions) =
                        node_runtime_details(node_runtime_info.get(&plain_node_id), include_extra);
                    Some(NodeStatus {
                        node_id: plain_node_id.to_string(),
                        name: node_config.name.clone(),
                        roles,
                        state: NodeStateView {
                            liveness: NodeLiveness::Dead,
                            storage: include_extra.then(|| {
                                normalize_display_enum(node_config.log_server_config.storage_state)
                            }),
                            worker: include_extra.then(|| {
                                normalize_display_enum(node_config.worker_config.worker_state)
                            }),
                        },
                        status,
                        partitions: NodePartitionSummary {
                            leader: 0,
                            follower: 0,
                            upgrading: 0,
                            downgrading: 0,
                        },
                        logs: NodeLogSummary {
                            nodeset_member_count: 0,
                            sequencer_count: 0,
                            sequencer_placement_optimal: false,
                        },
                        uptime_seconds: None,
                        process_age_seconds,
                        generation: include_extra
                            .then_some(node_config.current_generation.generation()),
                        address: include_extra.then_some(node_config.address.to_string()),
                        location: include_extra
                            .then(|| string_or_none(node_config.location.to_string()))
                            .flatten(),
                        versions,
                    })
                }
                None => None,
            }
        })
        .collect()
}

fn partition_counters(alive: &AliveNode) -> (u16, u16, u16, u16) {
    let mut leaders = 0;
    let mut followers = 0;
    let mut upgrading = 0;
    let mut downgrading = 0;

    for status in alive.partitions.values() {
        let planned = RunMode::try_from(status.planned_mode).unwrap_or(RunMode::Unknown);
        let effective = RunMode::try_from(status.effective_mode).unwrap_or(RunMode::Unknown);
        match (effective, planned) {
            (RunMode::Leader, RunMode::Leader) => leaders += 1,
            (RunMode::Follower, RunMode::Follower) => followers += 1,
            (RunMode::Follower, RunMode::Leader) => upgrading += 1,
            (RunMode::Leader, RunMode::Follower) => downgrading += 1,
            _ => {}
        }
    }

    (leaders, followers, upgrading, downgrading)
}

fn log_counters(
    logs: &Logs,
    alive: &AliveNode,
    node_id: GenerationalNodeId,
) -> (usize, usize, bool) {
    let mut nodesets = 0usize;
    let mut sequencers = 0usize;
    let mut optimal = true;

    for (log_id, chain) in logs.iter() {
        let tail = chain.tail();
        if tail.config.kind != ProviderKind::Replicated {
            continue;
        }

        let Ok(params) = ReplicatedLogletParams::deserialize_from(tail.config.params.as_bytes())
        else {
            continue;
        };

        if params.nodeset.contains(node_id) {
            nodesets += 1;
        }

        if params.sequencer != node_id {
            continue;
        }

        sequencers += 1;

        let Some(partition_status) = alive.partitions.get(&u32::from(*log_id)) else {
            optimal = false;
            continue;
        };
        let planned = RunMode::try_from(partition_status.planned_mode).unwrap_or(RunMode::Unknown);
        let effective =
            RunMode::try_from(partition_status.effective_mode).unwrap_or(RunMode::Unknown);
        if planned != RunMode::Leader && effective != RunMode::Leader {
            optimal = false;
        }
    }

    (nodesets, sequencers, optimal)
}

fn node_runtime_details(
    node_runtime_info: Option<&Result<IdentResponse, String>>,
    include_extra: bool,
) -> (Option<u64>, NodeStatusView, Option<NodeObservedVersions>) {
    if !include_extra {
        return (None, NodeStatusView::default(), None);
    }

    match node_runtime_info {
        Some(Ok(node_runtime_info)) => (
            Some(node_runtime_info.age_s),
            NodeStatusView {
                metadata: optional_proto_enum(
                    node_runtime_info.metadata_server_status().as_str_name(),
                ),
                node: optional_proto_enum(node_runtime_info.status().as_str_name()),
                admin: optional_proto_enum(node_runtime_info.admin_status().as_str_name()),
                worker: optional_proto_enum(node_runtime_info.worker_status().as_str_name()),
                log_server: optional_proto_enum(
                    node_runtime_info.log_server_status().as_str_name(),
                ),
            },
            Some(NodeObservedVersions {
                nodes_config: node_runtime_info.nodes_config_version,
                logs: node_runtime_info.logs_version,
                schema: node_runtime_info.schema_version,
                partition_table: node_runtime_info.partition_table_version,
            }),
        ),
        Some(Err(_)) => (None, NodeStatusView::default(), None),
        None => (None, NodeStatusView::default(), None),
    }
}

fn build_logs_section(logs: &Logs) -> LogsSection {
    let entries = logs
        .iter()
        .sorted_by_key(|(log_id, _)| *log_id)
        .map(|(log_id, chain)| {
            let tail = chain.tail();
            if tail.config.kind == ProviderKind::Replicated
                && let Ok(params) =
                    ReplicatedLogletParams::deserialize_from(tail.config.params.as_bytes())
            {
                let mut nodeset = params
                    .nodeset
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>();
                nodeset.sort();
                return LogEntry {
                    log_id: (*log_id).into(),
                    segment_count: chain.num_segments(),
                    from_lsn: tail.base_lsn.into(),
                    provider_kind: tail.config.kind.to_string(),
                    loglet_id: Some(params.loglet_id.to_string()),
                    replication_property: Some(params.replication.clone()),
                    sequencer: Some(params.sequencer.to_string()),
                    nodeset: Some(nodeset),
                };
            }

            LogEntry {
                log_id: (*log_id).into(),
                segment_count: chain.num_segments(),
                from_lsn: tail.base_lsn.into(),
                provider_kind: tail.config.kind.to_string(),
                loglet_id: None,
                replication_property: None,
                sequencer: None,
                nodeset: None,
            }
        })
        .collect();

    LogsSection {
        version: logs.version().into(),
        default_provider: logs_provider_view(logs.configuration().default_provider.clone()),
        entries,
    }
}

fn build_partitions_section(cluster_state: &ClusterState) -> PartitionsSection {
    #[derive(Clone)]
    struct TempRow {
        partition_id: u32,
        host_node: GenerationalNodeId,
        status: restate_types::protobuf::cluster::PartitionProcessorStatus,
    }

    let mut rows = Vec::<TempRow>::new();
    let mut dead_nodes = Vec::<(PlainNodeId, DeadNodeRow)>::new();
    let mut max_epoch_per_partition = HashMap::<u32, u64>::new();

    for (node_id, node_state) in &cluster_state.nodes {
        match node_state.state.as_ref() {
            Some(node_state::State::Alive(alive)) => {
                let host_node = alive
                    .generational_node_id
                    .map(GenerationalNodeId::from)
                    .unwrap_or_else(|| PlainNodeId::from(*node_id).with_generation(0));

                for (partition_id, status) in &alive.partitions {
                    rows.push(TempRow {
                        partition_id: *partition_id,
                        host_node,
                        status: *status,
                    });
                    let epoch = status
                        .last_observed_leader_epoch
                        .map(|e| e.value)
                        .unwrap_or(0);
                    max_epoch_per_partition
                        .entry(*partition_id)
                        .and_modify(|current| *current = (*current).max(epoch))
                        .or_insert(epoch);
                }
            }
            Some(node_state::State::Dead(dead)) => {
                let plain_node_id = PlainNodeId::from(*node_id);
                dead_nodes.push((
                    plain_node_id,
                    DeadNodeRow {
                        node_id: plain_node_id.to_string(),
                        last_seen_alive_unix_millis: dead
                            .last_seen_alive
                            .as_ref()
                            .and_then(|ts| timestamp_to_unix_millis(ts.seconds, ts.nanos)),
                    },
                ));
            }
            None => {}
        }
    }

    dead_nodes.sort_by_key(|(node_id, _)| *node_id);

    let processors = rows
        .into_iter()
        .sorted_by(|a, b| {
            a.partition_id
                .cmp(&b.partition_id)
                .then_with(|| a.host_node.cmp(&b.host_node))
        })
        .map(|row| {
            let planned = RunMode::try_from(row.status.planned_mode).unwrap_or(RunMode::Unknown);
            let effective =
                RunMode::try_from(row.status.effective_mode).unwrap_or(RunMode::Unknown);
            let replay =
                ReplayStatus::try_from(row.status.replay_status).unwrap_or(ReplayStatus::Unknown);

            let observed_leader_epoch = row
                .status
                .last_observed_leader_epoch
                .as_ref()
                .map(|e| e.value);
            let observed_leader_node = row.status.last_observed_leader_node.as_ref().map(|node| {
                let generation = node.generation.unwrap_or_default();
                PlainNodeId::from(node.id)
                    .with_generation(generation)
                    .to_string()
            });

            let sees_itself_as_leader = row
                .status
                .last_observed_leader_node
                .as_ref()
                .and_then(|n| {
                    n.generation
                        .map(|g| PlainNodeId::from(n.id).with_generation(g) == row.host_node)
                })
                .unwrap_or(false);

            let leadership_epoch = observed_leader_epoch.unwrap_or(0);
            let leadership_epoch_outdated = leadership_epoch
                < max_epoch_per_partition
                    .get(&row.partition_id)
                    .copied()
                    .unwrap_or(0);

            let last_applied_lsn = row
                .status
                .last_applied_log_lsn
                .as_ref()
                .map(|lsn| lsn.value);
            let target_tail_lsn = row.status.target_tail_lsn.as_ref().map(|lsn| lsn.value);
            let lsn_lag = target_tail_lsn
                .zip(last_applied_lsn)
                .map(|(tail, applied)| tail.saturating_sub(applied.saturating_add(1)));

            PartitionProcessorEntry {
                partition_id: row.partition_id,
                host_node: row.host_node.to_string(),
                planned_mode: normalize_proto_enum(planned.as_str_name()),
                effective_mode: normalize_proto_enum(effective.as_str_name()),
                replay_status: normalize_proto_enum(replay.as_str_name()),
                observed_leader_epoch,
                observed_leader_node,
                last_applied_lsn,
                durable_lsn: row.status.durable_lsn.as_ref().map(|lsn| lsn.value),
                last_archived_lsn: row
                    .status
                    .last_archived_log_lsn
                    .as_ref()
                    .map(|lsn| lsn.value),
                target_tail_lsn,
                lsn_lag,
                updated_at_unix_millis: row
                    .status
                    .updated_at
                    .as_ref()
                    .and_then(|ts| timestamp_to_unix_millis(ts.seconds, ts.nanos)),
                sees_itself_as_leader,
                leadership_epoch_outdated,
            }
        })
        .collect();

    PartitionsSection {
        node_configuration_version: cluster_state
            .nodes_config_version
            .map(|v| v.value)
            .unwrap_or_default(),
        partition_table_version: cluster_state
            .partition_table_version
            .map(|v| v.value)
            .unwrap_or_default(),
        processors,
        dead_nodes: dead_nodes.into_iter().map(|(_, row)| row).collect(),
    }
}

fn build_metadata_servers_section(
    metadata_statuses: &MetadataServerStatusMap,
) -> MetadataServersSection {
    let mut servers = Vec::new();
    let mut unreachable_nodes = Vec::new();

    for (node_id, status) in metadata_statuses
        .iter()
        .sorted_by_key(|(node_id, _)| *node_id)
    {
        match status {
            Ok(status) => servers.push(MetadataServerEntry {
                node_id: node_id.to_string(),
                status: normalize_proto_enum(status.status().as_str_name()),
                configuration_version: status
                    .configuration
                    .as_ref()
                    .and_then(|config| config.version.map(|version| version.value)),
                leader: status
                    .leader
                    .map(|leader| PlainNodeId::new(leader).to_string()),
                members: status
                    .configuration
                    .as_ref()
                    .map(|config| {
                        config
                            .members
                            .keys()
                            .copied()
                            .map(PlainNodeId::from)
                            .sorted()
                            .map(|node_id| node_id.to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
                raft_applied: status
                    .raft
                    .as_ref()
                    .map(|raft| raft.applied)
                    .unwrap_or_default(),
                raft_committed: status
                    .raft
                    .as_ref()
                    .map(|raft| raft.committed)
                    .unwrap_or_default(),
                raft_term: status
                    .raft
                    .as_ref()
                    .map(|raft| raft.term)
                    .unwrap_or_default(),
                raft_log_length: status
                    .raft
                    .as_ref()
                    .map(|raft| {
                        raft.last_index
                            .saturating_add(1)
                            .saturating_sub(raft.first_index)
                    })
                    .unwrap_or_default(),
                snapshot_index: status
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.index)
                    .unwrap_or_default(),
                snapshot_size_bytes: status
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.size)
                    .unwrap_or_default(),
            }),
            Err(reason) => unreachable_nodes.push(UnreachableNodeRow {
                node_id: node_id.to_string(),
                error: reason.to_string(),
            }),
        }
    }

    MetadataServersSection {
        servers,
        unreachable_nodes,
    }
}

fn compact_metadata_status(
    metadata_status: Option<&Result<MetadataServerStatusResponse, tonic::Status>>,
) -> String {
    match metadata_status {
        Some(Ok(status)) => normalize_proto_enum(status.status().as_str_name()),
        Some(Err(status)) if status.code() == Code::Unimplemented => "local".to_owned(),
        Some(Err(_)) => "unreachable".to_owned(),
        None => normalize_proto_enum(MetadataServerStatus::Unknown.as_str_name()),
    }
}

fn normalize_proto_enum(name: &str) -> String {
    // The protobuf `as_str_name()` values passed here follow the `EnumPrefix_VARIANT` pattern.
    name.split_once('_')
        .map(|(_, suffix)| suffix)
        .unwrap_or(name)
        .to_ascii_lowercase()
}

fn optional_proto_enum(name: &str) -> Option<String> {
    let value = normalize_proto_enum(name);
    (value != "unknown").then_some(value)
}

fn normalize_display_enum(value: impl ToString) -> String {
    value.to_string().replace('-', "_")
}

fn string_or_none(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn timestamp_to_unix_millis(seconds: i64, nanos: i32) -> Option<i64> {
    let seconds_millis = seconds.checked_mul(1_000)?;
    let nanos_millis = i64::from(nanos) / 1_000_000;
    seconds_millis.checked_add(nanos_millis)
}
