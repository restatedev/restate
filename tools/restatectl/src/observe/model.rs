// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster as the status, partitions and logs views see it. Every type serializes to the
//! `--json` output of those commands, so field names are part of restatectl's interface.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Serialize;

use restate_core::protobuf::node_ctl_svc::IdentResponse;
use restate_types::health::MetadataServerStatus;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::{ProviderConfiguration, ProviderKind};
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::{
    BrokenReason, DetailedRunMode, PartitionProcessorStatus, ReplayStatus, RunMode,
};
use restate_types::replication::NodeSetChecker;
use restate_types::{GenerationalNodeId, PlainNodeId, Version, Versioned};

use super::sql::{LogletWorkerRow, SqlData};
use super::{Sample, Snapshot};
use crate::commands::log::deserialize_replicated_log_params;

#[derive(Serialize)]
pub struct ClusterView {
    pub cluster_name: String,
    pub captured_at: DateTime<Utc>,
    pub versions: Versions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_replication: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_provider: Option<LogProvider>,
    pub nodes: Vec<NodeView>,
    pub partitions: Vec<PartitionView>,
    pub logs: Vec<LogView>,
    /// Data sources that could not be read; the views lack what they would have contributed.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// whether nodes were asked for their own report
    #[serde(skip)]
    pub nodes_probed: bool,
    /// whether the cluster controller's view of the nodes and partition processors was read;
    /// without it, which nodes are alive and which processors run is unknown
    #[serde(skip)]
    pub has_cluster_state: bool,
    /// the file the status was read from, when it isn't the cluster's current one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

/// The error of a metadata server that can't report its status: it isn't replicated.
pub const NOT_REPLICATED: &str = "not a replicated metadata server";

#[derive(Serialize, Clone, PartialEq, Eq)]
pub struct Versions {
    pub nodes_config: u32,
    pub partition_table: Option<u32>,
    pub logs: Option<u32>,
    pub schema: Option<u32>,
}

#[derive(Serialize)]
pub struct LogProvider {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<String>,
    /// `0` lets the cluster pick the nodeset size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodeset_size: Option<u16>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Liveness {
    Alive,
    Dead,
    /// The cluster controller has no record of the node, or its view couldn't be read
    Unknown,
}

#[derive(Serialize)]
pub struct NodeView {
    #[serde(serialize_with = "ser::display")]
    pub id: GenerationalNodeId,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub location: String,
    /// the fabric address, for callers without a more specific entry
    pub address: String,
    /// fabric addresses for callers in specific locations, in the node's order of preference
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<LocatedAddress>,
    /// the control address; the fabric address when the node advertises none
    pub ctrl_address: String,
    /// control addresses for callers in specific locations, in the node's order of preference
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ctrl_addresses: Vec<LocatedAddress>,
    /// the control address restatectl dials to reach the node, unless it only talks to one
    /// address
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialed_address: Option<String>,
    /// the node's servers as it reports them (ingress, admin, fabric, control); empty when it
    /// couldn't be reached
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<ServerAddresses>,
    pub roles: Vec<String>,
    pub liveness: Liveness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_alive: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary_version: Option<String>,
    /// Log-server storage state, for nodes with the log-server role
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_state: Option<String>,
    /// Worker state, for nodes with the worker role
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_server: Option<MetadataServerView>,
    /// Metadata versions this node has loaded, when it could be reached
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seen_versions: Option<Versions>,
    pub partitions: ReplicaCounts,
    pub logs: LogCounts,
}

impl NodeView {
    pub fn plain(&self) -> PlainNodeId {
        self.id.as_plain()
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

/// Where one of a node's servers is advertised and where it listens, as the node reports it.
#[derive(Serialize, Clone)]
pub struct ServerAddresses {
    /// the server as the node names it, e.g. `http-ingress-server`
    pub server: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertised: Option<String>,
    /// TCP addresses and Unix sockets (`unix:<path>`) it listens on
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub listens_on: Vec<String>,
}

/// An address a node advertises for callers in a location.
#[derive(Serialize, Clone)]
pub struct LocatedAddress {
    /// the callers' location, or every caller when unset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub address: String,
}

#[derive(Serialize, Default)]
pub struct ReplicaCounts {
    pub leaders: u32,
    pub followers: u32,
    /// becoming leader, candidates and leaders stepping down
    pub transitioning: u32,
    pub catching_up: u32,
    pub broken: u32,
}

#[derive(Serialize, Default)]
pub struct LogCounts {
    /// tail segments this node sequences
    pub sequencer: u32,
    /// of which the partition's leader runs elsewhere
    pub sequencer_away_from_leader: u32,
    /// tail segments whose nodeset includes this node
    pub nodeset: u32,
    /// read streams open on this node
    pub readers: u32,
}

#[derive(Serialize)]
pub struct MetadataServerView {
    /// configured membership state
    pub state: String,
    /// state reported by the running server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(serialize_with = "ser::opt_display")]
    pub leader: Option<PlainNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_version: Option<u32>,
    #[serde(serialize_with = "ser::seq_display")]
    pub members: Vec<PlainNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raft: Option<RaftView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct RaftView {
    pub term: u64,
    pub applied: u64,
    pub committed: u64,
    pub log_length: u64,
    pub snapshot_index: u64,
    pub snapshot_size: u64,
}

#[derive(Serialize)]
pub struct PartitionView {
    pub id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_range: Option<KeyRangeView>,
    pub health: PartitionHealth,
    /// the replica running as the leader in the newest epoch
    #[serde(serialize_with = "ser::opt_display")]
    pub leader: Option<GenerationalNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// replica set observed by the cluster; `next` is set while it's being reconfigured
    #[serde(skip_serializing_if = "Option::is_none")]
    pub membership: Option<Membership>,
    /// partition processors running on alive nodes
    pub replicas: Vec<ReplicaView>,
    /// current members whose node runs no processor for the partition
    #[serde(serialize_with = "ser::seq_display")]
    pub missing: Vec<PlainNodeId>,
    #[serde(serialize_with = "ser::opt_display")]
    pub sequencer: Option<GenerationalNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_tail_lsn: Option<u64>,
}

impl PartitionView {
    pub fn leader_replica(&self) -> Option<&ReplicaView> {
        self.leader
            .and_then(|leader| self.replicas.iter().find(|r| r.node == leader))
    }

    pub fn is_led(&self) -> bool {
        self.leader_replica().is_some_and(|r| r.is_healthy())
    }

    pub fn replica_on(&self, node: PlainNodeId) -> Option<&ReplicaView> {
        self.replicas.iter().find(|r| r.node.as_plain() == node)
    }

    pub fn is_member(&self, node: PlainNodeId) -> bool {
        self.membership
            .as_ref()
            .is_some_and(|m| m.members.contains(&node))
    }

    /// Whether `replica` counts towards the partition's health: it's in the current or next
    /// replica set, leads, or the replica sets are unknown.
    pub fn counts(&self, replica: &ReplicaView) -> bool {
        self.membership.is_none()
            || replica.member
            || replica.next_member
            || replica.mode == Mode::Leader
    }

    pub fn is_next_member(&self, node: PlainNodeId) -> bool {
        self.membership
            .as_ref()
            .and_then(|m| m.next.as_ref())
            .is_some_and(|n| n.members.contains(&node))
    }

    /// The replicas that report leading the newest epoch, when more than one does.
    pub fn rival_leaders(&self) -> Vec<GenerationalNodeId> {
        let leaders: Vec<_> = self
            .replicas
            .iter()
            .filter(|r| {
                r.mode == Mode::Leader
                    && !r.stale_epoch
                    && r.epoch.is_some()
                    && r.epoch == self.epoch
            })
            .map(|r| r.node)
            .collect();
        if leaders.len() > 1 {
            leaders
        } else {
            Vec::new()
        }
    }
}

#[derive(Serialize, Clone, Copy)]
pub struct KeyRangeView {
    pub start: u64,
    pub end: u64,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionHealth {
    Healthy,
    /// healthy, while its replica set changes
    Reconfiguring,
    /// led, with replicas missing or not running normally
    Degraded,
    /// without an active leader, or with a log that can't take appends
    Unavailable,
    /// its processors couldn't be observed
    Unknown,
}

#[derive(Serialize)]
pub struct Membership {
    pub version: u32,
    #[serde(serialize_with = "ser::seq_display")]
    pub members: Vec<PlainNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<NextMembership>,
}

#[derive(Serialize)]
pub struct NextMembership {
    pub version: u32,
    #[serde(serialize_with = "ser::seq_display")]
    pub members: Vec<PlainNodeId>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    Leader,
    Follower,
    /// asked to lead, not leading yet
    Candidate,
    /// elected, preparing to serve (migrations, feature enablement)
    BecomingLeader,
    /// leading, asked to follow
    SteppingDown,
    /// parked by the node, see `broken_reason`
    Broken,
    Unknown,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Replay {
    Starting,
    Active,
    CatchingUp,
    Unknown,
}

#[derive(Serialize)]
pub struct ReplicaView {
    #[serde(serialize_with = "ser::display")]
    pub node: GenerationalNodeId,
    pub mode: Mode,
    pub replay: Replay,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broken_reason: Option<&'static str>,
    /// member of the current replica set
    pub member: bool,
    /// member of the next replica set, if one is being installed
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub next_member: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// claims leadership in an epoch older than the newest one observed
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub stale_epoch: bool,
    pub applied_lsn: Option<u64>,
    pub durable_lsn: Option<u64>,
    pub archived_lsn: Option<u64>,
    pub target_tail_lsn: Option<u64>,
    /// records between the applied LSN and the log tail
    pub lag: Option<u64>,
    /// LSN of the record that turned byte offsets on for the partition, which they count from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_offsets_base_lsn: Option<u64>,
    /// how far the replica applied its log, in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_byte_offset: Option<u64>,
    /// the log byte offset of the snapshot at the archived LSN, and the LSN it counts from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived_log_byte_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived_byte_offsets_base_lsn: Option<u64>,
    pub schema_version: Option<u32>,
    pub rule_book_version: Option<u32>,
    pub features: Vec<String>,
    pub storage_features: Vec<String>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_record_applied_at: Option<DateTime<Utc>>,
    /// the node's tailing read stream of the partition's log
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader: Option<ReaderView>,
}

impl ReplicaView {
    /// Runs normally: leads or follows, active, in the newest epoch. How far it is behind the
    /// log tail doesn't count: lag comes and goes with load and has no threshold that fits
    /// every cluster.
    pub fn is_healthy(&self) -> bool {
        matches!(self.mode, Mode::Leader | Mode::Follower)
            && self.replay == Replay::Active
            && !self.stale_epoch
    }
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum SequencerState {
    /// runs on the sequencer node's current generation
    Alive,
    /// the node restarted since; the segment needs a reconfiguration
    Restarted,
    Dead,
    Unknown,
}

#[derive(Serialize)]
pub struct LogView {
    pub id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<u32>,
    pub segments: usize,
    pub tail_segment: u32,
    pub base_lsn: u64,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loglet_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u8>,
    #[serde(serialize_with = "ser::opt_display")]
    pub sequencer: Option<GenerationalNodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequencer_state: Option<SequencerState>,
    #[serde(serialize_with = "ser::seq_display")]
    pub nodeset: Vec<PlainNodeId>,
    /// nodeset members that are alive and accept writes; unknown without the nodes' liveness
    pub writable_nodes: Option<usize>,
    /// whether the writable members are enough for the log's replication, across its
    /// failure domains
    pub write_quorum: Option<bool>,
    /// first LSN past the last committed record
    pub tail_lsn: Option<u64>,
    /// last trimmed LSN
    pub trim_point: Option<u64>,
    /// records between the trim point (or the log's start) and the tail
    pub retained: Option<u64>,
    /// the tail segment is sealed; writes wait for the chain to be extended
    pub sealed: bool,
    #[serde(serialize_with = "ser::opt_display")]
    pub partition_leader: Option<GenerationalNodeId>,
    /// read streams of this log, on any node
    pub readers: Vec<ReaderView>,
    /// the segments of the log's chain, oldest first
    pub chain: Vec<SegmentView>,
}

/// A segment of a log's chain.
#[derive(Serialize, Clone, Debug)]
pub struct SegmentView {
    pub index: u32,
    pub base_lsn: u64,
    pub kind: String,
    #[serde(serialize_with = "ser::opt_display")]
    pub sequencer: Option<GenerationalNodeId>,
}

/// What a read stream is doing, from its state machine's state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReaderPhase {
    Reading,
    /// starting, or moving to another segment after a reconfiguration
    Switching,
    /// reached the end of a sealed loglet and waits for the chain to be sealed
    AwaitingSeal,
    /// gave up waiting and seals the chain itself
    Sealing,
}

/// A bifrost read stream.
#[derive(Serialize, Clone, Debug)]
pub struct ReaderView {
    #[serde(serialize_with = "ser::display")]
    pub node: PlainNodeId,
    /// state of the read stream, e.g. `Reading` or `AwaitingOrSealChain`
    pub state: String,
    /// reads up to the tail as records are appended
    pub tailing: bool,
    /// next LSN to read
    pub read_pointer: Option<u64>,
    /// last LSN a bounded read stream reads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_lsn: Option<u64>,
    /// index of the segment it reads from
    pub segment: Option<u32>,
    /// records between the read pointer and the log's tail
    pub behind: Option<u64>,
    /// who reads, as the stream's owner names it, e.g. `pp-3` for partition 3's processor
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// what the owner does with the stream, e.g. `leader` or `follower`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// records the stream delivered since it was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_read: Option<u64>,
    /// the size of those records, as the log stores them
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_read: Option<u64>,
}

impl ReaderView {
    /// Who reads and what for, e.g. `pp-3 leader`, if the server reports it.
    pub fn owner(&self) -> Option<String> {
        match (&self.name, &self.role) {
            (Some(name), Some(role)) => Some(format!("{name} {role}")),
            (name, role) => name.clone().or_else(|| role.clone()),
        }
    }

    pub fn phase(&self) -> ReaderPhase {
        match self.state.as_str() {
            "Reading" => ReaderPhase::Reading,
            "AwaitingOrSealChain" => ReaderPhase::AwaitingSeal,
            "SealingChain" => ReaderPhase::Sealing,
            _ => ReaderPhase::Switching,
        }
    }
}

impl LogView {
    /// The log can't take appends until it's reconfigured or nodes come back.
    pub fn is_down(&self) -> bool {
        self.sequencer_state == Some(SequencerState::Dead) || self.write_quorum == Some(false)
    }

    /// Some nodeset members can't take writes, while enough others can.
    pub fn is_nodeset_degraded(&self) -> bool {
        self.write_quorum == Some(true)
            && self
                .writable_nodes
                .is_some_and(|writable| writable < self.nodeset.len())
    }

    /// the sequencer runs on the node leading the log's partition
    pub fn sequencer_with_leader(&self) -> Option<bool> {
        Some(self.sequencer?.as_plain() == self.partition_leader?.as_plain())
    }
}

impl ClusterView {
    pub fn build(snapshot: Snapshot) -> Self {
        let Snapshot {
            nodes_config,
            partition_table,
            logs,
            idents,
            mut metadata_servers,
            mut tables,
            nodes_probed,
            mut dialed,
            mut warnings,
            sample,
        } = snapshot;
        let Sample {
            captured_at,
            cluster_state,
            tables: sampled,
            warnings: sample_warnings,
        } = sample;
        warnings.extend(sample_warnings);
        tables.extend(sampled);
        let sql = SqlData::parse(&tables, &mut warnings);
        let has_cluster_state = cluster_state.is_some();

        let liveness = |node: PlainNodeId| -> (Liveness, Option<&State>) {
            let Some(state) = cluster_state.as_ref() else {
                return (Liveness::Unknown, None);
            };
            match state
                .nodes
                .get(&u32::from(node))
                .and_then(|s| s.state.as_ref())
            {
                Some(s @ State::Alive(_)) => (Liveness::Alive, Some(s)),
                Some(s @ State::Dead(_)) => (Liveness::Dead, Some(s)),
                None => (Liveness::Unknown, None),
            }
        };

        // -- nodes
        let mut nodes = Vec::with_capacity(nodes_config.len());
        for (node_id, config) in nodes_config.iter().sorted_by_key(|(id, _)| *id) {
            let (liveness, state) = liveness(node_id);
            let (id, uptime_s, last_seen_alive) = match state {
                Some(State::Alive(alive)) => (
                    alive
                        .generational_node_id
                        .map(GenerationalNodeId::from)
                        .unwrap_or(config.current_generation),
                    Some(alive.uptime_s),
                    None,
                ),
                Some(State::Dead(dead)) => (
                    config.current_generation,
                    None,
                    timestamp(dead.last_seen_alive),
                ),
                None => (config.current_generation, None, None),
            };
            let ident = idents.get(&node_id);

            nodes.push(NodeView {
                id,
                name: config.name.clone(),
                location: config.location.to_string(),
                address: config.address.to_string(),
                addresses: config
                    .addresses
                    .iter()
                    .map(|a| LocatedAddress {
                        location: a.location.as_ref().map(|l| l.to_string()),
                        address: a.address.to_string(),
                    })
                    .collect(),
                ctrl_address: config.ctrl_address().to_string(),
                ctrl_addresses: config
                    .ctrl_addresses
                    .iter()
                    .map(|a| LocatedAddress {
                        location: a.location.as_ref().map(|l| l.to_string()),
                        address: a.address.to_string(),
                    })
                    .collect(),
                dialed_address: dialed.remove(&node_id),
                servers: ident.map(server_addresses).unwrap_or_default(),
                roles: config
                    .roles
                    .iter()
                    .map(|r| r.to_string())
                    .sorted()
                    .collect(),
                liveness,
                uptime_s,
                last_seen_alive,
                binary_version: ident
                    .map(|i| i.restate_version.clone())
                    .filter(|v| !v.is_empty()),
                storage_state: config
                    .has_role(Role::LogServer)
                    .then(|| config.log_server_config.storage_state.to_string()),
                worker_state: config
                    .has_role(Role::Worker)
                    .then(|| config.worker_config.worker_state.to_string()),
                metadata_server: config.has_role(Role::MetadataServer).then(|| {
                    metadata_server_view(
                        config
                            .metadata_server_config
                            .metadata_server_state
                            .to_string(),
                        metadata_servers.remove(&node_id),
                    )
                }),
                seen_versions: ident.map(|i| Versions {
                    nodes_config: i.nodes_config_version,
                    partition_table: Some(i.partition_table_version),
                    logs: Some(i.logs_version),
                    schema: Some(i.schema_version),
                }),
                partitions: ReplicaCounts::default(),
                logs: LogCounts::default(),
            });
        }

        // -- partition processors, grouped by partition
        let mut processors: HashMap<u32, Vec<(GenerationalNodeId, &PartitionProcessorStatus)>> =
            HashMap::new();
        if let Some(state) = &cluster_state {
            for node_state in state.nodes.values() {
                let Some(State::Alive(alive)) = &node_state.state else {
                    continue;
                };
                let Some(node) = alive.generational_node_id.map(GenerationalNodeId::from) else {
                    continue;
                };
                for (partition_id, status) in &alive.partitions {
                    processors
                        .entry(*partition_id)
                        .or_default()
                        .push((node, status));
                }
            }
        }

        let partition_ids: BTreeSet<u32> = match &partition_table {
            Some(table) => table.iter_ids().map(|id| u32::from(*id)).collect(),
            None => processors.keys().copied().collect(),
        };
        let desired_copies = partition_table
            .as_ref()
            .map(|t| t.replication_property(&nodes_config).num_copies());

        // log id -> partition id
        let mut partition_of_log = HashMap::new();
        let mut partitions = Vec::with_capacity(partition_ids.len());
        for partition_id in partition_ids {
            let partition = partition_table
                .as_ref()
                .and_then(|t| t.get(&PartitionId::from(partition_id as u16)));
            let log_id = partition.map(|p| u32::from(p.log_id()));
            if let Some(log_id) = log_id {
                partition_of_log.insert(log_id, partition_id);
            }

            let membership = sql.memberships.get(&partition_id).and_then(|row| {
                Some(Membership {
                    version: row.v_current?,
                    members: parse_node_list(row.current_replica_set.as_deref()?),
                    next: row.v_next.zip(row.next_replica_set.as_deref()).map(
                        |(version, members)| NextMembership {
                            version,
                            members: parse_node_list(members),
                        },
                    ),
                })
            });

            let running = processors.remove(&partition_id).unwrap_or_default();
            let max_epoch = running
                .iter()
                .filter_map(|(_, s)| s.last_observed_leader_epoch.map(|e| e.value))
                .max();
            let mut replicas: Vec<ReplicaView> = running
                .into_iter()
                .map(|(node, status)| replica_view(node, status, max_epoch))
                .collect();
            replicas.sort_by_key(|r| r.node);
            for replica in &mut replicas {
                if let Some(m) = &membership {
                    replica.member = m.members.contains(&replica.node.as_plain());
                    replica.next_member = m
                        .next
                        .as_ref()
                        .is_some_and(|n| n.members.contains(&replica.node.as_plain()));
                }
            }

            let leader = replicas
                .iter()
                .filter(|r| {
                    matches!(
                        r.mode,
                        Mode::Leader | Mode::BecomingLeader | Mode::SteppingDown
                    ) && !r.stale_epoch
                })
                .max_by_key(|r| r.epoch)
                .map(|r| r.node);

            let missing = membership
                .as_ref()
                .map(|m| {
                    m.members
                        .iter()
                        .filter(|member| !replicas.iter().any(|r| r.node.as_plain() == **member))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();

            let mut view = PartitionView {
                id: partition_id,
                log_id,
                key_range: partition.map(|p| KeyRangeView {
                    start: p.key_range.start(),
                    end: p.key_range.end(),
                }),
                health: PartitionHealth::Unknown,
                leader,
                epoch: max_epoch,
                membership,
                replicas,
                missing,
                sequencer: None,
                log_tail_lsn: None,
            };
            if has_cluster_state {
                view.health = classify(&view, desired_copies);
            } else {
                view.missing.clear();
            }
            partitions.push(view);
        }

        // -- logs
        let alive_generation = |node: PlainNodeId| {
            nodes
                .iter()
                .find(|n| n.plain() == node)
                .map(|n| (n.liveness, n.id))
        };
        let writable = |node: &PlainNodeId| {
            nodes_config
                .find_node_by_id(*node)
                .is_ok_and(|config| config.log_server_config.storage_state.can_write_to())
                && alive_generation(*node).is_some_and(|(l, _)| l == Liveness::Alive)
        };

        let mut log_views = Vec::new();
        let log_provider = logs
            .as_ref()
            .map(|logs| log_provider(&logs.configuration().default_provider));
        if let Some(logs) = &logs {
            for (log_id, chain) in logs.iter().sorted_by_key(|(id, _)| **id) {
                let log_id = u32::from(*log_id);
                let tail = chain.tail();
                let params = deserialize_replicated_log_params(&tail);
                let all_workers = sql
                    .loglet_workers
                    .get(&log_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let workers = all_workers
                    .iter()
                    .filter(|w| w.segment_index == u32::from(tail.index()))
                    .collect::<Vec<_>>();
                let segment_base_lsns: HashMap<u32, u64> = chain
                    .iter()
                    .map(|s| (u32::from(s.index()), s.base_lsn.as_u64()))
                    .collect();
                // trim points are loglet offsets in whichever segment the log is trimmed to
                let trim_point = all_workers
                    .iter()
                    .filter(|w| w.trim_point.is_some_and(|t| t > 0))
                    .filter_map(|w| {
                        Some(offset_to_lsn(
                            *segment_base_lsns.get(&w.segment_index)?,
                            w.trim_point?,
                        ))
                    })
                    .max();
                let first_lsn = chain
                    .iter()
                    .next()
                    .map_or(1, |s| s.base_lsn.as_u64())
                    .max(trim_point.map_or(1, |t| t + 1));
                let partition_id = partition_of_log.get(&log_id).copied();
                let partition_leader = partition_id
                    .and_then(|p| partitions.iter().find(|v| v.id == p).and_then(|v| v.leader));
                let base_lsn = tail.base_lsn.as_u64();

                // which members can take writes is known only with their liveness
                let (writable_nodes, write_quorum) = match &params {
                    Some(p) if has_cluster_state => {
                        let members: Vec<PlainNodeId> =
                            p.nodeset.iter().copied().filter(|n| writable(n)).collect();
                        let mut checker =
                            NodeSetChecker::new(&p.nodeset, &nodes_config, &p.replication);
                        checker.set_attribute_on_each(members.iter().copied(), true);
                        (
                            Some(members.len()),
                            Some(checker.check_write_quorum(|writable| *writable)),
                        )
                    }
                    _ => (None, None),
                };
                let sequencer_state =
                    params
                        .as_ref()
                        .map(|p| match alive_generation(p.sequencer.as_plain()) {
                            Some((Liveness::Alive, current)) if current == p.sequencer => {
                                SequencerState::Alive
                            }
                            Some((Liveness::Alive, _)) => SequencerState::Restarted,
                            Some((Liveness::Dead, _)) => SequencerState::Dead,
                            _ => SequencerState::Unknown,
                        });

                log_views.push(LogView {
                    id: log_id,
                    partition_id,
                    segments: chain.num_segments(),
                    tail_segment: u32::from(tail.index()),
                    base_lsn,
                    kind: tail.config.kind.to_string(),
                    created_at: tail.config.created_at.and_then(|t| {
                        DateTime::from_timestamp_millis(t.to_unix_millis().as_u64() as i64)
                    }),
                    loglet_id: params.as_ref().map(|p| p.loglet_id.to_string()),
                    replication: params.as_ref().map(|p| p.replication.to_string()),
                    replication_factor: params.as_ref().map(|p| p.replication.num_copies()),
                    sequencer: params.as_ref().map(|p| p.sequencer),
                    sequencer_state,
                    nodeset: params
                        .as_ref()
                        .map(|p| p.nodeset.iter().copied().sorted().collect())
                        .unwrap_or_default(),
                    writable_nodes,
                    write_quorum,
                    tail_lsn: tail_lsn(base_lsn, &workers),
                    retained: tail_lsn(base_lsn, &workers).map(|t| t.saturating_sub(first_lsn)),
                    trim_point,
                    sealed: workers.iter().any(|w| w.is_sealed == Some(true))
                        || tail.config.kind.is_seal_marker(),
                    partition_leader,
                    readers: readers(
                        sql.read_streams
                            .get(&log_id)
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                        tail_lsn(base_lsn, &workers),
                    ),
                    chain: chain
                        .iter()
                        .map(|segment| SegmentView {
                            index: u32::from(segment.index()),
                            base_lsn: segment.base_lsn.as_u64(),
                            kind: segment.config.kind.to_string(),
                            sequencer: deserialize_replicated_log_params(&segment)
                                .map(|p| p.sequencer),
                        })
                        .collect(),
                });
            }
        }

        // -- cross references
        // nodes that name their read streams, from v1.8.2
        let naming: HashSet<PlainNodeId> = log_views
            .iter()
            .flat_map(|l| &l.readers)
            .filter(|r| r.name.is_some())
            .map(|r| r.node)
            .collect();
        for partition in &mut partitions {
            if let Some(log) = partition
                .log_id
                .and_then(|id| log_views.iter().find(|l| l.id == id))
            {
                partition.sequencer = log.sequencer;
                partition.log_tail_lsn = log.tail_lsn;
                // the processor names its reader after the partition; on nodes that don't name
                // readers, the node's tailing reader is the replica's
                let name = format!("pp-{}", partition.id);
                for replica in &mut partition.replicas {
                    let node = replica.node.as_plain();
                    let mut tailing = log.readers.iter().filter(|r| r.tailing && r.node == node);
                    replica.reader = if naming.contains(&node) {
                        tailing.find(|r| r.name.as_deref() == Some(name.as_str()))
                    } else {
                        tailing.next()
                    }
                    .cloned();
                }
                // a leader can't make progress without a writable log
                if log.is_down() {
                    partition.health = PartitionHealth::Unavailable;
                }
            }
        }
        for node in &mut nodes {
            let plain = node.plain();
            for partition in &partitions {
                let Some(replica) = partition.replica_on(plain) else {
                    continue;
                };
                let counts = &mut node.partitions;
                match replica.mode {
                    Mode::Leader => counts.leaders += 1,
                    Mode::Follower => counts.followers += 1,
                    Mode::Candidate | Mode::BecomingLeader | Mode::SteppingDown => {
                        counts.transitioning += 1
                    }
                    Mode::Broken => counts.broken += 1,
                    Mode::Unknown => {}
                }
                if replica.replay == Replay::CatchingUp {
                    counts.catching_up += 1;
                }
            }
            for log in &log_views {
                if log.nodeset.contains(&plain) {
                    node.logs.nodeset += 1;
                }
                node.logs.readers += log.readers.iter().filter(|r| r.node == plain).count() as u32;
                if log.sequencer.is_some_and(|s| s.as_plain() == plain) {
                    node.logs.sequencer += 1;
                    if log.sequencer_with_leader() == Some(false) {
                        node.logs.sequencer_away_from_leader += 1;
                    }
                }
            }
        }

        let versions = Versions {
            nodes_config: nodes_config.version().into(),
            partition_table: partition_table.as_ref().map(|t| t.version().into()),
            logs: logs.as_ref().map(|l| l.version().into()),
            // the nodes' own reports, else what the partition processors have applied
            schema: nodes
                .iter()
                .filter_map(|n| n.seen_versions.as_ref()?.schema)
                .max()
                .or_else(|| {
                    partitions
                        .iter()
                        .flat_map(|p| &p.replicas)
                        .filter_map(|r| r.schema_version)
                        .max()
                }),
        };

        ClusterView {
            cluster_name: nodes_config.cluster_name().to_owned(),
            captured_at,
            versions,
            partition_replication: partition_table
                .as_ref()
                .map(|t| t.replication_property(&nodes_config).to_string()),
            log_provider,
            nodes,
            partitions,
            logs: log_views,
            warnings,
            nodes_probed,
            has_cluster_state,
            file: None,
        }
    }

    pub fn node(&self, id: PlainNodeId) -> Option<&NodeView> {
        self.nodes.iter().find(|n| n.plain() == id)
    }

    pub fn alive_nodes(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.liveness == Liveness::Alive)
            .count()
    }

    /// How much of `log` lies between its partition's archived snapshot and its tail. The
    /// archived snapshot is the oldest one the partition retains (the only one by default), and
    /// the newest its replicas report. `None` if the log has no partition, or the partition no
    /// archived snapshot.
    pub fn snapshot_to_tail(&self, log: &LogView) -> Option<SnapshotToTail> {
        let partition = self
            .partitions
            .iter()
            .find(|p| Some(p.id) == log.partition_id)?;
        // of the replicas reporting the same snapshot, one that reports its byte offset: older
        // servers don't
        let snapshot = partition
            .replicas
            .iter()
            .max_by_key(|r| (r.archived_lsn, r.archived_log_byte_offset.is_some()))?;
        let snapshot_lsn = snapshot.archived_lsn?;
        // The bytes between the snapshot and the replica furthest along that counts from the same
        // base LSN: the log the replicas applied after the snapshot. Replicas' offsets may differ
        // slightly, so that replica's can be behind the snapshot's; then the bytes are unknown.
        let bytes = snapshot
            .archived_log_byte_offset
            .zip(snapshot.archived_byte_offsets_base_lsn)
            .and_then(|(snapshot_offset, base)| {
                let (applied, offset) = partition
                    .replicas
                    .iter()
                    .filter(|r| r.byte_offsets_base_lsn == Some(base))
                    .filter_map(|r| Some((r.applied_lsn?, r.log_byte_offset?)))
                    .filter(|(applied, _)| *applied >= snapshot_lsn)
                    .max_by_key(|(applied, _)| *applied)?;
                Some(SnapshotBytes {
                    bytes: offset.checked_sub(snapshot_offset)?,
                    unapplied: log.tail_lsn.map(|tail| tail.saturating_sub(applied + 1)),
                })
            });
        Some(SnapshotToTail {
            snapshot_lsn,
            bytes,
        })
    }

    /// The replicated metadata servers' members and how many are up, as far as their reports
    /// tell. `None` without replicated metadata servers, or when the nodes weren't asked.
    pub fn metadata_members(&self) -> Option<MetadataMembers> {
        let servers: Vec<(&NodeView, &MetadataServerView)> = self
            .nodes
            .iter()
            .filter_map(|n| Some((n, n.metadata_server.as_ref()?)))
            .collect();
        let reported = servers.iter().any(|(_, m)| m.status.is_some());
        let not_replicated = servers
            .iter()
            .any(|(_, m)| m.error.as_deref() == Some(NOT_REPLICATED));
        if !self.nodes_probed || servers.is_empty() || (!reported && not_replicated) {
            return None;
        }
        // the newest configuration a member reports, else the nodes configuration's
        let members: Vec<PlainNodeId> = servers
            .iter()
            .filter(|(_, m)| m.status.as_deref() == Some("member"))
            .max_by_key(|(_, m)| m.config_version)
            .map(|(_, m)| m.members.clone())
            .unwrap_or_else(|| {
                servers
                    .iter()
                    .filter(|(_, m)| m.state == "member")
                    .map(|(n, _)| n.plain())
                    .collect()
            });
        let server = |id: &PlainNodeId| servers.iter().find(|(n, _)| n.plain() == *id);
        Some(MetadataMembers {
            up: members
                .iter()
                .filter(|id| server(id).is_some_and(|(_, m)| m.status.as_deref() == Some("member")))
                .count(),
            unanswered: members
                .iter()
                .filter(|id| {
                    server(id)
                        .is_none_or(|(n, m)| m.status.is_none() && n.liveness != Liveness::Dead)
                })
                .count(),
            members,
            reported,
        })
    }
}

/// How much of a log lies between its partition's archived snapshot and its tail.
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotToTail {
    /// the applied LSN of the snapshot
    pub snapshot_lsn: u64,
    /// on servers that report the snapshot's byte offset (v1.8.2 and newer, with byte offsets
    /// on), if a replica's counts from the same base LSN and isn't behind it
    pub bytes: Option<SnapshotBytes>,
}

/// See [`SnapshotToTail::bytes`].
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotBytes {
    /// the log bytes between the snapshot and the replica furthest along
    pub bytes: u64,
    /// the records between that replica's applied LSN and the tail, which `bytes` doesn't count;
    /// `None` if the tail is unknown
    pub unapplied: Option<u64>,
}

/// See [`ClusterView::metadata_members`].
pub struct MetadataMembers {
    pub members: Vec<PlainNodeId>,
    /// members that report themselves as members
    pub up: usize,
    /// members that didn't answer, though they aren't known to be dead
    pub unanswered: usize,
    /// whether any server reported its status
    pub reported: bool,
}

fn classify(partition: &PartitionView, desired_copies: Option<u8>) -> PartitionHealth {
    if !partition.is_led() {
        return PartitionHealth::Unavailable;
    }
    let under_replicated = match (&partition.membership, desired_copies) {
        (Some(m), Some(copies)) => m.members.len() < usize::from(copies),
        _ => false,
    };
    if under_replicated
        || !partition.missing.is_empty()
        || !partition.rival_leaders().is_empty()
        || partition
            .replicas
            .iter()
            .any(|r| partition.counts(r) && !r.is_healthy())
    {
        return PartitionHealth::Degraded;
    }
    if partition
        .membership
        .as_ref()
        .is_some_and(|m| m.next.is_some())
    {
        return PartitionHealth::Reconfiguring;
    }
    PartitionHealth::Healthy
}

fn replica_view(
    node: GenerationalNodeId,
    status: &PartitionProcessorStatus,
    max_epoch: Option<u64>,
) -> ReplicaView {
    let broken_reason = match status.broken_reason() {
        BrokenReason::NotBroken => None,
        BrokenReason::AheadOfLog => Some("ahead-of-log"),
    };
    let planned = status.planned_mode();
    let detailed = match status.detailed_effective_mode() {
        DetailedRunMode::Unknown => status.effective_mode().into(),
        other => other,
    };
    let mode = match (broken_reason, planned, detailed) {
        (Some(_), _, _) => Mode::Broken,
        (None, RunMode::Leader, DetailedRunMode::Leader) => Mode::Leader,
        (None, RunMode::Leader, DetailedRunMode::BecomingLeader) => Mode::BecomingLeader,
        (None, RunMode::Leader, DetailedRunMode::Candidate | DetailedRunMode::Follower) => {
            Mode::Candidate
        }
        (None, RunMode::Follower, DetailedRunMode::Follower) => Mode::Follower,
        (None, RunMode::Follower, _) if detailed != DetailedRunMode::Unknown => Mode::SteppingDown,
        _ => Mode::Unknown,
    };
    let replay = match status.replay_status() {
        ReplayStatus::Starting => Replay::Starting,
        ReplayStatus::Active => Replay::Active,
        ReplayStatus::CatchingUp => Replay::CatchingUp,
        ReplayStatus::Unknown => Replay::Unknown,
    };
    let epoch = status.last_observed_leader_epoch.map(|e| e.value);
    let claims_leadership = status
        .last_observed_leader_node
        .is_some_and(|n| n.id == node.id() && n.generation == Some(node.generation()));
    let applied = status.last_applied_log_lsn.map(|l| l.value);
    let target_tail = status.target_tail_lsn.map(|l| l.value);

    ReplicaView {
        node,
        mode,
        replay,
        broken_reason,
        member: false,
        next_member: false,
        epoch,
        stale_epoch: claims_leadership && epoch < max_epoch,
        applied_lsn: applied,
        durable_lsn: status.durable_lsn.map(|l| l.value),
        archived_lsn: status.last_archived_log_lsn.map(|l| l.value),
        target_tail_lsn: target_tail,
        lag: target_tail
            .zip(applied)
            .map(|(tail, applied)| tail.saturating_sub(applied + 1)),
        byte_offsets_base_lsn: status.byte_offsets_base_lsn.map(|l| l.value),
        log_byte_offset: status.log_byte_offset,
        archived_log_byte_offset: status.last_archived_log_byte_offset,
        archived_byte_offsets_base_lsn: status.last_archived_byte_offsets_base_lsn.map(|l| l.value),
        schema_version: status
            .last_applied_schema_version
            .map(|v| Version::from(v).into()),
        rule_book_version: status
            .last_applied_rule_book_version
            .map(|v| Version::from(v).into()),
        features: status.enabled_features.clone(),
        storage_features: status.enabled_storage_features.clone(),
        updated_at: timestamp(status.updated_at),
        last_record_applied_at: timestamp(status.last_record_applied_at),
        reader: None,
    }
}

fn metadata_server_view(
    state: String,
    status: Option<Result<restate_metadata_server_grpc::grpc::StatusResponse, String>>,
) -> MetadataServerView {
    let mut view = MetadataServerView {
        state,
        status: None,
        leader: None,
        config_version: None,
        members: Vec::new(),
        raft: None,
        error: None,
    };
    match status {
        Some(Ok(status)) => {
            view.status = Some(
                match status.status() {
                    MetadataServerStatus::Unknown => "unknown",
                    MetadataServerStatus::StartingUp => "starting",
                    MetadataServerStatus::AwaitingProvisioning => "awaiting-provisioning",
                    MetadataServerStatus::Member => "member",
                    MetadataServerStatus::Standby => "standby",
                }
                .to_owned(),
            );
            view.leader = status.leader.map(PlainNodeId::new);
            if let Some(config) = status.configuration {
                view.config_version = config.version.map(|v| Version::from(v).into());
                view.members = config
                    .members
                    .into_keys()
                    .map(PlainNodeId::from)
                    .sorted()
                    .collect();
            }
            view.raft = status.raft.map(|raft| RaftView {
                term: raft.term,
                applied: raft.applied,
                committed: raft.committed,
                // first and last index are inclusive
                log_length: (raft.last_index + 1).saturating_sub(raft.first_index),
                snapshot_index: status.snapshot.map(|s| s.index).unwrap_or_default(),
                snapshot_size: status.snapshot.map(|s| s.size).unwrap_or_default(),
            });
        }
        Some(Err(err)) => view.error = Some(err),
        None => {}
    }
    view
}

fn log_provider(provider: &ProviderConfiguration) -> LogProvider {
    match provider {
        ProviderConfiguration::Replicated(config) => LogProvider {
            kind: ProviderKind::Replicated.to_string(),
            replication: Some(config.replication_property.to_string()),
            nodeset_size: Some(config.target_nodeset_size.as_u16()),
        },
        ProviderConfiguration::Local => LogProvider {
            kind: ProviderKind::Local.to_string(),
            replication: None,
            nodeset_size: None,
        },
        ProviderConfiguration::InMemory => LogProvider {
            kind: ProviderKind::InMemory.to_string(),
            replication: None,
            nodeset_size: None,
        },
    }
}

/// The LSN of loglet offset `offset` in a segment starting at `base_lsn`; offsets start at 1.
fn offset_to_lsn(base_lsn: u64, offset: u32) -> u64 {
    base_lsn + u64::from(offset) - 1
}

fn tail_lsn(base_lsn: u64, workers: &[&LogletWorkerRow]) -> Option<u64> {
    workers
        .iter()
        .filter_map(|w| w.known_global_tail)
        .filter(|t| *t > 0)
        .max()
        .map(|t| offset_to_lsn(base_lsn, t))
}

fn readers(rows: &[super::sql::ReadStreamRow], tail: Option<u64>) -> Vec<ReaderView> {
    rows.iter()
        .filter_map(|row| {
            let tailing = row
                .is_tailing
                .unwrap_or_else(|| row.end_lsn == Some(u64::MAX));
            Some(ReaderView {
                node: row.plain_node_id.parse().ok()?,
                state: row.state.clone().unwrap_or_default(),
                tailing,
                read_pointer: row.read_pointer,
                end_lsn: row.end_lsn.filter(|_| !tailing),
                segment: row.current_segment,
                behind: tail
                    .zip(row.read_pointer)
                    .filter(|_| tailing)
                    .map(|(tail, read)| tail.saturating_sub(read)),
                name: row.name.clone(),
                role: row.role.clone(),
                records_read: row.records_read,
                bytes_read: row.bytes_read,
            })
        })
        .sorted_by_key(|r| (r.node, !r.tailing))
        .collect()
}

/// The servers in a node's report, in the order it lists them.
fn server_addresses(ident: &IdentResponse) -> Vec<ServerAddresses> {
    use restate_core::protobuf::node_ctl_svc::ident_response::AddressKind;

    fn entry<'a>(servers: &'a mut Vec<ServerAddresses>, name: &str) -> &'a mut ServerAddresses {
        let at = match servers.iter().position(|s| s.server == name) {
            Some(at) => at,
            None => {
                servers.push(ServerAddresses {
                    server: name.to_owned(),
                    advertised: None,
                    listens_on: Vec::new(),
                });
                servers.len() - 1
            }
        };
        &mut servers[at]
    }

    let mut servers = Vec::new();
    for address in &ident.advertised_addresses {
        entry(&mut servers, &address.name)
            .advertised
            .get_or_insert_with(|| address.address.clone());
    }
    for address in &ident.bound_addresses {
        entry(&mut servers, &address.name)
            .listens_on
            .push(match address.kind() {
                AddressKind::Unix => format!("unix:{}", address.address),
                _ => address.address.clone(),
            });
    }
    servers
}

fn parse_node_list(list: &str) -> Vec<PlainNodeId> {
    list.split(',')
        .filter_map(|n| n.trim().parse().ok())
        .sorted()
        .collect()
}

fn timestamp(ts: Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    let ts: SystemTime = ts?.try_into().ok()?;
    Some(ts.into())
}

/// Serializes ids with their display form (`N1`, `N1:3`), as the rest of restatectl prints them.
mod ser {
    use std::fmt::Display;

    use serde::Serializer;

    pub fn display<T: Display, S: Serializer>(value: &T, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(value)
    }

    pub fn opt_display<T: Display, S: Serializer>(
        value: &Option<T>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => s.collect_str(value),
            None => s.serialize_none(),
        }
    }

    pub fn seq_display<T: Display, S: Serializer>(value: &[T], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(value.iter().map(|v| v.to_string()))
    }
}
