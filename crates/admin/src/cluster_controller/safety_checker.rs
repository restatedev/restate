// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Checks whether the logs and partitions of the cluster are available given the set of alive
//! nodes, and which ones would lose availability if a single failure domain failed.
//!
//! ## Logs
//!
//! A replicated segment is considered:
//! - *readable/sealable* if its alive nodes form an f-majority of its nodeset.
//! - *writeable* if its sequencer is alive and its alive, writeable nodes form a write quorum.
//!
//! A log is write-available if its tail segment is writeable, or if the tail can be sealed and
//! a new segment (using the default replication) can find a write quorum among the alive
//! log-servers that are candidates for new nodesets. A log is read-available if all of its
//! non-empty segments are readable.
//!
//! Logs with non-replicated segments (local/in-memory) can't be evaluated and are reported as
//! unchecked.
//!
//! ## Partitions
//!
//! A partition is available if any node of its current or pending replica set is alive. Unless
//! its placement is frozen, the cluster controller moves it to other nodes, so it's also
//! available if a new replica set can be selected among the alive, active workers.
//!
//! ## Failure domains
//!
//! The failure domains whose loss is simulated are those at the greatest scope of the
//! replication property: the segments' replication for logs and the partition replication for
//! partitions. For instance, with `{zone: 2}` the check reports the logs and partitions that
//! would lose availability if any one zone failed. Alive nodes without a location at that scope
//! are treated as their own failure domain.

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;

use ahash::HashMap;
use metrics::gauge;

use restate_core::{Metadata, TaskCenter};
use restate_types::clock::WallClock;
use restate_types::cluster_state::ClusterState;
use restate_types::identifiers::PartitionId;
use restate_types::locality::{LocationScope, NodeLocation};
use restate_types::logs::metadata::{InternalKind, Logs, ProviderConfiguration};
use restate_types::logs::{LogId, LogletId};
use restate_types::net::node::NodeState;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partitions::{PartitionConfiguration, worker_candidate_filter};
use restate_types::replicated_loglet::logserver_candidate_filter;
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};
use restate_types::replication::{NodeSet, NodeSetChecker, ReplicationProperty};
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::metric_definitions::{
    CLUSTER_SAFETY_AT_RISK_PARTITIONS, CLUSTER_SAFETY_LAST_CHECK_TIMESTAMP_SECONDS,
    CLUSTER_SAFETY_READ_AT_RISK_LOGS, CLUSTER_SAFETY_READ_UNAVAILABLE_LOGS,
    CLUSTER_SAFETY_UNAVAILABLE_PARTITIONS, CLUSTER_SAFETY_UNCHECKED_LOGS,
    CLUSTER_SAFETY_UNCHECKED_PARTITIONS, CLUSTER_SAFETY_WRITE_AT_RISK_LOGS,
    CLUSTER_SAFETY_WRITE_UNAVAILABLE_LOGS,
};

/// Scopes that nodes can be located in, indexed by `LocationScope as usize`.
const SCOPES: [LocationScope; 3] = [
    LocationScope::Node,
    LocationScope::Zone,
    LocationScope::Region,
];

/// The nodes considered alive by a safety check.
#[derive(Debug, Default, Clone)]
pub struct AliveNodes(HashMap<PlainNodeId, GenerationalNodeId>);

impl AliveNodes {
    pub fn from_cluster_state(cluster_state: &ClusterState) -> Self {
        cluster_state
            .all()
            .into_iter()
            .filter(|(_, state)| *state == NodeState::Alive)
            .map(|(node_id, _)| node_id)
            .collect()
    }

    /// Considers this node dead.
    pub fn remove(&mut self, node_id: PlainNodeId) {
        self.0.remove(&node_id);
    }

    /// The sequencer of a loglet is bound to the generation it was started on.
    fn is_generation_alive(&self, node_id: GenerationalNodeId) -> bool {
        self.0.get(&node_id.as_plain()) == Some(&node_id)
    }
}

impl FromIterator<GenerationalNodeId> for AliveNodes {
    fn from_iter<T: IntoIterator<Item = GenerationalNodeId>>(iter: T) -> Self {
        Self(iter.into_iter().map(|id| (id.as_plain(), id)).collect())
    }
}

/// A node, zone, or region whose nodes can be lost together.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FailureDomain {
    pub scope: LocationScope,
    /// The node id at the node scope, the location (e.g. `region1.zone1`) otherwise.
    pub name: String,
}

impl FailureDomain {
    fn of(node_id: PlainNodeId, location: &NodeLocation, scope: LocationScope) -> Self {
        if scope != LocationScope::Node && location.is_scope_defined(scope) {
            Self {
                scope,
                name: location.domain_string(scope, None),
            }
        } else {
            Self {
                scope: LocationScope::Node,
                name: node_id.to_string(),
            }
        }
    }
}

/// Where a partition runs.
#[derive(Debug)]
pub struct PartitionPlacement {
    pub partition_id: PartitionId,
    /// Nodes of the current and of the pending (if any) replica sets.
    pub replicas: Vec<PlainNodeId>,
    /// Whether the cluster controller may move the partition to other nodes.
    pub can_move: bool,
}

impl PartitionPlacement {
    pub fn new(
        partition_id: PartitionId,
        current: &PartitionConfiguration,
        next: Option<&PartitionConfiguration>,
        placement_frozen: bool,
    ) -> Self {
        let replicas = current
            .replica_set()
            .iter()
            .chain(next.into_iter().flat_map(|next| next.replica_set().iter()))
            .copied()
            .collect();

        Self {
            partition_id,
            replicas,
            // partitions without a valid configuration get placed even if their placement is
            // frozen
            can_move: !placement_frozen || !current.is_valid(),
        }
    }
}

#[derive(Debug, Default)]
pub struct SafetyReport {
    /// Logs that can't accept writes, neither on their tail segment nor after a reconfiguration.
    pub write_unavailable_logs: Vec<LogId>,
    /// Logs with at least one segment whose records can't be reliably read.
    pub read_unavailable_logs: Vec<LogId>,
    /// Write-available logs that lose write availability if a single failure domain fails.
    pub write_at_risk_logs: Vec<LogId>,
    /// Read-available logs that lose read availability if a single failure domain fails.
    pub read_at_risk_logs: Vec<LogId>,
    /// Logs that can't be evaluated because they have non-replicated segments.
    pub unchecked_logs: Vec<LogId>,
    /// Partitions without an alive replica that the cluster controller can't move elsewhere.
    pub unavailable_partitions: Vec<PartitionId>,
    /// Available partitions that lose availability if a single failure domain fails.
    pub at_risk_partitions: Vec<PartitionId>,
    /// Partitions whose placement isn't known yet.
    pub unchecked_partitions: Vec<PartitionId>,
    /// What losing each failure domain alone would make unavailable, for the domains whose loss
    /// makes anything unavailable.
    pub domain_impacts: BTreeMap<FailureDomain, DomainImpact>,
    /// The greatest scope of the replication of the checked logs and partitions, the scope of
    /// the failure domains that the cluster is expected to tolerate the loss of. `None` if there
    /// was nothing to check.
    pub scope: Option<LocationScope>,
    /// Nodes that are considered dead, not counting the ones assumed to be dead.
    pub dead_nodes: Vec<PlainNodeId>,
    pub num_logs: usize,
    pub num_partitions: usize,
}

/// What losing a failure domain makes unavailable.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DomainImpact {
    pub write_unavailable_logs: Vec<LogId>,
    pub read_unavailable_logs: Vec<LogId>,
    pub unavailable_partitions: Vec<PartitionId>,
}

/// Checks the current logs and the partitions' placements (as returned by `placement_of`)
/// against this node's view of the cluster state, with `assume_dead` considered dead on top of
/// the nodes that are not alive.
pub fn check_current_safety(
    placement_of: impl Fn(PartitionId) -> Option<PartitionPlacement>,
    assume_dead: &[PlainNodeId],
) -> SafetyReport {
    let mut alive =
        TaskCenter::with_current(|tc| AliveNodes::from_cluster_state(tc.cluster_state()));
    let metadata = Metadata::current();
    let nodes_config = metadata.nodes_config_snapshot();
    let dead_nodes = nodes_config
        .iter()
        .map(|(node_id, _)| node_id)
        .filter(|node_id| !alive.0.contains_key(node_id))
        .collect();
    for node_id in assume_dead {
        alive.remove(*node_id);
    }

    let partition_table = metadata.partition_table_snapshot();

    let mut placements = Vec::new();
    let mut unchecked_partitions = Vec::new();
    for partition_id in partition_table.iter_ids().copied() {
        match placement_of(partition_id) {
            Some(placement) => placements.push(placement),
            None => unchecked_partitions.push(partition_id),
        }
    }

    let mut report = check_safety(
        &metadata.logs_snapshot(),
        &nodes_config,
        &placements,
        &partition_table.replication_property(&nodes_config),
        &alive,
    );
    report.num_partitions += unchecked_partitions.len();
    report.unchecked_partitions = unchecked_partitions;
    report.dead_nodes = dead_nodes;
    report
}

pub fn check_safety(
    logs: &Logs,
    nodes_config: &NodesConfiguration,
    partitions: &[PartitionPlacement],
    partition_replication: &ReplicationProperty,
    alive: &AliveNodes,
) -> SafetyReport {
    let topology = Topology::new(nodes_config, alive);
    let mut report = SafetyReport {
        num_logs: logs.num_logs(),
        num_partitions: partitions.len(),
        ..Default::default()
    };
    let mut impacts = BTreeMap::new();
    check_logs(
        &mut report,
        &mut impacts,
        logs,
        nodes_config,
        alive,
        &topology,
    );
    if !partitions.is_empty() {
        check_partitions(
            &mut report,
            &mut impacts,
            partitions,
            partition_replication,
            nodes_config,
            &topology,
        );
    }
    report.domain_impacts = impacts
        .into_iter()
        .map(|(id, impact)| (topology.domains[id].clone(), impact))
        .collect();
    report
}

fn check_logs(
    report: &mut SafetyReport,
    impacts: &mut BTreeMap<DomainId, DomainImpact>,
    logs: &Logs,
    nodes_config: &NodesConfiguration,
    alive: &AliveNodes,
    topology: &Topology,
) {
    // A segment that a reconfiguration would create.
    let new_segment = match &logs.configuration().default_provider {
        ProviderConfiguration::Replicated(config) => {
            let candidates: NodeSet = nodes_config
                .iter()
                .filter(|(node_id, config)| logserver_candidate_filter(*node_id, config))
                .map(|(node_id, _)| node_id)
                .collect();
            Some(SegmentChecker::new(
                &candidates,
                &config.replication_property,
                nodes_config,
                topology,
            ))
        }
        ProviderConfiguration::Local | ProviderConfiguration::InMemory => None,
    };

    let mut sealed = Vec::new();

    'logs: for (log_id, chain) in logs.iter() {
        sealed.clear();
        let mut tail = None;

        for segment in chain.iter() {
            match segment.config.kind {
                // seal marker of a permanently sealed chain, it has no records
                InternalKind::Sealed => {}
                InternalKind::Local | InternalKind::InMemory => {
                    report.unchecked_logs.push(*log_id);
                    continue 'logs;
                }
                InternalKind::Replicated => {
                    // sealed empty segments have no records to read
                    if segment.tail_lsn == Some(segment.base_lsn) {
                        continue;
                    }
                    let Some(loglet) =
                        logs.get_replicated_loglet(&LogletId::new(*log_id, segment.index()))
                    else {
                        report.unchecked_logs.push(*log_id);
                        continue 'logs;
                    };
                    let params = &loglet.params;
                    let checker = SegmentChecker::new(
                        &params.nodeset,
                        &params.replication,
                        nodes_config,
                        topology,
                    );
                    if segment.tail_lsn.is_none() {
                        tail = Some((params.sequencer, checker));
                    } else {
                        sealed.push(checker);
                    }
                }
            }
        }

        let segments = || tail.iter().map(|(_, segment)| segment).chain(&sealed);
        let is_readable =
            |lost: Option<DomainId>| segments().all(|segment| segment.has_f_majority(lost));
        let is_writeable = |lost: Option<DomainId>| {
            // permanently sealed chains are not expected to accept writes
            let Some((sequencer, segment)) = &tail else {
                return true;
            };
            let sequencer_alive = alive.is_generation_alive(*sequencer)
                && topology.survives(sequencer.as_plain(), lost);
            let can_reconfigure = segment.has_f_majority(lost)
                && new_segment
                    .as_ref()
                    .is_some_and(|new_segment| new_segment.has_write_quorum(lost));
            (sequencer_alive && segment.has_write_quorum(lost)) || can_reconfigure
        };

        // the failure domains whose loss we simulate for this log
        let Some(scope) = segments().map(|segment| segment.scope).max() else {
            continue;
        };
        report.scope = report.scope.max(Some(scope));
        let domains = topology.domains_at(scope);

        let on_loss = |domain| {
            impacts
                .entry(domain)
                .or_default()
                .read_unavailable_logs
                .push(*log_id)
        };
        match check_availability(domains, is_readable, on_loss) {
            Availability::Unavailable => report.read_unavailable_logs.push(*log_id),
            Availability::AtRisk => report.read_at_risk_logs.push(*log_id),
            Availability::Available => {}
        }
        let on_loss = |domain| {
            impacts
                .entry(domain)
                .or_default()
                .write_unavailable_logs
                .push(*log_id)
        };
        match check_availability(domains, is_writeable, on_loss) {
            Availability::Unavailable => report.write_unavailable_logs.push(*log_id),
            Availability::AtRisk => report.write_at_risk_logs.push(*log_id),
            Availability::Available => {}
        }
    }
}

fn check_partitions(
    report: &mut SafetyReport,
    impacts: &mut BTreeMap<DomainId, DomainImpact>,
    partitions: &[PartitionPlacement],
    replication: &ReplicationProperty,
    nodes_config: &NodesConfiguration,
    topology: &Topology,
) {
    let scope = replication.greatest_defined_scope();
    report.scope = report.scope.max(Some(scope));
    let domains = topology.domains_at(scope);

    // Can the cluster controller select a new replica set after losing a domain?
    let can_place = |lost: Option<DomainId>| {
        BalancedSpreadSelector::select(
            nodes_config,
            replication,
            |node_id, config| {
                topology.survives(node_id, lost) && worker_candidate_filter(node_id, config)
            },
            &SelectorOptions::default(),
        )
        .is_ok()
    };
    let can_place_now = can_place(None);
    let can_place_without: HashMap<DomainId, bool> = domains
        .iter()
        .map(|domain| (*domain, can_place(Some(*domain))))
        .collect();

    for partition in partitions {
        let is_available = |lost: Option<DomainId>| {
            partition
                .replicas
                .iter()
                .any(|node_id| topology.survives(*node_id, lost))
                || (partition.can_move
                    && lost.map_or(can_place_now, |lost| can_place_without[&lost]))
        };

        let on_loss = |domain| {
            impacts
                .entry(domain)
                .or_default()
                .unavailable_partitions
                .push(partition.partition_id)
        };
        match check_availability(domains, is_available, on_loss) {
            Availability::Unavailable => report.unavailable_partitions.push(partition.partition_id),
            Availability::AtRisk => report.at_risk_partitions.push(partition.partition_id),
            Availability::Available => {}
        }
    }
}

enum Availability {
    Available,
    /// Available, but losing one of the failure domains makes it unavailable.
    AtRisk,
    Unavailable,
}

/// Checks `is_available` now and, if it holds, after losing each of `domains`, calling
/// `on_loss` with the domains whose loss makes it unavailable.
fn check_availability(
    domains: &[DomainId],
    is_available: impl Fn(Option<DomainId>) -> bool,
    mut on_loss: impl FnMut(DomainId),
) -> Availability {
    if !is_available(None) {
        return Availability::Unavailable;
    }

    let mut at_risk = false;
    for domain in domains {
        if !is_available(Some(*domain)) {
            on_loss(*domain);
            at_risk = true;
        }
    }

    if at_risk {
        Availability::AtRisk
    } else {
        Availability::Available
    }
}

/// Exports the results of the safety checks as metrics. Resets them when dropped so that a node
/// that stops running the checks doesn't keep reporting stale results.
pub(crate) struct SafetyMetrics;

impl SafetyMetrics {
    pub fn record(&self, report: &SafetyReport) {
        Self::set(report, WallClock::now_ms().as_u64() as f64 / 1000.0);
    }

    fn set(report: &SafetyReport, last_check_timestamp_seconds: f64) {
        gauge!(CLUSTER_SAFETY_LAST_CHECK_TIMESTAMP_SECONDS).set(last_check_timestamp_seconds);
        let gauges = [
            (
                CLUSTER_SAFETY_WRITE_UNAVAILABLE_LOGS,
                report.write_unavailable_logs.len(),
            ),
            (
                CLUSTER_SAFETY_READ_UNAVAILABLE_LOGS,
                report.read_unavailable_logs.len(),
            ),
            (
                CLUSTER_SAFETY_WRITE_AT_RISK_LOGS,
                report.write_at_risk_logs.len(),
            ),
            (
                CLUSTER_SAFETY_READ_AT_RISK_LOGS,
                report.read_at_risk_logs.len(),
            ),
            (CLUSTER_SAFETY_UNCHECKED_LOGS, report.unchecked_logs.len()),
            (
                CLUSTER_SAFETY_UNAVAILABLE_PARTITIONS,
                report.unavailable_partitions.len(),
            ),
            (
                CLUSTER_SAFETY_AT_RISK_PARTITIONS,
                report.at_risk_partitions.len(),
            ),
            (
                CLUSTER_SAFETY_UNCHECKED_PARTITIONS,
                report.unchecked_partitions.len(),
            ),
        ];
        for (name, value) in gauges {
            gauge!(name).set(value as f64);
        }
    }
}

impl Drop for SafetyMetrics {
    fn drop(&mut self) {
        Self::set(&SafetyReport::default(), 0.0);
    }
}

/// Index of a failure domain in [`Topology::domains`].
type DomainId = usize;

/// The failure domains of a node at each scope, indexed by `LocationScope as usize`.
type NodeDomains = [DomainId; SCOPES.len()];

/// Whether a node in `domains` (`None` if it's dead) is alive after losing `lost`.
fn survives(domains: Option<&NodeDomains>, lost: Option<DomainId>) -> bool {
    domains.is_some_and(|domains| lost.is_none_or(|lost| !domains.contains(&lost)))
}

/// The failure domains of the alive nodes at every scope.
struct Topology {
    domains: Vec<FailureDomain>,
    node_domains: HashMap<PlainNodeId, NodeDomains>,
    /// The domains to simulate the loss of at each scope.
    domains_at_scope: [Vec<DomainId>; SCOPES.len()],
}

impl Topology {
    fn new(nodes_config: &NodesConfiguration, alive: &AliveNodes) -> Self {
        let mut ids = HashMap::default();
        let mut domains = Vec::new();
        let mut domains_at_scope: [Vec<DomainId>; SCOPES.len()] = Default::default();

        let unknown_location = NodeLocation::new();
        let node_domains = alive
            .0
            .keys()
            .map(|node_id| {
                let location = nodes_config
                    .find_node_by_id(*node_id)
                    .map_or(&unknown_location, |config| &config.location);
                let node_domains = SCOPES.map(|scope| {
                    let (id, is_new) = match ids.entry(FailureDomain::of(*node_id, location, scope))
                    {
                        Entry::Occupied(entry) => (*entry.get(), false),
                        Entry::Vacant(entry) => {
                            domains.push(entry.key().clone());
                            (*entry.insert(domains.len() - 1), true)
                        }
                    };
                    // a node without a location at this scope is its own (node) domain
                    if is_new || domains[id].scope != scope {
                        domains_at_scope[scope as usize].push(id);
                    }
                    id
                });
                (*node_id, node_domains)
            })
            .collect();

        Self {
            domains,
            node_domains,
            domains_at_scope,
        }
    }

    fn domains_at(&self, scope: LocationScope) -> &[DomainId] {
        &self.domains_at_scope[scope as usize]
    }

    /// The failure domains of the node, `None` if the node is dead.
    fn domains_of(&self, node_id: PlainNodeId) -> Option<&NodeDomains> {
        self.node_domains.get(&node_id)
    }

    /// Is the node alive and outside of the lost domain?
    fn survives(&self, node_id: PlainNodeId, lost: Option<DomainId>) -> bool {
        survives(self.domains_of(node_id), lost)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeAttr {
    /// The failure domains of the node, `None` if the node is dead.
    domains: Option<NodeDomains>,
    can_write: bool,
}

/// Checks the quorums of a replicated segment.
struct SegmentChecker {
    /// The greatest scope of the segment's replication property.
    scope: LocationScope,
    checker: NodeSetChecker<NodeAttr>,
}

impl SegmentChecker {
    fn new(
        nodeset: &NodeSet,
        replication: &ReplicationProperty,
        nodes_config: &NodesConfiguration,
        topology: &Topology,
    ) -> Self {
        let mut checker = NodeSetChecker::new(nodeset, nodes_config, replication);
        for node_id in nodeset.iter().copied() {
            let can_write = nodes_config
                .find_node_by_id(node_id)
                .is_ok_and(|config| config.log_server_config.storage_state.can_write_to());
            checker.set_attribute(
                node_id,
                NodeAttr {
                    domains: topology.domains_of(node_id).copied(),
                    can_write,
                },
            );
        }

        Self {
            scope: replication.greatest_defined_scope(),
            checker,
        }
    }

    /// Do the nodes that survive losing `lost` form an f-majority, i.e. can the segment be read
    /// and sealed?
    fn has_f_majority(&self, lost: Option<DomainId>) -> bool {
        self.checker
            .check_fmajority(|attr| survives(attr.domains.as_ref(), lost))
            .passed()
    }

    /// Do the writeable nodes that survive losing `lost` form a write quorum?
    fn has_write_quorum(&self, lost: Option<DomainId>) -> bool {
        self.checker
            .check_write_quorum(|attr| attr.can_write && survives(attr.domains.as_ref(), lost))
    }
}

#[cfg(test)]
mod tests {
    use restate_types::RestateVersion;
    use restate_types::logs::Lsn;
    use restate_types::logs::metadata::{
        Chain, LogsConfiguration, ProviderKind, ReplicatedLogletConfig, SegmentIndex,
    };
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, Role, StorageState, WorkerConfig, WorkerState,
    };
    use restate_types::replicated_loglet::ReplicatedLogletParams;

    use super::*;

    const LOG: LogId = LogId::new(0);

    /// Nodes `N1, N2, ...` at the given locations, all active workers and log-servers.
    fn nodes_config(locations: &[&str]) -> NodesConfiguration {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        for (id, location) in (1..).zip(locations) {
            nodes_config.upsert_node(
                NodeConfig::builder()
                    .name(format!("node-{id}"))
                    .current_generation(GenerationalNodeId::new(id, 1))
                    .location(location.parse().unwrap())
                    .address(format!("unix:/tmp/node-{id}").parse().unwrap())
                    .roles(Role::Worker | Role::LogServer)
                    .log_server_config(LogServerConfig {
                        storage_state: StorageState::ReadWrite,
                    })
                    .worker_config(WorkerConfig {
                        worker_state: WorkerState::Active,
                    })
                    .binary_version(RestateVersion::current())
                    .build(),
            );
        }
        nodes_config
    }

    /// Alive nodes at their first generation.
    fn alive(ids: &[u32]) -> AliveNodes {
        ids.iter()
            .map(|id| GenerationalNodeId::new(*id, 1))
            .collect()
    }

    fn node_set(ids: &[u32]) -> NodeSet {
        ids.iter().copied().map(PlainNodeId::from).collect()
    }

    /// A single log with segments on the given nodesets, the first node of each nodeset being
    /// its sequencer. All but the last segment are sealed and non-empty. All segments, existing
    /// and new, use `replication`.
    fn log(replication: &str, nodesets: &[&[u32]]) -> Logs {
        let replication: ReplicationProperty = replication.parse().unwrap();
        let mut builder = Logs::with_logs_configuration(LogsConfiguration {
            default_provider: ProviderConfiguration::Replicated(ReplicatedLogletConfig {
                replication_property: replication.clone(),
                target_nodeset_size: Default::default(),
            }),
        })
        .try_into_builder()
        .unwrap();

        for (index, nodeset) in nodesets.iter().enumerate() {
            let params = ReplicatedLogletParams {
                loglet_id: LogletId::new(LOG, SegmentIndex::from(index as u32)),
                sequencer: GenerationalNodeId::new(nodeset[0], 1),
                replication: replication.clone(),
                nodeset: node_set(nodeset),
            }
            .serialize()
            .unwrap()
            .into();
            let base_lsn = Lsn::new(1 + 10 * index as u64);
            if index == 0 {
                builder
                    .add_log(
                        LOG,
                        Chain::with_base_lsn(base_lsn, ProviderKind::Replicated, params),
                    )
                    .unwrap();
            } else {
                builder
                    .chain(LOG)
                    .unwrap()
                    .append_segment(base_lsn, ProviderKind::Replicated, params)
                    .unwrap();
            }
        }
        builder.build()
    }

    /// The names of the domains whose loss makes something `impacted` unavailable.
    fn lost(report: &SafetyReport, impacted: fn(&DomainImpact) -> bool) -> Vec<&str> {
        report
            .domain_impacts
            .iter()
            .filter(|(_, impact)| impacted(impact))
            .map(|(domain, _)| domain.name.as_str())
            .collect()
    }

    fn loses_writes(impact: &DomainImpact) -> bool {
        !impact.write_unavailable_logs.is_empty()
    }

    fn loses_reads(impact: &DomainImpact) -> bool {
        !impact.read_unavailable_logs.is_empty()
    }

    fn loses_partitions(impact: &DomainImpact) -> bool {
        !impact.unavailable_partitions.is_empty()
    }

    fn check(logs: &Logs, nodes_config: &NodesConfiguration, alive: &AliveNodes) -> SafetyReport {
        check_safety(
            logs,
            nodes_config,
            &[],
            &"{node: 1}".parse().unwrap(),
            alive,
        )
    }

    #[test]
    fn logs_with_node_replication() {
        let nodes_config = nodes_config(&["", "", ""]);
        let logs = log("{node: 2}", &[&[1, 2, 3]]);

        // losing any node leaves an f-majority and a write quorum
        let report = check(&logs, &nodes_config, &alive(&[1, 2, 3]));
        assert!(report.write_unavailable_logs.is_empty());
        assert!(report.read_unavailable_logs.is_empty());
        assert!(report.write_at_risk_logs.is_empty());
        assert!(report.read_at_risk_logs.is_empty());

        // losing one of the two remaining nodes breaks both quorums
        let report = check(&logs, &nodes_config, &alive(&[1, 2]));
        assert!(report.write_unavailable_logs.is_empty());
        assert!(report.read_unavailable_logs.is_empty());
        assert_eq!(report.write_at_risk_logs, [LOG]);
        assert_eq!(report.read_at_risk_logs, [LOG]);
        assert_eq!(lost(&report, loses_writes), ["N1", "N2"]);
        assert_eq!(lost(&report, loses_reads), ["N1", "N2"]);

        let report = check(&logs, &nodes_config, &alive(&[1]));
        assert_eq!(report.write_unavailable_logs, [LOG]);
        assert_eq!(report.read_unavailable_logs, [LOG]);
        assert!(report.write_at_risk_logs.is_empty());
        assert!(report.domain_impacts.is_empty());
    }

    #[test]
    fn log_writes_need_the_sequencer_or_a_reconfiguration() {
        let nodes_config = nodes_config(&["", "", "", "", ""]);
        let logs = log("{node: 2}", &[&[1, 2, 3, 4, 5]]);

        // three alive nodes form a write quorum but not an f-majority, so the log can't be
        // sealed and reconfigured: it only accepts writes while its sequencer N1 is alive
        let report = check(&logs, &nodes_config, &alive(&[1, 2, 3]));
        assert!(report.write_unavailable_logs.is_empty());
        assert_eq!(report.write_at_risk_logs, [LOG]);
        assert_eq!(lost(&report, loses_writes), ["N1"]);

        // a restarted N1 no longer runs the sequencer
        let restarted = [
            GenerationalNodeId::new(1, 2),
            GenerationalNodeId::new(2, 1),
            GenerationalNodeId::new(3, 1),
        ];
        let report = check(&logs, &nodes_config, &restarted.into_iter().collect());
        assert_eq!(report.write_unavailable_logs, [LOG]);
    }

    #[test]
    fn sealed_segments_only_affect_reads() {
        let nodes_config = nodes_config(&["", "", "", "", "", ""]);
        let logs = log("{node: 2}", &[&[1, 2, 3], &[4, 5, 6]]);

        let report = check(&logs, &nodes_config, &alive(&[3, 4, 5, 6]));
        assert_eq!(report.read_unavailable_logs, [LOG]);
        assert!(report.write_unavailable_logs.is_empty());
        assert!(report.write_at_risk_logs.is_empty());
    }

    #[test]
    fn critical_domains_follow_the_replication_scope() {
        let nodes_config = nodes_config(&[
            "region1.zone1",
            "region1.zone1",
            "region1.zone2",
            "region1.zone2",
            "region1.zone3",
        ]);
        let logs = log("{zone: 2}", &[&[1, 2, 3, 4, 5]]);

        // two zones are left after losing any zone
        let report = check(&logs, &nodes_config, &alive(&[1, 2, 3, 4, 5]));
        assert!(report.write_at_risk_logs.is_empty());
        assert!(report.read_at_risk_logs.is_empty());

        // with zone3 down, losing another zone loses both quorums
        let report = check(&logs, &nodes_config, &alive(&[1, 2, 3, 4]));
        assert!(report.write_unavailable_logs.is_empty());
        assert!(report.read_unavailable_logs.is_empty());
        assert_eq!(report.write_at_risk_logs, [LOG]);
        assert_eq!(report.read_at_risk_logs, [LOG]);
        assert_eq!(
            lost(&report, loses_writes),
            ["region1.zone1", "region1.zone2"]
        );
        assert_eq!(report.scope, Some(LocationScope::Zone));
        assert_eq!(
            report.domain_impacts[&FailureDomain {
                scope: LocationScope::Zone,
                name: "region1.zone1".to_owned(),
            }],
            DomainImpact {
                write_unavailable_logs: vec![LOG],
                read_unavailable_logs: vec![LOG],
                unavailable_partitions: vec![],
            }
        );
    }

    #[test]
    fn partitions_move_unless_frozen() {
        let nodes_config = nodes_config(&["", "", ""]);
        let replication: ReplicationProperty = "{node: 2}".parse().unwrap();
        let configuration = |ids: &[u32]| {
            PartitionConfiguration::new(replication.clone(), node_set(ids), Default::default())
        };
        let partitions = [
            PartitionPlacement::new(
                PartitionId::new_unchecked(0),
                &configuration(&[1]),
                None,
                false,
            ),
            PartitionPlacement::new(
                PartitionId::new_unchecked(1),
                &configuration(&[1]),
                None,
                true,
            ),
            // replicas of the pending configuration count too
            PartitionPlacement::new(
                PartitionId::new_unchecked(2),
                &configuration(&[1]),
                Some(&configuration(&[2])),
                true,
            ),
            // partitions without a valid configuration get placed even if frozen
            PartitionPlacement::new(
                PartitionId::new_unchecked(3),
                &PartitionConfiguration::default(),
                None,
                true,
            ),
        ];
        let check = |alive_ids: &[u32]| {
            check_safety(
                &Logs::default(),
                &nodes_config,
                &partitions,
                &replication,
                &alive(alive_ids),
            )
        };
        let ids = |ids: &[u16]| -> Vec<PartitionId> {
            ids.iter()
                .copied()
                .map(PartitionId::new_unchecked)
                .collect()
        };

        let report = check(&[1, 2, 3]);
        assert!(report.unavailable_partitions.is_empty());
        assert_eq!(report.at_risk_partitions, ids(&[1]));
        assert_eq!(lost(&report, loses_partitions), ["N1"]);

        // a single node can't host a new replica set of two
        let report = check(&[1]);
        assert_eq!(report.unavailable_partitions, ids(&[3]));
        assert_eq!(report.at_risk_partitions, ids(&[0, 1, 2]));
        assert_eq!(lost(&report, loses_partitions), ["N1"]);

        let report = check(&[2, 3]);
        assert_eq!(report.unavailable_partitions, ids(&[1]));
        assert_eq!(report.at_risk_partitions, ids(&[0, 2, 3]));
        assert_eq!(lost(&report, loses_partitions), ["N2", "N3"]);
        // losing N2 also takes out the frozen partition whose pending replica runs there
        let impact = |node: &str| {
            &report.domain_impacts[&FailureDomain {
                scope: LocationScope::Node,
                name: node.to_owned(),
            }]
                .unavailable_partitions
        };
        assert_eq!(*impact("N2"), ids(&[0, 2, 3]));
        assert_eq!(*impact("N3"), ids(&[0, 3]));
    }
}
