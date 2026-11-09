// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Findings: what deserves an operator's attention in a [`ClusterView`].

use std::collections::BTreeMap;

use itertools::Itertools;
use serde::Serialize;

use restate_types::PlainNodeId;
use restate_types::replication::ReplicationProperty;

use super::model::*;
use crate::preview;

/// The code of the findings about data sources that couldn't be read
pub const SOURCE_UNAVAILABLE: &str = "source-unavailable";

/// A processor that hasn't reported for this long is considered stale
const STALE_REPORT_SECS: i64 = 30;
/// A node that started less than this long ago has recently restarted
const RECENT_RESTART_SECS: u64 = 600;

#[derive(Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Ongoing activity worth knowing about, e.g. a reconfiguration
    Info,
    /// The cluster works, with reduced redundancy or performance
    Warning,
    /// Some part of the cluster is unavailable or needs intervention
    Critical,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Subject {
    Cluster,
    Metadata,
    Node(#[serde(serialize_with = "display")] PlainNodeId),
    Partition(u32),
    Log(u32),
}

impl Subject {
    pub fn kind(&self) -> &'static str {
        match self {
            Subject::Cluster => "cluster",
            Subject::Metadata => "metadata",
            Subject::Node(_) => "node",
            Subject::Partition(_) => "partition",
            Subject::Log(_) => "log",
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct Finding {
    pub severity: Severity,
    /// Stable identifier of the kind of finding
    pub code: &'static str,
    pub subject: Subject,
    pub message: String,
    /// A command that shows more about the subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl Finding {
    fn new(severity: Severity, code: &'static str, subject: Subject, message: String) -> Self {
        let hint = match subject {
            Subject::Partition(id) => Some(preview::command(format_args!(
                "partitions list {id} --replicas"
            ))),
            Subject::Log(id) => Some(format!("restatectl logs describe {id}")),
            Subject::Node(_) => Some("restatectl nodes list --extra".to_owned()),
            Subject::Metadata => Some("restatectl metadata-server list-servers".to_owned()),
            Subject::Cluster => None,
        };
        Self {
            severity,
            code,
            subject,
            message,
            hint,
        }
    }
}

/// Everything in `view` that deserves attention, most severe first.
pub fn analyze(view: &ClusterView) -> Vec<Finding> {
    let mut findings = Vec::new();
    nodes(view, &mut findings);
    metadata(view, &mut findings);
    let copies = view
        .partition_replication
        .as_deref()
        .and_then(|r| r.parse::<ReplicationProperty>().ok())
        .map(|r| usize::from(r.num_copies()));
    for partition in &view.partitions {
        partition_findings(view, partition, copies, &mut findings);
    }
    for log in &view.logs {
        log_findings(view, log, &mut findings);
    }
    for warning in &view.warnings {
        findings.push(Finding::new(
            Severity::Warning,
            SOURCE_UNAVAILABLE,
            Subject::Cluster,
            warning.clone(),
        ));
    }
    findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then(a.code.cmp(b.code))
            .then(a.subject.cmp(&b.subject))
    });
    findings
}

fn nodes(view: &ClusterView, findings: &mut Vec<Finding>) {
    let now = view.captured_at;
    for node in &view.nodes {
        let subject = Subject::Node(node.plain());
        match node.liveness {
            Liveness::Dead => {
                let in_use = view.partitions.iter().any(|p| p.is_member(node.plain()))
                    || view.logs.iter().any(|l| l.nodeset.contains(&node.plain()))
                    || node
                        .metadata_server
                        .as_ref()
                        .is_some_and(|m| m.state == "member");
                let last_seen = node
                    .last_seen_alive
                    .map(|t| {
                        format!(
                            ", last seen {} ago",
                            short_duration((now - t).num_seconds())
                        )
                    })
                    .unwrap_or_default();
                // what the loss costs shows in the partitions, logs and metadata findings
                findings.push(Finding::new(
                    Severity::Warning,
                    "node-dead",
                    subject,
                    format!(
                        "{} is dead{last_seen}{}",
                        node.name,
                        if in_use { "" } else { "; it holds no replicas" }
                    ),
                ));
            }
            // without the cluster state, no node's liveness is known
            Liveness::Unknown if view.has_cluster_state => findings.push(Finding::new(
                Severity::Warning,
                "node-unknown",
                subject,
                format!("{} is not tracked by the cluster controller", node.name),
            )),
            Liveness::Unknown => {}
            Liveness::Alive => {
                if view.nodes_probed && node.seen_versions.is_none() {
                    findings.push(Finding::new(
                        Severity::Warning,
                        "node-unreachable",
                        subject,
                        format!(
                            "{} is alive but restatectl can't reach it at {}",
                            node.name,
                            node.dialed_address.as_ref().unwrap_or(&node.ctrl_address)
                        ),
                    ));
                }
                if let Some(uptime) = node.uptime_s.filter(|u| *u < RECENT_RESTART_SECS) {
                    findings.push(Finding::new(
                        Severity::Info,
                        "node-started",
                        subject,
                        format!(
                            "{} started {} ago",
                            node.name,
                            short_duration(uptime as i64)
                        ),
                    ));
                }
            }
        }

        match node.storage_state.as_deref() {
            None | Some("read-write") => {}
            Some(state) => findings.push(Finding::new(
                match state {
                    "data-loss" => Severity::Critical,
                    "read-only" | "gone" => Severity::Warning,
                    _ => Severity::Info,
                },
                "storage-state",
                subject,
                format!("log-server storage is {state}"),
            )),
        }
        if let Some(state @ ("draining" | "provisioning")) = node.worker_state.as_deref() {
            findings.push(Finding::new(
                Severity::Info,
                "worker-state",
                subject,
                format!("worker is {state}"),
            ));
        }

        if let Some(seen) = &node.seen_versions
            && seen.nodes_config < view.versions.nodes_config
        {
            findings.push(Finding::new(
                Severity::Info,
                "metadata-behind",
                subject,
                format!(
                    "has nodes configuration v{} of v{}",
                    seen.nodes_config, view.versions.nodes_config
                ),
            ));
        }
    }

    let versions: BTreeMap<&str, Vec<PlainNodeId>> = view
        .nodes
        .iter()
        .filter_map(|n| Some((n.binary_version.as_deref()?, n.plain())))
        .into_group_map()
        .into_iter()
        .collect();
    if versions.len() > 1 {
        findings.push(Finding::new(
            Severity::Info,
            "version-skew",
            Subject::Cluster,
            format!(
                "nodes run different versions: {}",
                versions
                    .iter()
                    .map(|(v, nodes)| format!("{v} on {}", nodes.iter().join(",")))
                    .join("; ")
            ),
        ));
    }
}

fn metadata(view: &ClusterView, findings: &mut Vec<Finding>) {
    let Some(MetadataMembers {
        members,
        up,
        unanswered,
        reported,
    }) = view.metadata_members()
    else {
        return;
    };
    let servers: Vec<(&NodeView, &MetadataServerView)> = view
        .nodes
        .iter()
        .filter_map(|n| Some((n, n.metadata_server.as_ref()?)))
        .collect();

    // members that didn't answer may be up
    if !members.is_empty() && up + unanswered <= members.len() / 2 {
        findings.push(Finding::new(
            Severity::Critical,
            "metadata-quorum",
            Subject::Metadata,
            format!(
                "only {up} of {} metadata members are up, a majority is needed",
                members.len()
            ),
        ));
    } else if up < members.len() {
        findings.push(Finding::new(
            Severity::Warning,
            "metadata-member-down",
            Subject::Metadata,
            format!(
                "{up} of {} metadata members are up{}",
                members.len(),
                if unanswered > 0 {
                    format!(", {unanswered} didn't answer")
                } else {
                    String::new()
                }
            ),
        ));
    }

    let leaders: Vec<PlainNodeId> = servers
        .iter()
        .filter_map(|(_, m)| m.leader)
        .unique()
        .collect();
    match leaders.len() {
        // only servers that answered know a leader
        0 if !reported => {}
        0 => findings.push(Finding::new(
            Severity::Critical,
            "metadata-no-leader",
            Subject::Metadata,
            "no metadata server knows a leader".to_owned(),
        )),
        1 => {}
        _ => findings.push(Finding::new(
            Severity::Warning,
            "metadata-leader-disagreement",
            Subject::Metadata,
            format!(
                "metadata servers disagree on the leader: {}",
                leaders.iter().join(", ")
            ),
        )),
    }

    for (node, server) in &servers {
        if node.liveness != Liveness::Alive {
            continue;
        }
        let subject = Subject::Node(node.plain());
        if view.nodes_probed && node.seen_versions.is_none() {
            // reported as unreachable already
            continue;
        }
        if let Some(err) = &server.error {
            findings.push(Finding::new(
                Severity::Warning,
                "metadata-server-unreachable",
                subject,
                format!("metadata server status unavailable: {err}"),
            ));
        } else if server.state == "member"
            && let Some(status @ ("starting" | "unknown" | "awaiting-provisioning")) =
                server.status.as_deref()
        {
            findings.push(Finding::new(
                Severity::Warning,
                "metadata-server-not-ready",
                subject,
                format!("metadata server is {status}"),
            ));
        }
    }
}

fn partition_findings(
    view: &ClusterView,
    p: &PartitionView,
    copies: Option<usize>,
    findings: &mut Vec<Finding>,
) {
    let subject = Subject::Partition(p.id);
    let now = view.captured_at;

    let rivals = p.rival_leaders();
    if !rivals.is_empty() {
        findings.push(Finding::new(
            Severity::Critical,
            "rival-leaders",
            subject,
            format!(
                "{} all report leading epoch e{}",
                rivals.iter().join(", "),
                p.epoch.unwrap_or_default()
            ),
        ));
    }

    if p.health == PartitionHealth::Unavailable && p.is_led() {
        findings.push(Finding::new(
            Severity::Critical,
            "partition-log-down",
            subject,
            format!(
                "led by {}, but log {} can't take appends",
                p.leader.expect("is led"),
                opt_id(p.log_id)
            ),
        ));
    } else if p.health == PartitionHealth::Unavailable {
        let replicas = if p.replicas.is_empty() {
            "no processors running".to_owned()
        } else {
            p.replicas
                .iter()
                .map(|r| format!("{} {}", r.node, replica_state(r)))
                .join(", ")
        };
        findings.push(Finding::new(
            Severity::Critical,
            "partition-unavailable",
            subject,
            format!("no active leader ({replicas})"),
        ));
    }

    if let Some(membership) = &p.membership {
        if let Some(next) = &membership.next {
            findings.push(Finding::new(
                Severity::Info,
                "reconfiguring",
                subject,
                format!(
                    "replica set v{} [{}] → v{} [{}]",
                    membership.version,
                    membership.members.iter().join(","),
                    next.version,
                    next.members.iter().join(",")
                ),
            ));
        }
        if let Some(copies) = copies
            && membership.members.len() < copies
        {
            findings.push(Finding::new(
                Severity::Warning,
                "under-replicated",
                subject,
                format!(
                    "{} of {copies} replicas in the replica set",
                    membership.members.len()
                ),
            ));
        }
    }

    for missing in &p.missing {
        let why = match view.node(*missing).map(|n| n.liveness) {
            Some(Liveness::Dead) => "its node is dead",
            Some(Liveness::Alive) => "it runs no processor",
            _ => "its node is unknown",
        };
        findings.push(Finding::new(
            Severity::Warning,
            "replica-missing",
            subject,
            format!("member {missing} is not running: {why}"),
        ));
    }

    for r in &p.replicas {
        if !p.counts(r) {
            continue;
        }
        if let Some(reason) = r.broken_reason {
            findings.push(Finding::new(
                Severity::Critical,
                "replica-broken",
                subject,
                format!(
                    "{} parked its processor ({reason}); its store must be replaced",
                    r.node
                ),
            ));
            continue;
        }
        if r.stale_epoch {
            findings.push(Finding::new(
                Severity::Warning,
                "stale-leader",
                subject,
                format!(
                    "{} still leads epoch e{}, newest is e{}",
                    r.node,
                    r.epoch.unwrap_or_default(),
                    p.epoch.unwrap_or_default()
                ),
            ));
        }
        match r.replay {
            Replay::CatchingUp => findings.push(Finding::new(
                Severity::Warning,
                "catching-up",
                subject,
                format!(
                    "{} is catching up{}",
                    r.node,
                    r.lag
                        .map(|lag| format!(", {lag} records behind"))
                        .unwrap_or_default()
                ),
            )),
            Replay::Starting => findings.push(Finding::new(
                Severity::Info,
                "replica-starting",
                subject,
                format!("{} is starting", r.node),
            )),
            Replay::Active | Replay::Unknown => {}
        }
        if matches!(
            r.mode,
            Mode::Candidate | Mode::BecomingLeader | Mode::SteppingDown
        ) {
            findings.push(Finding::new(
                Severity::Info,
                "leadership-change",
                subject,
                format!("{} is {}", r.node, mode_label(r.mode)),
            ));
        }
        if let Some(updated) = r.updated_at
            && (now - updated).num_seconds() > STALE_REPORT_SECS
        {
            findings.push(Finding::new(
                Severity::Warning,
                "stale-report",
                subject,
                format!(
                    "{} last reported {} ago",
                    r.node,
                    short_duration((now - updated).num_seconds())
                ),
            ));
        }
    }
}

fn log_findings(view: &ClusterView, log: &LogView, findings: &mut Vec<Finding>) {
    let subject = Subject::Log(log.id);
    match (log.sequencer, log.sequencer_state) {
        (Some(sequencer), Some(SequencerState::Dead)) => findings.push(Finding::new(
            Severity::Critical,
            "sequencer-dead",
            subject,
            format!("sequencer {sequencer} is dead; appends wait for a reconfiguration"),
        )),
        (Some(sequencer), Some(SequencerState::Restarted)) => findings.push(Finding::new(
            Severity::Warning,
            "sequencer-restarted",
            subject,
            format!(
                "sequencer {sequencer} has restarted as {}; appends wait for a reconfiguration",
                view.node(sequencer.as_plain())
                    .map(|n| n.id.to_string())
                    .unwrap_or_default()
            ),
        )),
        _ => {}
    }
    if log.sealed {
        findings.push(Finding::new(
            Severity::Warning,
            "log-sealed",
            subject,
            format!(
                "tail segment {} is sealed; appends wait for the chain to be extended",
                log.tail_segment
            ),
        ));
    }
    if let Some(writable) = log.writable_nodes {
        if log.write_quorum == Some(false) {
            findings.push(Finding::new(
                Severity::Critical,
                "nodeset-unwritable",
                subject,
                format!(
                    "{writable} of {} nodeset members take writes, too few for replication {}",
                    log.nodeset.len(),
                    log.replication.as_deref().unwrap_or("?")
                ),
            ));
        } else if log.is_nodeset_degraded() {
            findings.push(Finding::new(
                Severity::Warning,
                "nodeset-degraded",
                subject,
                format!(
                    "{writable} of {} nodeset members take writes",
                    log.nodeset.len()
                ),
            ));
        }
    }
    reader_findings(view, log, findings);
    if log.sequencer_with_leader() == Some(false)
        && log.sequencer_state == Some(SequencerState::Alive)
    {
        findings.push(Finding::new(
            Severity::Info,
            "sequencer-away-from-leader",
            subject,
            format!(
                "sequencer {} is not on leader {}",
                log.sequencer.expect("checked"),
                log.partition_leader.expect("checked")
            ),
        ));
    }
}

/// What the log's read streams wait for, if anything: a finding per log and kind, naming the
/// nodes. How far readers are behind the tail isn't judged, like a replica's lag.
fn reader_findings(view: &ClusterView, log: &LogView, findings: &mut Vec<Finding>) {
    let subject = Subject::Log(log.id);
    let nodes = |phase: ReaderPhase| {
        log.readers
            .iter()
            .filter(|r| r.phase() == phase)
            .map(|r| r.node)
            .unique()
            .join(", ")
    };
    let sealing = nodes(ReaderPhase::Sealing);
    let awaiting = nodes(ReaderPhase::AwaitingSeal);
    if !sealing.is_empty() {
        findings.push(Finding::new(
            Severity::Warning,
            "reader-sealing",
            subject,
            format!(
                "readers on {sealing} gave up waiting for a seal and seal the chain themselves"
            ),
        ));
    } else if !awaiting.is_empty() {
        findings.push(Finding::new(
            Severity::Warning,
            "reader-awaiting-seal",
            subject,
            format!(
                "readers on {awaiting} reached the end of a sealed loglet and wait for the chain to \
                 be sealed"
            ),
        ));
    }
    let switching = nodes(ReaderPhase::Switching);
    if !switching.is_empty() {
        findings.push(Finding::new(
            Severity::Info,
            "reader-switching",
            subject,
            format!("readers on {switching} are switching segments or starting"),
        ));
    }
    if let Some(p) = log
        .partition_id
        .and_then(|id| view.partitions.iter().find(|p| p.id == id))
        .filter(|_| view.has_cluster_state)
    {
        let orphans = log
            .readers
            .iter()
            .filter(|r| r.tailing && p.replica_on(r.node).is_none())
            .map(|r| r.node)
            .unique()
            .join(", ");
        if !orphans.is_empty() {
            findings.push(Finding::new(
                Severity::Info,
                "reader-without-processor",
                subject,
                format!(
                    "{orphans} read the log but run no processor for partition {}",
                    p.id
                ),
            ));
        }
    }
}

pub fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Leader => "leader",
        Mode::Follower => "follower",
        Mode::Candidate => "candidate",
        Mode::BecomingLeader => "becoming leader",
        Mode::SteppingDown => "stepping down",
        Mode::Broken => "broken",
        Mode::Unknown => "unknown",
    }
}

fn replica_state(r: &ReplicaView) -> &'static str {
    match (r.mode, r.replay) {
        (Mode::Broken, _) => "broken",
        (_, Replay::CatchingUp) => "catching up",
        (_, Replay::Starting) => "starting",
        (mode, _) => mode_label(mode),
    }
}

/// `90` → `1m30s`, `7200` → `2h`
pub fn short_duration(secs: i64) -> String {
    let secs = secs.max(0);
    let (d, h, m, s) = (secs / 86_400, secs / 3_600 % 24, secs / 60 % 60, secs % 60);
    match (d, h, m, s) {
        (0, 0, 0, s) => format!("{s}s"),
        (0, 0, m, 0) => format!("{m}m"),
        (0, 0, m, s) => format!("{m}m{s}s"),
        (0, h, 0, _) => format!("{h}h"),
        (0, h, m, _) => format!("{h}h{m}m"),
        (d, 0, _, _) => format!("{d}d"),
        (d, h, _, _) => format!("{d}d{h}h"),
    }
}

fn opt_id(id: Option<u32>) -> String {
    id.map(|id| id.to_string())
        .unwrap_or_else(|| "?".to_owned())
}

fn display<T: std::fmt::Display, S: serde::Serializer>(v: &T, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(v)
}
