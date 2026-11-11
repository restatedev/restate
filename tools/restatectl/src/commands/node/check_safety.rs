// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod legacy;

use std::collections::BTreeMap;

use anyhow::bail;
use cling::prelude::*;
use crossterm::style::Color;
use itertools::Itertools;
use ratatui::buffer::Buffer;
use ratatui::layout::{Margin, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Paragraph, Widget};
use serde::Serialize;

use restate_cli_util::_comfy_table::Table;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_print, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{
    CheckClusterSafetyRequest, CheckClusterSafetyResponse, new_cluster_ctrl_client,
};
use restate_types::PlainNodeId;
use restate_types::locality::{LocationScope, NodeLocation};
use restate_types::nodes_config::{NodesConfiguration, Role};

use crate::connection::ConnectionInfo;
use crate::observe::Severity;
use crate::preview;
use crate::render::{
    FindingLine, bold, compress_ids, dim, finding_lines_text, paint, print_json, truncate,
};
use crate::tui::theme::{self, AMBER, GREEN, RED, Rgb};
use crate::tui::views::Page;
use crate::tui::widgets::{RichTable, node_span};
use crate::tui::{self, Presentation};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "check_safety")]
#[command(after_long_help = preview::enabled().then_some(HELP))]
pub struct CheckSafetyOpts {
    /// Check whether the cluster stays available if these nodes go down, e.g. before stopping
    /// them. Specify multiple nodes as a comma-separated list or specify the option multiple
    /// times.
    #[arg(long, value_delimiter = ',')]
    assume_dead: Vec<PlainNodeId>,
}

const HELP: &str = "\
The verdict is SAFE when all logs and partitions are available and stay available if any one \
failure domain fails, AT RISK when losing one more failure domain would make some of them \
unavailable, and UNSAFE when some already are. Failure domains are at the greatest scope of the \
replication, e.g. zones for {zone: 2}. Each failure domain lists its nodes (● alive, ↓ assumed \
down, ✗ dead) and what losing it would make unavailable.

Exits with an error when the verdict is UNSAFE. With --json, the output is one document with the \
verdict, the failure domains and the affected logs and partitions.";

async fn check_safety(connection: &ConnectionInfo, opts: &CheckSafetyOpts) -> anyhow::Result<()> {
    let request = CheckClusterSafetyRequest {
        assume_dead_nodes: opts.assume_dead.iter().map(|id| u32::from(*id)).collect(),
    };
    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .check_cluster_safety(request.clone())
                .await
        })
        .await?
        .into_inner();

    if !preview::enabled() {
        return legacy::print(&response, &opts.assume_dead);
    }

    let nodes_config = connection.get_nodes_configuration().await?;
    let report = Report::new(response, &nodes_config, &opts.assume_dead);
    match Presentation::current() {
        Presentation::Json => print_json(&report)?,
        Presentation::Plain => print_plain(&report),
        Presentation::Rich => print_rich(&report),
    }
    if report.verdict == Verdict::Unsafe {
        bail!(
            "Some logs or partitions are unavailable{}",
            report.condition()
        );
    }
    Ok(())
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Verdict {
    /// Everything is available and stays so if any one failure domain fails
    Safe,
    /// Everything is available, but losing a failure domain makes something unavailable
    AtRisk,
    /// Something is unavailable
    Unsafe,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
enum NodeState {
    Alive,
    AssumedDead,
    Dead,
}

#[derive(Serialize)]
struct Node {
    id: PlainNodeId,
    state: NodeState,
}

#[derive(Serialize, Default)]
struct Impact {
    write_unavailable_logs: Vec<u32>,
    read_unavailable_logs: Vec<u32>,
    unavailable_partitions: Vec<u32>,
}

impl Impact {
    /// Everything that any of the impacts makes unavailable, `None` if there are none.
    fn union<'a>(impacts: impl Iterator<Item = &'a Impact> + Clone) -> Option<Self> {
        impacts.clone().next()?;
        let ids = |ids: fn(&Impact) -> &Vec<u32>| -> Vec<u32> {
            impacts
                .clone()
                .flat_map(ids)
                .copied()
                .sorted_unstable()
                .dedup()
                .collect()
        };
        Some(Self {
            write_unavailable_logs: ids(|impact| &impact.write_unavailable_logs),
            read_unavailable_logs: ids(|impact| &impact.read_unavailable_logs),
            unavailable_partitions: ids(|impact| &impact.unavailable_partitions),
        })
    }
}

#[derive(Serialize)]
struct Domain {
    scope: String,
    name: String,
    nodes: Vec<Node>,
    /// What losing the domain would make unavailable, if anything
    #[serde(skip_serializing_if = "Option::is_none")]
    if_lost: Option<Impact>,
}

#[derive(Serialize)]
struct Logs {
    total: u32,
    write_unavailable: Vec<u32>,
    read_unavailable: Vec<u32>,
    write_at_risk: Vec<u32>,
    read_at_risk: Vec<u32>,
    unchecked: Vec<u32>,
}

#[derive(Serialize)]
struct Partitions {
    total: u32,
    unavailable: Vec<u32>,
    at_risk: Vec<u32>,
    unchecked: Vec<u32>,
}

#[derive(Serialize)]
struct Report {
    verdict: Verdict,
    assume_dead: Vec<PlainNodeId>,
    /// The scope of the failure domains the cluster is expected to tolerate the loss of
    scope: Option<String>,
    logs: Logs,
    partitions: Partitions,
    domains: Vec<Domain>,
}

impl Report {
    fn new(
        response: CheckClusterSafetyResponse,
        nodes_config: &NodesConfiguration,
        assume_dead: &[PlainNodeId],
    ) -> Self {
        let logs = Logs {
            total: response.num_logs,
            write_unavailable: response.write_unavailable_logs,
            read_unavailable: response.read_unavailable_logs,
            write_at_risk: response.write_at_risk_logs,
            read_at_risk: response.read_at_risk_logs,
            unchecked: response.unchecked_logs,
        };
        let partitions = Partitions {
            total: response.num_partitions,
            unavailable: response.unavailable_partitions,
            at_risk: response.at_risk_partitions,
            unchecked: response.unchecked_partitions,
        };
        let verdict = if !logs.write_unavailable.is_empty()
            || !logs.read_unavailable.is_empty()
            || !partitions.unavailable.is_empty()
        {
            Verdict::Unsafe
        } else if !logs.write_at_risk.is_empty()
            || !logs.read_at_risk.is_empty()
            || !partitions.at_risk.is_empty()
        {
            Verdict::AtRisk
        } else {
            Verdict::Safe
        };

        let state = |id: PlainNodeId| {
            if response.dead_nodes.contains(&u32::from(id)) {
                NodeState::Dead
            } else if assume_dead.contains(&id) {
                NodeState::AssumedDead
            } else {
                NodeState::Alive
            }
        };
        // the nodes of each failure domain at `scope`
        let group = |scope: LocationScope| {
            let mut domains: BTreeMap<(String, String), Vec<Node>> = BTreeMap::new();
            for (id, config) in nodes_config.iter() {
                domains
                    .entry(domain_of(id, &config.location, scope))
                    .or_default()
                    .push(Node {
                        id,
                        state: state(id),
                    });
            }
            for nodes in domains.values_mut() {
                nodes.sort_by_key(|node| node.id);
            }
            domains
        };
        let mut impacts: BTreeMap<(String, String), Impact> = response
            .domain_impacts
            .into_iter()
            .filter_map(|impact| {
                let domain = impact.domain?;
                Some((
                    (domain.scope, domain.name),
                    Impact {
                        write_unavailable_logs: impact.write_unavailable_logs,
                        read_unavailable_logs: impact.read_unavailable_logs,
                        unavailable_partitions: impact.unavailable_partitions,
                    },
                ))
            })
            .collect();

        let scope = response
            .scope
            .as_deref()
            .and_then(|scope| scope.parse().ok())
            .unwrap_or(LocationScope::Node);
        let mut domains: Vec<Domain> = group(scope)
            .into_iter()
            .map(|((scope, name), nodes)| {
                let if_lost = impacts.remove(&(scope.clone(), name.clone()));
                Domain {
                    scope,
                    name,
                    nodes,
                    if_lost,
                }
            })
            .collect();
        // losses at a narrower scope, e.g. of the partitions when logs replicate across zones
        for ((scope, name), impact) in impacts {
            let nodes = scope
                .parse()
                .ok()
                .and_then(|scope| group(scope).remove(&(scope_name(scope), name.clone())))
                .unwrap_or_default();
            domains.push(Domain {
                scope,
                name,
                nodes,
                if_lost: Some(impact),
            });
        }
        // losing a domain also loses the narrower domains within it, and what they'd take down
        let if_lost: Vec<Option<Impact>> = domains
            .iter()
            .map(|outer| {
                Impact::union(
                    domains
                        .iter()
                        .filter(|inner| std::ptr::eq(*inner, outer) || inner.is_within(outer))
                        .filter_map(|inner| inner.if_lost.as_ref()),
                )
            })
            .collect();
        for (domain, if_lost) in domains.iter_mut().zip(if_lost) {
            domain.if_lost = if_lost;
        }
        domains.sort_by_key(|domain| domain.nodes.iter().map(|node| node.id).min());

        Self {
            verdict,
            assume_dead: assume_dead.to_vec(),
            scope: response.scope,
            logs,
            partitions,
            domains,
        }
    }

    /// e.g. " if N3 goes down"
    fn condition(&self) -> String {
        match self.assume_dead.as_slice() {
            [] => String::new(),
            [node] => format!(" if {node} goes down"),
            nodes => format!(" if {} go down", nodes.iter().join(", ")),
        }
    }

    fn scope(&self) -> &str {
        self.scope.as_deref().unwrap_or("node")
    }

    /// Whether the failure domains are the nodes themselves.
    fn is_flat(&self) -> bool {
        self.scope() == "node"
    }

    fn title(&self) -> String {
        match self.assume_dead.as_slice() {
            [] => "current cluster state".to_owned(),
            _ => self.condition().trim_start().to_owned(),
        }
    }

    /// The verdict in a sentence.
    fn summary(&self) -> String {
        match self.verdict {
            Verdict::Unsafe => [
                count(
                    self.logs.write_unavailable.len(),
                    "log",
                    " can't take writes",
                ),
                count(self.logs.read_unavailable.len(), "log", " can't be read"),
                count(self.partitions.unavailable.len(), "partition", " down"),
            ]
            .into_iter()
            .flatten()
            .join(", "),
            Verdict::AtRisk => {
                let logs = self
                    .logs
                    .write_at_risk
                    .iter()
                    .chain(&self.logs.read_at_risk)
                    .unique()
                    .count();
                let affected = [
                    count(logs, "log", ""),
                    count(self.partitions.at_risk.len(), "partition", ""),
                ]
                .into_iter()
                .flatten()
                .join(" and ");
                format!(
                    "Losing one more {} would make {affected} unavailable",
                    self.scope(),
                )
            }
            Verdict::Safe if self.logs.total + self.partitions.total == 0 => {
                "Nothing to check".to_owned()
            }
            Verdict::Safe => {
                let unchecked = [
                    count(self.logs.unchecked.len(), "log", ""),
                    count(self.partitions.unchecked.len(), "partition", ""),
                ]
                .into_iter()
                .flatten()
                .join(" and ");
                let checked = self.logs.total + self.partitions.total
                    - (self.logs.unchecked.len() + self.partitions.unchecked.len()) as u32;
                if unchecked.is_empty() {
                    format!(
                        "All logs and partitions are available and tolerate losing any one {}",
                        self.scope()
                    )
                } else if checked == 0 {
                    format!("Nothing could be checked: {unchecked} not checked")
                } else {
                    format!(
                        "All checked logs and partitions are available and tolerate losing any \
                         one {}, {unchecked} not checked",
                        self.scope()
                    )
                }
            }
        }
    }

    /// The unavailable, at risk and unchecked logs and partitions, most severe first.
    fn findings(&self) -> Vec<FindingLine> {
        let one_more = format!("if one more {} failed", self.scope());
        let groups = [
            (
                Severity::Critical,
                "log",
                &self.logs.write_unavailable,
                "can't take writes".to_owned(),
            ),
            (
                Severity::Critical,
                "log",
                &self.logs.read_unavailable,
                "can't be read reliably".to_owned(),
            ),
            (
                Severity::Critical,
                "partition",
                &self.partitions.unavailable,
                "no alive replica and can't be moved".to_owned(),
            ),
            (
                Severity::Warning,
                "log",
                &self.logs.write_at_risk,
                format!("would stop taking writes {one_more}"),
            ),
            (
                Severity::Warning,
                "log",
                &self.logs.read_at_risk,
                format!("could no longer be read {one_more}"),
            ),
            (
                Severity::Warning,
                "partition",
                &self.partitions.at_risk,
                format!("would become unavailable {one_more}"),
            ),
            (
                Severity::Info,
                "log",
                &self.logs.unchecked,
                "not checked, doesn't use the replicated loglet".to_owned(),
            ),
            (
                Severity::Info,
                "partition",
                &self.partitions.unchecked,
                "not checked, its placement isn't known yet".to_owned(),
            ),
        ];

        let mut lines = Vec::new();
        for (severity, kind, ids, message) in groups {
            if ids.is_empty() {
                continue;
            }
            let compressed = compress_ids(ids.iter().copied());
            let (subject, message) = match ids.as_slice() {
                [id] => (format!("{kind} {id}"), message),
                _ => (
                    format!("{} {kind}s", ids.len()),
                    format!("{message}: {}", truncate(&compressed, 60)),
                ),
            };
            // a command that shows them, when the ids fit
            let hint = (severity > Severity::Info && compressed.len() <= 40).then(|| {
                preview::command(format!("{kind}s list {}", compressed.replace(',', " ")))
            });
            lines.push(FindingLine {
                first: lines.len(),
                severity,
                subject,
                message,
                hint,
            });
        }
        lines
    }
}

/// The failure domain of a node at `scope`, as `(scope, name)`, as the cluster controller
/// computes it: nodes without a location at that scope are their own domain.
fn domain_of(id: PlainNodeId, location: &NodeLocation, scope: LocationScope) -> (String, String) {
    if scope != LocationScope::Node && location.is_scope_defined(scope) {
        (scope_name(scope), location.domain_string(scope, None))
    } else {
        (scope_name(LocationScope::Node), id.to_string())
    }
}

fn scope_name(scope: LocationScope) -> String {
    scope.to_string().to_lowercase()
}

/// e.g. `count(2, "log", " can't take writes")` → "2 logs can't take writes"; `None` for 0.
fn count(count: usize, kind: &str, what: &str) -> Option<String> {
    let plural = if count == 1 { "" } else { "s" };
    (count > 0).then(|| format!("{count} {kind}{plural}{what}"))
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Verdict::Safe => "● SAFE",
            Verdict::AtRisk => "▲ AT RISK",
            Verdict::Unsafe => "✗ UNSAFE",
        }
    }

    fn color(self) -> Rgb {
        match self {
            Verdict::Safe => GREEN,
            Verdict::AtRisk => AMBER,
            Verdict::Unsafe => RED,
        }
    }
}

impl NodeState {
    fn glyph(self) -> &'static str {
        match self {
            NodeState::Alive => "●",
            NodeState::AssumedDead => "↓",
            NodeState::Dead => "✗",
        }
    }

    fn label(self) -> &'static str {
        match self {
            NodeState::Alive => "alive",
            NodeState::AssumedDead => "assumed down",
            NodeState::Dead => "dead",
        }
    }
}

impl Domain {
    /// Whether losing `other` loses all the nodes of this domain.
    fn is_within(&self, other: &Domain) -> bool {
        !self.nodes.is_empty()
            && self
                .nodes
                .iter()
                .all(|node| other.nodes.iter().any(|other| other.id == node.id))
    }

    fn all_down(&self) -> bool {
        self.nodes.iter().all(|node| node.state != NodeState::Alive)
    }

    /// What losing the domain would make unavailable, and whether that's anything.
    fn if_lost(&self) -> (String, Option<bool>) {
        if self.all_down() {
            let going = self
                .nodes
                .iter()
                .any(|node| node.state == NodeState::AssumedDead);
            return (
                if going {
                    "↓ going down"
                } else {
                    "already down"
                }
                .to_owned(),
                None,
            );
        }
        match &self.if_lost {
            Some(impact) => {
                let parts = [
                    count(impact.write_unavailable_logs.len(), "log", " unwritable"),
                    count(impact.read_unavailable_logs.len(), "log", " unreadable"),
                    count(impact.unavailable_partitions.len(), "partition", " down"),
                ];
                (
                    format!("✗ {}", parts.into_iter().flatten().join(" · ")),
                    Some(true),
                )
            }
            None => ("✓ safe to lose".to_owned(), Some(false)),
        }
    }
}

// -- rich

fn print_rich(report: &Report) {
    let width = tui::width();
    let mut page = Page::new(width);
    page.add(Header(report), width, 4);
    page.table(domains_table(report));
    page.finding_lines(report.findings());
    page.print();
}

/// The verdict with the availability of logs and partitions.
struct Header<'a>(&'a Report);

impl Widget for Header<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let report = self.0;
        let color = report.verdict.color();
        let title = Line::from(vec![
            Span::raw(" "),
            Span::styled(format!(" {} ", report.verdict.label()), theme::pill(color)),
            Span::raw("  "),
            Span::styled(report.title(), theme::bold()),
            Span::raw(" "),
        ]);
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(theme::fg(color))
            .title(title);
        let inner = block.inner(area).inner(Margin::new(1, 0));
        block.render(area, buf);

        let ratio = |unavailable: usize, total: u32| {
            let available = (total as usize).saturating_sub(unavailable);
            let color = if unavailable == 0 { GREEN } else { RED };
            Span::styled(
                format!("{available}/{total}"),
                theme::fg(color).add_modifier(Modifier::BOLD),
            )
        };
        let (logs, partitions) = (&report.logs, &report.partitions);
        let numbers = Line::from(vec![
            ratio(logs.write_unavailable.len(), logs.total),
            Span::styled(" logs writable    ", theme::muted()),
            ratio(logs.read_unavailable.len(), logs.total),
            Span::styled(" logs readable    ", theme::muted()),
            ratio(partitions.unavailable.len(), partitions.total),
            Span::styled(" partitions available", theme::muted()),
        ]);
        let summary = Line::styled(report.summary(), theme::fg(color));
        Paragraph::new(vec![numbers, summary]).render(inner, buf);
    }
}

fn domains_table(report: &Report) -> RichTable {
    let flat = report.is_flat();
    let mut states = Vec::new();
    let rows = report
        .domains
        .iter()
        .map(|domain| {
            let (name, nodes) = match domain.nodes.as_slice() {
                // each node is its own domain: its name and state
                [node] if flat => {
                    states.push(node.state);
                    (
                        Line::from(node_spans(node)),
                        Line::styled(node.state.label(), state_style(node.state)),
                    )
                }
                nodes => {
                    let mut name = vec![Span::styled(domain.name.clone(), theme::bold())];
                    if domain.scope != report.scope() {
                        name.push(Span::styled(format!(" ({})", domain.scope), theme::muted()));
                    }
                    let nodes = Itertools::intersperse(
                        nodes.iter().map(|node| {
                            states.push(node.state);
                            node_spans(node)
                        }),
                        vec![Span::raw("  ")],
                    )
                    .flatten()
                    .collect::<Vec<_>>();
                    (Line::from(name), Line::from(nodes))
                }
            };
            let (if_lost, impacted) = domain.if_lost();
            let style = match impacted {
                Some(true) => theme::fg(RED),
                Some(false) => theme::fg(GREEN),
                None if domain
                    .nodes
                    .iter()
                    .any(|n| n.state == NodeState::AssumedDead) =>
                {
                    theme::fg(AMBER)
                }
                None => theme::muted(),
            };
            vec![name, nodes, Line::styled(if_lost, style)]
        })
        .collect();

    let legend = Itertools::intersperse(
        states.into_iter().sorted().dedup().map(|state| {
            vec![
                Span::styled(state.glyph(), state_style(state)),
                Span::styled(format!(" {}", state.label()), theme::muted()),
            ]
        }),
        vec![Span::raw("  ")],
    )
    .flatten()
    .collect::<Vec<_>>();
    RichTable {
        title: vec![
            Span::styled("Failure domains", theme::bold()),
            Span::styled(format!(" · {}s", report.scope()), theme::muted()),
        ],
        header: if flat {
            vec!["NODE", "STATE", "IF LOST"]
        } else {
            vec!["DOMAIN", "NODES", "IF LOST"]
        },
        right: vec![],
        rows,
        legend: Some(Line::from(legend)),
        sorted: None,
    }
}

fn state_style(state: NodeState) -> ratatui::style::Style {
    match state {
        NodeState::Alive => theme::fg(GREEN),
        NodeState::AssumedDead => theme::fg(AMBER).add_modifier(Modifier::BOLD),
        NodeState::Dead => theme::fg(RED).add_modifier(Modifier::BOLD),
    }
}

fn node_spans(node: &Node) -> Vec<Span<'static>> {
    let id = match node.state {
        NodeState::Alive => node_span(node.id),
        state => Span::styled(node.id.to_string(), state_style(state)),
    };
    vec![
        Span::styled(node.state.glyph(), state_style(node.state)),
        Span::raw(" "),
        id,
    ]
}

// -- plain

fn print_plain(report: &Report) {
    let color = |rgb: Rgb| match rgb {
        GREEN => Color::Green,
        AMBER => Color::Yellow,
        _ => Color::Red,
    };
    let verdict_color = color(report.verdict.color());
    c_println!(
        " {}  {}",
        paint(bold(report.verdict.label()), verdict_color),
        bold(report.title())
    );
    let (logs, partitions) = (&report.logs, &report.partitions);
    let ratio = |unavailable: usize, total: u32| {
        format!("{}/{total}", (total as usize).saturating_sub(unavailable))
    };
    c_println!(
        " {} logs writable · {} logs readable · {} partitions available",
        ratio(logs.write_unavailable.len(), logs.total),
        ratio(logs.read_unavailable.len(), logs.total),
        ratio(partitions.unavailable.len(), partitions.total),
    );
    c_println!(" {}", paint(report.summary(), verdict_color));
    c_println!();

    c_println!(
        " {} {}",
        bold("FAILURE DOMAINS"),
        dim(format!("{}s", report.scope()))
    );
    let flat = report.is_flat();
    let node_text = |node: &Node, text: String| match node.state {
        NodeState::Alive => text,
        NodeState::AssumedDead => paint(bold(text), Color::Yellow),
        NodeState::Dead => paint(text, Color::Red),
    };
    let mut table = Table::new_styled();
    table.set_styled_header(if flat {
        vec!["NODE", "STATE", "IF LOST"]
    } else {
        vec!["DOMAIN", "NODES", "IF LOST"]
    });
    for domain in &report.domains {
        let (name, nodes) = match domain.nodes.as_slice() {
            [node] if flat => (
                node_text(node, format!("{} {}", node.state.glyph(), node.id)),
                node_text(node, node.state.label().to_owned()),
            ),
            nodes => {
                let mut name = domain.name.clone();
                if domain.scope != report.scope() {
                    name.push_str(&dim(format!(" ({})", domain.scope)));
                }
                let nodes = nodes
                    .iter()
                    .map(|node| node_text(node, format!("{} {}", node.state.glyph(), node.id)))
                    .join("  ");
                (name, nodes)
            }
        };
        let (if_lost, impacted) = domain.if_lost();
        let if_lost = match impacted {
            Some(true) => paint(if_lost, Color::Red),
            Some(false) => paint(if_lost, Color::Green),
            None => dim(if_lost),
        };
        table.add_row(vec![name, nodes, if_lost]);
    }
    c_println!("{table}");

    let findings = report.findings();
    if !findings.is_empty() {
        c_println!();
        c_println!(" {}", bold("FINDINGS"));
        c_print!("{}", finding_lines_text(findings));
    }
}
