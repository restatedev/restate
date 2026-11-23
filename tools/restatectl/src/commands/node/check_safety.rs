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

use std::cmp::Reverse;

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
    CheckClusterSafetyRequest, CheckClusterSafetyResponse, MetadataAvailability,
    new_cluster_ctrl_client,
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
The verdict is SAFE when the metadata store, all logs and partitions are available and stay \
available if any one failure domain fails, AT RISK when losing one more failure domain would make \
some of them unavailable, UNSAFE when some already are, and INCOMPLETE when nothing checked is at \
risk but some could not be checked. A failure domain is at the greatest scope \
of a log's or partition's replication, e.g. a zone for {zone: 2}. The replicated metadata store \
is available while a majority of its members is alive, and is checked against the same failure \
domains. The failure domains whose loss would make \
something unavailable are listed with their nodes (● alive, ↓ assumed down, ✗ dead). Losing several \
of them, or a wider failure domain, can make more unavailable: check it with --assume-dead.

Exits with an error when the verdict is UNSAFE, or, with --assume-dead, when some logs or \
partitions could not be checked. With --json, the output is one document with the \
verdict, the critical failure domains and the affected logs and partitions.";

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
        bail!("{}{}", report.summary(), report.condition());
    }
    if !opts.assume_dead.is_empty() && report.has_unchecked() {
        bail!(
            "Some logs, partitions or the metadata store could not be checked, so it's unknown \
             whether they stay available{}",
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
    /// Everything checked is safe, but some logs or partitions could not be checked
    Incomplete,
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

/// Whether the replicated metadata store has a majority of alive members.
#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum MetadataStore {
    Available,
    /// Losing a failure domain makes it unavailable
    AtRisk,
    Unavailable,
    /// None of its members are known
    Unchecked,
}

#[derive(Serialize, Default)]
struct Impact {
    write_unavailable_logs: Vec<u32>,
    read_unavailable_logs: Vec<u32>,
    unavailable_partitions: Vec<u32>,
    metadata_unavailable: bool,
}

#[derive(Serialize)]
struct Domain {
    scope: String,
    name: String,
    nodes: Vec<Node>,
    /// What losing the domain alone would make unavailable
    if_lost: Impact,
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
    /// Unset if the cluster doesn't use the replicated metadata store
    metadata_store: Option<MetadataStore>,
    assume_dead: Vec<PlainNodeId>,
    /// Nodes that are dead, not counting `assume_dead`
    dead_nodes: Vec<PlainNodeId>,
    logs: Logs,
    partitions: Partitions,
    /// The failure domains whose loss alone would make something unavailable
    critical_domains: Vec<Domain>,
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
        let metadata_store = response
            .metadata_availability
            .and_then(|availability| MetadataAvailability::try_from(availability).ok())
            .and_then(|availability| match availability {
                MetadataAvailability::Unknown => None,
                MetadataAvailability::Available => Some(MetadataStore::Available),
                MetadataAvailability::AtRisk => Some(MetadataStore::AtRisk),
                MetadataAvailability::Unavailable => Some(MetadataStore::Unavailable),
                MetadataAvailability::Unchecked => Some(MetadataStore::Unchecked),
            });
        let verdict = if !logs.write_unavailable.is_empty()
            || !logs.read_unavailable.is_empty()
            || !partitions.unavailable.is_empty()
            || metadata_store == Some(MetadataStore::Unavailable)
        {
            Verdict::Unsafe
        } else if !logs.write_at_risk.is_empty()
            || !logs.read_at_risk.is_empty()
            || !partitions.at_risk.is_empty()
            || metadata_store == Some(MetadataStore::AtRisk)
        {
            Verdict::AtRisk
        } else if !logs.unchecked.is_empty()
            || !partitions.unchecked.is_empty()
            || metadata_store == Some(MetadataStore::Unchecked)
        {
            Verdict::Incomplete
        } else {
            Verdict::Safe
        };

        let dead_nodes: Vec<PlainNodeId> = response
            .dead_nodes
            .iter()
            .map(|id| PlainNodeId::from(*id))
            .collect();
        let state = |id: PlainNodeId| {
            if dead_nodes.contains(&id) {
                NodeState::Dead
            } else if assume_dead.contains(&id) {
                NodeState::AssumedDead
            } else {
                NodeState::Alive
            }
        };
        // the nodes of the failure domain `name` at `scope`
        let nodes = |scope: &str, name: &str| -> Vec<Node> {
            let Ok(location_scope) = scope.parse() else {
                return Vec::new();
            };
            nodes_config
                .iter()
                .filter(|(id, config)| {
                    let (domain_scope, domain_name) =
                        domain_of(*id, &config.location, location_scope);
                    domain_scope == scope && domain_name == name
                })
                .map(|(id, _)| Node {
                    id,
                    state: state(id),
                })
                .sorted_by_key(|node| node.id)
                .collect()
        };
        let mut critical_domains: Vec<Domain> = response
            .domain_impacts
            .into_iter()
            .filter_map(|impact| {
                let domain = impact.domain?;
                Some(Domain {
                    nodes: nodes(&domain.scope, &domain.name),
                    scope: domain.scope,
                    name: domain.name,
                    if_lost: Impact {
                        write_unavailable_logs: impact.write_unavailable_logs,
                        read_unavailable_logs: impact.read_unavailable_logs,
                        unavailable_partitions: impact.unavailable_partitions,
                        metadata_unavailable: impact.metadata_unavailable,
                    },
                })
            })
            .collect();
        critical_domains.sort_by_key(|domain| domain.nodes.iter().map(|node| node.id).min());

        Self {
            verdict,
            metadata_store,
            assume_dead: assume_dead.to_vec(),
            dead_nodes,
            logs,
            partitions,
            critical_domains,
        }
    }

    fn has_unchecked(&self) -> bool {
        !self.logs.unchecked.is_empty()
            || !self.partitions.unchecked.is_empty()
            || self.metadata_store == Some(MetadataStore::Unchecked)
    }

    /// e.g. " if N3 goes down"
    fn condition(&self) -> String {
        match self.assume_dead.as_slice() {
            [] => String::new(),
            [node] => format!(" if {node} goes down"),
            nodes => format!(" if {} go down", nodes.iter().join(", ")),
        }
    }

    /// Whether the critical failure domains are all single nodes.
    fn is_flat(&self) -> bool {
        self.critical_domains
            .iter()
            .all(|domain| domain.scope == scope_name(LocationScope::Node))
    }

    /// The nodes that are down, by state, e.g. `[(Dead, "N3, N5"), (AssumedDead, "N4")]`.
    fn down_nodes(&self) -> Vec<(NodeState, String)> {
        let mut assumed_dead = self
            .assume_dead
            .iter()
            .filter(|id| !self.dead_nodes.contains(id));
        [
            (NodeState::Dead, self.dead_nodes.iter().join(", ")),
            (NodeState::AssumedDead, assumed_dead.join(", ")),
        ]
        .into_iter()
        .filter(|(_, ids)| !ids.is_empty())
        .collect()
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
                (self.metadata_store == Some(MetadataStore::Unavailable))
                    .then(|| "the metadata store has no quorum".to_owned()),
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
                    (self.metadata_store == Some(MetadataStore::AtRisk))
                        .then(|| "the metadata store".to_owned()),
                    count(logs, "log", ""),
                    count(self.partitions.at_risk.len(), "partition", ""),
                ]
                .into_iter()
                .flatten()
                .join(" and ");
                format!("Losing one more failure domain would make {affected} unavailable")
            }
            Verdict::Safe
                if self.logs.total + self.partitions.total == 0
                    && self.metadata_store.is_none() =>
            {
                "Nothing to check".to_owned()
            }
            Verdict::Safe if self.metadata_store.is_some() => {
                "The metadata store, all logs and partitions are available and tolerate losing \
                 any one failure domain"
                    .to_owned()
            }
            Verdict::Safe => {
                "All logs and partitions are available and tolerate losing any one failure domain"
                    .to_owned()
            }
            Verdict::Incomplete => {
                let metadata_unchecked = self.metadata_store == Some(MetadataStore::Unchecked);
                let unchecked = [
                    metadata_unchecked.then(|| "the metadata store".to_owned()),
                    count(self.logs.unchecked.len(), "log", ""),
                    count(self.partitions.unchecked.len(), "partition", ""),
                ]
                .into_iter()
                .flatten()
                .join(" and ");
                let checked = self.logs.total + self.partitions.total
                    - (self.logs.unchecked.len() + self.partitions.unchecked.len()) as u32
                    + u32::from(self.metadata_store.is_some() && !metadata_unchecked);
                if checked == 0 {
                    format!("Nothing could be checked: {unchecked} not checked")
                } else {
                    format!(
                        "Everything checked is available and not at risk, {unchecked} not checked"
                    )
                }
            }
        }
    }

    /// The unavailable, at risk and unchecked metadata store, logs and partitions, most severe
    /// first.
    fn findings(&self) -> Vec<FindingLine> {
        let one_more = "if one more failure domain failed";
        let mut lines = Vec::new();
        let metadata_store = match self.metadata_store {
            Some(MetadataStore::Unavailable) => Some((
                Severity::Critical,
                "less than a majority of its members is alive".to_owned(),
            )),
            Some(MetadataStore::AtRisk) => Some((
                Severity::Warning,
                format!("would lose the majority of its members {one_more}"),
            )),
            Some(MetadataStore::Unchecked) => Some((
                Severity::Info,
                "not checked, none of its members are known".to_owned(),
            )),
            Some(MetadataStore::Available) | None => None,
        };
        if let Some((severity, message)) = metadata_store {
            lines.push(FindingLine {
                first: 0,
                severity,
                subject: "metadata store".to_owned(),
                message,
                hint: Some(preview::command("metadata-server list-servers")),
            });
        }
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
                "not checked, its placement or leader isn't known".to_owned(),
            ),
        ];

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
        lines.sort_by_key(|line| Reverse(line.severity));
        for (index, line) in lines.iter_mut().enumerate() {
            line.first = index;
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
            Verdict::Incomplete => "? INCOMPLETE",
        }
    }

    fn color(self) -> Rgb {
        match self {
            Verdict::Safe => GREEN,
            Verdict::AtRisk | Verdict::Incomplete => AMBER,
            Verdict::Unsafe => RED,
        }
    }
}

impl MetadataStore {
    /// e.g. "✓ metadata quorum"
    fn label(self) -> &'static str {
        match self {
            MetadataStore::Available => "✓ metadata quorum",
            MetadataStore::AtRisk => "▲ metadata quorum at risk",
            MetadataStore::Unavailable => "✗ no metadata quorum",
            MetadataStore::Unchecked => "? metadata quorum unknown",
        }
    }

    fn color(self) -> Rgb {
        match self {
            MetadataStore::Available => GREEN,
            MetadataStore::AtRisk | MetadataStore::Unchecked => AMBER,
            MetadataStore::Unavailable => RED,
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

impl Impact {
    /// e.g. "✗ metadata store down · 2 logs unwritable · 1 partition down"
    fn describe(&self) -> String {
        let parts = [
            self.metadata_unavailable
                .then(|| "metadata store down".to_owned()),
            count(self.write_unavailable_logs.len(), "log", " unwritable"),
            count(self.read_unavailable_logs.len(), "log", " unreadable"),
            count(self.unavailable_partitions.len(), "partition", " down"),
        ];
        format!("✗ {}", parts.into_iter().flatten().join(" · "))
    }
}

// -- rich

fn print_rich(report: &Report) {
    let width = tui::width();
    let mut page = Page::new(width);
    let header = Header::new(report, width);
    let height = header.lines.len() as u16 + 2;
    page.add(header, width, height);
    if !report.critical_domains.is_empty() {
        page.table(domains_table(report));
    }
    page.finding_lines(report.findings());
    page.print();
}

/// The verdict with the availability of logs and partitions, and the nodes that are down.
struct Header {
    verdict: Verdict,
    title: String,
    lines: Vec<Line<'static>>,
}

impl Header {
    /// Wraps the lines to fit a panel of `width` columns.
    fn new(report: &Report, width: u16) -> Self {
        // the border and margin on each side
        let width = usize::from(width.saturating_sub(4));
        let ratio = |unavailable: usize, total: u32, what: &str| {
            let available = (total as usize).saturating_sub(unavailable);
            let color = if unavailable == 0 { GREEN } else { RED };
            vec![
                Span::styled(
                    format!("{available}/{total}"),
                    theme::fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!(" {what}"), theme::muted()),
            ]
        };
        let (logs, partitions) = (&report.logs, &report.partitions);
        let mut numbers = vec![
            ratio(logs.write_unavailable.len(), logs.total, "logs writable"),
            ratio(logs.read_unavailable.len(), logs.total, "logs readable"),
            ratio(
                partitions.unavailable.len(),
                partitions.total,
                "partitions available",
            ),
        ];
        if let Some(metadata_store) = report.metadata_store {
            numbers.push(vec![Span::styled(
                metadata_store.label(),
                theme::fg(metadata_store.color()).add_modifier(Modifier::BOLD),
            )]);
        }
        let down = report
            .down_nodes()
            .into_iter()
            .map(|(state, ids)| {
                vec![Span::styled(
                    format!("{} {ids} {}", state.glyph(), state.label()),
                    state_style(state),
                )]
            })
            .collect();
        let summary_style = theme::fg(report.verdict.color());
        let summary = report
            .summary()
            .split(' ')
            .map(|word| vec![Span::styled(word.to_owned(), summary_style)])
            .collect();

        let mut lines = pack(numbers, Span::raw("    "), width);
        lines.extend(pack(down, Span::styled(" · ", theme::muted()), width));
        lines.extend(pack(summary, Span::raw(" "), width));
        Self {
            verdict: report.verdict,
            title: report.title(),
            lines,
        }
    }
}

/// Packs `segments` into as few lines of at most `width` columns as possible, separating the
/// segments on a line by `separator`.
fn pack(
    segments: Vec<Vec<Span<'static>>>,
    separator: Span<'static>,
    width: usize,
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line> = Vec::new();
    for segment in segments {
        let segment_width: usize = segment.iter().map(Span::width).sum();
        match lines.last_mut() {
            Some(line) if line.width() + separator.width() + segment_width <= width => {
                line.spans.push(separator.clone());
                line.spans.extend(segment);
            }
            _ => lines.push(Line::from(segment)),
        }
    }
    lines
}

impl Widget for Header {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let color = self.verdict.color();
        let title = Line::from(vec![
            Span::raw(" "),
            Span::styled(format!(" {} ", self.verdict.label()), theme::pill(color)),
            Span::raw("  "),
            Span::styled(self.title, theme::bold()),
            Span::raw(" "),
        ]);
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(theme::fg(color))
            .title(title);
        let inner = block.inner(area).inner(Margin::new(1, 0));
        block.render(area, buf);
        Paragraph::new(self.lines).render(inner, buf);
    }
}

fn domains_table(report: &Report) -> RichTable {
    let flat = report.is_flat();
    let mut states = Vec::new();
    let rows = report
        .critical_domains
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
                    let name = vec![
                        Span::styled(domain.name.clone(), theme::bold()),
                        Span::styled(format!(" ({})", domain.scope), theme::muted()),
                    ];
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
            let if_lost = Line::styled(domain.if_lost.describe(), theme::fg(RED));
            vec![name, nodes, if_lost]
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
            Span::styled("Critical failure domains", theme::bold()),
            Span::styled(" · losing one alone", theme::muted()),
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
    let metadata_store = report
        .metadata_store
        .map(|metadata_store| {
            format!(
                " · {}",
                paint(metadata_store.label(), color(metadata_store.color()))
            )
        })
        .unwrap_or_default();
    c_println!(
        " {} logs writable · {} logs readable · {} partitions available{metadata_store}",
        ratio(logs.write_unavailable.len(), logs.total),
        ratio(logs.read_unavailable.len(), logs.total),
        ratio(partitions.unavailable.len(), partitions.total),
    );
    let state_color = |state: NodeState| match state {
        NodeState::Alive => Color::Green,
        NodeState::AssumedDead => Color::Yellow,
        NodeState::Dead => Color::Red,
    };
    let down = report.down_nodes();
    if !down.is_empty() {
        c_println!(
            " {}",
            down.into_iter()
                .map(|(state, ids)| paint(
                    format!("{} {ids} {}", state.glyph(), state.label()),
                    state_color(state)
                ))
                .join(" · ")
        );
    }
    c_println!(" {}", paint(report.summary(), verdict_color));

    if !report.critical_domains.is_empty() {
        c_println!();
        c_println!(
            " {} {}",
            bold("CRITICAL FAILURE DOMAINS"),
            dim("losing one alone")
        );
        let flat = report.is_flat();
        let node_text = |node: &Node, text: String| match node.state {
            NodeState::Alive => text,
            state => paint(bold(text), state_color(state)),
        };
        let mut table = Table::new_styled();
        table.set_styled_header(if flat {
            vec!["NODE", "STATE", "IF LOST"]
        } else {
            vec!["DOMAIN", "NODES", "IF LOST"]
        });
        for domain in &report.critical_domains {
            let (name, nodes) = match domain.nodes.as_slice() {
                [node] if flat => (
                    node_text(node, format!("{} {}", node.state.glyph(), node.id)),
                    node_text(node, node.state.label().to_owned()),
                ),
                nodes => (
                    format!("{}{}", domain.name, dim(format!(" ({})", domain.scope))),
                    nodes
                        .iter()
                        .map(|node| node_text(node, format!("{} {}", node.state.glyph(), node.id)))
                        .join("  "),
                ),
            };
            table.add_row(vec![
                name,
                nodes,
                paint(domain.if_lost.describe(), Color::Red),
            ]);
        }
        c_println!("{table}");
    }

    let findings = report.findings();
    if !findings.is_empty() {
        c_println!();
        c_println!(" {}", bold("FINDINGS"));
        c_print!("{}", finding_lines_text(findings));
    }
}
