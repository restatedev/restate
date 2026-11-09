// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Building blocks of the rich output, shared by the inline views and the dashboard.

use std::collections::BTreeSet;

use chrono::Utc;
use itertools::Itertools;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Margin, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Cell, Paragraph, Row, Table, Widget};

use restate_types::{GenerationalNodeId, PlainNodeId};

use super::theme::{self, AMBER, BLUE, GREEN, RED, TRACK};
use crate::observe::{
    ClusterView, Liveness, LogPlacement, LogView, Mode, NodeView, PartitionHealth,
    PartitionPlacement, PartitionView, ReaderPhase, ReaderView, Replay, ReplicaView,
    SequencerState, Severity, Summary, imbalanced, mode_label, short_duration,
};
use crate::render::{FindingLine, Verdict, ago, health_label};

fn span(text: impl Into<String>, style: Style) -> Span<'static> {
    Span::styled(text.into(), style)
}

fn raw(text: impl Into<String>) -> Span<'static> {
    Span::raw(text.into())
}

/// A block with rounded borders in `border` style; its titles keep their own colors.
fn rounded(border: Style) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border)
        .title_style(Style::new().fg(ratatui::style::Color::Reset))
}

/// A bordered panel with a title.
pub fn panel(title: Vec<Span<'static>>) -> Block<'static> {
    let mut spans = vec![raw(" ")];
    spans.extend(title);
    spans.push(raw(" "));
    rounded(theme::border()).title(Line::from(spans))
}

pub fn node_span(id: PlainNodeId) -> Span<'static> {
    span(id.to_string(), theme::node_style(id))
}

pub fn generational_span(id: GenerationalNodeId) -> Span<'static> {
    span(id.to_string(), theme::node_style(id.as_plain()))
}

fn opt_num(value: Option<u64>) -> Span<'static> {
    match value {
        Some(v) => raw(v.to_string()),
        None => span("-", theme::muted()),
    }
}

fn lag_span(lag: Option<u64>) -> Span<'static> {
    match lag {
        None => span("-", theme::muted()),
        Some(0) => span("0", theme::muted()),
        Some(lag) => raw(lag.to_string()),
    }
}

// -- header

pub const HEADER_HEIGHT: u16 = 4;

/// The cluster's verdict and its key numbers.
pub struct Header<'a> {
    pub view: &'a ClusterView,
    pub verdict: Verdict,
    /// right side of the title, e.g. when the view was captured
    pub status: Line<'static>,
}

impl Widget for Header<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let view = self.view;
        let (label, color) = match self.verdict {
            Verdict::Healthy => (" ● HEALTHY ", GREEN),
            Verdict::Degraded => (" ▲ DEGRADED ", AMBER),
            Verdict::Critical => (" ✗ CRITICAL ", RED),
        };
        let versions = view
            .nodes
            .iter()
            .filter_map(|n| n.binary_version.as_deref())
            .unique()
            .join(", ");
        let mut title = vec![
            raw(" "),
            span(label, theme::pill(color)),
            raw("  "),
            span(view.cluster_name.clone(), theme::bold()),
        ];
        if !versions.is_empty() {
            title.push(span(format!("  restate {versions}"), theme::muted()));
        }
        title.push(raw(" "));
        let block = rounded(theme::fg(color))
            .title(Line::from(title))
            .title_top(self.status.right_aligned());
        let inner = block.inner(area);
        block.render(area, buf);

        let s = Summary::new(view);
        let ratio = |good: usize, total: usize, bad: Option<theme::Rgb>| {
            let color = match (good == total, bad) {
                (true, _) => GREEN,
                (false, Some(bad)) => bad,
                (false, None) => AMBER,
            };
            span(
                format!("{good}/{total}"),
                theme::fg(color).add_modifier(Modifier::BOLD),
            )
        };
        let note = |count: usize, text: &str, color| {
            (count > 0).then(|| span(format!(" · {count} {text}"), theme::fg(color)))
        };

        let mut kpis: Vec<(Line<'static>, Line<'static>)> = Vec::new();

        let mut label = vec![span("nodes alive", theme::muted())];
        label.extend(note(s.nodes.dead, "dead", RED));
        kpis.push((
            Line::from(ratio(s.nodes.alive, s.nodes.total, None)),
            Line::from(label),
        ));

        let mut label = vec![span("partitions led", theme::muted())];
        label.extend(note(s.partitions.unavailable, "down", RED));
        label.extend(note(s.partitions.degraded, "degraded", AMBER));
        label.extend(note(s.partitions.reconfiguring, "moving", BLUE));
        let partitions_value = if s.partitions.unavailable > 0 {
            ratio(
                s.partitions.total - s.partitions.unavailable,
                s.partitions.total,
                Some(RED),
            )
        } else {
            ratio(s.partitions.led, s.partitions.total, Some(RED))
        };
        kpis.push((Line::from(partitions_value), Line::from(label)));

        let mut label = vec![span("logs writable", theme::muted())];
        label.extend(note(s.logs.sealed, "sealed", AMBER));
        kpis.push((
            Line::from(ratio(s.logs.total - s.logs.down, s.logs.total, Some(RED))),
            Line::from(label),
        ));

        match &s.metadata {
            Some(m) => {
                let mut label = vec![span("metadata up", theme::muted())];
                match m
                    .leader
                    .as_deref()
                    .and_then(|l| l.parse::<PlainNodeId>().ok())
                {
                    Some(leader) => {
                        label.push(span(" · leader ", theme::muted()));
                        label.push(node_span(leader));
                    }
                    None => label.push(span(" · no leader", theme::fg(RED))),
                }
                let bad = (m.up * 2 <= m.members).then_some(RED);
                kpis.push((Line::from(ratio(m.up, m.members, bad)), Line::from(label)));
            }
            None => kpis.push((
                Line::from(span("local", theme::bold())),
                Line::from(span("metadata", theme::muted())),
            )),
        }

        let v = &view.versions;
        let version = |v: Option<u32>| v.map_or_else(|| "-".to_owned(), |v| format!("v{v}"));
        kpis.push((
            Line::from(span(
                format!(
                    "v{} · {} · {}",
                    v.nodes_config,
                    version(v.logs),
                    version(v.schema)
                ),
                theme::bold(),
            )),
            Line::from(span("nodes · logs · schema", theme::muted())),
        ));

        // each as wide as its content, the spare room spread between them
        let columns =
            Layout::horizontal(kpis.iter().map(|(value, label)| {
                Constraint::Length(value.width().max(label.width()) as u16 + 2)
            }))
            .flex(Flex::SpaceBetween)
            .split(Rect {
                width: inner.width.saturating_sub(1),
                ..inner
            });
        for ((value, label), column) in kpis.into_iter().zip(columns.iter()) {
            let column = Rect {
                x: column.x + 2,
                width: column.width.saturating_sub(2),
                ..*column
            };
            Paragraph::new(vec![value, label]).render(column, buf);
        }
    }
}

// -- node cards

const CARD_MIN_WIDTH: u16 = 38;
const CARD_HEIGHT: u16 = 6;

/// A card per node, side by side, wrapping into rows.
pub struct NodeCards<'a> {
    pub view: &'a ClusterView,
    pub selected: Option<PlainNodeId>,
}

impl NodeCards<'_> {
    fn per_row(&self, width: u16) -> usize {
        (usize::from(width / CARD_MIN_WIDTH)).clamp(1, self.view.nodes.len().max(1))
    }

    pub fn height(&self, width: u16) -> u16 {
        let rows = self.view.nodes.len().div_ceil(self.per_row(width));
        rows as u16 * CARD_HEIGHT
    }

    /// Where each node's card goes in `area`, as many as fit.
    pub fn cards(&self, area: Rect) -> Vec<(Rect, &NodeView)> {
        let per_row = self.per_row(area.width);
        let card_width = area.width / per_row as u16;
        self.view
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let rect = Rect {
                    x: area.x + (i % per_row) as u16 * card_width,
                    y: area.y + (i / per_row) as u16 * CARD_HEIGHT,
                    width: card_width,
                    height: CARD_HEIGHT,
                };
                (rect, node)
            })
            .take_while(|(rect, _)| rect.bottom() <= area.bottom())
            .collect()
    }
}

impl Widget for NodeCards<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let max_replicas = self
            .view
            .nodes
            .iter()
            .map(|n| n.partitions.leaders + n.partitions.followers)
            .max()
            .unwrap_or(0)
            .max(1);
        let usual_version = usual_version(self.view);
        for (rect, node) in self.cards(area) {
            node_card(
                self.view,
                node,
                max_replicas,
                usual_version.as_deref(),
                self.selected == Some(node.plain()),
                rect,
                buf,
            );
        }
    }
}

fn node_card(
    view: &ClusterView,
    node: &NodeView,
    max_replicas: u32,
    usual_version: Option<&str>,
    selected: bool,
    area: Rect,
    buf: &mut Buffer,
) {
    let unreachable =
        node.liveness == Liveness::Alive && view.nodes_probed && node.seen_versions.is_none();
    let border = match node.liveness {
        _ if selected => theme::node_style(node.plain()),
        Liveness::Dead => theme::fg(RED),
        Liveness::Alive if unreachable => theme::fg(AMBER),
        Liveness::Unknown => theme::fg(AMBER),
        Liveness::Alive => theme::border(),
    };
    let uptime = match node.liveness {
        Liveness::Alive => span(
            format!(
                " up {} ",
                node.uptime_s
                    .map_or_else(|| "-".to_owned(), |u| short_duration(u as i64))
            ),
            theme::muted(),
        ),
        Liveness::Dead => span(
            format!(" dead {} ", ago(node.last_seen_alive)),
            theme::fg(RED),
        ),
        Liveness::Unknown => span(" unknown ", theme::fg(AMBER)),
    };
    let block = rounded(border)
        .title(Line::from(vec![
            raw(" "),
            span(node.id.to_string(), theme::node_style(node.plain())),
            raw(" "),
            span(node.name.clone(), theme::bold()),
            raw(" "),
        ]))
        .title_top(Line::from(uptime).right_aligned());
    let inner = block.inner(area);
    block.render(area, buf);
    let inner = Rect {
        x: inner.x + 1,
        width: inner.width.saturating_sub(2),
        ..inner
    };
    let alive = node.liveness == Liveness::Alive;

    // liveness and metadata role
    let mut first = match node.liveness {
        Liveness::Alive if unreachable => vec![span("▲ unreachable", theme::fg(AMBER))],
        Liveness::Alive => vec![span("● alive", theme::fg(GREEN))],
        Liveness::Dead => vec![span("✗ dead", theme::fg(RED).add_modifier(Modifier::BOLD))],
        Liveness::Unknown => vec![span("? unknown", theme::fg(AMBER))],
    };
    if let Some(m) = &node.metadata_server {
        first.push(raw("   "));
        first.push(match (m.error.as_ref(), m.status.as_deref()) {
            (Some(_), _) => span("metadata error", theme::fg(RED)),
            (None, Some("member")) if m.leader == Some(node.plain()) => {
                span("★ metadata leader", theme::bold())
            }
            (None, Some("member")) => span("metadata member", theme::muted()),
            (None, Some("standby")) => span("metadata standby", theme::muted()),
            (None, Some(status)) => span(format!("metadata {status}"), theme::fg(AMBER)),
            (None, None) => span(format!("metadata {}", m.state), theme::muted()),
        });
    }

    // leadership: leaders, then followers, on a scale shared by all cards
    let p = &node.partitions;
    let counts = format!(" {} lead · {} follow", p.leaders, p.followers);
    let bar_width = usize::from(inner.width).saturating_sub(counts.chars().count());
    let scale = |n: u32| (n as usize * bar_width).div_ceil(max_replicas as usize);
    let leaders = scale(p.leaders).min(bar_width);
    let followers = scale(p.leaders + p.followers).min(bar_width) - leaders;
    let color = theme::fg(theme::node(node.plain()));
    let second = vec![
        span("█".repeat(leaders), color),
        span("▒".repeat(followers), color),
        span(
            "·".repeat(bar_width - leaders - followers),
            theme::fg(TRACK),
        ),
        span(counts, if alive { Style::new() } else { theme::muted() }),
    ];

    // logs
    let sequencers = if !alive && node.logs.sequencer > 0 {
        span(
            node.logs.sequencer.to_string(),
            theme::fg(RED).add_modifier(Modifier::BOLD),
        )
    } else {
        raw(node.logs.sequencer.to_string())
    };
    let mut third = vec![
        sequencers,
        span(" sequencers · ", theme::muted()),
        raw(node.logs.nodeset.to_string()),
        span(" nodesets · ", theme::muted()),
        raw(node.logs.readers.to_string()),
        span(" readers", theme::muted()),
    ];
    if node.logs.sequencer_away_from_leader > 0 {
        third.push(span(
            format!(" · {} away", node.logs.sequencer_away_from_leader),
            theme::fg(AMBER),
        ));
    }

    // what's unusual about the node's replicas or storage, else its states
    let mut fourth = Vec::new();
    for (count, text, color) in [
        (p.broken, "broken", RED),
        (p.catching_up, "catching up", AMBER),
        (p.transitioning, "changing leadership", BLUE),
    ] {
        if count > 0 {
            if !fourth.is_empty() {
                fourth.push(span(" · ", theme::muted()));
            }
            fourth.push(span(format!("{count} {text}"), theme::fg(color)));
        }
    }
    if fourth.is_empty() {
        let state = |label: &str, state: Option<&str>, good: &str| match state {
            None => None,
            Some(s) if s == good => Some(span(format!("{label} {s}"), theme::muted())),
            Some(s @ ("data-loss" | "gone" | "disabled")) => {
                Some(span(format!("{label} {s}"), theme::fg(RED)))
            }
            Some(s) => Some(span(format!("{label} {s}"), theme::fg(AMBER))),
        };
        let states: Vec<_> = [
            state("storage", node.storage_state.as_deref(), "read-write"),
            state("worker", node.worker_state.as_deref(), "active"),
        ]
        .into_iter()
        .flatten()
        .collect();
        fourth = Itertools::intersperse(states.into_iter(), span(" · ", theme::muted())).collect();
    }
    if let Some(version) = node.binary_version.as_deref()
        && Some(version) != usual_version
    {
        fourth.push(span(format!(" · runs {version}"), theme::fg(AMBER)));
    }

    let lines = vec![
        Line::from(first),
        Line::from(second),
        Line::from(third),
        Line::from(fourth),
    ];
    let style = if alive { Style::new() } else { theme::muted() };
    Paragraph::new(lines).style(style).render(inner, buf);
}

// -- partition tiles

/// A tile per partition: its color tells the partition's health, its left edge the color of the
/// node leading it.
pub struct PartitionTiles<'a> {
    pub view: &'a ClusterView,
    pub partitions: Vec<&'a PartitionView>,
    pub selected: Option<u32>,
}

impl PartitionTiles<'_> {
    /// Columns per tile including the gap after it, and whether tiles show the partition id.
    fn tile_width(&self, inner_width: u16) -> (u16, bool) {
        let digits = self
            .partitions
            .iter()
            .map(|p| p.id)
            .max()
            .unwrap_or(0)
            .to_string()
            .len() as u16;
        let labeled = digits + 3;
        // drop the ids when they would make the map taller than a dozen rows
        let rows = (self.partitions.len() as u16).div_ceil((inner_width / labeled).max(1));
        if rows <= 12 {
            (labeled, true)
        } else {
            (2, false)
        }
    }

    pub fn height(&self, width: u16) -> u16 {
        let inner = width.saturating_sub(4);
        let (tile, _) = self.tile_width(inner);
        let per_row = (inner / tile).max(1);
        (self.partitions.len() as u16).div_ceil(per_row).max(1) + 2
    }

    /// Where each partition's tile goes in the panel drawn in `area`, as many as fit.
    pub fn tiles(&self, area: Rect) -> Vec<(Rect, &PartitionView)> {
        let inner = area.inner(Margin::new(2, 1));
        let (tile, _) = self.tile_width(inner.width);
        grid(inner, tile, &self.partitions)
            .map(|(rect, p)| (rect, *p))
            .collect()
    }
}

/// `items` placed in `area` left to right, then top to bottom, in cells `width` wide, as many as
/// fit.
fn grid<T>(area: Rect, width: u16, items: &[T]) -> impl Iterator<Item = (Rect, &T)> {
    let per_row = (area.width / width).max(1);
    items
        .iter()
        .enumerate()
        .map(move |(i, item)| {
            let (col, row) = (i as u16 % per_row, i as u16 / per_row);
            (
                Rect::new(area.x + col * width, area.y + row, width, 1),
                item,
            )
        })
        .take_while(move |(rect, _)| rect.y < area.bottom())
}

impl Widget for PartitionTiles<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut counts = Vec::new();
        for health in [
            PartitionHealth::Unavailable,
            PartitionHealth::Degraded,
            PartitionHealth::Reconfiguring,
            PartitionHealth::Healthy,
        ] {
            let count = self
                .partitions
                .iter()
                .filter(|p| p.health == health)
                .count();
            if count > 0 {
                counts.push(span("■ ", theme::fg(theme::health(health))));
                counts.push(raw(format!("{count} {}  ", health_label(health))));
            }
        }
        let mut title = vec![span("Partitions", theme::bold())];
        if let Some(replication) = &self.view.partition_replication {
            title.push(span(format!("  replication {replication}"), theme::muted()));
        }
        let leaders: BTreeSet<PlainNodeId> = self
            .partitions
            .iter()
            .filter_map(|p| p.leader.map(|l| l.as_plain()))
            .collect();
        let mut legend = vec![raw(" "), span("leader ", theme::muted())];
        for leader in leaders {
            legend.push(span("▎", theme::fg(theme::node(leader))));
            legend.push(node_span(leader));
            legend.push(raw(" "));
        }
        let block = panel(title)
            .title_top(Line::from(counts).right_aligned())
            .title_bottom(Line::from(legend));
        block.render(area, buf);

        let (tile, labeled) = self.tile_width(area.inner(Margin::new(2, 1)).width);
        let digits = usize::from(tile.saturating_sub(3));
        for (Rect { x, y, .. }, p) in self.tiles(area) {
            let mut style = theme::tile(p.health);
            if self.selected == Some(p.id) {
                style = style.add_modifier(Modifier::REVERSED | Modifier::BOLD);
            }
            let edge_color = p.leader.map_or_else(
                || theme::rgb(RED),
                |l| theme::rgb(theme::node(l.as_plain())),
            );
            buf.set_string(x, y, "▎", style.fg(edge_color));
            let label = if labeled {
                format!("{:>digits$} ", p.id)
            } else {
                " ".to_owned()
            };
            buf.set_string(x + 1, y, label, style);
        }
    }
}

// -- findings

/// The findings with a severity pill each; a follow-up command goes on its own line when it
/// doesn't fit after the message.
pub struct Findings {
    pub lines: Vec<FindingLine>,
    /// first line to show, for scrolling
    pub scroll: u16,
    /// the line to highlight; the list scrolls to keep it in view
    pub selected: Option<usize>,
}

impl Findings {
    /// The rendered lines, each with the index of the finding line it belongs to.
    fn rendered(&self, width: u16) -> Vec<(usize, Line<'static>)> {
        if self.lines.is_empty() {
            return vec![(
                0,
                Line::from(span(
                    "✓ All clear, nothing needs attention",
                    theme::fg(GREEN),
                )),
            )];
        }
        let subject_width = self
            .lines
            .iter()
            .map(|l| l.subject.chars().count())
            .max()
            .unwrap_or(0);
        let selecting = self.selected.is_some();
        let mut out = Vec::new();
        for (i, line) in self.lines.iter().enumerate() {
            let label = match line.severity {
                Severity::Critical => " CRIT ",
                Severity::Warning => " WARN ",
                Severity::Info => " INFO ",
            };
            let mut spans = Vec::new();
            if selecting {
                spans.push(if self.selected == Some(i) {
                    span("▶ ", theme::bold())
                } else {
                    raw("  ")
                });
            }
            spans.extend([
                span(label, theme::pill(theme::severity(line.severity))),
                raw("  "),
                span(format!("{:subject_width$}", line.subject), theme::bold()),
                raw("  "),
                raw(line.message.clone()),
            ]);
            let used: usize = spans.iter().map(|s| s.width()).sum();
            match &line.hint {
                Some(hint) if used + hint.chars().count() + 4 <= usize::from(width) => {
                    spans.push(span(format!("  → {hint}"), theme::muted()));
                    out.push((i, Line::from(spans)));
                }
                Some(hint) => {
                    out.push((i, Line::from(spans)));
                    let indent = subject_width + 10 + if selecting { 2 } else { 0 };
                    out.push((
                        i,
                        Line::from(vec![
                            raw(" ".repeat(indent)),
                            span(format!("→ {hint}"), theme::muted()),
                        ]),
                    ));
                }
                None => out.push((i, Line::from(spans))),
            }
        }
        out
    }

    pub fn height(&self, width: u16) -> u16 {
        self.rendered(width.saturating_sub(4)).len() as u16 + 2
    }

    /// The first line shown of `rendered` in `height` rows.
    fn scroll(&self, rendered: &[(usize, Line<'static>)], height: u16) -> u16 {
        match self.selected {
            Some(selected) => {
                // keep the selected finding, and its hint line, in view
                let last = rendered
                    .iter()
                    .rposition(|(i, _)| *i == selected)
                    .unwrap_or(0);
                (last + 1).saturating_sub(usize::from(height)) as u16
            }
            None => self.scroll,
        }
    }

    /// Where each line shows in the panel drawn in `area`, with the index of its finding line.
    pub fn rows(&self, area: Rect) -> Vec<(Rect, usize)> {
        if self.lines.is_empty() {
            return Vec::new();
        }
        let inner = area.inner(Margin::new(2, 1));
        let rendered = self.rendered(inner.width);
        let scroll = self.scroll(&rendered, inner.height);
        rendered
            .iter()
            .skip(usize::from(scroll))
            .zip(inner.rows())
            .map(|((i, _), row)| (row, *i))
            .collect()
    }
}

impl Widget for Findings {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut title = vec![span("Findings", theme::bold())];
        for severity in [Severity::Critical, Severity::Warning, Severity::Info] {
            let count = self.lines.iter().filter(|l| l.severity == severity).count();
            if count > 0 {
                title.push(span(
                    format!(
                        "  {count} {}",
                        match severity {
                            Severity::Critical => "critical",
                            Severity::Warning => "warning",
                            Severity::Info => "info",
                        }
                    ),
                    theme::fg(theme::severity(severity)),
                ));
            }
        }
        let block = panel(title);
        block.render(area, buf);
        let inner = area.inner(Margin::new(2, 1));
        let rendered = self.rendered(inner.width);
        let scroll = self.scroll(&rendered, inner.height);
        let lines: Vec<Line<'static>> = rendered
            .into_iter()
            .map(|(i, line)| {
                if self.selected == Some(i) {
                    // highlight the whole row
                    let mut line = line;
                    let pad = usize::from(inner.width).saturating_sub(line.width());
                    line.spans.push(raw(" ".repeat(pad)));
                    line.patch_style(Style::new().bg(theme::rgb(TRACK)))
                } else {
                    line
                }
            })
            .collect();
        Paragraph::new(lines).scroll((scroll, 0)).render(inner, buf);
    }
}

// -- placement

/// Bars per node for a few counts, each with a tick at an even split over the eligible nodes.
pub struct PlacementBars {
    pub title: Vec<Span<'static>>,
    /// per count: its label and the even split
    pub metrics: Vec<(&'static str, f64)>,
    /// per node: per count, the count and whether the node is eligible for more
    pub rows: Vec<(PlainNodeId, Vec<(u32, bool)>)>,
}

impl PlacementBars {
    pub fn partitions(placement: &PartitionPlacement) -> Self {
        let eligible = placement.nodes.iter().filter(|s| s.eligible).count();
        Self {
            title: vec![
                span("Partition placement", theme::bold()),
                span(
                    format!(
                        "  even split over {eligible} active workers: {:.1} leaders · {:.1} replicas",
                        placement.even_leaders, placement.even_replicas
                    ),
                    theme::muted(),
                ),
            ],
            metrics: vec![
                ("leaders", placement.even_leaders),
                ("replicas", placement.even_replicas),
            ],
            rows: placement
                .nodes
                .iter()
                .map(|s| {
                    (
                        s.node,
                        vec![(s.leaders, s.eligible), (s.replicas, s.eligible)],
                    )
                })
                .collect(),
        }
    }

    pub fn logs(placement: &LogPlacement) -> Self {
        Self {
            title: vec![
                span("Log placement", theme::bold()),
                span(
                    format!(
                        "  even split: {:.1} sequencers per active worker · {:.1} nodesets per writable log-server",
                        placement.even_sequencers, placement.even_nodesets
                    ),
                    theme::muted(),
                ),
            ],
            metrics: vec![
                ("sequencers", placement.even_sequencers),
                ("nodesets", placement.even_nodesets),
            ],
            rows: placement
                .nodes
                .iter()
                .map(|s| {
                    (
                        s.node,
                        vec![
                            (s.sequencers, s.sequencer_eligible),
                            (s.nodesets, s.nodeset_eligible),
                        ],
                    )
                })
                .collect(),
        }
    }

    /// Up to a handful of nodes get a row each; more get a column each.
    fn as_columns(&self) -> bool {
        self.rows.len() > 8
    }

    pub fn height(&self) -> u16 {
        if self.as_columns() {
            COLUMN_CHART_HEIGHT + 2
        } else {
            self.rows.len() as u16 + 2
        }
    }

    fn notable(count: u32, eligible: bool, even: f64) -> bool {
        // a node that can't take more still holding some is as notable as an imbalance
        if eligible {
            imbalanced(count, even)
        } else {
            count > 0
        }
    }

    /// A chart per count, side by side, with a column per node and a line at the even split.
    fn render_columns(&self, inner: Rect, buf: &mut Buffer) {
        let charts = self.metrics.len().max(1) as u16;
        let chart_width = inner.width.saturating_sub(2 * (charts - 1)) / charts;
        let nodes = self.rows.len() as u16;
        // the widest columns that fit: 2 cells and a gap, else 1 cell and a gap
        let stride = (chart_width / nodes.max(1)).clamp(2, 3);
        let bar = stride - 1;
        let bars_height = COLUMN_CHART_HEIGHT - 3;
        for (m, (label, even)) in self.metrics.iter().enumerate() {
            let x0 = inner.x + m as u16 * (chart_width + 2);
            let scale = self
                .rows
                .iter()
                .map(|(_, counts)| f64::from(counts[m].0))
                .fold(even.ceil(), f64::max)
                .max(1.0);
            buf.set_line(
                x0,
                inner.y,
                &Line::from(vec![
                    span(*label, theme::bold()),
                    span(format!("  even split {even:.1}"), theme::muted()),
                ]),
                chart_width,
            );
            // the even split, from the bottom of the bars
            let even_row = ((even / scale) * f64::from(bars_height)).round() as u16;
            for (i, (node, counts)) in self.rows.iter().enumerate() {
                let (count, eligible) = counts[m];
                let x = x0 + i as u16 * stride;
                if x + bar > x0 + chart_width {
                    break;
                }
                let color = if eligible {
                    theme::fg(theme::node(*node))
                } else {
                    theme::muted()
                };
                // eighths of a cell
                let eighths =
                    ((f64::from(count) / scale) * f64::from(bars_height) * 8.0).round() as u16;
                for row in 0..bars_height {
                    let y = inner.y + 1 + (bars_height - 1 - row);
                    let filled = eighths.saturating_sub(row * 8).min(8);
                    let symbol = match filled {
                        0 if row + 1 == even_row => "╌",
                        0 => " ",
                        n => ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"][usize::from(n) - 1],
                    };
                    let style = if filled == 0 { theme::bold() } else { color };
                    for dx in 0..bar {
                        buf.set_string(x + dx, y, symbol, style);
                    }
                    if row + 1 == even_row && x + bar < x0 + chart_width {
                        buf.set_string(x + bar, y, "╌", theme::bold());
                    }
                }
                let value_style = if Self::notable(count, eligible, *even) {
                    theme::fg(AMBER).add_modifier(Modifier::BOLD)
                } else if eligible {
                    Style::new()
                } else {
                    theme::muted()
                };
                let id = u32::from(*node);
                // the numbers are too wide for narrow columns: every other one then
                if stride >= 3 || i % 2 == 0 {
                    buf.set_string(
                        x,
                        inner.y + 1 + bars_height,
                        format!("{count}"),
                        value_style,
                    );
                    buf.set_string(
                        x,
                        inner.y + 2 + bars_height,
                        format!("{id}"),
                        if eligible {
                            theme::node_style(*node)
                        } else {
                            theme::muted()
                        },
                    );
                }
            }
        }
    }
}

const COLUMN_CHART_HEIGHT: u16 = 9;

impl Widget for PlacementBars {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = panel(self.title.clone()).title_bottom(Line::from(vec![
            raw(" "),
            span(if self.as_columns() { "╌" } else { "│" }, theme::bold()),
            span(" even split  ", theme::muted()),
            span("amber", theme::fg(AMBER)),
            span(
                " off by more than a fifth  dimmed: not eligible ",
                theme::muted(),
            ),
        ]));
        let inner = block.inner(area);
        block.render(area, buf);
        if self.metrics.is_empty() || inner.width < 20 {
            return;
        }
        if self.as_columns() {
            self.render_columns(inner, buf);
            return;
        }
        let label_width = self.metrics.iter().map(|(l, _)| l.len()).max().unwrap_or(0) + 1;
        let group = (usize::from(inner.width) - 7) / self.metrics.len();
        let bar_width = group.saturating_sub(label_width + 9).max(4);
        let scales: Vec<f64> = self
            .metrics
            .iter()
            .enumerate()
            .map(|(m, (_, even))| {
                self.rows
                    .iter()
                    .map(|(_, counts)| f64::from(counts[m].0))
                    .fold(even.ceil(), f64::max)
                    .max(1.0)
            })
            .collect();

        for (row, (node, counts)) in self.rows.iter().enumerate() {
            let y = inner.y + row as u16;
            if y >= inner.bottom() {
                break;
            }
            let any_eligible = counts.iter().any(|(_, eligible)| *eligible);
            let mut spans = vec![
                raw(" "),
                span(
                    format!("{:<5}", node.to_string()),
                    if any_eligible {
                        theme::node_style(*node)
                    } else {
                        theme::muted()
                    },
                ),
            ];
            for (m, ((label, even), (count, eligible))) in
                self.metrics.iter().zip(counts).enumerate()
            {
                let scale = scales[m];
                let cell = |v: f64| ((v / scale) * bar_width as f64).round() as usize;
                let filled = cell(f64::from(*count)).min(bar_width);
                let tick = cell(*even).min(bar_width - 1);
                let bar_color = if *eligible {
                    theme::fg(theme::node(*node))
                } else {
                    theme::muted()
                };
                // a node that can't take more still holding some is as notable as an imbalance
                let notable = if *eligible {
                    imbalanced(*count, *even)
                } else {
                    *count > 0
                };
                let value_style = if notable {
                    theme::fg(AMBER).add_modifier(Modifier::BOLD)
                } else if *eligible {
                    Style::new()
                } else {
                    theme::muted()
                };
                // `leaders    8 ████│` and a gap before the next count
                spans.push(span(format!("  {label:>label_width$}"), theme::muted()));
                spans.push(span(format!("{count:>5} "), value_style));
                for i in 0..bar_width {
                    spans.push(if i == tick {
                        span("│", theme::bold())
                    } else if i < filled {
                        span("█", bar_color)
                    } else {
                        span("·", theme::fg(TRACK))
                    });
                }
            }
            buf.set_line(inner.x, y, &Line::from(spans), inner.width);
        }
    }
}

// -- readers

/// Where a replica's read stream is: how far behind the tail, or what it waits for.
fn read_cell(reader: Option<&ReaderView>) -> Line<'static> {
    let Some(reader) = reader else {
        return Line::from(span("-", theme::muted()));
    };
    match reader.phase() {
        ReaderPhase::Reading => match reader.behind {
            Some(0) => Line::from(span("at tail", theme::muted())),
            Some(behind) => Line::from(raw(behind.to_string())),
            None => Line::from(span("reading", theme::muted())),
        },
        ReaderPhase::Switching => Line::from(span("switching", theme::fg(AMBER))),
        ReaderPhase::AwaitingSeal => Line::from(span("awaiting seal", theme::fg(RED))),
        ReaderPhase::Sealing => Line::from(span(
            "sealing chain",
            theme::fg(RED).add_modifier(Modifier::BOLD),
        )),
    }
}

/// The log's chain: a span per segment (the newest few), drawn in the color of the node that
/// sequences it, with trimmed records shaded and the partition's newest snapshot marked; then a
/// row per reader with where it reads; then each segment's base LSN.
pub fn chain_view(view: &ClusterView, log: &LogView, width: u16) -> Vec<Line<'static>> {
    const LABEL: usize = 6;
    const MAX_SEGMENTS: usize = 6;
    let width = usize::from(width);
    let hidden = log.chain.len().saturating_sub(MAX_SEGMENTS);
    let segments = &log.chain[hidden..];
    let Some(last) = segments.last() else {
        return Vec::new();
    };
    let n = segments.len();
    if width < LABEL + n * 4 {
        return Vec::new();
    }
    let tail = log.tail_lsn.unwrap_or(last.base_lsn).max(last.base_lsn);
    // each segment covers [base, next base), the tail segment [base, tail]
    let bounds: Vec<(u64, u64)> = segments
        .iter()
        .enumerate()
        .map(|(i, s)| {
            (
                s.base_lsn,
                segments.get(i + 1).map_or(tail + 1, |n| n.base_lsn),
            )
        })
        .collect();
    let avail = width - LABEL - (n - 1);
    let widths: Vec<usize> = (0..n)
        .map(|i| avail / n + usize::from(i + 1 == n) * (avail % n))
        .collect();
    let offsets: Vec<usize> = widths
        .iter()
        .scan(0, |at, w| {
            let offset = *at;
            *at += w + 1;
            Some(offset)
        })
        .collect();
    // the column of `lsn`, counted from the first segment
    let locate = |lsn: u64| {
        let i = bounds
            .iter()
            .position(|(_, end)| lsn < *end)
            .unwrap_or(n - 1);
        let (start, end) = bounds[i];
        let span = end.saturating_sub(start).max(1);
        let cell =
            (u128::from(lsn.saturating_sub(start)) * widths[i] as u128 / u128::from(span)) as usize;
        offsets[i] + cell.min(widths[i] - 1)
    };
    let separators: Vec<usize> = offsets.iter().skip(1).map(|o| o - 1).collect();
    let label = |text: String, style: Style| span(format!("{text:<LABEL$}"), style);
    let mut lines = Vec::new();

    // segment indexes
    let mut header = vec![label(
        if hidden > 0 {
            format!("+{hidden}…")
        } else {
            String::new()
        },
        theme::muted(),
    )];
    for (i, segment) in segments.iter().enumerate() {
        // `#27 N1 tail`: what fits of the index, the sequencer drawing it, and the tail mark
        let tail = i + 1 == n;
        let index = format!("#{}", segment.index);
        let sequencer = segment.sequencer.map(|s| s.as_plain());
        let mut parts = vec![span(
            index.clone(),
            if tail { theme::bold() } else { theme::muted() },
        )];
        let mut used = index.chars().count();
        if let Some(sequencer) = sequencer {
            let text = format!(" {sequencer}");
            if used + text.len() <= widths[i] {
                used += text.len();
                parts.push(span(text, theme::node_style(sequencer)));
            }
        }
        if tail && used + 5 <= widths[i] {
            used += 5;
            parts.push(span(" tail", theme::muted()));
        }
        if used > widths[i] {
            parts = vec![span(
                index.chars().take(widths[i]).collect::<String>(),
                theme::muted(),
            )];
            used = widths[i];
        }
        header.extend(parts);
        header.push(raw(" ".repeat(widths[i] - used)));
        if i + 1 < n {
            header.push(raw(" "));
        }
    }
    lines.push(Line::from(header));

    // the chain
    let snapshot = log
        .partition_id
        .and_then(|id| view.partitions.iter().find(|p| p.id == id))
        .and_then(|p| p.replicas.iter().filter_map(|r| r.archived_lsn).max())
        .filter(|s| *s >= segments[0].base_lsn)
        .map(locate);
    let mut chain = vec![label("chain".to_owned(), theme::muted())];
    for (i, segment) in segments.iter().enumerate() {
        let (start, end) = bounds[i];
        let color = segment
            .sequencer
            .map_or_else(|| theme::fg(BLUE), |s| theme::fg(theme::node(s.as_plain())));
        for cell in 0..widths[i] {
            let column = offsets[i] + cell;
            let lsn = start
                + (u128::from(end.saturating_sub(start)) * cell as u128 / widths[i] as u128) as u64;
            chain.push(if snapshot == Some(column) {
                span("┃", theme::fg(BLUE).add_modifier(Modifier::BOLD))
            } else if log.trim_point.is_some_and(|t| lsn <= t) {
                span("░", theme::muted())
            } else {
                span("━", color)
            });
        }
        if i + 1 < n {
            chain.push(span("│", theme::border()));
        }
    }
    lines.push(Line::from(chain));

    // readers
    for reader in &log.readers {
        let at = reader.read_pointer.map(locate);
        let mut row = vec![label(
            reader.node.to_string(),
            theme::node_style(reader.node),
        )];
        for column in 0..(width - LABEL) {
            row.push(if at == Some(column) {
                span("●", theme::node_style(reader.node))
            } else if separators.contains(&column) {
                span("│", theme::fg(TRACK))
            } else {
                raw(" ")
            });
        }
        lines.push(Line::from(row));
    }

    // base LSNs
    let mut bases = vec![label(String::new(), theme::muted())];
    for (i, segment) in segments.iter().enumerate() {
        let text: String = segment
            .base_lsn
            .to_string()
            .chars()
            .take(widths[i])
            .collect();
        bases.push(span(format!("{text:<w$}", w = widths[i]), theme::muted()));
        if i + 1 < n {
            bases.push(raw(" "));
        }
    }
    lines.push(Line::from(bases));
    // segments and readers take their node's color, named where they're drawn
    lines.push(Line::from(vec![
        span("░", theme::muted()),
        span(" trimmed  ", theme::muted()),
        span("┃", theme::fg(BLUE).add_modifier(Modifier::BOLD)),
        span(" newest snapshot", theme::muted()),
    ]));
    lines
}

// -- tables

/// A table in a panel, its columns as wide as their content.
pub struct RichTable {
    pub title: Vec<Span<'static>>,
    pub header: Vec<&'static str>,
    /// columns whose content is aligned to the right
    pub right: Vec<usize>,
    pub rows: Vec<Vec<Line<'static>>>,
    pub legend: Option<Line<'static>>,
    /// the column the rows are ordered by, highlighted in the header with the direction
    pub sorted: Option<SortedBy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SortedBy {
    pub column: usize,
    pub descending: bool,
}

impl RichTable {
    pub fn height(&self) -> u16 {
        self.rows.len() as u16 + 3
    }

    /// A column's header, with an arrow for the direction when the rows are ordered by it.
    fn header_text(&self, column: usize) -> String {
        let name = self.header[column];
        match self.sorted {
            Some(sorted) if sorted.column == column => {
                let arrow = if sorted.descending { "▼" } else { "▲" };
                if name.is_empty() {
                    arrow.to_owned()
                } else {
                    format!("{name} {arrow}")
                }
            }
            _ => name.to_owned(),
        }
    }

    /// Where the header's cells and the rows show once the table is drawn in `area`, scrolled
    /// to `offset`, with a highlight column `selection` wide: the cells by column, the rows with
    /// their index.
    pub fn layout(
        &self,
        area: Rect,
        offset: usize,
        selection: u16,
    ) -> (Vec<Rect>, Vec<(Rect, usize)>) {
        let inner = area.inner(Margin::new(1, 1));
        if inner.is_empty() {
            return Default::default();
        }
        // as `Table` lays out its columns
        let [_, columns] = Layout::horizontal([Constraint::Length(selection), Constraint::Fill(0)])
            .areas(Rect { height: 1, ..inner });
        let cells = Layout::horizontal(self.column_widths().into_iter().map(Constraint::Length))
            .flex(Flex::Start)
            .spacing(2)
            .split(columns)
            .to_vec();
        let rows = inner.rows().skip(1).zip(offset..self.rows.len()).collect();
        (cells, rows)
    }

    fn column_widths(&self) -> Vec<u16> {
        (0..self.header.len())
            .map(|i| {
                let content = self
                    .rows
                    .iter()
                    .filter_map(|r| r.get(i))
                    .map(Line::width)
                    .max()
                    .unwrap_or(0);
                content.max(self.header_text(i).chars().count()) as u16
            })
            .collect()
    }

    /// The width the table needs, borders and titles included.
    pub fn width(&self) -> u16 {
        let widths = self.column_widths();
        let columns = widths.iter().sum::<u16>() + 2 * widths.len().saturating_sub(1) as u16;
        let title = self.title.iter().map(|s| s.width() as u16).sum::<u16>() + 4;
        let legend = self.legend.as_ref().map_or(0, |l| l.width() as u16 + 4);
        columns.max(title).max(legend) + 2
    }

    pub fn table(&self) -> Table<'static> {
        let widths: Vec<Constraint> = self
            .column_widths()
            .into_iter()
            .map(Constraint::Length)
            .collect();
        let align = |i: usize, line: Line<'static>| {
            if self.right.contains(&i) {
                line.right_aligned()
            } else {
                line
            }
        };
        let header = Row::new((0..self.header.len()).map(|i| {
            let style = if self.sorted.is_some_and(|s| s.column == i) {
                theme::bold()
            } else {
                theme::muted().add_modifier(Modifier::BOLD)
            };
            Cell::from(align(i, Line::from(span(self.header_text(i), style))))
        }));
        let rows = self.rows.iter().map(|row| {
            Row::new(
                row.iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, line)| Cell::from(align(i, line))),
            )
        });
        let mut block = panel(self.title.clone());
        if let Some(legend) = &self.legend {
            let mut legend = legend.clone();
            legend.spans.insert(0, raw(" "));
            legend.spans.push(raw(" "));
            block = block.title_bottom(legend);
        }
        Table::new(rows, widths)
            .header(header)
            .column_spacing(2)
            .block(block)
    }
}

impl Widget for RichTable {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Widget::render(self.table(), area, buf);
    }
}

/// A quiet dot for a healthy partition, a word for anything else.
fn health_cell(health: PartitionHealth) -> Line<'static> {
    let style = theme::fg(theme::health(health));
    match health {
        PartitionHealth::Healthy => Line::from(span("●", style)),
        PartitionHealth::Reconfiguring => Line::from(span("◆ moving", style)),
        PartitionHealth::Degraded => Line::from(span("◐ degraded", style)),
        PartitionHealth::Unavailable => {
            Line::from(span("○ unavailable", style.add_modifier(Modifier::BOLD)))
        }
    }
}

/// The marker and color for a replica that doesn't run normally.
fn replica_marker(r: &ReplicaView) -> Option<(&'static str, theme::Rgb, &'static str)> {
    if r.mode == Mode::Broken {
        return Some(("⊘", RED, "broken"));
    }
    if r.stale_epoch {
        return Some(("!", RED, "stale leader"));
    }
    match (r.mode, r.replay) {
        (_, Replay::CatchingUp) => Some(("↻", AMBER, "catching up")),
        (_, Replay::Starting) => Some(("…", AMBER, "starting")),
        (Mode::Candidate | Mode::BecomingLeader, _) => Some(("⇡", BLUE, "taking over")),
        (Mode::SteppingDown, _) => Some(("⇣", BLUE, "stepping down")),
        (Mode::Unknown, _) => Some(("?", RED, "unknown")),
        _ => None,
    }
}

/// `★N1 N3↻ → ★N1 +N2`: the replica set and how its members run.
fn replica_set(
    p: &PartitionView,
    legend: &mut BTreeSet<(&'static str, &'static str)>,
) -> Line<'static> {
    let mut spans = Vec::new();
    let mut token = |spans: &mut Vec<Span<'static>>, node: PlainNodeId, joining: bool| {
        if !spans.is_empty() {
            spans.push(raw(" "));
        }
        let leads = p.leader.is_some_and(|l| l.as_plain() == node);
        if leads {
            legend.insert(("★", "leader"));
            spans.push(span("★", theme::node_style(node)));
        }
        if joining {
            legend.insert(("+", "joining"));
            spans.push(span("+", theme::fg(BLUE)));
        }
        match p.replica_on(node) {
            None if joining => spans.push(span(node.to_string(), theme::fg(BLUE))),
            None => {
                legend.insert(("✗", "missing"));
                spans.push(span(
                    format!("{node}✗"),
                    theme::fg(RED).add_modifier(Modifier::BOLD),
                ));
            }
            Some(r) => match replica_marker(r) {
                Some((marker, color, label)) => {
                    legend.insert((marker, label));
                    spans.push(span(format!("{node}{marker}"), theme::fg(color)));
                }
                None => spans.push(node_span(node)),
            },
        }
    };
    match &p.membership {
        Some(m) => {
            for node in &m.members {
                token(&mut spans, *node, false);
            }
            if let Some(next) = &m.next {
                spans.push(span(" →", theme::fg(BLUE).add_modifier(Modifier::BOLD)));
                let mut next_spans = Vec::new();
                for node in &next.members {
                    token(&mut next_spans, *node, !m.members.contains(node));
                }
                spans.push(raw(" "));
                spans.extend(next_spans);
            }
        }
        None => {
            for r in &p.replicas {
                token(&mut spans, r.node.as_plain(), false);
            }
        }
    }
    Line::from(spans)
}

/// A legend marker, styled as the tables draw it.
fn marker_style(marker: &str) -> Style {
    match marker {
        "✗" => theme::fg(RED).add_modifier(Modifier::BOLD),
        "⊘" | "!" | "?" => theme::fg(RED),
        "↻" | "…" | "↗" => theme::fg(AMBER),
        "⇡" | "⇣" | "+" => theme::fg(BLUE),
        _ => theme::bold(),
    }
}

fn legend_line(legend: &BTreeSet<(&'static str, &'static str)>) -> Option<Line<'static>> {
    if legend.is_empty() {
        return None;
    }
    let mut spans = Vec::new();
    for (marker, label) in legend {
        if !spans.is_empty() {
            spans.push(raw("  "));
        }
        spans.push(span(*marker, marker_style(marker)));
        spans.push(span(format!(" {label}"), theme::muted()));
    }
    Some(Line::from(spans))
}

fn sequencer_cell(log: Option<&LogView>) -> Line<'static> {
    let Some(log) = log else {
        return Line::from(span("-", theme::muted()));
    };
    let Some(sequencer) = log.sequencer else {
        return Line::from(span("-", theme::muted()));
    };
    match log.sequencer_state {
        Some(SequencerState::Dead) => Line::from(span(
            format!("{sequencer} dead"),
            theme::fg(RED).add_modifier(Modifier::BOLD),
        )),
        Some(SequencerState::Restarted) => {
            Line::from(span(format!("{sequencer} restarted"), theme::fg(RED)))
        }
        _ if log.sequencer_with_leader() == Some(false) => Line::from(vec![
            generational_span(sequencer),
            span(" ↗", theme::fg(AMBER)),
        ]),
        _ => Line::from(generational_span(sequencer)),
    }
}

/// A row per partition.
pub fn partitions_table<'a>(
    view: &ClusterView,
    partitions: impl IntoIterator<Item = &'a PartitionView>,
    extra: bool,
) -> RichTable {
    let mut legend = BTreeSet::new();
    let mut rows = Vec::new();
    let mut away = false;
    for p in partitions {
        let leader = p.leader_replica();
        let counted = || p.replicas.iter().filter(|r| p.counts(r));
        let log = p
            .log_id
            .and_then(|id| view.logs.iter().find(|l| l.id == id));
        away |= log.is_some_and(|l| l.sequencer_with_leader() == Some(false));
        let mut row = vec![
            Line::from(span(p.id.to_string(), theme::bold())),
            health_cell(p.health),
            match p.leader {
                Some(l) if p.is_led() => Line::from(generational_span(l)),
                Some(l) => Line::from(span(l.to_string(), theme::fg(AMBER))),
                None => Line::from(span("none", theme::fg(RED).add_modifier(Modifier::BOLD))),
            },
            Line::from(span(
                p.epoch.map_or_else(|| "-".to_owned(), |e| format!("e{e}")),
                theme::muted(),
            )),
            replica_set(p, &mut legend),
            Line::from(opt_num(leader.and_then(|r| r.applied_lsn))),
            Line::from(lag_span(counted().filter_map(|r| r.lag).max())),
            Line::from(opt_num(counted().filter_map(|r| r.durable_lsn).min())),
            Line::from(opt_num(
                p.replicas.iter().filter_map(|r| r.archived_lsn).max(),
            )),
            sequencer_cell(log),
        ];
        if extra {
            row.push(Line::from(opt_num(p.log_id.map(u64::from))));
            row.push(Line::from(opt_num(p.log_tail_lsn)));
            row.push(Line::from(span(
                p.key_range.map_or_else(
                    || "-".to_owned(),
                    |k| format!("{:016x}-{:016x}", k.start, k.end),
                ),
                theme::muted(),
            )));
        }
        rows.push(row);
    }
    if away {
        legend.insert(("↗", "sequencer away from leader"));
    }
    let mut header = vec![
        "P",
        "",
        "LEADER",
        "EPOCH",
        "REPLICAS",
        "APPLIED",
        "LAG",
        "DURABLE",
        "ARCHIVED",
        "SEQUENCER",
    ];
    if extra {
        header.extend(["LOG", "LOG-TAIL", "KEYS"]);
    }
    let led = view.partitions.iter().filter(|p| p.is_led()).count();
    let mut title = vec![
        span("Partitions", theme::bold()),
        span(
            format!("  {led}/{} led", view.partitions.len()),
            theme::muted(),
        ),
    ];
    if let Some(replication) = &view.partition_replication {
        title.push(span(
            format!(" · replication {replication}"),
            theme::muted(),
        ));
    }
    RichTable {
        title,
        header,
        right: vec![0, 5, 6, 7, 8, 10, 11],
        rows,
        legend: legend_line(&legend),
        sorted: None,
    }
}

/// A row per partition processor, grouped by partition, and a row per replica set member that
/// runs none.
pub fn replicas_table(partitions: &[&PartitionView], extra: bool) -> RichTable {
    let mut rows = Vec::new();
    for p in partitions {
        let mut members: Vec<(PlainNodeId, Option<&ReplicaView>)> = p
            .replicas
            .iter()
            .map(|r| (r.node.as_plain(), Some(r)))
            .chain(p.missing.iter().map(|m| (*m, None)))
            .collect();
        // the leader first
        members.sort_by_key(|(node, _)| (p.leader.map(|l| l.as_plain()) != Some(*node), *node));
        let count = members.len();
        for (i, (node, replica)) in members.into_iter().enumerate() {
            let tree = if i + 1 == count { "└─" } else { "├─" };
            let mut row = vec![
                if i == 0 {
                    Line::from(span(p.id.to_string(), theme::bold()))
                } else {
                    Line::default()
                },
                if i == 0 {
                    health_cell(p.health)
                } else {
                    Line::default()
                },
            ];
            let member = |current: bool, next: bool| {
                let text = match (current, next) {
                    (true, true) => "current,next",
                    (true, false) => "current",
                    (false, true) => "next",
                    (false, false) => "none",
                };
                let style = if current || next {
                    Style::new()
                } else {
                    theme::muted()
                };
                Line::from(span(text, style))
            };
            let Some(r) = replica else {
                row.push(Line::from(vec![
                    span(tree, theme::border()),
                    raw(" "),
                    span(
                        format!("{node}"),
                        theme::fg(RED).add_modifier(Modifier::BOLD),
                    ),
                ]));
                row.push(Line::from(span(
                    "missing",
                    theme::fg(RED).add_modifier(Modifier::BOLD),
                )));
                row.push(Line::from(span("-", theme::muted())));
                row.push(member(p.is_member(node), p.is_next_member(node)));
                rows.push(row);
                continue;
            };
            let mode_style = match (r.mode, replica_marker(r)) {
                (_, Some((_, color, _))) => theme::fg(color),
                (Mode::Leader, None) => theme::fg(GREEN).add_modifier(Modifier::BOLD),
                _ => Style::new(),
            };
            let replay = match r.replay {
                _ if r.broken_reason.is_some() => span(
                    format!("broken ({})", r.broken_reason.unwrap_or_default()),
                    theme::fg(RED).add_modifier(Modifier::BOLD),
                ),
                Replay::Active => span("active", theme::muted()),
                Replay::CatchingUp => span(
                    format!(
                        "catching up to {}",
                        r.target_tail_lsn
                            .map_or_else(|| "-".to_owned(), |t| t.to_string())
                    ),
                    theme::fg(AMBER),
                ),
                Replay::Starting => span("starting", theme::fg(AMBER)),
                Replay::Unknown => span("unknown", theme::fg(RED)),
            };
            let stale = r
                .updated_at
                .is_some_and(|t| (Utc::now() - t).num_seconds() > 30);
            row.extend([
                Line::from(vec![
                    span(tree, theme::border()),
                    raw(" "),
                    generational_span(r.node),
                ]),
                Line::from(span(mode_label(r.mode), mode_style)),
                Line::from(replay),
                if p.membership.is_some() {
                    member(r.member, r.next_member)
                } else {
                    Line::from(span("?", theme::muted()))
                },
                Line::from(span(
                    r.epoch.map_or_else(|| "-".to_owned(), |e| format!("e{e}")),
                    if r.stale_epoch {
                        theme::fg(RED)
                    } else {
                        theme::muted()
                    },
                )),
                Line::from(opt_num(r.applied_lsn)),
                Line::from(lag_span(r.lag)),
                read_cell(r.reader.as_ref()),
                Line::from(opt_num(r.durable_lsn)),
                Line::from(opt_num(r.archived_lsn)),
                Line::from(span(
                    ago(r.updated_at),
                    if stale {
                        theme::fg(AMBER)
                    } else {
                        theme::muted()
                    },
                )),
            ]);
            if extra {
                let list = |items: &[String]| {
                    if items.is_empty() {
                        "-".to_owned()
                    } else {
                        items.join(",")
                    }
                };
                row.extend([
                    Line::from(raw(r
                        .schema_version
                        .map_or_else(|| "-".to_owned(), |v| format!("v{v}")))),
                    Line::from(raw(r
                        .rule_book_version
                        .map_or_else(|| "-".to_owned(), |v| format!("v{v}")))),
                    Line::from(span(ago(r.last_record_applied_at), theme::muted())),
                    Line::from(span(list(&r.features), theme::muted())),
                    Line::from(span(list(&r.storage_features), theme::muted())),
                ]);
            }
            rows.push(row);
        }
    }
    let mut header = vec![
        "P", "", "NODE", "MODE", "REPLAY", "MEMBER", "EPOCH", "APPLIED", "LAG", "READ", "DURABLE",
        "ARCHIVED", "UPDATED",
    ];
    if extra {
        header.extend([
            "SCHEMA",
            "RULE-BOOK",
            "LAST-APPLIED",
            "FEATURES",
            "STORAGE-FEATURES",
        ]);
    }
    // members without a processor have no values for the processor's columns
    for row in &mut rows {
        row.resize_with(header.len(), || Line::from(span("-", theme::muted())));
    }
    RichTable {
        title: vec![
            span("Partition replicas", theme::bold()),
            span(format!("  {} partitions", partitions.len()), theme::muted()),
        ],
        header,
        right: vec![0, 7, 8, 10, 11, 12],
        rows,
        legend: None,
        sorted: None,
    }
}

fn log_state(log: &LogView) -> Line<'static> {
    let (text, color) = if log.is_down() {
        ("○ down", RED)
    } else if log.sealed {
        ("◐ sealed", AMBER)
    } else if matches!(log.sequencer_state, Some(SequencerState::Restarted)) {
        ("◐ stalled", AMBER)
    } else if log.replication_factor.is_some() && log.writable_nodes < log.nodeset.len() {
        ("◐ degraded", AMBER)
    } else {
        ("●", GREEN)
    };
    Line::from(span(text, theme::fg(color)))
}

/// A row per log, describing its tail segment.
pub fn logs_table<'a>(
    view: &ClusterView,
    logs: impl IntoIterator<Item = &'a LogView>,
) -> RichTable {
    let logs: Vec<&LogView> = logs.into_iter().collect();
    let default_replication = view
        .log_provider
        .as_ref()
        .and_then(|p| p.replication.as_deref());
    let show_replication = logs
        .iter()
        .any(|l| l.replication.as_deref() != default_replication);
    let show_since = logs.iter().any(|l| l.created_at.is_some());
    let max_retained = logs
        .iter()
        .filter_map(|l| l.retained)
        .max()
        .unwrap_or(0)
        .max(1);
    let writable = |id: &PlainNodeId| {
        view.node(*id).is_some_and(|n| {
            n.liveness == Liveness::Alive
                && n.storage_state
                    .as_deref()
                    .is_some_and(|s| matches!(s, "read-write" | "read-only"))
        })
    };

    let mut rows = Vec::new();
    for log in &logs {
        let segments = if log.segments > 1 {
            Line::from(vec![
                raw(log.tail_segment.to_string()),
                span(format!(" · {} segs", log.segments), theme::muted()),
            ])
        } else {
            Line::from(raw(log.tail_segment.to_string()))
        };
        let retained = match log.retained {
            Some(retained) => {
                const BAR: usize = 8;
                let filled = (retained as usize * BAR)
                    .div_ceil(max_retained as usize)
                    .min(BAR);
                Line::from(vec![
                    raw(format!("{retained} ")),
                    span("▮".repeat(filled), theme::fg(BLUE)),
                    span("▯".repeat(BAR - filled), theme::fg(TRACK)),
                ])
            }
            None => Line::from(span("-", theme::muted())),
        };
        let nodeset = Itertools::intersperse(
            log.nodeset.iter().map(|id| {
                if writable(id) {
                    node_span(*id)
                } else {
                    span(
                        format!("{id}✗"),
                        theme::fg(RED).add_modifier(Modifier::BOLD),
                    )
                }
            }),
            raw(" "),
        )
        .collect::<Vec<_>>();
        let mut row = vec![
            Line::from(span(log.id.to_string(), theme::bold())),
            log_state(log),
            segments,
            Line::from(raw(log.base_lsn.to_string())),
            Line::from(opt_num(log.tail_lsn)),
            Line::from(opt_num(log.trim_point)),
            retained,
            sequencer_cell(Some(log)),
            Line::from(nodeset),
        ];
        if show_since {
            row.push(Line::from(span(ago(log.created_at), theme::muted())));
        }
        if show_replication {
            row.push(Line::from(raw(log
                .replication
                .clone()
                .unwrap_or_else(|| "-".to_owned()))));
        }
        rows.push(row);
    }

    let mut header = vec![
        "LOG",
        "",
        "SEGMENT",
        "BASE-LSN",
        "TAIL",
        "TRIM",
        "RETAINED",
        "SEQUENCER",
        "NODESET",
    ];
    if show_since {
        header.push("SINCE");
    }
    if show_replication {
        header.push("REPLICATION");
    }
    let mut title = vec![
        span("Logs", theme::bold()),
        span(format!("  {}", logs.len()), theme::muted()),
    ];
    if let Some(provider) = &view.log_provider {
        title.push(span(
            format!(" · {}", crate::commands::status::provider_label(provider)),
            theme::muted(),
        ));
    }
    let mut legend = BTreeSet::new();
    if logs
        .iter()
        .any(|l| l.sequencer_with_leader() == Some(false))
    {
        legend.insert(("↗", "sequencer away from leader"));
    }
    if logs.iter().any(|l| l.nodeset.iter().any(|n| !writable(n))) {
        legend.insert(("✗", "can't take writes"));
    }
    RichTable {
        title,
        header,
        right: vec![0, 3, 4, 5, 6],
        rows,
        legend: legend_line(&legend),
        sorted: None,
    }
}

/// A row per metadata server.
pub fn metadata_table(view: &ClusterView) -> RichTable {
    let mut rows = Vec::new();
    for node in &view.nodes {
        let Some(m) = &node.metadata_server else {
            continue;
        };
        let status = match (&m.error, m.status.as_deref()) {
            (Some(err), _) => span(err.clone(), theme::fg(RED)),
            (None, Some("member")) if m.leader == Some(node.plain()) => {
                span("★ leader", theme::fg(GREEN).add_modifier(Modifier::BOLD))
            }
            (None, Some("member")) => span("member", theme::fg(GREEN)),
            (None, Some("standby")) => span("standby", theme::muted()),
            (None, Some(status)) => span(status.to_owned(), theme::fg(AMBER)),
            (None, None) => span(m.state.clone(), theme::muted()),
        };
        let raft = m.raft.as_ref();
        let num = |v: Option<u64>| Line::from(opt_num(v));
        rows.push(vec![
            Line::from(node_span(node.plain())),
            Line::from(status),
            Line::from(span(
                m.config_version
                    .map_or_else(|| "-".to_owned(), |v| format!("v{v}")),
                theme::muted(),
            )),
            Line::from(
                Itertools::intersperse(m.members.iter().map(|n| node_span(*n)), raw(" "))
                    .collect::<Vec<_>>(),
            ),
            num(raft.map(|r| r.term)),
            num(raft.map(|r| r.applied)),
            num(raft.map(|r| r.committed)),
            num(raft.map(|r| r.log_length)),
            num(raft.map(|r| r.snapshot_index)),
            Line::from(raw(raft.map_or_else(
                || "-".to_owned(),
                |r| {
                    bytesize::ByteSize::b(r.snapshot_size)
                        .display()
                        .iec()
                        .to_string()
                },
            ))),
        ]);
    }
    RichTable {
        title: vec![span("Metadata servers", theme::bold())],
        header: vec![
            "NODE",
            "STATUS",
            "CONFIG",
            "MEMBERS",
            "TERM",
            "APPLIED",
            "COMMITTED",
            "LOG",
            "SNAPSHOT",
            "SIZE",
        ],
        right: vec![4, 5, 6, 7, 8, 9],
        rows,
        legend: None,
        sorted: None,
    }
}

/// The version most nodes run.
fn usual_version(view: &ClusterView) -> Option<String> {
    view.nodes
        .iter()
        .filter_map(|n| n.binary_version.clone())
        .counts()
        .into_iter()
        .max_by_key(|(version, count)| (*count, version.clone()))
        .map(|(version, _)| version)
}

/// How a node is doing, as the health of the tile that shows it.
fn node_health(view: &ClusterView, node: &NodeView) -> PartitionHealth {
    let p = &node.partitions;
    match node.liveness {
        Liveness::Dead | Liveness::Unknown => PartitionHealth::Unavailable,
        _ if view.nodes_probed && node.seen_versions.is_none() => PartitionHealth::Unavailable,
        _ if p.broken > 0
            || p.catching_up > 0
            || node
                .storage_state
                .as_deref()
                .is_some_and(|s| s != "read-write")
            || node
                .metadata_server
                .as_ref()
                .is_some_and(|m| m.error.is_some()) =>
        {
            PartitionHealth::Degraded
        }
        _ if p.transitioning > 0 || node.worker_state.as_deref().is_some_and(|s| s != "active") => {
            PartitionHealth::Reconfiguring
        }
        _ => PartitionHealth::Healthy,
    }
}

/// A tile per node, colored by how it's doing, for when cards would take too much room.
pub struct NodeTiles<'a> {
    pub view: &'a ClusterView,
}

impl NodeTiles<'_> {
    fn tile_width(&self) -> u16 {
        self.view
            .nodes
            .iter()
            .map(|n| n.plain().to_string().len())
            .max()
            .unwrap_or(2) as u16
            + 4
    }

    pub fn height(&self, width: u16) -> u16 {
        let per_row = (width.saturating_sub(4) / self.tile_width()).max(1);
        (self.view.nodes.len() as u16).div_ceil(per_row) + 2
    }

    /// Where each node's tile goes in the panel drawn in `area`, as many as fit.
    pub fn tiles(&self, area: Rect) -> Vec<(Rect, &NodeView)> {
        grid(
            area.inner(Margin::new(2, 1)),
            self.tile_width(),
            &self.view.nodes,
        )
        .collect()
    }
}

impl Widget for NodeTiles<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut counts = Vec::new();
        for (health, label) in [
            (PartitionHealth::Unavailable, "down"),
            (PartitionHealth::Degraded, "need attention"),
            (PartitionHealth::Reconfiguring, "changing"),
            (PartitionHealth::Healthy, "healthy"),
        ] {
            let count = self
                .view
                .nodes
                .iter()
                .filter(|n| node_health(self.view, n) == health)
                .count();
            if count > 0 {
                counts.push(span("■ ", theme::fg(theme::health(health))));
                counts.push(raw(format!("{count} {label}  ")));
            }
        }
        let block = panel(vec![
            span("Nodes", theme::bold()),
            span(
                format!(
                    "  {}/{} alive",
                    self.view.alive_nodes(),
                    self.view.nodes.len()
                ),
                theme::muted(),
            ),
        ])
        .title_top(Line::from(counts).right_aligned())
        .title_bottom(Line::from(vec![
            raw(" "),
            span("details in the Nodes view (4) ", theme::muted()),
        ]));
        block.render(area, buf);
        let tile = self.tile_width();
        for (Rect { x, y, .. }, node) in self.tiles(area) {
            let style = theme::tile(node_health(self.view, node));
            buf.set_string(x, y, "▎", style.fg(theme::rgb(theme::node(node.plain()))));
            buf.set_string(
                x + 1,
                y,
                format!(
                    "{:<w$}",
                    node.plain().to_string(),
                    w = usize::from(tile - 2)
                ),
                style,
            );
        }
    }
}

/// Node cards read well for a handful of nodes; more get a table.
pub fn show_node_cards(view: &ClusterView) -> bool {
    view.nodes.len() <= 8
}

/// A row per node of `nodes`, with a bar of the partitions it leads and follows on a scale
/// shared by all. `compact` leaves out the location and the readers, for a narrow pane.
pub fn nodes_table(view: &ClusterView, nodes: &[&NodeView], compact: bool) -> RichTable {
    const BAR: usize = 16;
    let max_replicas = view
        .nodes
        .iter()
        .map(|n| n.partitions.leaders + n.partitions.followers)
        .max()
        .unwrap_or(0)
        .max(1) as usize;
    let usual_version = usual_version(view);
    let show_location = !compact && view.nodes.iter().any(|n| !n.location.is_empty());
    let mut rows = Vec::new();
    for node in nodes {
        let alive = node.liveness == Liveness::Alive;
        let status = match node.liveness {
            Liveness::Alive if view.nodes_probed && node.seen_versions.is_none() => {
                span("▲ unreachable", theme::fg(AMBER))
            }
            Liveness::Alive => span("● alive", theme::fg(GREEN)),
            Liveness::Dead => span(
                format!("✗ dead {}", ago(node.last_seen_alive)),
                theme::fg(RED).add_modifier(Modifier::BOLD),
            ),
            Liveness::Unknown => span("? unknown", theme::fg(AMBER)),
        };
        let metadata = match &node.metadata_server {
            None => span("-", theme::muted()),
            Some(m) => match (&m.error, m.status.as_deref()) {
                (Some(_), _) => span("error", theme::fg(RED)),
                (None, Some("member")) if m.leader == Some(node.plain()) => {
                    span("★ leader", theme::bold())
                }
                (None, Some("member")) => span("member", theme::muted()),
                (None, Some(status)) => span(status.to_owned(), theme::fg(AMBER)),
                (None, None) => span(m.state.clone(), theme::muted()),
            },
        };
        let p = &node.partitions;
        let scale = |n: u32| (n as usize * BAR).div_ceil(max_replicas).min(BAR);
        let leaders = scale(p.leaders);
        let followers = scale(p.leaders + p.followers) - leaders;
        let color = theme::fg(theme::node(node.plain()));
        let bar = Line::from(vec![
            span("█".repeat(leaders), color),
            span("▒".repeat(followers), color),
            span("·".repeat(BAR - leaders - followers), theme::fg(TRACK)),
        ]);

        let mut notes = Vec::new();
        for (count, text, color) in [
            (p.broken, "broken", RED),
            (p.catching_up, "catching up", AMBER),
            (p.transitioning, "changing leadership", BLUE),
        ] {
            if count > 0 {
                notes.push(span(format!("{count} {text}"), theme::fg(color)));
            }
        }
        for (label, state, good) in [
            ("storage", node.storage_state.as_deref(), "read-write"),
            ("worker", node.worker_state.as_deref(), "active"),
        ] {
            if let Some(state) = state.filter(|s| *s != good) {
                notes.push(span(format!("{label} {state}"), theme::fg(AMBER)));
            }
        }
        if let Some(version) = &node.binary_version
            && Some(version) != usual_version.as_ref()
        {
            notes.push(span(format!("runs {version}"), theme::fg(AMBER)));
        }
        let notes = Itertools::intersperse(notes.into_iter(), span(" · ", theme::muted()))
            .collect::<Vec<_>>();

        let number = |n: u32| {
            if alive {
                raw(n.to_string())
            } else {
                span(n.to_string(), theme::muted())
            }
        };
        let mut row = vec![
            Line::from(generational_span(node.id)),
            Line::from(raw(node.name.clone())),
            Line::from(status),
            Line::from(span(
                node.uptime_s
                    .map_or_else(|| "-".to_owned(), |u| short_duration(u as i64)),
                theme::muted(),
            )),
            Line::from(metadata),
            bar,
            Line::from(number(p.leaders)),
            Line::from(number(p.followers)),
            Line::from(if !alive && node.logs.sequencer > 0 {
                span(
                    node.logs.sequencer.to_string(),
                    theme::fg(RED).add_modifier(Modifier::BOLD),
                )
            } else {
                number(node.logs.sequencer)
            }),
            Line::from(number(node.logs.nodeset)),
        ];
        if !compact {
            row.push(Line::from(number(node.logs.readers)));
        }
        if show_location {
            row.push(Line::from(span(node.location.clone(), theme::muted())));
        }
        row.push(Line::from(notes));
        rows.push(row);
    }
    let mut header = vec![
        "NODE",
        "NAME",
        "STATUS",
        "UPTIME",
        "METADATA",
        "LEADERSHIP",
        "LEADS",
        "FOLLOWS",
        "SEQUENCERS",
        "NODESETS",
    ];
    if !compact {
        header.push("READERS");
    }
    if show_location {
        header.push("LOCATION");
    }
    header.push("NOTES");
    RichTable {
        title: vec![
            span("Nodes", theme::bold()),
            span(
                format!("  {}/{} alive", view.alive_nodes(), view.nodes.len()),
                theme::muted(),
            ),
        ],
        header,
        right: if compact {
            vec![6, 7, 8, 9]
        } else {
            vec![6, 7, 8, 9, 10]
        },
        rows,
        legend: Some(Line::from(vec![
            span("█", Style::new()),
            span(" leads  ", theme::muted()),
            span("▒", Style::new()),
            span(" follows, in each node's color", theme::muted()),
        ])),
        sorted: None,
    }
}
