// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared building blocks for the human output of the cluster views: colors, glyphs, the
//! partition map, and the findings list.

use std::collections::BTreeSet;
use std::fmt::{Display, Write};

use chrono::{DateTime, Local, Utc};
use crossterm::style::{Attribute, Color, Stylize, style};
use itertools::Itertools;
use serde::Serialize;

use restate_cli_util::_comfy_table::{Cell, Color as CellColor};
use restate_cli_util::{CliContext, c_println};

use crate::observe::{
    ClusterView, Finding, NodeView, PartitionHealth, PartitionView, Severity, Subject,
    short_duration,
};
use crate::preview;

/// Prints `value` as pretty JSON on stdout.
pub fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    c_println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn paint(text: impl Display, color: Color) -> String {
    if CliContext::get().colors_enabled() {
        style(text).with(color).to_string()
    } else {
        text.to_string()
    }
}

pub fn bold(text: impl Display) -> String {
    if CliContext::get().colors_enabled() {
        style(text).attribute(Attribute::Bold).to_string()
    } else {
        text.to_string()
    }
}

pub fn dim(text: impl Display) -> String {
    paint(text, Color::DarkGrey)
}

pub fn severity_color(severity: Severity) -> Color {
    match severity {
        Severity::Critical => Color::Red,
        Severity::Warning => Color::Yellow,
        Severity::Info => Color::Cyan,
    }
}

pub fn severity_glyph(severity: Severity) -> &'static str {
    match severity {
        Severity::Critical => "✗",
        Severity::Warning => "▲",
        Severity::Info => "•",
    }
}

/// The cluster's overall verdict, from the most severe finding.
#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    Healthy,
    Degraded,
    Critical,
}

impl Verdict {
    pub fn of(findings: &[Finding]) -> Self {
        match findings.iter().map(|f| f.severity).max() {
            Some(Severity::Critical) => Verdict::Critical,
            Some(Severity::Warning) => Verdict::Degraded,
            _ => Verdict::Healthy,
        }
    }

    pub fn render(self) -> String {
        let (glyph, label, color) = match self {
            Verdict::Healthy => ("●", "HEALTHY", Color::Green),
            Verdict::Degraded => ("▲", "DEGRADED", Color::Yellow),
            Verdict::Critical => ("✗", "CRITICAL", Color::Red),
        };
        paint(bold(format!("{glyph} {label}")), color)
    }
}

pub fn health_glyph(health: PartitionHealth) -> String {
    match health {
        PartitionHealth::Healthy => paint("●", Color::Green),
        PartitionHealth::Reconfiguring => paint("◆", Color::Cyan),
        PartitionHealth::Degraded => paint("◐", Color::Yellow),
        PartitionHealth::Unavailable => paint(bold("○"), Color::Red),
        PartitionHealth::Unknown => dim("?"),
    }
}

pub fn health_label(health: PartitionHealth) -> &'static str {
    match health {
        PartitionHealth::Healthy => "healthy",
        PartitionHealth::Reconfiguring => "reconfiguring",
        PartitionHealth::Degraded => "degraded",
        PartitionHealth::Unavailable => "unavailable",
        PartitionHealth::Unknown => "unknown",
    }
}

/// Width available for output; falls back to 120 columns when not on a terminal.
fn term_width() -> usize {
    crossterm::terminal::size()
        .map(|(w, _)| usize::from(w))
        .ok()
        .or_else(|| std::env::var("COLUMNS").ok()?.parse().ok())
        .filter(|w| *w >= 40)
        .unwrap_or(120)
}

/// A strip with one glyph per partition telling its health, wrapped to fit the terminal, with
/// tick marks over every tenth partition.
pub fn partition_health_strip(view: &ClusterView) -> String {
    let mut out = String::new();
    let partitions = &view.partitions;
    if partitions.is_empty() {
        return out;
    }
    let block = (term_width().saturating_sub(4) / 10 * 10).clamp(10, 100);
    let mut used = BTreeSet::new();
    for chunk in &partitions.iter().chunks(block) {
        let chunk: Vec<&PartitionView> = chunk.collect();
        let mut ticks = String::new();
        for (i, p) in chunk.iter().enumerate() {
            if ticks.chars().count() > i {
                continue;
            }
            if p.id % 10 == 0 {
                ticks.push_str(&p.id.to_string());
            } else {
                ticks.push(' ');
            }
        }
        let _ = writeln!(out, " {}", dim(ticks.trim_end()));
        let mut row = String::new();
        for p in &chunk {
            used.insert(p.health);
            row.push_str(&health_glyph(p.health));
        }
        let _ = writeln!(out, " {row}");
    }
    let legend = used
        .iter()
        .map(|h| format!("{} {}", health_glyph(*h), health_label(*h)))
        .join("  ");
    let _ = writeln!(out, " {}", dim(legend));
    out
}

/// One line of a findings list: a finding, or a group of findings of the same kind.
pub struct FindingLine {
    /// the index of the finding the line stands for, the first of its group's
    pub first: usize,
    pub severity: Severity,
    pub subject: String,
    pub message: String,
    /// A command that shows more; set for warnings and worse
    pub hint: Option<String>,
}

/// Findings, most severe first. Findings of the same kind collapse into one line once there are
/// more than `expand_limit` of them.
pub fn group_findings(findings: &[Finding], expand_limit: usize) -> Vec<FindingLine> {
    let mut lines = Vec::new();
    for ((severity, code), group) in &findings
        .iter()
        .enumerate()
        .chunk_by(|(_, f)| (f.severity, f.code))
    {
        let (indexes, group): (Vec<usize>, Vec<&Finding>) = group.unzip();
        let actionable = |hint: Option<String>| hint.filter(|_| severity > Severity::Info);
        if group.len() <= expand_limit {
            lines.extend(
                indexes
                    .into_iter()
                    .zip(group)
                    .map(|(first, f)| FindingLine {
                        first,
                        severity,
                        subject: subject_label(&f.subject),
                        message: f.message.clone(),
                        hint: actionable(f.hint.clone()),
                    }),
            );
            continue;
        }
        let subject = group[0].subject;
        let summary = group_summary(code).unwrap_or(&group[0].message);
        let ids = compress_ids(group.iter().filter_map(|f| subject_id(&f.subject)));
        let message = if ids.is_empty() {
            summary.to_owned()
        } else {
            format!("{summary}: {}", truncate(&ids, 60))
        };
        let hint = match subject {
            Subject::Partition(_) => Some(preview::command("partitions list --unhealthy")),
            Subject::Log(_) => Some(preview::command("logs list --unhealthy")),
            _ => group[0].hint.clone(),
        };
        let count = group.iter().map(|f| f.subject).unique().count();
        lines.push(FindingLine {
            first: indexes[0],
            severity,
            subject: format!("{count} {}s", subject.kind()),
            message,
            hint: actionable(hint),
        });
    }
    lines
}

/// The findings as plain text lines.
pub fn findings_list(findings: &[Finding], expand_limit: usize) -> String {
    let mut out = String::new();
    if findings.is_empty() {
        let _ = writeln!(out, " {}", paint("✓ No issues found", Color::Green));
        return out;
    }
    let lines = group_findings(findings, expand_limit);
    let subject_width = lines.iter().map(|l| l.subject.len()).max().unwrap_or(0);
    for line in lines {
        let color = severity_color(line.severity);
        let _ = write!(
            out,
            " {} {}  {}",
            paint(severity_glyph(line.severity), color),
            paint(format!("{:subject_width$}", line.subject), color),
            line.message
        );
        if let Some(hint) = line.hint {
            let _ = write!(out, "  {}", dim(format!("→ {hint}")));
        }
        out.push('\n');
    }
    out
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let cut = text[..max].rfind(',').unwrap_or(max);
    format!("{},…", &text[..cut])
}

/// Describes many findings of the same kind, each about its own subject.
fn group_summary(code: &str) -> Option<&'static str> {
    Some(match code {
        "partition-unavailable" => "no active leader",
        "partition-log-down" => "its log can't take appends",
        "replica-broken" => "a replica is broken; its store must be replaced",
        "replica-missing" => "a replica set member isn't running",
        "under-replicated" => "fewer replicas than the replication factor",
        "catching-up" => "a replica is catching up",
        "replica-starting" => "a replica is starting",
        "stale-leader" => "a replica still leads a stale epoch",
        "stale-report" => "a replica hasn't reported recently",
        "leadership-change" => "leadership is changing",
        "reconfiguring" => "replica set is being reconfigured",
        "sequencer-dead" => "sequencer is dead; appends wait for a reconfiguration",
        "sequencer-restarted" => "sequencer restarted; appends wait for a reconfiguration",
        "log-sealed" => "tail segment is sealed; appends wait for the chain to be extended",
        "nodeset-unwritable" => "too few nodeset members take writes for the replication",
        "nodeset-degraded" => "a nodeset member doesn't take writes",
        "sequencer-away-from-leader" => "sequencer runs away from the partition's leader",
        "reader-awaiting-seal" => "readers wait for the chain to be sealed",
        "reader-sealing" => "readers are sealing the chain",
        "reader-switching" => "readers are switching segments or starting",
        "reader-without-processor" => "a node reads the log without running its partition",
        "node-started" => "started recently",
        "node-dead" => "dead",
        _ => return None,
    })
}

fn subject_id(subject: &Subject) -> Option<u32> {
    match subject {
        Subject::Partition(id) | Subject::Log(id) => Some(*id),
        Subject::Node(id) => Some(u32::from(*id)),
        _ => None,
    }
}

pub fn subject_label(subject: &Subject) -> String {
    match subject {
        Subject::Cluster => "cluster".to_owned(),
        Subject::Metadata => "metadata".to_owned(),
        Subject::Node(id) => format!("node {id}"),
        Subject::Partition(id) => format!("partition {id}"),
        Subject::Log(id) => format!("log {id}"),
    }
}

/// `[1,2,3,5,7,8]` → `1-3,5,7-8`
pub fn compress_ids(ids: impl Iterator<Item = u32>) -> String {
    let ids: Vec<u32> = ids.sorted().dedup().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < ids.len() {
        let start = ids[i];
        let mut end = start;
        while i + 1 < ids.len() && ids[i + 1] == end + 1 {
            i += 1;
            end = ids[i];
        }
        out.push(if start == end {
            start.to_string()
        } else {
            format!("{start}-{end}")
        });
        i += 1;
    }
    out.join(",")
}

/// When a status read from a file was captured, and how long ago that is.
pub fn snapshot_taken(captured_at: DateTime<Utc>) -> String {
    let at: DateTime<Local> = captured_at.into();
    format!(
        "snapshot taken {} ({} ago)",
        at.format("%Y-%m-%d %H:%M:%S"),
        short_duration((Utc::now() - captured_at).num_seconds())
    )
}

/// How long before `now` `ts` was, compactly (`3s`, `2m10s`, `1d4h`).
pub fn ago(ts: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    ts.map(|ts| short_duration((now - ts).num_seconds()))
        .unwrap_or_else(|| "-".to_owned())
}

pub fn opt<T: Display>(value: Option<T>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

pub fn cell(text: impl Display) -> Cell {
    Cell::new(text)
}

pub fn cell_fg(text: impl Display, color: CellColor) -> Cell {
    Cell::new(text).fg(color)
}

/// The role flags of a node, one letter per role, e.g. `AWLMI`.
pub fn role_flags(node: &NodeView) -> String {
    [
        ("admin", 'A'),
        ("worker", 'W'),
        ("log-server", 'L'),
        ("metadata-server", 'M'),
        ("http-ingress", 'I'),
    ]
    .iter()
    .map(|(role, flag)| {
        if node.has_role(role) {
            flag.to_string()
        } else {
            dim('·')
        }
    })
    .collect()
}

pub const ROLE_LEGEND: &str = "A admin  W worker  L log-server  M metadata-server  I ingress";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::short_duration;

    #[test]
    fn compact_formats() {
        assert_eq!(compress_ids([7, 1, 2, 3, 5, 8, 2].into_iter()), "1-3,5,7-8");
        assert_eq!(compress_ids(std::iter::empty()), "");
        assert_eq!(truncate("1,3,5,7,9", 5), "1,3,…");
        assert_eq!(short_duration(42), "42s");
        assert_eq!(short_duration(90), "1m30s");
        assert_eq!(short_duration(7_260), "2h1m");
        assert_eq!(short_duration(90_000), "1d1h");
    }
}
