// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Local, Utc};
use clap::Parser;
use cling::{Collect, Run};
use crossterm::style::Color;
use itertools::Itertools;
use serde::Serialize;
use tracing::warn;

use restate_cli_util::_comfy_table::{Color as CellColor, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_eprintln, c_print, c_println, c_success, c_tip, c_warn};
use restate_memory::ByteCount;

use crate::commands::log::list_logs::logs_table;
use crate::commands::metadata_server::list_servers::metadata_servers_table;
use crate::commands::node::list_nodes::{ListNodesOpts, list_nodes_lite};
use crate::commands::partition::list::partitions_table;
use crate::connection::{ConnectionInfo, ConnectionInfoError};
use crate::observe::dump::{self, Dump};
use crate::observe::{
    ClusterView, Finding, LogProvider, LogView, NodeView, PartitionView, Summary, Versions,
    analyze, short_duration,
};
use crate::preview;
use crate::render::{
    ROLE_LEGEND, Verdict, bold, cell, cell_fg, dim, findings_list, paint, partition_health_strip,
    print_json, role_flags, snapshot_taken,
};
use crate::tui::dashboard::Feed;
use crate::tui::{self, Presentation};

mod legacy;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_status")]
#[command(after_long_help = preview::enabled().then_some(STATUS_HELP))]
pub struct ClusterStatusOpts {
    /// Display additional status information
    #[arg(long, short = preview::enabled().then_some('x'))]
    extra: bool,

    /// Save the cluster's status to a file, to look at later or elsewhere with --file. Takes 5
    /// seconds to sample what changes by the second. Defaults to
    /// restate-status-<cluster>-<time>.json.zst in the current directory
    #[arg(
        long,
        value_name = "PATH",
        num_args = 0..=1,
        conflicts_with_all = ["file", "watch"],
        help_heading = preview::HEADING
    )]
    dump: Option<Option<PathBuf>>,

    #[clap(flatten)]
    source: Source,

    #[clap(flatten, next_help_heading = preview::HEADING)]
    watch: Watch,
}

/// Where the status comes from: the cluster, or a file written by `status --dump`.
#[derive(clap::Args, Clone, Collect, Debug)]
#[command(next_help_heading = preview::HEADING)]
pub struct Source {
    /// Show the status saved to a file by `restatectl status --dump` instead of the cluster's
    #[arg(long, value_name = "PATH")]
    file: Option<PathBuf>,
}

impl Source {
    pub fn is_file(&self) -> bool {
        self.file.is_some()
    }

    /// The cluster's status: its current one, or the newest in the file.
    pub async fn view(&self, connection: &ConnectionInfo) -> anyhow::Result<ClusterView> {
        match &self.file {
            Some(path) => Ok(dump::load(path)?.pop().expect("a dump has a sample")),
            None => Ok(ClusterView::fetch(connection).await?),
        }
    }

    /// What the dashboard shows: the cluster as it changes, or the file's samples.
    pub fn feed<'a>(&self, connection: &'a ConnectionInfo) -> anyhow::Result<Feed<'a>> {
        Ok(match &self.file {
            Some(path) => Feed::Recorded(dump::load(path)?),
            None => Feed::Live(connection),
        })
    }
}

const STATUS_HELP: &str = "\
The verdict is CRITICAL when part of the cluster is unavailable or needs intervention, DEGRADED \
when it runs with reduced redundancy, and HEALTHY otherwise. Ongoing activity such as replica set \
reconfigurations is listed without affecting the verdict.

On a terminal that shows colors, the cluster is drawn as panels: a card per node with its \
leadership, and a tile per partition whose color tells its health (green healthy, blue \
reconfiguring, amber degraded, red unavailable) and whose left edge has the color of the node \
leading it. With -w, a live dashboard opens instead: switch views with Tab or 1-5, select a \
partition, log or node for its details and rates over time, and press ? for the keys. Use \
--plain for text tables; output that isn't a terminal gets them anyway.

With --json, the output is one document with the verdict, a summary, the nodes and the findings \
(each with a severity, a stable code, its subject and a follow-up command); --extra adds the \
partitions and logs.

--dump saves everything restatectl reads about the cluster's status to one file, for a closer \
look later or to send to someone helping you: the metadata, each node's report and the cluster's \
system tables, with what changes by the second (partition processors, log tails, read streams) \
sampled 5 times, a second apart, so that rates can be computed. It has the cluster's node \
addresses and host names, and no application data. `status`, `partitions list` and `logs list` \
show such a file with --file, and -w opens it in the dashboard.";

async fn cluster_status(
    connection: &ConnectionInfo,
    opts: &ClusterStatusOpts,
) -> anyhow::Result<()> {
    if !preview::enabled() {
        return legacy::cluster_status(connection, opts.extra).await;
    }
    if let Some(path) = &opts.dump {
        return dump_status(connection, path.as_deref()).await;
    }
    if opts.watch.is_enabled() && tui::can_run_dashboard() {
        return tui::dashboard::run(
            opts.source.feed(connection)?,
            tui::dashboard::Options {
                tab: tui::dashboard::Tab::Overview,
                interval: opts.watch.interval(),
                unhealthy_only: false,
                partition_sort: tui::dashboard::PartitionSort::Partition,
            },
        )
        .await;
    }
    if opts.source.is_file() {
        // a file doesn't change: there's nothing to watch
        return status(connection, opts).await;
    }
    opts.watch
        .run(|| async {
            connection.reset().await;
            status(connection, opts).await
        })
        .await
}

/// Saves the cluster's status to `path`, or to a file named after the cluster and the time.
async fn dump_status(connection: &ConnectionInfo, path: Option<&Path>) -> anyhow::Result<()> {
    c_eprintln!(
        "Reading the cluster's status, {} times a second apart…",
        dump::SAMPLES
    );
    let dump = Dump::collect(connection).await?;
    let path = path.map_or_else(|| default_dump_path(&dump), Path::to_owned);
    let size = dump.write(&path)?;
    let view = dump.views().pop().expect("a dump has a sample");
    let findings = analyze(&view);
    let file = path.display().to_string();

    if Presentation::current() == Presentation::Json {
        return print_json(&DumpReport {
            file: &file,
            bytes: size,
            health: Verdict::of(&findings),
            cluster_name: &view.cluster_name,
            captured_at: view.captured_at,
            samples: dump::SAMPLES,
            warnings: &view.warnings,
        });
    }
    c_success!("Saved the status of {} to {file}", view.cluster_name);
    let summary = Summary::new(&view);
    c_println!(
        " {}  {}/{} nodes alive · {} partitions · {} logs · {} samples · {}",
        Verdict::of(&findings).render(),
        summary.nodes.alive,
        summary.nodes.total,
        view.partitions.len(),
        view.logs.len(),
        dump::SAMPLES,
        ByteCount::from(size),
    );
    if !view.warnings.is_empty() {
        c_warn!(
            "Some of the status couldn't be read:\n{}",
            view.warnings.join("\n")
        );
    }
    c_tip!(
        "The file has the cluster's node addresses and host names, and no application data. Look \
         at it with `{}`, and -w for the dashboard.",
        preview::command(format_args!("status --file {file}"))
    );
    Ok(())
}

/// `restate-status-<cluster>-<UTC time>.json.zst`
fn default_dump_path(dump: &Dump) -> PathBuf {
    let cluster: String = dump
        .cluster_name()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    PathBuf::from(format!(
        "restate-status-{cluster}-{}.json.zst",
        dump.captured_at().format("%Y%m%dT%H%M%SZ")
    ))
}

#[derive(Serialize)]
struct DumpReport<'a> {
    file: &'a str,
    bytes: u64,
    health: Verdict,
    cluster_name: &'a str,
    captured_at: DateTime<Utc>,
    samples: usize,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    warnings: &'a [String],
}

async fn status(connection: &ConnectionInfo, opts: &ClusterStatusOpts) -> anyhow::Result<()> {
    let view = match opts.source.view(connection).await {
        Ok(view) => view,
        Err(err) => {
            if let Some(ConnectionInfoError::MetadataValueNotAvailable { contacted_nodes }) =
                err.downcast_ref()
            {
                warn!(
                    "Could not read nodes configuration from cluster, using GetIdent responses to render basic list"
                );
                list_nodes_lite(contacted_nodes, &ListNodesOpts { extra: false });
                return Err(ConnectionInfoError::ClusterNotProvisioned.into());
            }
            return Err(err);
        }
    };
    let findings = analyze(&view);

    match Presentation::current() {
        Presentation::Json => {
            return print_json(&StatusReport::new(&view, &findings, opts.extra));
        }
        Presentation::Rich => {
            tui::views::status(&view, &findings, opts.extra, tui::width()).print();
            return Ok(());
        }
        Presentation::Plain => {}
    }

    c_println!("{}", header(&view, &findings));
    c_println!("{}", nodes_table(&view, opts.extra));
    c_println!(" {}", dim(format!("roles: {ROLE_LEGEND}")));

    if !view.partitions.is_empty() {
        c_println!();
        c_println!(" {}", bold("PARTITIONS"));
        c_println!("{}", partition_health_strip(&view));
    }

    if opts.extra {
        c_println!(" {}", bold("METADATA SERVERS"));
        c_println!("{}", metadata_servers_table(&view));
        c_println!();
        c_println!(" {}", bold("LOGS"));
        c_println!("{}", logs_table(&view, &view.logs));
        c_println!();
        c_println!(" {}", bold("PARTITION DETAILS"));
        c_println!("{}", partitions_table(&view, &view.partitions, false));
        c_println!();
    }

    c_println!(" {}", bold("FINDINGS"));
    c_print!("{}", findings_list(&findings, 3));
    Ok(())
}

fn header(view: &ClusterView, findings: &[Finding]) -> String {
    let summary = Summary::new(view);
    let mut lines = Vec::new();

    let versions = view
        .nodes
        .iter()
        .filter_map(|n| n.binary_version.as_deref())
        .unique()
        .join(", ");
    let captured: DateTime<Local> = view.captured_at.into();
    let captured = match &view.file {
        Some(_) => snapshot_taken(view.captured_at),
        None => captured.format("%H:%M:%S").to_string(),
    };
    lines.push(format!(
        " {}  {}{}  {}",
        Verdict::of(findings).render(),
        bold(&view.cluster_name),
        if versions.is_empty() {
            String::new()
        } else {
            dim(format!(" · restate {versions}"))
        },
        dim(captured),
    ));

    let ratio = |good: usize, total: usize| {
        let text = format!("{good}/{total}");
        if good == total {
            text
        } else {
            paint(text, Color::Yellow)
        }
    };
    let n = &summary.nodes;
    let mut nodes = format!("{} alive", ratio(n.alive, n.total));
    if n.dead > 0 {
        nodes.push_str(&paint(format!(" · {} dead", n.dead), Color::Red));
    }
    lines.push(kv("nodes", nodes));

    let p = &summary.partitions;
    let mut partitions = format!("{} led", ratio(p.led, p.total));
    for (count, label, color) in [
        (p.unavailable, "unavailable", Color::Red),
        (p.degraded, "degraded", Color::Yellow),
        (p.reconfiguring, "reconfiguring", Color::Cyan),
    ] {
        if count > 0 {
            partitions.push_str(&format!(" · {}", paint(format!("{count} {label}"), color)));
        }
    }
    if let Some(replication) = &view.partition_replication {
        partitions.push_str(&dim(format!(" · replication {replication}")));
    }
    lines.push(kv("partitions", partitions));

    let mut logs = format!("{}", summary.logs.total);
    if summary.logs.sealed > 0 {
        logs.push_str(&paint(
            format!(" · {} sealed", summary.logs.sealed),
            Color::Yellow,
        ));
    }
    if summary.logs.sequencer_down > 0 {
        logs.push_str(&paint(
            format!(" · {} without a sequencer", summary.logs.sequencer_down),
            Color::Red,
        ));
    }
    if let Some(provider) = &view.log_provider {
        logs.push_str(&dim(format!(" · {}", provider_label(provider))));
    }
    lines.push(kv("logs", logs));

    if let Some(m) = &summary.metadata {
        let mut metadata = format!("{} members up", ratio(m.up, m.members));
        match &m.leader {
            Some(leader) => metadata.push_str(&format!(" · leader {leader}")),
            None => metadata.push_str(&paint(" · no leader", Color::Red)),
        }
        lines.push(kv("metadata", metadata));
    }

    let v = &view.versions;
    lines.push(kv(
        "versions",
        dim(format!(
            "nodes-config v{} · partition-table {} · logs {} · schema {}",
            v.nodes_config,
            version(v.partition_table),
            version(v.logs),
            version(v.schema)
        )),
    ));
    lines.join("\n") + "\n"
}

fn kv(key: &str, value: String) -> String {
    format!("   {}{value}", bold(format!("{key:11}")))
}

fn version(v: Option<u32>) -> String {
    v.map(|v| format!("v{v}")).unwrap_or_else(|| "-".to_owned())
}

pub fn provider_label(provider: &LogProvider) -> String {
    let mut label = provider.kind.clone();
    if let Some(replication) = &provider.replication {
        label.push_str(&format!(" {replication}"));
    }
    match provider.nodeset_size {
        Some(0) => label.push_str(", nodeset size auto"),
        Some(size) => label.push_str(&format!(", nodeset size {size}")),
        None => {}
    }
    label
}

fn nodes_table(view: &ClusterView, extra: bool) -> Table {
    let show_location = view.nodes.iter().any(|n| !n.location.is_empty());
    let mut header = vec![
        "NODE",
        "NAME",
        "STATUS",
        "UPTIME",
        "ROLES",
        "METADATA",
        "STORAGE",
        "WORKER",
        "LEADERS",
        "FOLLOWERS",
        "SEQUENCERS",
        "NODESETS",
    ];
    if show_location {
        header.push("LOCATION");
    }
    let version_skew = view
        .nodes
        .iter()
        .filter_map(|n| n.binary_version.as_ref())
        .unique()
        .count()
        > 1;
    if extra || version_skew {
        header.push("VERSION");
    }
    if extra {
        // the metadata versions each node has loaded
        header.extend(["ADDRESS", "NODES-CFG", "LOGS", "SCHEMA", "P-TABLE"]);
    }

    let mut table = Table::new_styled();
    table.set_styled_header(header);
    for node in &view.nodes {
        let mut row = vec![
            node_id_cell(node),
            cell(&node.name),
            liveness_cell(view, node),
            cell(
                node.uptime_s
                    .map(|u| short_duration(u as i64))
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            cell(role_flags(node)),
            metadata_cell(node),
            state_cell(node.storage_state.as_deref(), "read-write"),
            state_cell(node.worker_state.as_deref(), "active"),
            cell(node.partitions.leaders),
            followers_cell(node),
            sequencers_cell(node),
            cell(node.logs.nodeset),
        ];
        if show_location {
            row.push(cell(&node.location));
        }
        if extra || version_skew {
            row.push(cell(node.binary_version.as_deref().unwrap_or("-")));
        }
        if extra {
            row.push(cell(&node.address));
            let seen = node.seen_versions.as_ref();
            let current = &view.versions;
            row.extend([
                seen_version(seen.map(|s| s.nodes_config), Some(current.nodes_config)),
                seen_version(seen.and_then(|s| s.logs), current.logs),
                seen_version(seen.and_then(|s| s.schema), current.schema),
                seen_version(
                    seen.and_then(|s| s.partition_table),
                    current.partition_table,
                ),
            ]);
        }
        table.add_row(row);
    }
    table
}

/// A version a node has loaded, yellow when it's behind the newest one.
fn seen_version(seen: Option<u32>, newest: Option<u32>) -> restate_cli_util::_comfy_table::Cell {
    match seen {
        Some(seen) if newest.is_some_and(|newest| seen < newest) => {
            cell_fg(format!("v{seen}"), CellColor::Yellow)
        }
        Some(seen) => cell(format!("v{seen}")),
        None => cell(dim("-")),
    }
}

fn node_id_cell(node: &NodeView) -> restate_cli_util::_comfy_table::Cell {
    use crate::observe::Liveness;
    match node.liveness {
        Liveness::Alive => cell(node.id),
        _ => cell_fg(node.id, CellColor::Red),
    }
}

fn liveness_cell(view: &ClusterView, node: &NodeView) -> restate_cli_util::_comfy_table::Cell {
    use crate::observe::Liveness;
    match node.liveness {
        Liveness::Alive if view.nodes_probed && node.seen_versions.is_none() => {
            cell_fg("unreachable", CellColor::Yellow)
        }
        Liveness::Alive => cell_fg("alive", CellColor::Green),
        Liveness::Dead => cell_fg(
            match node.last_seen_alive {
                Some(t) => format!(
                    "dead {}",
                    short_duration((view.captured_at - t).num_seconds())
                ),
                None => "dead".to_owned(),
            },
            CellColor::Red,
        ),
        Liveness::Unknown => cell_fg("unknown", CellColor::Yellow),
    }
}

fn metadata_cell(node: &NodeView) -> restate_cli_util::_comfy_table::Cell {
    let Some(m) = &node.metadata_server else {
        return cell(dim("-"));
    };
    if m.error.is_some() {
        return cell_fg("error", CellColor::Red);
    }
    match m.status.as_deref() {
        Some("member") if m.leader == Some(node.plain()) => cell_fg("leader", CellColor::Green)
            .add_attribute(restate_cli_util::_comfy_table::Attribute::Bold),
        Some("member") => cell_fg("member", CellColor::Green),
        Some("standby") => cell("standby"),
        Some(other) => cell_fg(other, CellColor::Yellow),
        // not a replicated metadata server, show the configured state
        None => cell(&m.state),
    }
}

fn state_cell(state: Option<&str>, good: &str) -> restate_cli_util::_comfy_table::Cell {
    match state {
        None => cell(dim("-")),
        Some(state) if state == good => cell(state),
        Some(state @ ("data-loss" | "gone" | "disabled")) => cell_fg(state, CellColor::Red),
        Some(state) => cell_fg(state, CellColor::Yellow),
    }
}

fn followers_cell(node: &NodeView) -> restate_cli_util::_comfy_table::Cell {
    let p = &node.partitions;
    let mut text = p.followers.to_string();
    let mut notes = Vec::new();
    if p.transitioning > 0 {
        notes.push(paint(
            format!("{} transitioning", p.transitioning),
            Color::Magenta,
        ));
    }
    if p.catching_up > 0 {
        notes.push(paint(
            format!("{} catching up", p.catching_up),
            Color::Yellow,
        ));
    }
    if p.broken > 0 {
        notes.push(paint(format!("{} broken", p.broken), Color::Red));
    }
    if !notes.is_empty() {
        text.push_str(&format!(" ({})", notes.join(", ")));
    }
    cell(text)
}

fn sequencers_cell(node: &NodeView) -> restate_cli_util::_comfy_table::Cell {
    let l = &node.logs;
    if l.sequencer_away_from_leader > 0 {
        cell(format!(
            "{} {}",
            l.sequencer,
            paint(
                format!("({} away from leader)", l.sequencer_away_from_leader),
                Color::Yellow
            )
        ))
    } else {
        cell(l.sequencer)
    }
}

#[derive(Serialize)]
struct StatusReport<'a> {
    /// the file the status was read from, when it isn't the cluster's current one
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<&'a str>,
    health: Verdict,
    cluster_name: &'a str,
    captured_at: DateTime<Utc>,
    versions: &'a Versions,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_replication: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_provider: Option<&'a LogProvider>,
    summary: Summary,
    nodes: &'a [NodeView],
    findings: &'a [Finding],
    #[serde(skip_serializing_if = "Option::is_none")]
    partitions: Option<&'a [PartitionView]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<&'a [LogView]>,
}

impl<'a> StatusReport<'a> {
    fn new(view: &'a ClusterView, findings: &'a [Finding], extra: bool) -> Self {
        Self {
            file: view.file.as_deref(),
            health: Verdict::of(findings),
            cluster_name: &view.cluster_name,
            captured_at: view.captured_at,
            versions: &view.versions,
            partition_replication: view.partition_replication.as_deref(),
            log_provider: view.log_provider.as_ref(),
            summary: Summary::new(view),
            nodes: &view.nodes,
            findings,
            partitions: extra.then_some(view.partitions.as_slice()),
            logs: extra.then_some(view.logs.as_slice()),
        }
    }
}
