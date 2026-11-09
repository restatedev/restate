// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use cling::prelude::*;
use crossterm::style::Color;
use itertools::Itertools;
use serde::Serialize;

use restate_cli_util::_comfy_table::{Attribute, Cell, Color as CellColor, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_print, c_println};
use restate_types::PlainNodeId;

use crate::commands::status::Source;
use crate::connection::ConnectionInfo;
use crate::observe::{
    ClusterView, Finding, Mode, PartitionHealth, PartitionPlacement, PartitionView, ReaderPhase,
    Replay, ReplicaView, SOURCE_UNAVAILABLE, SequencerState, Subject, analyze, imbalanced,
    mode_label,
};
use crate::preview;
use crate::render::{
    ago, bold, cell, cell_fg, dim, findings_list, health_glyph, opt, paint, print_json,
};
use crate::tui::{self, Presentation};
use crate::util::RangeParam;

pub(crate) mod legacy;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "list_partitions")]
#[clap(alias = "ls")]
#[command(after_long_help = if preview::enabled() { PARTITIONS_HELP } else { legacy::HELP })]
pub struct ListPartitionsOpts {
    /// Sort order
    #[arg(long, default_value = "partition")]
    sort: SortMode,

    /// Partition ids or ranges to show, e.g. "0", "1-4"; all partitions by default
    #[arg(help_heading = preview::HEADING)]
    partitions: Vec<RangeParam>,

    /// Only show the partitions with a replica set member or processor on this node, e.g. "N1"
    #[arg(long, help_heading = preview::HEADING)]
    node: Option<PlainNodeId>,

    /// Only show the partitions that aren't healthy
    #[arg(long, help_heading = preview::HEADING)]
    unhealthy: bool,

    /// Show a row per partition processor instead of a row per partition
    #[arg(long, short, help_heading = preview::HEADING)]
    replicas: bool,

    /// Show additional columns: log and key range, or versions and features with --replicas
    #[arg(long, short = 'x', help_heading = preview::HEADING)]
    extra: bool,

    #[clap(flatten)]
    source: Source,

    #[clap(flatten, next_help_heading = preview::HEADING)]
    watch: Watch,
}

const PARTITIONS_HELP: &str = "\
Each partition row shows its health (● healthy, ◆ reconfiguring, ◐ degraded, ○ unavailable, ? \
unknown when the cluster state can't be read), its leader and leadership epoch, and its replica \
set; a replica set being reconfigured shows as `current → next`. Replicas that aren't running \
normally are marked: (c) catching up, (s) starting, (^) becoming leader, (v) stepping down, (?) \
missing, (!) leading a stale epoch, (X) broken. LAG is the largest distance of a replica from the \
log tail, DURABLE the lowest LSN all replicas have persisted, ARCHIVED the newest snapshot's LSN. \
With --replicas, READ tells where each replica's read stream of the log is: at the tail, how far \
behind it, or what it waits for; a replica can lag because it can't read the log fast enough or \
because it applies slowly. The sequencer is yellow when it runs away from the leader and red when \
it's down.

With --json, the output is one document with the partitions (each with its replicas) and the \
findings about them.";

#[derive(ValueEnum, Collect, Clone, Debug, Default)]
#[clap(rename_all = "kebab-case")]
pub(crate) enum SortMode {
    /// Order list by partition id
    #[default]
    Partition,
    /// Order list by node id
    Node,
    /// Order list by processor leadership state
    Active,
    /// Order by health: the least healthy first
    #[value(hide = !preview::enabled())]
    Health,
}

async fn list_partitions(
    connection: &ConnectionInfo,
    opts: &ListPartitionsOpts,
) -> anyhow::Result<()> {
    if let SortMode::Health = opts.sort {
        preview::require("--sort health")?;
    }
    if !preview::enabled() {
        return legacy::list_partitions(connection, &opts.sort).await;
    }
    // the dashboard shows every partition; a selection refreshes the listing instead
    let selects = !opts.partitions.is_empty() || opts.node.is_some() || opts.replicas;
    if opts.watch.is_enabled() && !selects && tui::can_run_dashboard() {
        return tui::dashboard::run(
            opts.source.feed(connection)?,
            tui::dashboard::Options {
                tab: tui::dashboard::Tab::Partitions,
                interval: opts.watch.interval(),
                unhealthy_only: opts.unhealthy,
                partition_sort: match opts.sort {
                    SortMode::Partition => tui::dashboard::PartitionSort::Partition,
                    SortMode::Node | SortMode::Active => tui::dashboard::PartitionSort::Leader,
                    SortMode::Health => tui::dashboard::PartitionSort::Health,
                },
            },
        )
        .await;
    }
    if opts.source.is_file() {
        return show_partitions(connection, opts).await;
    }
    opts.watch
        .run(|| async {
            connection.reset().await;
            show_partitions(connection, opts).await
        })
        .await
}

async fn show_partitions(
    connection: &ConnectionInfo,
    opts: &ListPartitionsOpts,
) -> anyhow::Result<()> {
    let view = opts.source.view(connection).await?;
    let findings = analyze(&view);

    let selected: Vec<&PartitionView> = view
        .partitions
        .iter()
        .filter(|p| {
            opts.partitions.is_empty()
                || opts
                    .partitions
                    .iter()
                    .any(|range| range.into_iter().contains(&p.id))
        })
        .filter(|p| {
            opts.node.is_none_or(|node| {
                p.is_member(node) || p.is_next_member(node) || p.replica_on(node).is_some()
            })
        })
        .filter(|p| !opts.unhealthy || p.health != PartitionHealth::Healthy)
        .sorted_by(|a, b| match opts.sort {
            SortMode::Partition => a.id.cmp(&b.id),
            SortMode::Node => a.leader.cmp(&b.leader).then(a.id.cmp(&b.id)),
            SortMode::Active => b.is_led().cmp(&a.is_led()).then(a.id.cmp(&b.id)),
            SortMode::Health => b.health.cmp(&a.health).then(a.id.cmp(&b.id)),
        })
        .collect();
    // and the sources that couldn't be read, which explain what's missing
    let findings: Vec<Finding> = findings
        .into_iter()
        .filter(|f| match f.subject {
            Subject::Partition(id) => selected.iter().any(|p| p.id == id),
            _ => f.code == SOURCE_UNAVAILABLE,
        })
        .collect();

    let placement = PartitionPlacement::of(&view, &selected);
    // the spread over the nodes reads as the cluster's only when nothing is left out
    let unfiltered = opts.partitions.is_empty() && opts.node.is_none() && !opts.unhealthy;

    let presentation = Presentation::current();
    if presentation == Presentation::Json {
        #[derive(Serialize)]
        struct Report<'a> {
            captured_at: DateTime<Utc>,
            partition_table_version: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            replication: Option<&'a str>,
            partitions: Vec<&'a PartitionView>,
            /// how the listed partitions spread over the nodes
            placement: PartitionPlacement,
            findings: Vec<Finding>,
        }
        return print_json(&Report {
            captured_at: view.captured_at,
            partition_table_version: view.versions.partition_table,
            replication: view.partition_replication.as_deref(),
            partitions: selected,
            placement,
            findings,
        });
    }

    if presentation == Presentation::Rich {
        if selected.is_empty() {
            c_println!(" No partitions match.");
        } else {
            tui::views::partitions(
                &view,
                &selected,
                &findings,
                unfiltered.then_some(&placement),
                opts.replicas,
                opts.extra,
                tui::width(),
            )
            .print();
        }
        return Ok(());
    }

    let led = selected.iter().filter(|p| p.is_led()).count();
    let mut summary = format!(
        "{} partitions · {}/{} led",
        selected.len(),
        led,
        selected.len()
    );
    if let Some(replication) = &view.partition_replication {
        summary.push_str(&format!(" · replication {replication}"));
    }
    summary.push_str(&format!(
        " · partition table v{} · nodes config v{}",
        opt(view.versions.partition_table),
        view.versions.nodes_config
    ));
    c_println!(" {}", dim(summary));

    if selected.is_empty() {
        c_println!(" No partitions match.");
        return Ok(());
    }

    let table = if opts.replicas {
        replicas_table(&selected, &opts.sort, opts.extra, view.captured_at)
    } else {
        partitions_table(&view, selected.iter().copied(), opts.extra)
    };
    c_println!("{table}");

    if unfiltered {
        c_println!();
        c_println!(
            " {} {}",
            bold("PLACEMENT"),
            dim(format!(
                "even split over the active workers: {:.1} leaders, {:.1} replicas",
                placement.even_leaders, placement.even_replicas
            ))
        );
        let mut table = Table::new_styled();
        table.set_styled_header(vec!["NODE", "LEADERS", "REPLICAS"]);
        for share in &placement.nodes {
            let count = |count: u32, even: f64| {
                let notable = if share.eligible {
                    imbalanced(count, even)
                } else {
                    count > 0
                };
                if notable {
                    cell_fg(count, CellColor::Yellow)
                } else {
                    cell(count)
                }
            };
            let node = if share.eligible {
                cell(share.node)
            } else {
                cell(dim(format!("{} (not eligible)", share.node)))
            };
            table.add_row(vec![
                node,
                count(share.leaders, placement.even_leaders),
                count(share.replicas, placement.even_replicas),
            ]);
        }
        c_println!("{table}");
    }

    if !findings.is_empty() {
        c_println!();
        c_println!(" {}", bold("FINDINGS"));
        c_print!("{}", findings_list(&findings, 5));
    }
    Ok(())
}

/// A row per partition: health, leader, replica set, and how far its replicas have come.
pub fn partitions_table<'a>(
    view: &ClusterView,
    partitions: impl IntoIterator<Item = &'a PartitionView>,
    extra: bool,
) -> Table {
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
    let mut table = Table::new_styled();
    table.set_styled_header(header);

    for p in partitions {
        let leader = p.leader_replica();
        let counted = || p.replicas.iter().filter(|r| p.counts(r));
        let lag = counted().filter_map(|r| r.lag).max();

        let mut row = vec![
            cell(p.id).add_attribute(Attribute::Bold),
            cell(health_glyph(p.health)),
            match p.leader {
                Some(leader) if p.is_led() => cell_fg(leader, CellColor::Green),
                Some(leader) => cell_fg(leader, CellColor::Yellow),
                None => cell_fg("none", CellColor::Red),
            },
            cell(opt(p.epoch.map(|e| format!("e{e}")))),
            cell(replica_set(p)),
            cell(opt(leader.and_then(|r| r.applied_lsn))),
            cell(opt(lag)),
            cell(opt(counted().filter_map(|r| r.durable_lsn).min())),
            cell(opt(p.replicas.iter().filter_map(|r| r.archived_lsn).max())),
            sequencer_cell(view, p),
        ];
        if extra {
            row.push(cell(opt(p.log_id)));
            row.push(cell(opt(p.log_tail_lsn)));
            row.push(cell(opt(p
                .key_range
                .map(|k| format!("{:016x}-{:016x}", k.start, k.end)))));
        }
        table.add_row(row);
    }
    table
}

/// `N1 N2(c)`, or `N1 N2 → N1 N3` while the replica set is reconfigured.
fn replica_set(p: &PartitionView) -> String {
    let token = |node: PlainNodeId| {
        let Some(replica) = p.replica_on(node) else {
            return paint(format!("{node}(?)"), Color::Red);
        };
        let (marker, color) = replica_marker(replica);
        let text = match marker {
            Some(marker) => format!("{node}({marker})"),
            None => node.to_string(),
        };
        match color {
            Some(color) => paint(text, color),
            None => text,
        }
    };
    let Some(membership) = &p.membership else {
        // unknown replica sets: show the running processors
        return p
            .replicas
            .iter()
            .map(|r| token(r.node.as_plain()))
            .join(" ");
    };
    let current = membership.members.iter().map(|n| token(*n)).join(" ");
    match &membership.next {
        Some(next) => format!(
            "{current} {} {}",
            paint("→", Color::Cyan),
            next.members
                .iter()
                .map(|n| if membership.members.contains(n) {
                    token(*n)
                } else {
                    paint(format!("{n}(+)"), Color::Cyan)
                })
                .join(" ")
        ),
        None => current,
    }
}

fn replica_marker(r: &ReplicaView) -> (Option<char>, Option<Color>) {
    if r.mode == Mode::Broken {
        return (Some('X'), Some(Color::Red));
    }
    if r.stale_epoch {
        return (Some('!'), Some(Color::Red));
    }
    match (r.mode, r.replay) {
        (_, Replay::CatchingUp) => (Some('c'), Some(Color::Yellow)),
        (_, Replay::Starting) => (Some('s'), Some(Color::Yellow)),
        (Mode::Candidate | Mode::BecomingLeader, _) => (Some('^'), Some(Color::Magenta)),
        (Mode::SteppingDown, _) => (Some('v'), Some(Color::Magenta)),
        (Mode::Unknown, _) => (Some('?'), Some(Color::Red)),
        _ if !r.is_healthy() => (None, Some(Color::Yellow)),
        (Mode::Leader, _) => (None, Some(Color::Green)),
        _ => (None, None),
    }
}

fn sequencer_cell(view: &ClusterView, p: &PartitionView) -> Cell {
    let Some(sequencer) = p.sequencer else {
        return cell(dim("-"));
    };
    let log = p
        .log_id
        .and_then(|id| view.logs.iter().find(|l| l.id == id));
    match log.and_then(|l| l.sequencer_state) {
        Some(SequencerState::Dead | SequencerState::Restarted) => {
            cell_fg(sequencer, CellColor::Red)
        }
        _ if p
            .leader
            .is_some_and(|l| l.as_plain() != sequencer.as_plain()) =>
        {
            cell_fg(sequencer, CellColor::Yellow)
        }
        _ => cell(sequencer),
    }
}

/// A row per partition processor, and a row per replica set member that runs none.
fn replicas_table(
    partitions: &[&PartitionView],
    sort: &SortMode,
    extra: bool,
    now: DateTime<Utc>,
) -> Table {
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
    let mut table = Table::new_styled();
    table.set_styled_header(header);

    let mut rows: Vec<(&PartitionView, Option<&ReplicaView>, PlainNodeId)> = Vec::new();
    for p in partitions {
        for r in &p.replicas {
            rows.push((p, Some(r), r.node.as_plain()));
        }
        for missing in &p.missing {
            rows.push((p, None, *missing));
        }
    }
    rows.sort_by(|a, b| match sort {
        SortMode::Node => a.2.cmp(&b.2).then(a.0.id.cmp(&b.0.id)),
        SortMode::Active => {
            let leads = |r: Option<&ReplicaView>| r.is_some_and(|r| r.mode == Mode::Leader);
            leads(b.1)
                .cmp(&leads(a.1))
                .then(a.0.id.cmp(&b.0.id))
                .then(a.2.cmp(&b.2))
        }
        // keep the partitions' order, list each partition's leader first
        _ => partitions
            .iter()
            .position(|p| p.id == a.0.id)
            .cmp(&partitions.iter().position(|p| p.id == b.0.id))
            .then_with(|| {
                let leads = |r: Option<&ReplicaView>, p: &PartitionView| {
                    r.is_some_and(|r| Some(r.node) == p.leader)
                };
                leads(b.1, b.0).cmp(&leads(a.1, a.0))
            })
            .then(a.2.cmp(&b.2)),
    });

    let mut previous = None;
    for (p, replica, node) in rows {
        let first_of_partition = previous != Some(p.id);
        previous = Some(p.id);
        let (id, health) = if first_of_partition {
            (
                cell(p.id).add_attribute(Attribute::Bold),
                cell(health_glyph(p.health)),
            )
        } else {
            (cell(""), cell(""))
        };

        let Some(r) = replica else {
            let mut row = vec![
                id,
                health,
                cell_fg(node, CellColor::Red),
                cell_fg("missing", CellColor::Red),
                cell(dim("-")),
                cell(membership_label(p.is_member(node), p.is_next_member(node))),
            ];
            row.extend((0..7).map(|_| cell(dim("-"))));
            table.add_row(row);
            continue;
        };

        let (_, color) = replica_marker(r);
        let mode = match color {
            Some(color) => paint(mode_label(r.mode), color),
            None => mode_label(r.mode).to_owned(),
        };
        let replay = match r.replay {
            _ if r.broken_reason.is_some() => paint(
                bold(format!("broken ({})", r.broken_reason.unwrap_or_default())),
                Color::Red,
            ),
            Replay::Active => "active".to_owned(),
            Replay::CatchingUp => paint(
                format!("catching up to {}", opt(r.target_tail_lsn)),
                Color::Yellow,
            ),
            Replay::Starting => paint("starting", Color::Yellow),
            Replay::Unknown => paint("unknown", Color::Red),
        };
        let member = if p.membership.is_some() {
            let label = membership_label(r.member, r.next_member);
            if r.member || r.next_member {
                label
            } else {
                dim(label)
            }
        } else {
            dim("?")
        };
        let epoch = match r.epoch {
            Some(e) if r.stale_epoch => paint(format!("e{e}"), Color::Red),
            Some(e) => format!("e{e}"),
            None => "-".to_owned(),
        };
        let updated = match r.updated_at {
            Some(t) if (now - t).num_seconds() > 30 => paint(ago(Some(t), now), Color::Yellow),
            t => ago(t, now),
        };

        let mut row = vec![
            id,
            health,
            cell(r.node),
            cell(mode),
            cell(replay),
            cell(member),
            cell(epoch),
            cell(opt(r.applied_lsn)),
            cell(opt(r.lag)),
            read_cell(r),
            cell(opt(r.durable_lsn)),
            cell(opt(r.archived_lsn)),
            cell(updated),
        ];
        if extra {
            row.extend([
                cell(opt(r.schema_version.map(|v| format!("v{v}")))),
                cell(opt(r.rule_book_version.map(|v| format!("v{v}")))),
                cell(ago(r.last_record_applied_at, now)),
                cell(list_or_dash(&r.features)),
                cell(list_or_dash(&r.storage_features)),
            ]);
        }
        table.add_row(row);
    }
    table
}

fn membership_label(member: bool, next_member: bool) -> String {
    match (member, next_member) {
        (true, true) => "current,next",
        (true, false) => "current",
        (false, true) => "next",
        (false, false) => "none",
    }
    .to_owned()
}

fn list_or_dash(items: &[String]) -> String {
    if items.is_empty() {
        "-".to_owned()
    } else {
        items.join(",")
    }
}

/// Where the replica's read stream is: at the tail, behind it, or stuck.
fn read_cell(r: &ReplicaView) -> Cell {
    let Some(reader) = &r.reader else {
        return cell(dim("-"));
    };
    match reader.phase() {
        ReaderPhase::Reading => match reader.behind {
            Some(0) => cell(dim("at tail")),
            Some(behind) => cell(behind),
            None => cell(dim("reading")),
        },
        ReaderPhase::Switching => cell_fg("switching", CellColor::Yellow),
        ReaderPhase::AwaitingSeal => cell_fg("awaiting seal", CellColor::Red),
        ReaderPhase::Sealing => cell_fg("sealing chain", CellColor::Red),
    }
}
