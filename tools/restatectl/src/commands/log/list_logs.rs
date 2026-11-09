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

use crate::commands::status::{Source, provider_label};
use crate::connection::ConnectionInfo;
use crate::observe::{
    ClusterView, Finding, LogPlacement, LogProvider, LogView, SOURCE_UNAVAILABLE, SequencerState,
    Severity, Subject, analyze, imbalanced,
};
use crate::preview;
use crate::render::{ago, bold, cell, cell_fg, dim, findings_list, opt, paint, print_json};
use crate::tui::{self, Presentation};
use crate::util::RangeParam;

pub(crate) mod legacy;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "ls")]
#[cling(run = "list_logs")]
#[command(after_long_help = preview::enabled().then_some(LOGS_HELP))]
pub struct ListLogsOpts {
    /// Log ids or ranges to show, e.g. "0", "1-4"; all logs by default
    #[arg(help_heading = preview::HEADING)]
    logs: Vec<RangeParam>,

    /// Only show the logs that need attention
    #[arg(long, help_heading = preview::HEADING)]
    unhealthy: bool,

    #[clap(flatten)]
    source: Source,

    #[clap(flatten, next_help_heading = preview::HEADING)]
    watch: Watch,
}

const LOGS_HELP: &str = "\
Each row describes a log's tail segment, the one appends go to: ● writable, ◐ writable with \
reduced redundancy or waiting for a reconfiguration (sealed, sequencer restarted), ○ unable to \
take appends (sequencer dead, too few writable nodeset members). SEG is the tail segment's index, \
with the number of segments in the chain when there are more than one; TAIL is the next LSN to \
be written, TRIM the trim point and RETAINED the number of records between them. The sequencer \
is yellow when it runs away from the partition's leader and red when it's down; nodeset members \
that can't take writes are red. SINCE is the age of the tail segment, \
i.e. the time since the log's last reconfiguration.

With --json, the output is one document with the logs and the findings about them.";

pub async fn list_logs(connection: &ConnectionInfo, opts: &ListLogsOpts) -> anyhow::Result<()> {
    if !preview::enabled() {
        return legacy::list_logs(connection).await;
    }
    // the dashboard shows every log; a selection refreshes the listing instead
    if opts.watch.is_enabled() && opts.logs.is_empty() && tui::can_run_dashboard() {
        return tui::dashboard::run(
            opts.source.feed(connection)?,
            tui::dashboard::Options {
                tab: tui::dashboard::Tab::Logs,
                interval: opts.watch.interval(),
                unhealthy_only: opts.unhealthy,
                partition_sort: tui::dashboard::PartitionSort::Partition,
            },
        )
        .await;
    }
    if opts.source.is_file() {
        return show_logs(connection, opts).await;
    }
    opts.watch
        .run(|| async {
            connection.reset().await;
            show_logs(connection, opts).await
        })
        .await
}

async fn show_logs(connection: &ConnectionInfo, opts: &ListLogsOpts) -> anyhow::Result<()> {
    let view = opts.source.view(connection).await?;
    let findings = analyze(&view);
    let severity = |log: &LogView| {
        findings
            .iter()
            .filter(|f| f.subject == Subject::Log(log.id))
            .map(|f| f.severity)
            .max()
    };

    let selected: Vec<&LogView> = view
        .logs
        .iter()
        .filter(|l| {
            opts.logs.is_empty()
                || opts
                    .logs
                    .iter()
                    .any(|range| range.into_iter().contains(&l.id))
        })
        .filter(|l| !opts.unhealthy || severity(l).is_some_and(|s| s > Severity::Info))
        .collect();
    // and the sources that couldn't be read, which explain what's missing
    let findings: Vec<Finding> = findings
        .iter()
        .filter(|f| match f.subject {
            Subject::Log(id) => selected.iter().any(|l| l.id == id),
            _ => f.code == SOURCE_UNAVAILABLE,
        })
        .cloned()
        .collect();

    let placement = LogPlacement::of(&view, &selected);
    // the spread over the nodes reads as the cluster's only when nothing is left out
    let unfiltered = opts.logs.is_empty() && !opts.unhealthy;

    let presentation = Presentation::current();
    if presentation == Presentation::Json {
        #[derive(Serialize)]
        struct Report<'a> {
            captured_at: DateTime<Utc>,
            logs_version: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            provider: Option<&'a LogProvider>,
            logs: Vec<&'a LogView>,
            /// how the listed logs' sequencers and nodesets spread over the nodes
            placement: LogPlacement,
            findings: Vec<Finding>,
        }
        return print_json(&Report {
            captured_at: view.captured_at,
            logs_version: view.versions.logs,
            provider: view.log_provider.as_ref(),
            logs: selected,
            placement,
            findings,
        });
    }

    if presentation == Presentation::Rich {
        if selected.is_empty() {
            c_println!(" No logs match.");
        } else {
            tui::views::logs(
                &view,
                &selected,
                &findings,
                unfiltered.then_some(&placement),
                tui::width(),
            )
            .print();
        }
        return Ok(());
    }

    let mut summary = format!("{} logs", selected.len());
    if let Some(provider) = &view.log_provider {
        summary.push_str(&format!(" · {}", provider_label(provider)));
    }
    summary.push_str(&format!(" · logs v{}", opt(view.versions.logs)));
    c_println!(" {}", dim(summary));

    if selected.is_empty() {
        c_println!(" No logs match.");
        return Ok(());
    }
    c_println!("{}", logs_table(&view, selected));

    if unfiltered {
        c_println!();
        c_println!(
            " {} {}",
            bold("PLACEMENT"),
            dim(format!(
                "even split: {:.1} sequencers per active worker, {:.1} nodesets per writable log-server",
                placement.even_sequencers, placement.even_nodesets
            ))
        );
        let mut table = Table::new_styled();
        table.set_styled_header(vec!["NODE", "SEQUENCERS", "NODESETS"]);
        for share in &placement.nodes {
            let count = |count: u32, eligible: bool, even: f64| {
                let notable = if eligible {
                    imbalanced(count, even)
                } else {
                    count > 0
                };
                if notable {
                    cell_fg(count, CellColor::Yellow)
                } else if eligible {
                    cell(count)
                } else {
                    cell(dim(count))
                }
            };
            table.add_row(vec![
                cell(share.node),
                count(
                    share.sequencers,
                    share.sequencer_eligible,
                    placement.even_sequencers,
                ),
                count(
                    share.nodesets,
                    share.nodeset_eligible,
                    placement.even_nodesets,
                ),
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

pub fn logs_table<'a>(view: &ClusterView, logs: impl IntoIterator<Item = &'a LogView>) -> Table {
    let logs: Vec<&LogView> = logs.into_iter().collect();
    let default_replication = view
        .log_provider
        .as_ref()
        .and_then(|p| p.replication.as_deref());
    let show_replication = logs
        .iter()
        .any(|l| l.replication.as_deref() != default_replication);
    // older servers don't record when segments were created
    let show_since = logs.iter().any(|l| l.created_at.is_some());

    let mut header = vec![
        "LOG",
        "",
        "SEG",
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
    let mut table = Table::new_styled();
    table.set_styled_header(header);

    for log in logs {
        let segments = if log.segments > 1 {
            format!(
                "{} {}",
                log.tail_segment,
                dim(format!("({} segs)", log.segments))
            )
        } else {
            log.tail_segment.to_string()
        };
        let mut row = vec![
            cell(log.id).add_attribute(Attribute::Bold),
            cell(log_glyph(log)),
            cell(segments),
            cell(log.base_lsn),
            cell(opt(log.tail_lsn)),
            cell(opt(log.trim_point)),
            cell(opt(log.retained)),
            sequencer_cell(log),
            cell(nodeset(view, log)),
        ];
        if show_since {
            row.push(cell(ago(log.created_at, view.captured_at)));
        }
        if show_replication {
            row.push(cell(opt(log.replication.as_deref())));
        }
        table.add_row(row);
    }
    table
}

fn log_glyph(log: &LogView) -> String {
    let down = log.is_down();
    let degraded = log.sealed
        || matches!(log.sequencer_state, Some(SequencerState::Restarted))
        || log.is_nodeset_degraded();
    if down {
        paint(bold("○"), Color::Red)
    } else if degraded {
        paint("◐", Color::Yellow)
    } else {
        paint("●", Color::Green)
    }
}

fn sequencer_cell(log: &LogView) -> Cell {
    let Some(sequencer) = log.sequencer else {
        return cell(dim("-"));
    };
    match log.sequencer_state {
        Some(SequencerState::Dead | SequencerState::Restarted) => {
            cell_fg(sequencer, CellColor::Red)
        }
        _ if log.sequencer_with_leader() == Some(false) => cell_fg(sequencer, CellColor::Yellow),
        _ => cell(sequencer),
    }
}

fn nodeset(view: &ClusterView, log: &LogView) -> String {
    use crate::observe::Liveness;
    log.nodeset
        .iter()
        .map(|id| {
            let writable = view.node(*id).is_some_and(|n| {
                n.liveness == Liveness::Alive
                    && n.storage_state
                        .as_deref()
                        .is_some_and(|s| matches!(s, "read-write" | "read-only"))
            });
            if writable {
                id.to_string()
            } else {
                paint(id, Color::Red)
            }
        })
        .join(" ")
}
