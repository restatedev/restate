// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cling::prelude::*;
use itertools::{Itertools, Position};
use log::render_loglet_params;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_core::protobuf::cluster_ctrl_svc::{DescribeLogRequest, new_cluster_ctrl_client};
use restate_types::Versioned;
use restate_types::logs::LogId;
use restate_types::logs::metadata::{InternalKind, Logs, SealMetadata, Segment, SegmentIndex};
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};

use crate::commands::log;
use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "describe_logs")]
pub struct DescribeLogIdOpts {
    /// The log id or range to describe, e.g. "0", "1-4"; all logs are shown by default
    #[arg()]
    log_id: Vec<RangeParam>,

    /// The first segment id to display
    #[arg(long)]
    from_segment_id: Option<u32>,

    /// Skip over the first N segments
    #[arg(long, conflicts_with_all = ["from_segment_id"])]
    skip: Option<usize>,

    /// Print the last N log segments
    #[arg(
        long,
        default_value = "true",
        conflicts_with = "head",
        default_value = "25"
    )]
    tail: Option<usize>,

    /// Print the first N log segments
    #[arg(long)]
    head: Option<usize>,

    /// Display all available segments, ignoring max results
    #[arg(long, conflicts_with_all = ["head", "tail"])]
    all: bool,

    /// Display additional information such as replicated loglet config
    #[arg(long)]
    extra: bool,
}

async fn describe_logs(
    connection: &ConnectionInfo,
    opts: &DescribeLogIdOpts,
) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;

    let logs = connection.get_logs().await?;

    let log_ids = if opts.log_id.is_empty() {
        logs.iter()
            .sorted_by(|a, b| Ord::cmp(a.0, b.0))
            .map(|(id, _)| RangeParam::from(*id))
            .collect::<Vec<_>>()
    } else {
        opts.log_id.clone()
    };

    for range in log_ids {
        for log_id in range.iter() {
            describe_log(opts, &nodes_config, &logs, log_id.into(), connection).await?;
        }
    }

    Ok(())
}

async fn describe_log(
    opts: &DescribeLogIdOpts,
    nodes_configuration: &NodesConfiguration,
    logs: &Logs,
    log_id: LogId,
    connection: &ConnectionInfo,
) -> anyhow::Result<()> {
    let chain = logs
        .chain(&log_id)
        .ok_or_else(|| anyhow::anyhow!("Failed to get log chain"))?;

    c_println!("Log Id: {} ({})", log_id, logs.version());

    if opts.extra {
        let describe_log_request = DescribeLogRequest {
            log_id: log_id.into(),
        };

        let describe_log_response = connection
            .try_each(Some(Role::Admin), |channel| async {
                new_cluster_ctrl_client(channel)
                    .describe_log(describe_log_request)
                    .await
            })
            .await
            .with_context(|| "failed to send describe log request")?
            .into_inner();

        c_println!("Trim Point LSN: {}", describe_log_response.trim_point)
    };

    let mut chain_table = Table::new_styled();
    let mut header_row = vec![
        "", // tail segment marker
        "IDX",
        "FROM-LSN",
        "KIND",
        "LOGLET-ID",
        "REPLICATION",
        "SEQUENCER",
        "EFF-NODESET",
    ];
    if opts.extra {
        header_row.push("PARAMS");
    }
    // for notes
    header_row.push("");
    chain_table.set_styled_header(header_row);

    let mut last_segment_rendered = None;

    let segments: Box<dyn Iterator<Item = Segment>> = match (opts.skip, opts.from_segment_id) {
        (Some(n), _) => Box::new(chain.iter().skip(n)),
        (_, Some(from_segment_id)) => {
            let starting_segment_id = SegmentIndex::from(from_segment_id);
            Box::new(
                chain
                    .iter()
                    .skip_while(move |s| s.index() < starting_segment_id),
            )
        }
        _ => Box::new(chain.iter()),
    };

    let segments: Box<dyn Iterator<Item = Segment>> = if opts.all {
        segments
    } else if opts.head.is_some() {
        Box::new(segments.take(opts.head.unwrap()))
    } else {
        Box::new(segments.tail(opts.tail.unwrap()))
    };

    for (position, segment) in segments.with_position() {
        // For the purpose of this display, "is-tail" boils down to simply "is this the last known segment?"
        let is_tail_segment = [Position::Last, Position::Only].contains(&position)
            && segment.index() == chain.tail_index();

        match segment.config.kind {
            InternalKind::Replicated => {
                let params = log::deserialize_replicated_log_params(&segment);
                let mut segment_row = vec![
                    render_tail_segment_marker(is_tail_segment),
                    Cell::new(format!("{}", segment.index())),
                    Cell::new(format!("{}", segment.base_lsn)),
                    Cell::new(format!("{}", segment.config.kind)),
                    render_loglet_params(&params, |p| Cell::new(p.loglet_id)),
                    render_loglet_params(&params, |p| Cell::new(format!("{:#}", p.replication))),
                    render_loglet_params(&params, |p| {
                        render_sequencer(is_tail_segment, p, nodes_configuration)
                    }),
                    render_loglet_params(&params, |p| {
                        render_effective_nodeset(is_tail_segment, p, nodes_configuration)
                    }),
                ];
                if opts.extra {
                    segment_row.push(Cell::new(
                        params
                            .as_ref()
                            .and_then(|p| serde_json::to_string(&p).ok())
                            .unwrap_or_default(),
                    ))
                }
                chain_table.add_row(segment_row);
            }
            _ => {
                let mut row = vec![
                    render_tail_segment_marker(is_tail_segment),
                    Cell::new(format!("{}", segment.index())),
                    Cell::new(format!("{}", segment.base_lsn)),
                    Cell::new(format!("{}", segment.config.kind)),
                    Cell::new(""),
                    Cell::new(""),
                    Cell::new(""),
                    Cell::new(""),
                ];
                if opts.extra {
                    row.push(Cell::new(segment.config.params.to_string()))
                }
                row.push(render_loglet_notes(&segment));
                chain_table.add_row(row);
            }
        }

        last_segment_rendered = Some(segment.index());
    }

    let column = chain_table.column_mut(0).unwrap();
    column.set_padding((0, 0));

    if last_segment_rendered.is_none() {
        c_println!("No segments to display.");
        return Ok(());
    }

    c_println!("{}", chain_table);
    c_println!("---");
    c_println!(
        "{}/{} segments shown.",
        chain_table.row_count(),
        chain.num_segments()
    );
    c_println!();

    Ok(())
}

fn render_tail_segment_marker(is_tail: bool) -> Cell {
    if is_tail {
        Cell::new("▶︎").fg(Color::Green)
    } else {
        Cell::new("")
    }
}

fn render_effective_nodeset(
    is_tail: bool,
    params: &ReplicatedLogletParams,
    nodes_configuration: &NodesConfiguration,
) -> Cell {
    let effective_node_set = params.nodeset.to_effective(nodes_configuration);
    let mut cell = Cell::new(format!("{effective_node_set:#}"));
    if is_tail && effective_node_set.len() < params.replication.num_copies() as usize {
        cell = cell.fg(Color::Red);
    }
    cell
}

fn render_sequencer(
    is_tail: bool,
    params: &ReplicatedLogletParams,
    nodes_configuration: &NodesConfiguration,
) -> Cell {
    let cell = Cell::new(format!("{:#}", params.sequencer));
    if is_tail {
        let sequencer_generational =
            nodes_configuration.find_node_by_id(params.sequencer.as_plain());
        let color = if sequencer_generational
            .is_ok_and(|node| node.current_generation.generation() == params.sequencer.generation())
        {
            Color::Green
        } else {
            Color::Red
        };
        cell.fg(color)
    } else {
        cell
    }
}

pub fn render_loglet_notes(segment: &Segment<'_>) -> Cell {
    if segment.config.kind.is_seal_marker() {
        match SealMetadata::deserialize_from(segment.config.params.as_bytes()) {
            Ok(metadata) => {
                let mut cell = Cell::new(format!("At {}", metadata.sealed_at.into_timestamp()));
                if metadata.permanent_seal {
                    cell = cell.fg(Color::Green);
                }
                cell
            }
            Err(e) => Cell::new(format!("Cannot deserialize SealMetadata: {e}")).fg(Color::Red),
        }
    } else {
        Cell::new("")
    }
}
