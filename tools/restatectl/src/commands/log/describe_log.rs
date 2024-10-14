// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use itertools::Itertools;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::DescribeLogRequest;
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_types::logs::metadata::{Chain, ProviderKind, Segment, SegmentIndex};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::storage::StorageCodec;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "describe_log")]
pub struct DescribeLogIdOpts {
    /// The log id to describe
    #[arg(short, long)]
    log_id: u32,

    /// The first segment id to display
    #[arg(long)]
    from_segment_id: Option<u32>,

    /// Skip over the first N segments
    #[arg(long, conflicts_with_all = ["from_segment_id"])]
    skip: Option<usize>,

    /// Print the last N segments
    #[arg(long, default_value = "true", conflicts_with = "head")]
    tail: bool,

    /// Print the first N segments
    #[arg(long)]
    head: bool,

    /// Display at most N segments
    #[arg(long, default_value = "25")]
    max_results: usize,

    /// Display all available segments, ignoring max results
    #[arg(long, conflicts_with_all = ["head", "tail", "max_results"])]
    display_all: bool,
}

async fn describe_log(connection: &ConnectionInfo, opts: &DescribeLogIdOpts) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let req = DescribeLogRequest {
        log_id: opts.log_id,
    };
    let mut response = client.describe_log(req).await?.into_inner();

    let mut buf = response.chain.clone();
    let chain = StorageCodec::decode::<Chain, _>(&mut buf)?;

    let mut header = Table::new_styled();
    header.add_row(vec!["Log id", &format!("{}", response.log_id)]);
    header.add_row(vec![
        "Metadata version",
        &format!("v{}", response.logs_version),
    ]);
    header.add_row(vec!["Trim point", &format!("{}", response.trim_point)]);
    c_println!("{}", header);
    c_println!();

    let mut chain_table = Table::new_styled();
    chain_table.set_styled_header(vec![
        "", // tail segment marker
        "IDX",
        "KIND",
        "LOGLET-ID",
        "FROM-LSN",
        "REPLICATION",
        "SEQUENCER",
        "EFF-NODESET",
    ]);

    let last_segment = chain
        .iter()
        .last()
        .map(|s| s.index())
        .unwrap_or(SegmentIndex::from(u32::MAX));

    let mut first_segment_rendered = None;
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

    let segments: Box<dyn Iterator<Item = Segment>> = if opts.display_all {
        Box::new(segments)
    } else if opts.head {
        Box::new(segments.take(opts.max_results))
    } else {
        Box::new(segments.tail(opts.max_results))
    };

    for segment in segments {
        if first_segment_rendered.is_none() {
            first_segment_rendered = Some(segment.index());
        }

        // For the purpose of this display, "is-tail" boils down to simply "is this the last known segment?"
        let is_tail_segment = segment.index() == last_segment;

        match segment.config.kind {
            ProviderKind::Replicated => {
                let nodes_configuration = StorageCodec::decode::<NodesConfiguration, _>(
                    &mut response.nodes_configuration,
                )?;
                let replicated_log_params = get_replicated_log_params(&segment);
                match replicated_log_params {
                    Some(params) => {
                        chain_table.add_row(vec![
                            render_tail_segment_marker(is_tail_segment),
                            Cell::new(format!("{}", segment.index())),
                            Cell::new(format!("{:?}", segment.config.kind)),
                            Cell::new(format!("{}", params.loglet_id)),
                            Cell::new(format!("{}", segment.base_lsn)),
                            Cell::new(format!("{:#}", params.replication)),
                            render_sequencer(is_tail_segment, &params, &nodes_configuration),
                            render_effective_nodeset(
                                is_tail_segment,
                                &params,
                                &nodes_configuration,
                            ),
                        ]);
                    }
                    None => {
                        chain_table.add_row(vec![
                            render_tail_segment_marker(is_tail_segment),
                            Cell::new(format!("{}", segment.index())),
                            Cell::new(format!("{:?}", segment.config.kind)),
                            Cell::new("N/A"),
                            Cell::new(format!("{}", segment.base_lsn)),
                            Cell::new("N/A"),
                            Cell::new("N/A"),
                            Cell::new("N/A"),
                        ]);
                    }
                }
            }
            _ => {
                chain_table.add_row(vec![
                    render_tail_segment_marker(is_tail_segment),
                    Cell::new(format!("{}", segment.index())),
                    Cell::new(format!("{:?}", segment.config.kind)),
                    Cell::new(""),
                    Cell::new(format!("{}", segment.base_lsn)),
                    Cell::new(""),
                    Cell::new(""),
                    Cell::new(""),
                ]);
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

    c_title!(
        "🔗",
        format!(
            "LOG SEGMENTS [{}..{}]",
            first_segment_rendered.unwrap(),
            last_segment_rendered.unwrap()
        )
    );
    c_println!("{}", chain_table);

    Ok(())
}

fn render_tail_segment_marker(is_tail: bool) -> Cell {
    if is_tail {
        Cell::new("▶︎").fg(Color::Green)
    } else {
        Cell::new("")
    }
}

fn get_replicated_log_params(segment: &Segment) -> Option<ReplicatedLogletParams> {
    match segment.config.kind {
        ProviderKind::Replicated => Some(
            ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes()).unwrap(),
        ),
        _ => None,
    }
}

fn render_effective_nodeset(
    is_tail: bool,
    params: &ReplicatedLogletParams,
    nodes_configuration: &NodesConfiguration,
) -> Cell {
    let effective_node_set = params.nodeset.to_effective(nodes_configuration);

    let color = match (is_tail, effective_node_set.len()) {
        (false, _) => Color::DarkGrey,
        (_, n) => {
            // todo: update crude replication health color-coding to use majority check
            if n >= params.replication.num_copies() as usize {
                Color::Green
            } else if n == 0 {
                Color::Red
            } else {
                Color::DarkYellow
            }
        }
    };

    Cell::new(format!("{:#}", effective_node_set)).fg(color)
}

fn render_sequencer(
    is_tail: bool,
    params: &ReplicatedLogletParams,
    nodes_configuration: &NodesConfiguration,
) -> Cell {
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
        Cell::new(format!("{:#}", params.sequencer)).fg(color)
    } else {
        Cell::new("")
    }
}
