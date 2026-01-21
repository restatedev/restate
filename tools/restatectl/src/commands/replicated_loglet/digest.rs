// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use cling::prelude::*;
use tracing::{info, warn};

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_log_server_grpc::{GetDigestRequest, GetLogletInfoRequest, new_log_server_client};
use restate_types::PlainNodeId;
use restate_types::logs::TailOffsetWatch;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::RecordStatus;
use restate_types::nodes_config::Role;
use restate_types::replicated_loglet::LogNodeSetExt;
use restate_types::replication::NodeSetChecker;

use crate::commands::replicated_loglet::digest_util::DigestsHelper;
use crate::commands::replicated_loglet::info::gen_loglet_info_table;
use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "get_digest")]
pub struct DigestOpts {
    /// The replicated loglet id
    loglet_id: LogletId,

    /// From offset (inclusive)
    #[arg(long)]
    from: Option<u32>,
    /// to offset (inclusive)
    #[arg(long)]
    to: Option<u32>,
    /// Only print under-replicated offsets
    #[arg(long, short)]
    under_replicated_only: bool,
}

async fn get_digest(connection: &ConnectionInfo, opts: &DigestOpts) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;

    let Some(loglet_ref) = logs.get_replicated_loglet(&opts.loglet_id) else {
        return Err(anyhow::anyhow!("loglet {} not found", opts.loglet_id));
    };
    let nodes_config = connection.get_nodes_configuration().await?;

    let mut nodeset = loglet_ref.params.nodeset.to_effective(&nodes_config);
    nodeset.sort();
    let known_global_tail = TailOffsetWatch::new(TailState::new(false, LogletOffset::INVALID));
    let nodeset_channels: HashMap<PlainNodeId, _> = nodeset
        .iter()
        .copied()
        .filter_map(|node_id| {
            let node = nodes_config.find_node_by_id(node_id).unwrap_or_else(|_| {
                panic!("Node {node_id} doesn't seem to exist in nodes configuration");
            });
            info!(
                "Requesting digest from node {} at {}",
                node_id, node.address
            );
            if !node.has_role(Role::LogServer) {
                warn!(
                    "Node {} is not running the log-server role, will not connect to it",
                    node_id
                );
                return None;
            }
            Some((node_id, grpc_channel(node.address.clone())))
        })
        .collect();

    // get loglet info
    let mut loglet_infos: HashMap<PlainNodeId, _> = HashMap::default();
    for (node_id, channel) in nodeset_channels.iter() {
        let mut client = new_log_server_client(channel.clone(), &CliContext::get().network);
        let Ok(Some(loglet_info)) = client
            .get_loglet_info(GetLogletInfoRequest {
                loglet_id: opts.loglet_id.into(),
            })
            .await
            .map(|resp| resp.into_inner().info)
        else {
            continue;
        };
        loglet_infos.insert(*node_id, loglet_info);
    }
    // we want to request data that's within trim points -> max-tail to avoid blowing up Digest's
    // internal btree
    let min_trim_point = loglet_infos
        .values()
        .map(|info| info.trim_point)
        .min()
        .unwrap_or(0);
    // clamp from_offset at next point after the smallest trim point
    let from_offset = (min_trim_point + 1).max(opts.from.unwrap_or(1));
    let Some(max_local_tail) = loglet_infos
        .values()
        .map(|info| info.header.unwrap().local_tail)
        .max()
    else {
        return Err(anyhow::anyhow!(
            "Couldn't determine local-tail of any node in the nodeset"
        ));
    };
    // clamp to-offset to max-local-tail - 1;
    let to_offset = max_local_tail
        .saturating_sub(1)
        .min(opts.to.unwrap_or(u32::MAX - 1));

    // digests
    let mut digests = DigestsHelper::new(
        &loglet_ref.params,
        from_offset.into(),
        // target tail is one offset after the inclusive to_offset arg.
        (to_offset + 1).into(),
    );
    for (node_id, channel) in nodeset_channels.iter() {
        let req = GetDigestRequest {
            loglet_id: opts.loglet_id.into(),
            from_offset,
            to_offset,
        };
        let mut client = new_log_server_client(channel.clone(), &CliContext::get().network);
        let digest = match client.get_digest(req).await {
            Ok(response) => response.into_inner().digest.expect("always set by servers"),
            Err(err) => {
                warn!("Couldn't get digest from {}: {}", node_id, err);
                continue;
            }
        };
        digests.add_digest_message(*node_id, digest.into(), &known_global_tail);
    }

    let mut records_table = Table::new_styled();
    let node_ids = nodeset.iter().map(|n| {
        match digests.is_sealed(n) {
            Some(true) => Cell::new(format!("{n}(S)")).fg(Color::Magenta),
            Some(false) => Cell::new(n.to_string()),
            // we didn't hear from this node
            None => Cell::new(format!("{n}(E)")).fg(Color::Red),
        }
    });
    let mut heading = vec![Cell::new("OFFSET")];
    heading.extend(node_ids.into_iter());
    heading.extend(vec![Cell::new("ISSUES")]);
    records_table.set_header(heading);

    let mut checker = NodeSetChecker::new(&nodeset, &nodes_config, &loglet_ref.params.replication);
    for (offset, responses) in digests.iter() {
        checker.fill_with_default();
        if *offset >= digests.max_local_tail() {
            break;
        }
        if *offset == known_global_tail.latest_offset() {
            // divider to indicate that everything after global tail
            records_table.add_row(std::iter::repeat_n("────", nodeset.len() + 2));
        }
        let mut status_row = Vec::with_capacity(nodeset.len() + 2);
        status_row.push(Cell::new(offset.to_string()));
        for node in nodeset.iter() {
            if let Some(status) = responses.get(node) {
                status_row.push(Cell::new(status.to_string()));
                if let RecordStatus::Exists = status {
                    checker.set_attribute(*node, true);
                }
            } else if digests.is_known(node) {
                // record doesn't exist on this node
                status_row.push(Cell::new("-"));
            } else {
                // we don't know.
                status_row.push(Cell::new("?").fg(Color::Red));
            }
        }
        if !checker.check_write_quorum(|t| *t) {
            // record is under-replicated
            status_row.push(Cell::new("U").fg(Color::Red));
            records_table.add_row(status_row);
        } else if !opts.under_replicated_only {
            records_table.add_row(status_row);
        }
    }
    // empty separator
    records_table.add_row(vec![""]);
    records_table.add_row(std::iter::repeat_n("═════════", nodeset.len() + 1));
    // append the node-level info at the end
    {
        let mut row = Vec::with_capacity(nodeset.len() + 2);
        row.push(Cell::new("LOCAL TAIL"));
        for node in nodeset.iter() {
            if let Some(header) = digests.get_response_header(node) {
                let color = if header.sealed {
                    Color::Magenta
                } else {
                    Color::Reset
                };
                row.push(Cell::new(header.local_tail.to_string()).fg(color));
            } else {
                row.push(Cell::new("?").fg(Color::Red));
            }
        }
        records_table.add_row(row);
    }
    {
        let mut row = Vec::with_capacity(nodeset.len() + 2);
        row.push(Cell::new("GLOBAL TAIL"));
        for node in nodeset.iter() {
            if let Some(header) = digests.get_response_header(node) {
                row.push(Cell::new(header.known_global_tail.to_string()));
            } else {
                row.push(Cell::new("?").fg(Color::Red));
            }
        }
        records_table.add_row(row);
    }
    {
        let mut row = Vec::with_capacity(nodeset.len() + 2);
        row.push(Cell::new("TRIM POINT"));
        for node in nodeset.iter() {
            if let Some(header) = loglet_infos.get(node) {
                row.push(Cell::new(header.trim_point.to_string()));
            } else {
                row.push(Cell::new("?").fg(Color::Red));
            }
        }
        records_table.add_row(row);
    }

    c_println!("{}", records_table);
    c_println!();

    let mut info_table = gen_loglet_info_table(&logs, loglet_ref, &nodes_config);
    info_table.add_kv_row(
        "Observed Global Tail:",
        known_global_tail.latest_offset().to_string(),
    );
    c_println!("{}", info_table);
    Ok(())
}
