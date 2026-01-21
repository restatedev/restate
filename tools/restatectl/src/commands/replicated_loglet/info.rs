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
use tracing::warn;

use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_println};
use restate_log_server_grpc::{GetLogletInfoRequest, new_log_server_client};
use restate_types::PlainNodeId;
use restate_types::logs::LogletId;
use restate_types::logs::metadata::{LogletRef, Logs};
use restate_types::nodes_config::{NodesConfigError, NodesConfiguration, Role};
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletParams};

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "get_info")]
pub struct InfoOpts {
    /// The replicated loglet id
    loglet_id: LogletId,
}

pub(super) fn gen_loglet_info_table(
    logs: &Logs,
    loglet: &LogletRef<ReplicatedLogletParams>,
    nodes_config: &NodesConfiguration,
) -> Table {
    let references = loglet.references.iter().map(|(log_id, segment)| {
        let is_sealed_segment = logs
            .chain(log_id)
            .map(|chain| chain.tail_index() > *segment)
            .unwrap_or(false);

        let note = if is_sealed_segment {
            Styled(Style::Success, "(SEGMENT IS SEALED)")
        } else {
            Styled(Style::Normal, "")
        };
        format!("LogId={log_id} at Segment={segment} {note}")
    });
    let sequencer = loglet.params.sequencer;
    let sequencer_note_style = Style::Warn;
    let sequencer_note = match nodes_config.find_node_by_id(sequencer.as_plain()) {
        Ok(config) if config.current_generation.is_newer_than(sequencer) => Styled(
            sequencer_note_style,
            format!(
                "(gone; current generation is {})",
                config.current_generation
            ),
        ),
        Ok(config) if config.current_generation.is_same_but_different(&sequencer) => Styled(
            sequencer_note_style,
            format!(
                "(newer than nodes config, config has {})",
                config.current_generation
            ),
        ),
        // node is the same generation as config
        Ok(_) => Styled(Style::Normal, String::default()),
        Err(err @ NodesConfigError::Deleted(_)) => {
            Styled(sequencer_note_style, format!("(gone; {err})"))
        }
        Err(err) => Styled(sequencer_note_style, format!("({err})")),
    };

    let mut table = Table::new_styled();
    table.add_kv_row(
        "Loglet Id:",
        format!(
            "{} (raw={})",
            loglet.params.loglet_id, *loglet.params.loglet_id
        ),
    );
    table.add_kv_row("Used in:", itertools::join(references, "\n"));
    table.add_kv_row(
        "Sequencer:",
        Cell::new(format!("{sequencer} {sequencer_note}")),
    );
    table.add_kv_row("Replication:", loglet.params.replication.clone());
    table.add_kv_row("Nodeset:", format!("{:#}", loglet.params.nodeset));
    table
}

async fn get_info(connection: &ConnectionInfo, opts: &InfoOpts) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;
    let Some(loglet) = logs.get_replicated_loglet(&opts.loglet_id) else {
        return Err(anyhow::anyhow!("loglet {} not found", opts.loglet_id));
    };

    let nodes_config = connection.get_nodes_configuration().await?;
    let mut nodeset = loglet.params.nodeset.clone();
    // display nodes sorted
    nodeset.sort();

    c_println!("{}", gen_loglet_info_table(&logs, loglet, &nodes_config));
    c_println!();

    let mut loglet_infos: HashMap<PlainNodeId, _> = HashMap::default();
    let effective_node_set = EffectiveNodeSet::new(nodeset.clone(), &nodes_config);

    for node_id in effective_node_set {
        let node = nodes_config.find_node_by_id(node_id).unwrap_or_else(|_| {
            panic!("Node {node_id} doesn't seem to exist in nodes configuration");
        });

        if !node.has_role(Role::LogServer) {
            warn!(
                "Node {} is not running the log-server role, will not connect to it",
                node_id
            );
            continue;
        }
        let mut client = new_log_server_client(
            grpc_channel(node.address.clone()),
            &CliContext::get().network,
        );
        let Ok(Some(loglet_info)) = client
            .get_loglet_info(GetLogletInfoRequest {
                loglet_id: opts.loglet_id.into(),
            })
            .await
            .map(|resp| resp.into_inner().info)
        else {
            continue;
        };
        loglet_infos.insert(node_id, loglet_info);
    }

    let mut info_table = Table::new_styled();
    info_table.set_styled_header(vec![
        "",
        "LOCAL-TAIL",
        "GLOBAL-TAIL",
        "TRIM-POINT",
        "SEALED",
    ]);

    for node_id in nodeset.iter() {
        let mut row = Vec::with_capacity(5);
        row.push(Cell::new(node_id.to_string()).add_attribute(Attribute::Bold));
        if let Some(info) = loglet_infos.get(node_id) {
            let header = info.header.unwrap();
            row.push(Cell::new(header.local_tail.to_string()));
            row.push(Cell::new(header.known_global_tail.to_string()));
            row.push(Cell::new(info.trim_point.to_string()));
            row.push(if header.sealed {
                Cell::new("YES").fg(Color::Magenta)
            } else {
                Cell::new("NO")
            });
        } else {
            row.push(Cell::new("?").fg(Color::Red));
            row.push(Cell::new("?").fg(Color::Red));
            row.push(Cell::new("?").fg(Color::Red));
            row.push(Cell::new("?").fg(Color::Red));
        }
        info_table.add_row(row);
    }

    c_println!("{}", info_table);

    Ok(())
}
