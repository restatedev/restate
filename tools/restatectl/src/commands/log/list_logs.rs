// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::bail;
use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::output::Console;
use restate_types::Versioned;
use restate_types::logs::LogId;
use restate_types::logs::metadata::Chain;

use crate::commands::log::{deserialize_replicated_log_params, render_loglet_params};
use crate::connection::ConnectionInfo;
use crate::util::write_default_provider;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "ls")]
#[cling(run = "list_logs")]
pub struct ListLogsOpts {}

pub async fn list_logs(connection: &ConnectionInfo, _opts: &ListLogsOpts) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;

    let mut logs_table = Table::new_styled();

    c_println!("Logs {}", logs.version());

    write_default_provider(
        &mut Console::stdout(),
        0,
        &logs.configuration().default_provider,
    )?;

    // sort by log-id for display
    let log_chains: BTreeMap<LogId, &Chain> = logs.iter().map(|(id, chain)| (*id, chain)).collect();

    if log_chains.is_empty() {
        c_println!(
            "No logs were found. Check if the cluster has been provisioned and partition processors have started on a worker node. `restatectl provision`."
        );
        c_println!();
        c_println!("Use `restatectl provision` if you have not provisioned this cluster yet.");

        // short-circuits `restatectl status` to avoid trying to list partitions
        bail!(
            "The cluster appears to not be provisioned. You can do so with `restatectl provision`"
        );
    } else {
        for (log_id, chain) in log_chains {
            let params = deserialize_replicated_log_params(&chain.tail());
            logs_table.add_row(vec![
                Cell::new(log_id),
                Cell::new(format!("{}", &chain.num_segments())),
                Cell::new(format!("{}", &chain.tail().base_lsn)),
                Cell::new(format!("{}", chain.tail().config.kind)),
                render_loglet_params(&params, |p| Cell::new(p.loglet_id)),
                render_loglet_params(&params, |p| Cell::new(format!("{:#}", p.replication))),
                render_loglet_params(&params, |p| Cell::new(format!("{:#}", p.sequencer))),
                render_loglet_params(&params, |p| Cell::new(format!("{:#}", p.nodeset))),
            ]);
        }

        logs_table.set_styled_header(vec![
            "L-ID",
            "SEGMENTS",
            "FROM",
            "KIND",
            "LOGLET-ID",
            "REPLICATION",
            "SEQUENCER",
            "NODESET",
        ]);
        c_println!("{}", logs_table);
    }

    Ok(())
}
