// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License,

use std::collections::BTreeMap;

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};

use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_types::logs::metadata::Chain;
use restate_types::{
    logs::{metadata::Logs, LogId},
    Versioned,
};

use crate::commands::metadata::logs::KEY_METADATA_LOGS;
use crate::commands::metadata::{get_value, MetadataCommonOpts};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "get_logs")]
pub struct ListLogsOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,
}

async fn get_logs(opts: &ListLogsOpts) -> anyhow::Result<()> {
    let logs: Logs = get_value(&opts.metadata, KEY_METADATA_LOGS)
        .await?
        .context("no logs configured in metadata")?;

    let mut logs_table = Table::new_styled();
    c_title!("📋", format!("Log Configuration {}", logs.version()));

    // sort by log-id for display
    let logs: BTreeMap<LogId, &Chain> = logs.iter().map(|(id, chain)| (*id, chain)).collect();

    for (log_id, chain) in logs {
        logs_table.add_row(vec![
            Cell::new(log_id),
            Cell::new(chain.num_segments()).fg(Color::DarkGrey),
            Cell::new(format!("{}", &chain.tail().base_lsn))
                .fg(Color::Green)
                .add_attribute(Attribute::Bold),
            Cell::new(format!("{:?}", chain.tail().config.kind)),
        ]);
    }

    logs_table.set_styled_header(vec!["LOG-ID", "SEGMENTS", "TAIL BASE LSN", "KIND"]);
    c_println!("{}", logs_table);

    Ok(())
}
