// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};

use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;

use super::{RuleRow, render_concurrency};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::datetime::DateTimeExt;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Show additional columns (description, version, last modified)
    #[clap(long, short = 'x')]
    extra: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = DataFusionHttpClient::new(env).await?;
    let rows: Vec<RuleRow> = client
        .run_json_query(
            "SELECT pattern, concurrency, description, disabled, version, last_modified \
             FROM sys_rules ORDER BY pattern"
                .to_string(),
        )
        .await?;

    if rows.is_empty() {
        c_println!("No rules defined.");
        return Ok(());
    }

    let mut table = Table::new_styled();
    if opts.extra {
        table.set_styled_header(vec![
            "PATTERN",
            "CONCURRENCY",
            "DISABLED",
            "DESCRIPTION",
            "VERSION",
            "LAST MODIFIED",
        ]);
    } else {
        table.set_styled_header(vec!["PATTERN", "CONCURRENCY", "DISABLED"]);
    }

    for row in rows {
        let disabled = if row.disabled { "yes" } else { "no" };
        if opts.extra {
            let last_modified = row.last_modified.map(|dt| dt.display()).unwrap_or_default();
            table.add_row(vec![
                Cell::new(row.pattern),
                Cell::new(render_concurrency(row.concurrency)),
                Cell::new(disabled),
                Cell::new(row.description.unwrap_or_default()),
                Cell::new(row.version),
                Cell::new(last_modified),
            ]);
        } else {
            table.add_row(vec![
                Cell::new(row.pattern),
                Cell::new(render_concurrency(row.concurrency)),
                Cell::new(disabled),
            ]);
        }
    }

    c_println!("{table}");
    Ok(())
}
