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

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Filter by exact source URI (e.g. `kafka://my-cluster/orders`)
    #[clap(long)]
    source: Option<String>,

    /// Filter by exact sink URI (e.g. `service://Counter/count`)
    #[clap(long)]
    sink: Option<String>,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let mut subs = client
        .list_subscriptions(opts.sink.as_deref(), opts.source.as_deref())
        .await?
        .into_body()
        .await?
        .subscriptions;

    if subs.is_empty() {
        c_println!("No subscriptions registered.");
        return Ok(());
    }

    subs.sort_by(|a, b| a.id.to_string().cmp(&b.id.to_string()));

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["ID", "SOURCE", "SINK", "OPTIONS"]);
    for sub in subs {
        table.add_row(vec![
            Cell::new(sub.id.to_string()),
            Cell::new(sub.source),
            Cell::new(sub.sink),
            Cell::new(sub.options.len()),
        ]);
    }
    c_println!("{table}");
    Ok(())
}
