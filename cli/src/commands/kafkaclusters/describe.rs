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
use comfy_table::Table;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::datetime::DateTimeExt;

use super::utils::{brokers_property, render_properties_table};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Kafka cluster name
    name: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let cluster = client
        .get_kafka_cluster(&opts.name, true)
        .await?
        .into_body()
        .await?;

    let mut summary = Table::new_styled();
    summary.add_kv_row("Name:", cluster.name.as_str());
    summary.add_kv_row(
        "Brokers:",
        brokers_property(&cluster.properties).unwrap_or("-"),
    );
    summary.add_kv_row("Properties:", cluster.properties.len());
    summary.add_kv_row("Subscriptions:", cluster.subscriptions.len());
    summary.add_kv_row("Created at:", cluster.created_at.display());

    c_title!("📜", "Kafka Cluster");
    c_println!("{summary}");
    c_println!();

    c_title!("⚙️", "Properties");
    c_println!("{}", render_properties_table(&cluster.properties));

    if !cluster.subscriptions.is_empty() {
        c_println!();
        c_title!("📨", "Subscriptions");
        let mut sub_table = Table::new_styled();
        sub_table.set_styled_header(vec!["ID", "SOURCE", "SINK"]);
        for sub in cluster.subscriptions {
            sub_table.add_row(vec![sub.id.to_string(), sub.source, sub.sink]);
        }
        c_println!("{sub_table}");
    }

    Ok(())
}
