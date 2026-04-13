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
use crate::ui::datetime::DateTimeExt;

use super::utils::brokers_property;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env)).await
}

async fn list(env: &CliEnv) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let mut clusters = client
        .list_kafka_clusters()
        .await?
        .into_body()
        .await?
        .clusters;

    if clusters.is_empty() {
        c_println!("No Kafka clusters registered.");
        return Ok(());
    }

    clusters.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["NAME", "BROKERS", "PROPERTIES", "CREATED-AT"]);
    for cluster in clusters {
        let brokers = brokers_property(&cluster.properties)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            Cell::new(cluster.name.as_str()),
            Cell::new(brokers),
            Cell::new(cluster.properties.len()),
            Cell::new(cluster.created_at.display()),
        ]);
    }
    c_println!("{table}");
    Ok(())
}
