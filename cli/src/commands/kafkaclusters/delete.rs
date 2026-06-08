// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, bail};
use cling::prelude::*;
use comfy_table::Table;

use restate_cli_util::ui::console::{Styled, StyledTable, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_println, c_success, c_warn};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Kafka cluster name
    name: String,

    /// Delete the cluster even if subscriptions still reference it. Those
    /// subscriptions will be orphaned and will stop consuming.
    #[clap(long)]
    force: bool,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let client = AdminClient::new(&env).await?;

    let cluster = client
        .get_kafka_cluster(&opts.name, true)
        .await?
        .into_body()
        .await?;

    let mut table = Table::new_styled();
    table.add_kv_row("Name:", cluster.name.as_str());
    table.add_kv_row("Subscriptions:", cluster.subscriptions.len());
    c_println!("{table}");

    if !cluster.subscriptions.is_empty() && !opts.force {
        bail!(
            "Cluster {} has {} subscription(s) attached. Re-run with {} to orphan them.",
            Styled(Style::Info, &opts.name),
            cluster.subscriptions.len(),
            Styled(Style::Notice, "--force"),
        );
    }

    if !cluster.subscriptions.is_empty() {
        c_warn!(
            "{} subscription(s) will be orphaned and stop consuming.",
            cluster.subscriptions.len()
        );
    }

    confirm_or_exit(&format!(
        "Are you sure you want to delete Kafka cluster {}?",
        opts.name
    ))?;

    client
        .delete_kafka_cluster(&opts.name, opts.force)
        .await?
        .success_or_error()?;

    c_success!("Kafka cluster {} deleted", &opts.name);
    Ok(())
}
