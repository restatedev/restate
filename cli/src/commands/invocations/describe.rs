// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Result};
use cling::prelude::*;
use comfy_table::Table;
use dialoguer::console::style;
use indoc::indoc;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{get_invocation, get_invocation_journal, InvocationState};
use crate::clients::{self};
use crate::ui::console::StyledTable;
use crate::ui::duration_to_human_rough;
use crate::ui::invocations::{
    add_invocation_to_kv_table, format_journal_entry, invocation_qualified_name, invocation_status,
};
use crate::ui::watcher::Watch;
use crate::{c_println, c_tip, c_title};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// The ID of the invocation
    invocation_id: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let sql_client = clients::DataFusionHttpClient::new(env)?;

    let Some(inv) = get_invocation(&sql_client, &opts.invocation_id).await? else {
        bail!("Invocation {} not found!", opts.invocation_id);
    };

    let mut table = Table::new_styled(&env.ui_config);
    table.add_kv_row(
        "Created at:",
        format!(
            "{} ({})",
            &inv.created_at,
            duration_to_human_rough(
                chrono::Local::now().signed_duration_since(inv.created_at),
                chrono_humanize::Tense::Past
            )
        ),
    );
    if let Some(key) = &inv.key {
        table.add_kv_row(
            "Service:",
            format!("{} {} {}", inv.service, style("@").dim(), style(key).dim(),),
        );
    } else {
        table.add_kv_row("Service:", &inv.service);
    }
    table.add_kv_row("Method:", &inv.handler);
    add_invocation_to_kv_table(&mut table, &inv);
    table.add_kv_row_if(
        || inv.state_modified_at.is_some(),
        "Modified at:",
        format!("{}", &inv.state_modified_at.unwrap()),
    );

    c_title!("📜", "Invocation Information");
    c_println!("{}", table);
    c_println!();

    if inv.status != InvocationState::Pending {
        // Deployment
        if let Some(deployment_id) = &inv.pinned_deployment_id {
            c_tip!(
                indoc! {
                    "This invocation is bound to run on deployment '{}'. To guarantee
                safety and correctness, invocations that made progress on a deployment
                cannot move to newer deployments automatically."
                },
                deployment_id,
            );
        }
    }

    c_title!("🚂", "Invocation Progress");
    if let Some(parent) = &inv.invoked_by_id {
        c_println!(
            "{} {}",
            inv.invoked_by_service
                .as_ref()
                .map(|x| style(x.to_owned()).italic().blue())
                .unwrap_or_else(|| style("<UNKNOWN>".to_owned()).red()),
            style(parent).italic(),
        );
    } else {
        c_println!("[{}]", style("Ingress").dim().italic());
    }

    // This invocation..
    c_println!(" └──(this)─> {}", invocation_qualified_name(&inv));

    // filtering journal based on `should_present`
    let journal = get_invocation_journal(&sql_client, &opts.invocation_id).await?;
    let journal_entries = journal
        .iter()
        .filter(|entry| entry.should_present())
        .collect::<Vec<_>>();

    // Call graph
    if !journal_entries.is_empty() {
        c_println!("     {}", style("▸").dim());
        for entry in journal_entries {
            let tree_symbol = "├────";
            c_println!(
                "     {}{}",
                style(tree_symbol).dim(),
                format_journal_entry(entry)
            );
        }
        c_println!(
            "     {} {}",
            style("└────>>").dim(),
            invocation_status(inv.status)
        );
    }
    Ok(())
}
