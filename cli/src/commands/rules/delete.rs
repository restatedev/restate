// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, anyhow, bail};
use cling::prelude::*;
use comfy_table::Table;

use restate_admin_rest_model::rules::DeleteRuleRequest;
use restate_cli_util::c_println;
use restate_cli_util::c_success;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_types::Version;

use super::{fetch_rule, is_conflict, parse_pattern, render_concurrency};
use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Pattern of the rule to delete
    pattern: String,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let pattern = parse_pattern(&opts.pattern)?;
    let canonical = pattern.to_string();

    let sql_client = DataFusionHttpClient::new(&env).await?;
    let current = fetch_rule(&sql_client, &canonical)
        .await?
        .ok_or_else(|| anyhow!("No rule found with pattern '{canonical}'."))?;

    let mut table = Table::new_styled();
    table.add_kv_row("Pattern:", &canonical);
    table.add_kv_row("Concurrency:", render_concurrency(current.concurrency));
    if let Some(description) = &current.description {
        table.add_kv_row("Description:", description);
    }
    table.add_kv_row("Disabled:", if current.disabled { "yes" } else { "no" });
    c_println!("{table}");

    confirm_or_exit(&format!("Delete rule '{canonical}'?"))?;

    let client = AdminClient::new(&env).await?;
    let request = DeleteRuleRequest {
        pattern,
        expected_version: Some(Version::from(current.version)),
    };

    match client.delete_rules(vec![request]).await?.into_body().await {
        Ok(deleted) if deleted.is_empty() => c_println!("Rule '{canonical}' was already absent."),
        Ok(_) => c_success!("Deleted rule '{canonical}'"),
        Err(e) if is_conflict(&e) => {
            bail!("Rule '{canonical}' was modified concurrently; please re-run.")
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
