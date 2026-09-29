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
use serde_json::json;

use restate_admin_rest_model::rules::DeleteRuleRequest;
use restate_cli_util::{CliContext, c_println, c_success};
use restate_types::Version;

use super::{
    concurrency_field, disabled_field, fetch_existing_rule, is_conflict, json_only, parse_pattern,
    rules_list_step,
};
use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Pattern of the rule to delete
    pattern: String,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let pattern = parse_pattern(&opts.pattern)?;
    let canonical = pattern.to_string();

    let sql_client = DataFusionHttpClient::new(&env).await?;
    let current = fetch_existing_rule(&sql_client, &canonical).await?;
    let json = CliContext::get().json_output();

    let mut f = Formatter::new();
    f.detail(
        "rule",
        [
            ("pattern", Field::new(canonical.as_str())),
            ("concurrency", concurrency_field(current.concurrency)),
            ("description", Field::new(current.description.as_deref())),
            ("disabled", disabled_field(current.disabled)),
        ],
    );
    // The plan, JSON-only: the human output already describes the rule to delete.
    f.value(
        "changes",
        json_only(json!([{"pattern": canonical, "change": "delete"}])),
    );
    f.confirm(&opts.dry_run, &format!("Delete rule '{canonical}'?"))?;

    let client = AdminClient::new(&env).await?;
    let request = DeleteRuleRequest {
        pattern,
        expected_version: Some(Version::from(current.version)),
    };

    let result = match client.delete_rules(vec![request]).await?.into_body().await {
        Ok(deleted) if deleted.is_empty() => {
            if !json {
                c_println!("Rule '{canonical}' was already absent.");
            }
            "already_absent"
        }
        Ok(_) => {
            if !json {
                c_success!("Deleted rule '{canonical}'");
            }
            "deleted"
        }
        Err(e) if is_conflict(&e) => {
            bail!("Rule '{canonical}' was modified concurrently; please re-run.")
        }
        Err(e) => return Err(e.into()),
    };
    f.value("result", json_only(result));
    rules_list_step(&mut f);
    f.finish()
}
