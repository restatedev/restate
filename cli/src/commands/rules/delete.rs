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
use serde_json::json;

use restate_admin_rest_model::rules::DeleteRuleRequest;
use restate_types::Version;

use super::{
    concurrency_field, disabled_field, fetch_existing_rule, is_conflict, json_only,
    modified_concurrently, parse_pattern, rules_list_step,
};
use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};
use crate::ui::fmt::{DryRun, Field, Formatter, Outcome, OutputFormatter};

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

    let (result, message, outcome) =
        match client.delete_rules(vec![request]).await?.into_body().await {
            Ok(deleted) if deleted.is_empty() => (
                "already_absent",
                format!("Rule '{canonical}' was already absent."),
                Outcome::NothingToDo,
            ),
            Ok(_) => (
                "deleted",
                format!("Deleted rule '{canonical}'"),
                Outcome::Success,
            ),
            Err(e) if is_conflict(&e) => return Err(modified_concurrently(&canonical)),
            Err(e) => return Err(e.into()),
        };
    f.outcome("result", Field::with_display(result, message), outcome);
    rules_list_step(&mut f);
    f.finish()
}
