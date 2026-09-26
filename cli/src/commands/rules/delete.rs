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

use restate_admin_rest_model::rules::DeleteRuleRequest;
use restate_cli_util::{CliContext, c_println, c_success};
use restate_types::Version;

use super::{fetch_rule, is_conflict, parse_pattern, render_concurrency};
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
    let current = fetch_rule(&sql_client, &canonical)
        .await?
        .ok_or_else(|| anyhow!("No rule found with pattern '{canonical}'."))?;

    let json = CliContext::get().json_output();
    let mut f = Formatter::new();
    let mut rule = vec![
        ("pattern", Field::new(canonical.as_str())),
        (
            "concurrency",
            Field::with_display(current.concurrency, render_concurrency(current.concurrency)),
        ),
    ];
    if let Some(description) = &current.description {
        rule.push(("description", Field::new(description.as_str())));
    }
    rule.push((
        "disabled",
        Field::with_display(
            current.disabled,
            if current.disabled { "yes" } else { "no" },
        ),
    ));
    f.detail("rule", &rule);
    if json {
        f.table(
            "changes",
            &["pattern", "change"],
            &[vec![Field::new(canonical.as_str()), Field::new("delete")]],
        );
    }

    f.confirm(&opts.dry_run, &format!("Delete rule '{canonical}'?"))?;

    let client = AdminClient::new(&env).await?;
    let request = DeleteRuleRequest {
        pattern,
        expected_version: Some(Version::from(current.version)),
    };

    let deleted = match client.delete_rules(vec![request]).await?.into_body().await {
        Ok(deleted) => !deleted.is_empty(),
        Err(e) if is_conflict(&e) => {
            bail!("Rule '{canonical}' was modified concurrently; please re-run.")
        }
        Err(e) => return Err(e.into()),
    };
    if json {
        f.value("deleted", Field::new(deleted));
    } else if deleted {
        c_success!("Deleted rule '{canonical}'");
    } else {
        c_println!("Rule '{canonical}' was already absent.");
    }
    f.next_step("restate rules list", "see the remaining rules");
    f.finish()
}
