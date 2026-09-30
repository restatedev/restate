// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use anyhow::Result;
use cling::prelude::*;

use restate_admin_rest_model::rules::UpsertRuleRequest;
use restate_limiter::{Precondition, UserLimits};
use restate_types::Version;

use super::{RuleRow, fetch_rule, parse_pattern, rules_list_step, upsert_one};
use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, DataFusionHttpClient};
use crate::error::RestateCliError;
use crate::ui::fmt::{Field, Formatter, Outcome, OutputFormatter, shell_quote};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_set")]
pub struct Set {
    /// Rule pattern, e.g. `*`, `scope1/*`, `scope1/foo/bar`
    pattern: String,

    /// Maximum concurrent running invocations (>= 1). On a new rule, omitting this means
    /// unlimited; on an existing rule it leaves the current limit unchanged.
    #[clap(long, conflicts_with = "unlimited")]
    concurrency: Option<NonZeroU32>,

    /// Set the rule to unlimited concurrency
    #[clap(long)]
    unlimited: bool,

    /// Description for the rule
    #[clap(long)]
    description: Option<String>,

    /// Create the rule in a disabled (parked) state. Only valid for new rules;
    /// use `restate rules disable` to disable an existing rule.
    #[clap(long)]
    disabled: bool,
}

pub async fn run_set(State(env): State<CliEnv>, opts: &Set) -> Result<()> {
    let pattern = parse_pattern(&opts.pattern)?;
    let canonical = pattern.to_string();

    let sql_client = DataFusionHttpClient::new(&env).await?;
    let current = fetch_rule(&sql_client, &canonical).await?;
    let was_create = current.is_none();

    let request = match &current {
        None => UpsertRuleRequest {
            pattern,
            limits: UserLimits::new(if opts.unlimited {
                None
            } else {
                opts.concurrency
            }),
            description: opts.description.clone(),
            disabled: opts.disabled,
            precondition: Precondition::DoesNotExist,
        },
        Some(rule) => {
            if opts.disabled {
                return Err(RestateCliError::bad_input(format!(
                    "Rule '{canonical}' already exists. Use `restate rules disable` to disable it."
                ))
                .with_next_step(
                    format!("restate rules disable {}", shell_quote(&canonical)),
                    "disable the existing rule",
                )
                .into());
            }
            let concurrency = if opts.unlimited {
                None
            } else {
                opts.concurrency.or_else(|| rule.concurrency())
            };
            let description = opts
                .description
                .clone()
                .or_else(|| rule.description.clone());
            UpsertRuleRequest {
                pattern,
                limits: UserLimits::new(concurrency),
                description,
                disabled: rule.disabled,
                precondition: Precondition::Matches(Version::from(rule.version)),
            }
        }
    };

    let client = AdminClient::new(&env).await?;
    let rule = upsert_one(&client, request, &canonical).await?;

    let (verb, result) = if was_create {
        ("Created", "created")
    } else {
        ("Updated", "updated")
    };
    let message = match &rule {
        Some(rule) => format!("{verb} rule '{canonical}' (version {})", rule.version),
        None => format!("{verb} rule '{canonical}'"),
    };
    let rule = rule.map(RuleRow::from);

    let mut f = Formatter::new();
    f.value(
        "rule",
        Field::json_only(rule.as_ref().map(RuleRow::to_json)),
    );
    f.outcome(
        "result",
        Field::with_display(result, message),
        Outcome::Success,
    );
    rules_list_step(&mut f);
    f.finish()
}
