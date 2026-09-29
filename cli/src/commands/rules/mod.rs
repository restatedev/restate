// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod delete;
mod disable;
mod enable;
mod list;
mod set;

use std::num::NonZeroU32;
use std::str::FromStr;

use anyhow::Result;
use chrono::{DateTime, Local};
use cling::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};

use restate_admin_rest_model::rules::{RuleResponse, UpsertRuleRequest};
use restate_limiter::{Precondition, RulePattern, UserLimits};
use restate_types::Version;
use restate_util_string::ReString;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, ClientError, DataFusionHttpClient};
use crate::error::{ErrorKind, RestateCliError};
use crate::ui::datetime::DateTimeExt;
use crate::ui::fmt::{
    Field, Formatter, IncludeFormatting, Outcome, OutputFormatter, rerun_command,
};

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "rule")]
pub enum Rules {
    /// List the configured concurrency-limit rules
    List(list::List),
    /// Create or update a rule
    Set(set::Set),
    /// Enable a previously disabled rule
    Enable(enable::Enable),
    /// Disable a rule without removing it
    Disable(disable::Disable),
    /// Remove a rule
    Delete(delete::Delete),
}

/// A single rule as projected by the `sys_rules` introspection table.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RuleRow {
    pub pattern: String,
    #[serde(default)]
    pub concurrency: Option<u32>,
    #[serde(default)]
    pub description: Option<String>,
    pub disabled: bool,
    pub version: u32,
    #[serde(default)]
    pub last_modified: Option<DateTime<Local>>,
}

impl RuleRow {
    /// The concurrency limit as a `NonZeroU32` (the runtime shape).
    fn concurrency(&self) -> Option<NonZeroU32> {
        self.concurrency.and_then(NonZeroU32::new)
    }

    /// The rule as emitted in `--json` output.
    fn to_json(&self) -> Value {
        json!({
            "pattern": self.pattern,
            "concurrency": self.concurrency,
            "description": self.description,
            "disabled": self.disabled,
            "version": self.version,
            "last_modified": self.last_modified.map(|t| t.iso()),
        })
    }
}

impl From<RuleResponse> for RuleRow {
    fn from(rule: RuleResponse) -> Self {
        Self {
            pattern: rule.pattern.to_string(),
            concurrency: rule.limits.concurrency.map(NonZeroU32::get),
            description: rule.description,
            disabled: rule.disabled,
            version: rule.version.into(),
            last_modified: i64::try_from(rule.last_modified_millis_since_epoch)
                .ok()
                .and_then(DateTime::from_timestamp_millis)
                .map(|t| t.with_timezone(&Local)),
        }
    }
}

/// A concurrency limit: `unlimited` for humans, `null` in JSON when unset.
pub(crate) fn concurrency_field(limit: Option<u32>) -> Field {
    let display = match limit {
        Some(limit) => limit.to_string(),
        None => "unlimited".to_owned(),
    };
    Field::with_display(limit, display)
}

/// The `disabled` flag: `yes`/`no` for humans, a bool in JSON.
pub(crate) fn disabled_field(disabled: bool) -> Field {
    Field::with_display(disabled, if disabled { "yes" } else { "no" })
}

/// A value emitted in JSON only (human output describes it with its own messages).
pub(crate) fn json_only(value: impl Into<Value>) -> Field {
    Field::with_display(value, "")
}

/// Parses and validates a rule pattern, canonicalizing it client-side so we
/// fail fast on bad input and can match against the `sys_rules` table.
pub(crate) fn parse_pattern(pattern: &str) -> Result<RulePattern<ReString>> {
    RulePattern::<ReString>::from_str(pattern).map_err(|e| {
        RestateCliError::bad_input(format!("Invalid rule pattern '{pattern}': {e}")).into()
    })
}

/// Reads a single rule (by its canonical pattern) from the `sys_rules` table.
pub(crate) async fn fetch_rule(
    client: &DataFusionHttpClient,
    canonical_pattern: &str,
) -> Result<Option<RuleRow>> {
    let query = format!(
        "SELECT pattern, concurrency, description, disabled, version, last_modified \
         FROM sys_rules WHERE pattern = '{}'",
        escape_sql(canonical_pattern)
    );
    let rows: Vec<RuleRow> = client.run_json_query(query).await?;
    Ok(rows.into_iter().next())
}

/// Like [`fetch_rule`], but a missing rule is a not-found error.
pub(crate) async fn fetch_existing_rule(
    client: &DataFusionHttpClient,
    canonical_pattern: &str,
) -> Result<RuleRow> {
    fetch_rule(client, canonical_pattern).await?.ok_or_else(|| {
        RestateCliError::not_found(format!("No rule found with pattern '{canonical_pattern}'."))
            .into()
    })
}

/// Escapes single quotes for safe inlining into a SQL string literal. Canonical
/// patterns only contain `[a-zA-Z0-9_.-]`, `/` and `*`, so this is defensive.
fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

/// `true` when the error is an HTTP 409 Conflict (a failed precondition).
fn is_conflict(err: &ClientError) -> bool {
    matches!(err, ClientError::Api(api) if api.http_status_code == reqwest::StatusCode::CONFLICT)
}

/// The rule changed since it was read (a failed precondition, 409): re-running the
/// command applies it to the current rule.
fn modified_concurrently(canonical: &str) -> anyhow::Error {
    RestateCliError::new(
        ErrorKind::Generic,
        format!("Rule '{canonical}' was modified concurrently"),
    )
    .with_next_step(rerun_command(), "retry against the rule's current state")
    .into()
}

/// Sends a single-rule upsert, translating a precondition conflict (409) into
/// [`modified_concurrently`].
pub(crate) async fn upsert_one(
    client: &AdminClient,
    request: UpsertRuleRequest,
    canonical: &str,
) -> Result<Option<RuleResponse>> {
    match client.upsert_rules(vec![request]).await?.into_body().await {
        Ok(mut rules) => Ok(rules.drain(..).next()),
        Err(e) if is_conflict(&e) => Err(modified_concurrently(canonical)),
        Err(e) => Err(e.into()),
    }
}

/// Suggests listing the rules, after a change.
fn rules_list_step(f: &mut impl OutputFormatter) {
    f.next_step(
        "restate rules list",
        "see the rules",
        IncludeFormatting::Yes,
    );
}

/// Read-modify-write helper backing `enable`/`disable`: toggles a rule's
/// `disabled` flag while preserving its other fields, guarded by a CAS on the
/// version currently visible in `sys_rules`.
pub(crate) async fn toggle_disabled(env: &CliEnv, pattern: &str, disabled: bool) -> Result<()> {
    let pattern = parse_pattern(pattern)?;
    let canonical = pattern.to_string();
    let action = if disabled { "disabled" } else { "enabled" };

    let sql_client = DataFusionHttpClient::new(env).await?;
    let current = fetch_existing_rule(&sql_client, &canonical).await?;
    let (rule, outcome) = if current.disabled == disabled {
        let result = if disabled {
            "already_disabled"
        } else {
            "already_enabled"
        };
        let message = format!("Rule '{canonical}' is already {action}.");
        (Some(current), (result, message, Outcome::Success))
    } else {
        let client = AdminClient::new(env).await?;
        let request = UpsertRuleRequest {
            pattern,
            limits: UserLimits::new(current.concurrency()),
            description: current.description.clone(),
            disabled,
            precondition: Precondition::Matches(Version::from(current.version)),
        };
        let updated = upsert_one(&client, request, &canonical).await?;
        let message = format!("Rule '{canonical}' {action}");
        (
            updated.map(RuleRow::from),
            (action, message, Outcome::Success),
        )
    };

    let (result, message, outcome) = outcome;
    let mut f = Formatter::new();
    f.value("rule", json_only(rule.as_ref().map(RuleRow::to_json)));
    f.outcome("result", Field::with_display(result, message), outcome);
    rules_list_step(&mut f);
    f.finish()
}
