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

use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
use cling::prelude::*;
use serde::Deserialize;

use restate_admin_rest_model::rules::{RuleResponse, UpsertRuleRequest};
use restate_cli_util::{c_println, c_success};
use restate_limiter::{Precondition, RulePattern, UserLimits};
use restate_types::Version;
use restate_util_string::ReString;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient, MetasClientError};

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
}

/// Renders a concurrency limit for display (`unlimited` when unset).
pub(crate) fn render_concurrency(limit: Option<u32>) -> String {
    match limit {
        Some(limit) => limit.to_string(),
        None => "unlimited".to_string(),
    }
}

/// Parses and validates a rule pattern, canonicalizing it client-side so we
/// fail fast on bad input and can match against the `sys_rules` table.
pub(crate) fn parse_pattern(pattern: &str) -> Result<RulePattern<ReString>> {
    RulePattern::<ReString>::from_str(pattern)
        .map_err(|e| anyhow!("Invalid rule pattern '{pattern}': {e}"))
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

/// Escapes single quotes for safe inlining into a SQL string literal. Canonical
/// patterns only contain `[a-zA-Z0-9_.-]`, `/` and `*`, so this is defensive.
fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

/// `true` when the error is an HTTP 409 Conflict (a failed precondition).
fn is_conflict(err: &MetasClientError) -> bool {
    matches!(err, MetasClientError::Api(api) if api.http_status_code == reqwest::StatusCode::CONFLICT)
}

/// Sends a single-rule upsert, translating a precondition conflict (409) into
/// the supplied actionable message.
pub(crate) async fn upsert_one(
    client: &AdminClient,
    request: UpsertRuleRequest,
    conflict_msg: &str,
) -> Result<Option<RuleResponse>> {
    match client.upsert_rules(vec![request]).await?.into_body().await {
        Ok(mut rules) => Ok(rules.drain(..).next()),
        Err(e) if is_conflict(&e) => Err(anyhow!("{conflict_msg}")),
        Err(e) => Err(e.into()),
    }
}

/// Read-modify-write helper backing `enable`/`disable`: toggles a rule's
/// `disabled` flag while preserving its other fields, guarded by a CAS on the
/// version currently visible in `sys_rules`.
pub(crate) async fn toggle_disabled(env: &CliEnv, pattern: &str, disabled: bool) -> Result<()> {
    let pattern = parse_pattern(pattern)?;
    let canonical = pattern.to_string();
    let action = if disabled { "disabled" } else { "enabled" };

    let sql_client = DataFusionHttpClient::new(env).await?;
    let current = fetch_rule(&sql_client, &canonical)
        .await?
        .ok_or_else(|| anyhow!("No rule found with pattern '{canonical}'."))?;

    if current.disabled == disabled {
        c_println!("Rule '{canonical}' is already {action}.");
        return Ok(());
    }

    let client = AdminClient::new(env).await?;
    let request = UpsertRuleRequest {
        pattern,
        limits: UserLimits::new(current.concurrency()),
        description: current.description.clone(),
        disabled,
        precondition: Precondition::Matches(Version::from(current.version)),
    };
    upsert_one(
        &client,
        request,
        &format!("Rule '{canonical}' was modified concurrently; please re-run."),
    )
    .await?;

    c_success!("Rule '{canonical}' {action}");
    Ok(())
}
