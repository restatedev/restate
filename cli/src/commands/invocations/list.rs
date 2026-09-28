// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::time::Instant;

use anyhow::Result;
use clap::ValueEnum;
use clap::builder::{EnumValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue};
use cling::prelude::*;
use indicatif::ProgressBar;
use itertools::Itertools;

use restate_cli_util::c_eprintln;
use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    InvocationState, find_and_count_active_invocations, invocation_status_filter,
};
use crate::ui::fmt::{Formatter, OutputFormatter};

/// Timestamp to order `invocations list` by.
#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum OrderBy {
    /// Last status change
    Modified,
    /// Creation time
    Created,
}

/// Sort direction.
#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum SortOrder {
    Desc,
    Asc,
}

#[derive(Clone, Debug)]
pub enum CompletionResult {
    Success,
    Failure,
}

impl FromStr for CompletionResult {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            _ => Err(format!(
                "invalid completion result '{s}', expected 'success' or 'failure'"
            )),
        }
    }
}

impl fmt::Display for CompletionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompletionResult::Success => write!(f, "success"),
            CompletionResult::Failure => write!(f, "failure"),
        }
    }
}

/// Parses `--status` like [`InvocationState`]'s `ValueEnum`, pointing completion outcomes
/// (e.g. `failed`) to `--completion-result`.
#[derive(Clone)]
struct StatusParser;

impl TypedValueParser for StatusParser {
    type Value = InvocationState;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        EnumValueParser::<InvocationState>::new()
            .parse_ref(cmd, arg, value)
            .map_err(|mut err| {
                let outcome = value.to_string_lossy().to_lowercase();
                if matches!(
                    outcome.as_str(),
                    "failed" | "failure" | "succeeded" | "success" | "cancelled" | "killed"
                ) {
                    err.insert(
                        ContextKind::Suggested,
                        ContextValue::StyledStrs(vec![
                            "completed invocations are filtered by outcome: use '--completion-result failure' (or 'success')"
                                .into(),
                        ]),
                    );
                }
                err
            })
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        Some(Box::new(
            InvocationState::value_variants()
                .iter()
                .filter_map(ValueEnum::to_possible_value),
        ))
    }
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Only list invocations matching this query: an invocation id, or a target exact
    /// match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    /// * `workflowName`
    /// * `workflowName/key`
    /// * `workflowName/key/handler`
    ///
    /// Combines with the other filters.
    #[clap(verbatim_doc_comment)]
    query: Option<String>,
    /// Service to list invocations for
    #[clap(long, value_delimiter = ',')]
    service: Vec<String>,
    /// Filter by invocation on this handler name
    #[clap(long, value_delimiter = ',')]
    handler: Vec<String>,
    /// Show all invocations, including the completed ones that are hidden by default. This overrides the `status` filter.
    #[clap(long)]
    all: bool,
    /// Filter by status(es)
    #[clap(long, ignore_case = true, value_delimiter = ',', value_parser = StatusParser)]
    status: Vec<InvocationState>,
    /// Filter completed invocations by result: 'success' or 'failure'. Implies --status=completed.
    #[clap(long, ignore_case = true, conflicts_with_all = ["all", "status"])]
    completion_result: Option<CompletionResult>,
    /// Filter by deployment ID
    #[clap(long, visible_alias = "dp", value_delimiter = ',')]
    deployment: Vec<String>,
    /// Only list invocations on keyed services only
    #[clap(long)]
    virtual_objects_only: bool,
    /// Filter by invocations on this service key
    #[clap(long, value_delimiter = ',')]
    key: Vec<String>,
    /// Limit the number of results
    #[clap(long, default_value = "100")]
    limit: usize,
    /// Find zombie invocations (invocations pinned to removed deployments)
    #[clap(long)]
    zombie: bool,
    /// Which timestamp to order the results by
    #[clap(long, value_enum, default_value_t = OrderBy::Modified)]
    order_by: OrderBy,
    /// Sort direction: `desc` shows the most recent first
    #[clap(long, value_enum, default_value_t = SortOrder::Desc)]
    order: SortOrder,
    /// Same as `--order asc` (kept for compatibility)
    #[clap(long, hide = true, conflicts_with = "order")]
    oldest_first: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let sql_client = crate::clients::DataFusionHttpClient::new(env).await?;
    let statuses: HashSet<InvocationState> = HashSet::from_iter(opts.status.clone());
    // Prepare filters
    let mut active_filters: Vec<String> = vec![]; // "WHERE 1 = 1\n".to_string();
    if let Some(query) = &opts.query {
        active_filters.push(super::create_prefixed_query_filter(query, "inv.")?);
    }

    let column = match opts.order_by {
        OrderBy::Modified => "inv.modified_at",
        OrderBy::Created => "inv.created_at",
    };
    let direction = match (opts.order, opts.oldest_first) {
        (SortOrder::Asc, _) | (_, true) => "ASC",
        (SortOrder::Desc, false) => "DESC",
    };
    let order_by = &format!("ORDER BY {column} {direction}, inv.id");

    if !opts.service.is_empty() {
        active_filters.push(format!(
            "inv.target_service_name IN ({})",
            opts.service.iter().map(|x| format!("'{x}'")).format(",")
        ));
    }

    if !opts.handler.is_empty() {
        active_filters.push(format!(
            "inv.target_handler_name IN ({})",
            opts.handler.iter().map(|x| format!("'{x}'")).format(",")
        ));
    }

    if !opts.key.is_empty() {
        active_filters.push(format!(
            "inv.target_service_key IN ({})",
            opts.key.iter().map(|x| format!("'{x}'")).format(",")
        ));
    }

    if opts.virtual_objects_only {
        active_filters.push(
            "inv.target_service_name IN (select name from sys_service where ty = 'virtual_object')"
                .to_owned(),
        );
    }

    if opts.zombie {
        // a zombie invocation has a pinned deployment id, but the id isn't found in the list of deployments
        active_filters
            .push("(inv.pinned_deployment_id IS NOT NULL AND inv.pinned_deployment_id NOT IN (select id from sys_deployment))".to_owned());
    }

    // Only makes sense when querying active invocations;
    if !opts.deployment.is_empty() {
        active_filters.push(format!(
            "(inv.pinned_deployment_id IN ({0}) OR inv.last_attempt_deployment_id IN ({0}))",
            opts.deployment.iter().map(|x| format!("'{x}'")).join(",")
        ));
    }

    if let Some(completion) = &opts.completion_result {
        // --completion-result implies completed status
        active_filters.push("status = 'completed'".to_owned());
        active_filters.push(format!("inv.completion_result = '{completion}'"));
    } else if opts.all {
        // No filter
    } else if statuses.is_empty() {
        // Default hide completed invocations
        active_filters.push("status != 'completed'".to_owned());
    } else {
        // Apply status filters
        let statuses: Vec<InvocationState> = statuses.into_iter().collect();
        active_filters.push(invocation_status_filter(&sql_client, &statuses).await?);
    }

    let active_filter_str = if !active_filters.is_empty() {
        format!("WHERE {}", active_filters.join(" AND "))
    } else {
        String::new()
    };

    // Perform queries
    let start_time = Instant::now();
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Finding invocations...");

    let (mut results, count_estimate) =
        find_and_count_active_invocations(&sql_client, &active_filter_str, order_by, opts.limit)
            .await?;

    // Render Output UI
    progress.finish_and_clear();

    // Truncate the output to fit the requested limit
    results.truncate(opts.limit);

    let mut f = Formatter::new();
    f.list("invocations", &results)?;
    if let Some(inv) = results.first() {
        f.next_step(
            &format!("restate invocations describe {}", inv.id),
            "inspect the first invocation's status, progress, and journal",
        );
    }

    c_eprintln!(
        "Showing {}/{} invocations. Query took {:?}",
        results.len(),
        count_estimate,
        Styled(Style::Notice, start_time.elapsed())
    );

    f.finish()
}
