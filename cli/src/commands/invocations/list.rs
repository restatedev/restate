// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::time::Instant;

use crate::c_eprintln;
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    find_active_invocations, find_inbox_invocations, InvocationState,
};
use crate::ui::console::Styled;
use crate::ui::invocations::render_invocation_compact;
use crate::ui::stylesheet::Style;
use crate::ui::watcher::Watch;

use anyhow::Result;
use cling::prelude::*;
use indicatif::ProgressBar;
use itertools::Itertools;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Component to list invocations for
    #[clap(long, visible_alias = "component", value_delimiter = ',')]
    component: Vec<String>,
    /// Filter by invocation on this handler name
    #[clap(long, value_delimiter = ',')]
    handler: Vec<String>,
    /// Filter by status(es)
    #[clap(long, ignore_case = true, value_delimiter = ',')]
    status: Vec<InvocationState>,
    /// Filter by deployment ID
    #[clap(long, visible_alias = "dp", value_delimiter = ',')]
    deployment: Vec<String>,
    /// Only list invocations on keyed components only
    #[clap(long)]
    virtual_objects_only: bool,
    /// Filter by invocations on this component key
    #[clap(long, value_delimiter = ',')]
    key: Vec<String>,
    /// Limit the number of results
    #[clap(long, default_value = "100")]
    limit: usize,
    /// Find zombie invocations (invocations pinned to removed deployments)
    #[clap(long)]
    zombie: bool,
    /// Order the results by older invocations first
    #[clap(long)]
    oldest_first: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    let mut total: usize = 0;
    let statuses: HashSet<InvocationState> = HashSet::from_iter(opts.status.clone());
    // Prepare filters
    let mut inbox_filters: Vec<String> = vec![]; // "WHERE 1 = 1\n".to_string();
    let mut active_filters: Vec<String> = vec![];
    let mut post_filters: Vec<String> = vec![];

    let order_by = if opts.oldest_first {
        "ORDER BY ss.created_at ASC"
    } else {
        "ORDER BY ss.created_at DESC"
    };

    // query inbox?
    let should_query_inbox = (statuses.is_empty()
        || opts.status.contains(&InvocationState::Pending)
        || opts.status.contains(&InvocationState::Unknown))
        // Don't query inbox if deployment is set
        && (opts.deployment.is_empty())
        && !opts.zombie;

    // query active?
    let should_query_active = statuses.is_empty()
        || opts.status.contains(&InvocationState::Unknown)
        || !(opts.status.contains(&InvocationState::Pending) && statuses.len() == 1);

    if !opts.component.is_empty() {
        inbox_filters.push(format!(
            "ss.component IN ({})",
            opts.component
                .iter()
                .map(|x| format!("'{}'", x))
                .format(",")
        ));
    }

    if !opts.handler.is_empty() {
        inbox_filters.push(format!(
            "ss.handler IN ({})",
            opts.handler.iter().map(|x| format!("'{}'", x)).format(",")
        ));
    }

    if !opts.key.is_empty() {
        inbox_filters.push(format!(
            "ss.component_key IN ({})",
            opts.key.iter().map(|x| format!("'{}'", x)).format(",")
        ));
    }

    if opts.virtual_objects_only {
        inbox_filters.push("comp.ty = 'virtual_object'".to_owned());
    }

    if opts.zombie {
        // zombies cannot be in pending, don't query inbox.
        active_filters.push("dp.id IS NULL".to_owned());
    }

    // Only makes sense when querying active invocations;
    if !opts.deployment.is_empty() {
        active_filters.push(format!(
            "(ss.pinned_deployment_id IN ({0}) OR sis.last_attempt_deployment_id IN ({0}))",
            opts.deployment.iter().map(|x| format!("'{}'", x)).join(",")
        ));
    }

    // This is a post-filter as we filter by calculated column
    if !statuses.is_empty() {
        post_filters.push(format!(
            "combined_status IN ({})",
            statuses.iter().map(|x| format!("'{}'", x)).format(",")
        ));
    }

    let inbox_filter_str = if !inbox_filters.is_empty() {
        format!("WHERE {}", inbox_filters.join(" AND "))
    } else {
        String::new()
    };

    let active_filter_str = if !active_filters.is_empty() {
        format!(
            "WHERE {}",
            itertools::chain(inbox_filters, active_filters).join(" AND ")
        )
    } else {
        inbox_filter_str.clone()
    };

    let post_filter_str = if !post_filters.is_empty() {
        format!("WHERE {}", post_filters.join(" AND "))
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

    let mut results = vec![];
    // - Inbox invocations are always newer than active ones.
    if opts.oldest_first {
        // query active first
        if should_query_active {
            let (res, full_count) = find_active_invocations(
                &sql_client,
                &active_filter_str,
                &post_filter_str,
                order_by,
                opts.limit,
            )
            .await?;
            total += full_count;
            results.extend(res);
        }
        if should_query_inbox {
            let (res, full_count) =
                find_inbox_invocations(&sql_client, &inbox_filter_str, order_by, opts.limit)
                    .await?;
            total += full_count;
            results.extend(res);
        }
    } else {
        // query inbox first
        if should_query_inbox {
            let (res, full_count) =
                find_inbox_invocations(&sql_client, &inbox_filter_str, order_by, opts.limit)
                    .await?;
            total += full_count;
            results.extend(res);
        }
        if should_query_active {
            let (res, full_count) = find_active_invocations(
                &sql_client,
                &active_filter_str,
                &post_filter_str,
                order_by,
                opts.limit,
            )
            .await?;

            results.extend(res);
            total += full_count;
        }
    }

    // Render Output UI
    progress.finish_and_clear();

    // Sample of active invocations
    if !results.is_empty() {
        // Truncate the output to fit the requested limit
        results.truncate(opts.limit);
        for inv in &results {
            render_invocation_compact(env, inv);
        }
    }

    c_eprintln!(
        "Showing {}/{} invocations. Query took {:?}",
        results.len(),
        total,
        Styled(Style::Notice, start_time.elapsed())
    );

    Ok(())
}
