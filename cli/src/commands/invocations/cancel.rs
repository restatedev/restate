// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_cli_util::ui::console::{Styled, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_error, c_println, c_success};
use restate_types::identifiers::InvocationId;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{InvocationState, find_active_invocations_simple};
use crate::clients::{self, AdminClientInterface};
use crate::ui::invocations::render_simple_invocation_list;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_cancel")]
pub struct Cancel {
    /// Either an invocation id, or a target string exact match or prefix, e.g.:
    /// * `invocationId`
    /// * `serviceName`
    /// * `serviceName/handler`
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `virtualObjectName/key/handler`
    /// * `workflowName`
    /// * `workflowName/key`
    /// * `workflowName/key/handler`
    pub(super) query: String,
    /// Ungracefully kill the invocation and its children
    #[clap(long)]
    pub(super) kill: bool,
}

pub async fn run_cancel(State(env): State<CliEnv>, opts: &Cancel) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let q = opts.query.trim();
    let filter = if let Ok(id) = q.parse::<InvocationId>() {
        format!("id = '{id}'")
    } else {
        let query = match q.find('/').unwrap_or_default() {
            0 => format!("target LIKE '{q}/%' "),
            // If there's one slash, let's add the wildcard depending on the service type,
            // so we discriminate correctly with serviceName/handlerName with workflowName/workflowKey
            1 => format!(
                "(target = '{q}' AND target_service_ty = 'service') OR (target LIKE '{q}/%' AND target_service_ty != 'service'))"
            ),
            // Can only be exact match here
            _ => format!("target LIKE '{q}'"),
        };
        format!("{query} AND status != 'completed'")
    };

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!(
            "No invocations found for query {}! Note that the cancel command only works on non-completed invocations. \
            If you want to remove a completed invocation, consider using the purge command instead.",
            opts.query
        );
    };

    render_simple_invocation_list(&invocations);

    if invocations
        .iter()
        .all(|inv| matches!(inv.status, InvocationState::Completed))
    {
        // Should only happen with explicit invocation ids, as we filter the completed state out otherwise
        c_error!(
            "The invocations matching your query are completed; cancel/kill has no effect on them. \
            If you want to remove a completed invocation, consider using the purge command instead.",
        );
        return Ok(());
    }

    // Get the invocation and confirm
    let prompt = format!(
        "Are you sure you want to {} these invocations?",
        if opts.kill {
            Styled(Style::Danger, "kill")
        } else {
            Styled(Style::Warn, "cancel")
        },
    );
    confirm_or_exit(&prompt)?;

    for inv in invocations {
        let result = client.cancel_invocation(&inv.id, opts.kill).await?;
        let _ = result.success_or_error()?;
    }

    c_println!();
    c_success!("Request was sent successfully");

    Ok(())
}
