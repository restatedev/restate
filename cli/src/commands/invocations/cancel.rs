// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::find_active_invocations_simple;
use crate::clients::{self, AdminClientInterface};
use crate::ui::console::{confirm_or_exit, Styled};
use crate::ui::invocations::render_simple_invocation_list;
use crate::ui::stylesheet::Style;
use crate::{c_println, c_success};

use anyhow::{bail, Result};
use cling::prelude::*;
use restate_types::identifiers::InvocationId;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_cancel")]
#[clap(visible_alias = "rm")]
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
    query: String,
    /// Ungracefully kill the invocation and its children
    #[clap(long)]
    kill: bool,
}

pub async fn run_cancel(State(env): State<CliEnv>, opts: &Cancel) -> Result<()> {
    let client = clients::AdminClient::new(&env).await?;
    let sql_client = clients::DataFusionHttpClient::from(client.clone());

    let q = opts.query.trim();
    let filter = if let Ok(id) = q.parse::<InvocationId>() {
        format!("id = '{}'", id)
    } else {
        match q.find('/').unwrap_or_default() {
            0 => format!("target LIKE '{}/%'", q),
            // If there's one slash, let's add the wildcard depending on the service type,
            // so we discriminate correctly with serviceName/handlerName with workflowName/workflowKey
            1 => format!("(target = '{}' AND target_service_ty = 'service') OR (target LIKE '{}/%' AND target_service_ty != 'service'))", q, q),
            // Can only be exact match here
            _ => format!("target LIKE '{}'", q),
        }
    };

    let invocations = find_active_invocations_simple(&sql_client, &filter).await?;
    if invocations.is_empty() {
        bail!("No invocations found for query {}!", opts.query);
    };

    render_simple_invocation_list(&env, &invocations);

    // Get the invocation and confirm
    let prompt = format!(
        "Are you sure you want to {} these invocations?",
        if opts.kill {
            Styled(Style::Danger, "kill")
        } else {
            Styled(Style::Warn, "cancel")
        },
    );
    confirm_or_exit(&env, &prompt)?;

    for inv in invocations {
        let result = client.cancel_invocation(&inv.id, opts.kill).await?;
        let _ = result.success_or_error()?;
    }

    c_println!();
    c_success!("Request was sent successfully");

    Ok(())
}
