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
use crate::clients::datafusion_helpers::get_invocation;
use crate::clients::{self, MetaClientInterface};
use crate::ui::console::{confirm_or_exit, Styled};
use crate::ui::invocations::render_invocation_compact;
use crate::ui::stylesheet::Style;
use crate::{c_println, c_success};

use anyhow::{bail, Result};
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_cancel")]
#[clap(visible_alias = "rm")]
pub struct Cancel {
    /// The ID of the parent invocation to cancel
    invocation_id: String,
    /// Ungracefully kill the invocation and its children
    #[clap(long)]
    kill: bool,
}

pub async fn run_cancel(State(env): State<CliEnv>, opts: &Cancel) -> Result<()> {
    let client = crate::clients::MetasClient::new(&env)?;
    let sql_client = clients::DataFusionHttpClient::new(&env)?;
    let Some(inv) = get_invocation(&sql_client, &opts.invocation_id).await? else {
        bail!("Invocation {} not found!", opts.invocation_id);
    };

    render_invocation_compact(&env, &inv);
    // Get the invocation and confirm
    let prompt = format!(
        "Are you sure you want to {} this invocation",
        if opts.kill {
            Styled(Style::Danger, "kill")
        } else {
            Styled(Style::Warn, "cancel")
        }
    );
    confirm_or_exit(&env, &prompt)?;

    let result = client.cancel_invocation(&inv.id, opts.kill).await?;
    let _ = result.success_or_error()?;

    c_println!();
    c_success!("Request was sent successfully");

    Ok(())
}
