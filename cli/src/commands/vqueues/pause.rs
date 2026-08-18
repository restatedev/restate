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

use restate_cli_util::c_success;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_pause")]
pub struct Pause {
    /// Virtual queue ID
    vqueue_id: String,
}

pub async fn run_pause(State(env): State<CliEnv>, opts: &Pause) -> Result<()> {
    // Validate that the vqueue exists
    let client = DataFusionHttpClient::new(&env).await?;
    super::get_vqueue(&client, &opts.vqueue_id).await?;

    let client = AdminClient::new(&env).await?;
    client
        .pause_vqueue(&opts.vqueue_id)
        .await?
        .success_or_error()?;

    c_success!("Paused virtual queue {}", opts.vqueue_id);
    Ok(())
}
