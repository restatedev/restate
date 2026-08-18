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
use crate::clients::{AdminClient, AdminClientInterface};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_resume")]
pub struct Resume {
    /// Virtual queue ID
    vqueue_id: String,
}

pub async fn run_resume(State(env): State<CliEnv>, opts: &Resume) -> Result<()> {
    let client = AdminClient::new(&env).await?;
    client
        .resume_vqueue(&opts.vqueue_id)
        .await?
        .success_or_error()?;

    c_success!("Resumed virtual queue {}", opts.vqueue_id);
    Ok(())
}
