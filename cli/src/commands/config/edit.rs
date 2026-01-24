// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{cli_env::CliEnv, console};
use anyhow::Result;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
pub struct Edit {}

pub async fn run_edit(State(env): State<CliEnv>, _opts: &Edit) -> Result<()> {
    console::_gecho!(@nl_with_prefix, ("📝"), stderr, "Editing {}", env.config_file.display());

    env.open_default_editor(&env.config_file)?;

    Ok(())
}
