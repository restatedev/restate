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

use restate_cli_util::c_println;

use crate::{cli_env::CliEnv, console};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
pub struct View {}

pub async fn run_view(State(env): State<CliEnv>, _opts: &View) -> Result<()> {
    console::_gecho!(@nl_with_prefix, ("📝"), stderr, "Dumping {}:\n", env.config_file.display());

    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    c_println!("{}", config_data);

    Ok(())
}
