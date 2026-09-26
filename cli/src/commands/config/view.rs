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

use restate_cli_util::CliContext;
use restate_cli_util::c_println;

use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::{cli_env::CliEnv, console};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
pub struct View {}

pub async fn run_view(State(env): State<CliEnv>, _opts: &View) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        String::new()
    };

    if CliContext::get().json_output() {
        // Convert the TOML config into JSON so `config view --json` is machine-readable.
        let value: serde_json::Value = toml::from_str(&config_data)?;
        let mut f = Formatter::new();
        f.value("config", Field::json(value));
        return f.finish();
    }

    console::c_eprintln!("Dumping {}:\n", env.config_file.display());
    c_println!("{}", config_data);

    Ok(())
}
