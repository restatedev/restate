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
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{CliContext, exit};

use crate::cli_env::CliEnv;
use crate::commands::state::util::{as_json, get_current_state, pretty_print_json_object};
use crate::ui::fmt::{Field, Formatter, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_get")]
pub struct Get {
    /// Don't try to convert the values to a UTF-8 string
    #[clap(long, alias = "bin")]
    binary: bool,

    /// Only print the retrieved state as a JSON
    #[clap(long, short)]
    plain: bool,

    /// service name
    service: String,

    /// service key
    key: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_get(State(env): State<CliEnv>, opts: &Get) -> Result<()> {
    opts.watch.run(|| get(&env, opts)).await
}

async fn get(env: &CliEnv, opts: &Get) -> Result<()> {
    let current_state = get_current_state(env, &opts.service, &opts.key, true).await?;
    if current_state.is_empty() {
        return Err(
            exit::NotFound(format!("State not found for {}/{}", opts.service, opts.key)).into(),
        );
    }
    let current_state_json = as_json(current_state, opts.binary)?;

    // `--plain` prints the raw JSON document as-is, regardless of `--json`.
    if opts.plain {
        c_println!("{current_state_json}");
        return Ok(());
    }

    let mut f = Formatter::new();
    f.title("🤖", "State");

    if CliContext::get().json_output() {
        // Humans typed the service and key; scripts get them echoed back.
        f.detail(
            "info",
            &[
                ("service", Field::new(opts.service.as_str())),
                ("key", Field::new(opts.key.as_str())),
            ],
        );
        // Emit the real, structured state value so scripts get native JSON.
        f.value("state", Field::json(current_state_json));
    } else {
        // Human output keeps the familiar KEY / VALUE table with pretty-printed values.
        let pretty_json = pretty_print_json_object(&current_state_json)?;
        let rows: Vec<Vec<Field>> = pretty_json
            .into_iter()
            .map(|(k, v)| vec![Field::new(k), Field::new(v)])
            .collect();
        f.table("state", &["key", "value"], &rows);
    }

    f.finish()
}
