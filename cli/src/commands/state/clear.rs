// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::{Result, bail};
use cling::prelude::*;
use itertools::Itertools;
use serde_json::Value;

use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_println};

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::get_state_keys;
use crate::commands::state::util::{compute_version, update_state};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_clear")]
pub struct Clear {
    /// A string with either service name and key, or only the service name, e.g.:
    /// * `virtualObjectName`
    /// * `virtualObjectName/key`
    /// * `workflowName`
    /// * `workflowName/key`
    query: String,

    /// Force means, ignore the current version
    #[clap(long, short)]
    force: bool,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_clear(State(env): State<CliEnv>, opts: &Clear) -> Result<()> {
    clear(&env, opts).await
}

/// Columns of the `changes` plan table (one row per service key to clear).
const CHANGE_HEADERS: [&str; 4] = ["service", "key", "state_keys", "operation"];

async fn clear(env: &CliEnv, opts: &Clear) -> Result<()> {
    let sql_client = crate::clients::DataFusionHttpClient::new(env).await?;

    let (svc, key) = match opts.query.split_once('/') {
        None => (opts.query.as_str(), None),
        Some((svc, key)) => (svc, Some(key)),
    };

    #[allow(clippy::mutable_key_type)]
    let services_state = get_state_keys(&sql_client, svc, key).await?;
    let json = CliContext::get().json_output();
    if services_state.is_empty() {
        if !json {
            bail!("No state found!");
        }
        let mut f = Formatter::new();
        f.table("changes", &CHANGE_HEADERS, &[] as &[Vec<Field>]);
        return f.finish();
    }

    let mut f = Formatter::new();
    let rows: Vec<Vec<Field>> = services_state
        .iter()
        .sorted_by(|(a, _), (b, _)| (&a.service_name, &a.key).cmp(&(&b.service_name, &b.key)))
        .map(|(svc_id, svc_state)| {
            let state_keys: Vec<&String> = svc_state.keys().sorted().collect();
            vec![
                Field::styled(svc_id.service_name.to_string(), Style::Info),
                Field::styled(svc_id.key.to_string(), Style::Info),
                Field::with_display(
                    Value::from_iter(state_keys.iter().map(|k| Value::from(k.as_str()))),
                    format!("[{}]", state_keys.iter().join(", ")),
                ),
                Field::new("clear"),
            ]
        })
        .collect();
    f.table("changes", &CHANGE_HEADERS, &rows);

    if !json {
        c_println!();
        c_println!(
            "Going to {} all the aforementioned state entries.",
            Styled(Style::Danger, "remove")
        );
        c_println!("About to submit the new state mutation to the system for processing.");
        c_println!(
            "If there are currently active invocations, then this mutation will be enqueued to be processed after them."
        );
        c_println!();
    }
    f.confirm(&opts.dry_run, "Are you sure?")?;

    if !json {
        c_println!();
    }

    let single_key = services_state.len() == 1;
    for (svc_id, svc_state) in services_state {
        let version = if opts.force {
            None
        } else {
            Some(compute_version(&svc_state))
        };
        update_state(
            env,
            version,
            &svc_id.service_name,
            &svc_id.key,
            HashMap::default(),
        )
        .await?;
        if single_key {
            f.next_step(
                &format!("restate state get {} {}", svc_id.service_name, svc_id.key),
                "check the state once the mutation is processed",
            );
        }
    }

    if !json {
        c_println!();
        c_println!("Enqueued successfully for processing");
    }
    f.finish()
}
