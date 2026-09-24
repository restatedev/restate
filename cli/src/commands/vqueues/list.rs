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
use restate_cli_util::c_eprintln;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;

use super::{VQUEUE_COLUMNS, VQueueRow};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};
use crate::ui::invocations::short_ago;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Limit the number of results
    #[clap(long, default_value = "100")]
    limit: usize,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = DataFusionHttpClient::new(env).await?;
    let rows: Vec<VQueueRow> = client
        .run_json_query(format!(
            "SELECT {VQUEUE_COLUMNS} FROM sys_vqueue_meta ORDER BY created_at DESC, id LIMIT {}",
            opts.limit
        ))
        .await?;

    if rows.is_empty() && !CliContext::get().json_output() {
        c_eprintln!("No virtual queues found.");
        return Ok(());
    }

    let mut f = Formatter::new();
    f.list("vqueues", &rows)?;
    f.finish()
}

impl ListItem for VQueueRow {
    const HEADERS: &'static [&'static str] = &[
        "vqueue",
        "status",
        "inbox",
        "running",
        "suspended",
        "paused_entries",
        "finished",
    ];

    fn columns(&self) -> Vec<Field> {
        let target = self
            .lock_name
            .as_deref()
            .or(self.service_name.as_deref())
            .unwrap_or("-");
        let status = if self.queue_is_paused {
            Field::styled("paused", Style::Warn)
        } else if self.is_active {
            Field::styled("active", Style::Success)
        } else {
            Field::new("inactive")
        };
        vec![
            Field::new(format!("[{}] {target}", self.id)),
            status,
            Field::new(self.num_inbox),
            Field::new(self.num_running),
            Field::new(self.num_suspended),
            Field::new(self.num_paused),
            Field::new(self.num_finished),
        ]
    }

    fn details(&self) -> Vec<String> {
        let mut times = vec![format!("created {}", short_ago(self.created_at))];
        for (label, at) in [
            ("enqueued", self.last_enqueued_at),
            ("started", self.last_start_at),
            ("finished", self.last_finish_at),
        ] {
            if let Some(at) = at {
                times.push(format!("{label} {}", short_ago(at)));
            }
        }
        let mut lines = vec![times.join(" · ")];
        let scope = [("scope", &self.scope), ("limit key", &self.limit_key)]
            .into_iter()
            .filter_map(|(label, value)| value.as_ref().map(|v| format!("{label} {v}")))
            .collect::<Vec<_>>();
        if !scope.is_empty() {
            lines.push(scope.join(" · "));
        }
        lines
    }
}
