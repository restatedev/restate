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
use serde::Serialize;

use restate_cli_util::CliContext;
use restate_cli_util::c_eprintln;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;

use super::{RuleRow, render_concurrency};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};
use crate::ui::invocations::short_ago;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Show additional details (version, last modified)
    #[clap(long, short = 'x')]
    extra: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = DataFusionHttpClient::new(env).await?;
    let rows: Vec<RuleRow> = client
        .run_json_query(
            "SELECT pattern, concurrency, description, disabled, version, last_modified \
             FROM sys_rules ORDER BY pattern"
                .to_string(),
        )
        .await?;

    if rows.is_empty() && !CliContext::get().json_output() {
        c_eprintln!("No rules defined.");
        return Ok(());
    }

    let items: Vec<RuleItem> = rows
        .into_iter()
        .map(|row| RuleItem {
            row,
            extra: opts.extra,
        })
        .collect();

    let mut f = Formatter::new();
    f.list("rules", &items)?;
    f.finish()
}

/// A rule row: `[pattern]`, its limit and whether it's enabled, with the description
/// (and, with `--extra`, version and last modification) as details.
#[derive(Serialize)]
struct RuleItem {
    #[serde(flatten)]
    row: RuleRow,
    #[serde(skip)]
    extra: bool,
}

impl ListItem for RuleItem {
    const HEADERS: &'static [&'static str] = &["rule", "limit", "enabled"];

    fn columns(&self) -> Vec<Field> {
        let enabled = if self.row.disabled {
            Field::styled("no", Style::Warn)
        } else {
            Field::new("yes")
        };
        vec![
            Field::new(format!("[{}]", self.row.pattern)),
            Field::new(render_concurrency(self.row.concurrency)),
            enabled,
        ]
    }

    fn details(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .row
            .description
            .iter()
            .filter(|d| !d.is_empty())
            .cloned()
            .collect();
        if self.extra {
            let mut line = format!("version {}", self.row.version);
            if let Some(modified) = self.row.last_modified {
                line.push_str(&format!(" · modified {}", short_ago(modified)));
            }
            lines.push(line);
        }
        lines
    }
}
