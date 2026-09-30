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
use serde_json::Value;

use restate_cli_util::ui::watcher::Watch;

use super::{RuleRow, concurrency_field, disabled_field};
use crate::cli_env::CliEnv;
use crate::clients::DataFusionHttpClient;
use crate::ui::datetime::DateTimeExt;
use crate::ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter};

/// List the concurrency-limit rules
///
/// Shows each rule's pattern, concurrency limit and whether it's disabled; --extra adds the
/// description, version and last modification time.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[command(after_help = after_help!(
    examples: [
        "restate rules list --extra",
        "restate rules list --json",
    ],
    learn_more: "https://docs.restate.dev/services/flow-control",
))]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Show additional columns (description, version, last modified)
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

    let mut headers = vec!["pattern", "concurrency", "disabled"];
    if opts.extra {
        headers.extend(["description", "version", "last_modified"]);
    }
    let table_rows: Vec<Vec<Field>> = rows
        .iter()
        .map(|row| {
            let mut cells = vec![
                Field::new(row.pattern.as_str()),
                concurrency_field(row.concurrency),
                disabled_field(row.disabled),
            ];
            if opts.extra {
                cells.extend([
                    Field::new(row.description.as_deref()),
                    Field::new(row.version),
                    row.last_modified.map_or(Field::new(Value::Null), |t| {
                        Field::with_display(t.iso(), t.display())
                    }),
                ]);
            }
            cells
        })
        .collect();

    let mut f = Formatter::new();
    f.table(
        "rules",
        &headers,
        &table_rows,
        IfEmpty::Say("No rules defined."),
    );
    if !opts.extra && !rows.is_empty() {
        f.next_step(
            "restate rules list --extra",
            "also see the rules' descriptions, versions and last modified times",
            IncludeFormatting::Yes,
        );
    }
    f.finish()
}
