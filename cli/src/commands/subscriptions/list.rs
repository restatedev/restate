// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::Result;
use cling::prelude::*;
use serde::Serialize;

use restate_cli_util::CliContext;
use restate_cli_util::c_error;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    /// Filter by exact source URI (e.g. `kafka://my-cluster/orders`)
    #[clap(long)]
    source: Option<String>,

    /// Filter by exact sink URI (e.g. `service://Counter/count`)
    #[clap(long)]
    sink: Option<String>,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, opts: &List) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let subs = client
        .list_subscriptions(opts.sink.as_deref(), opts.source.as_deref())
        .await?
        .into_body()
        .await?
        .subscriptions;

    if subs.is_empty() && !CliContext::get().json_output() {
        c_error!("No subscriptions registered.");
        return Ok(());
    }

    let mut subs: Vec<SubscriptionItem> = subs
        .into_iter()
        .map(|sub| SubscriptionItem {
            id: sub.id.to_string(),
            source: sub.source,
            sink: sub.sink,
            options: sub.options.into_iter().collect(),
        })
        .collect();
    subs.sort_by(|a, b| a.id.cmp(&b.id));

    let mut f = Formatter::new();
    f.list("subscriptions", &subs)?;
    f.finish()
}

/// A subscription row: `[id] source → sink`, with its options as detail lines.
#[derive(Serialize)]
struct SubscriptionItem {
    id: String,
    source: String,
    sink: String,
    options: BTreeMap<String, String>,
}

impl ListItem for SubscriptionItem {
    const HEADERS: &'static [&'static str] = &["subscription"];

    fn columns(&self) -> Vec<Field> {
        vec![Field::new(format!(
            "[{}] {} → {}",
            self.id, self.source, self.sink
        ))]
    }

    fn details(&self) -> Vec<String> {
        self.options
            .iter()
            .map(|(k, v)| format!("{k} = {v}"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn item() -> SubscriptionItem {
        SubscriptionItem {
            id: "sub_1".to_owned(),
            source: "kafka://c/orders".to_owned(),
            sink: "service://Counter/count".to_owned(),
            options: [("b", "2"), ("a", "1")]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        }
    }

    #[test]
    fn json_keeps_fields_and_options() {
        assert_eq!(
            serde_json::to_value(item()).unwrap(),
            json!({
                "id": "sub_1",
                "source": "kafka://c/orders",
                "sink": "service://Counter/count",
                "options": {"a": "1", "b": "2"},
            })
        );
    }

    #[test]
    fn details_list_options_sorted() {
        assert_eq!(item().details(), ["a = 1", "b = 2"]);
    }
}
