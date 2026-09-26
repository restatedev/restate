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

use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::datetime::DateTimeExt;
use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::util::properties::REDACTION_PLACEHOLDER;

use super::utils::brokers_property;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Kafka cluster name
    name: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let cluster = client
        .get_kafka_cluster(&opts.name, true)
        .await?
        .into_body()
        .await?;

    let brokers = match brokers_property(&cluster.properties) {
        Some(b) => Field::new(b.to_string()),
        None => Field::with_display(serde_json::Value::Null, "-"),
    };
    let created_at = &cluster.created_at;

    let mut f = Formatter::new();

    f.title("📜", "Kafka Cluster");
    f.detail(
        "kafka_cluster",
        &[
            ("name", Field::new(cluster.name.as_str())),
            ("brokers", brokers),
            ("properties", Field::new(cluster.properties.len())),
            ("subscriptions", Field::new(cluster.subscriptions.len())),
            (
                "created_at",
                Field::with_display(created_at.to_string(), created_at.display()),
            ),
        ],
    );

    f.title("⚙️", "Properties");
    let mut keys: Vec<&String> = cluster.properties.keys().collect();
    keys.sort();
    let property_rows: Vec<Vec<Field>> = keys
        .into_iter()
        .map(|k| {
            let v = &cluster.properties[k];
            let value = if v == REDACTION_PLACEHOLDER {
                Field::styled(v.as_str(), Style::Warn)
            } else {
                Field::new(v.as_str())
            };
            vec![Field::new(k.as_str()), value]
        })
        .collect();
    f.table("properties", &["key", "value"], &property_rows);

    if !cluster.subscriptions.is_empty() {
        f.title("📨", "Subscriptions");
        let sub_rows: Vec<Vec<Field>> = cluster
            .subscriptions
            .into_iter()
            .map(|sub| {
                vec![
                    Field::new(sub.id.to_string()),
                    Field::new(sub.source),
                    Field::new(sub.sink),
                ]
            })
            .collect();
        f.table("subscriptions", &["id", "source", "sink"], &sub_rows);
    }

    f.finish()
}
