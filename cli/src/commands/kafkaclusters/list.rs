// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::SystemTime;

use anyhow::Result;
use chrono::{DateTime, Local};
use cling::prelude::*;
use serde::Serialize;

use restate_cli_util::CliContext;
use restate_cli_util::c_error;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};
use crate::ui::invocations::short_ago;

use super::utils::brokers_property;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[clap(visible_alias = "ls")]
pub struct List {
    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env)).await
}

async fn list(env: &CliEnv) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let mut clusters = client
        .list_kafka_clusters()
        .await?
        .into_body()
        .await?
        .clusters;

    if clusters.is_empty() && !CliContext::get().json_output() {
        c_error!("No Kafka clusters registered.");
        return Ok(());
    }

    clusters.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));

    let items: Vec<KafkaClusterItem> = clusters
        .into_iter()
        .map(|cluster| KafkaClusterItem {
            brokers: brokers_property(&cluster.properties).map(str::to_owned),
            properties: cluster.properties.len(),
            created_at: cluster.created_at.to_string(),
            created: SystemTime::from(cluster.created_at).into(),
            name: cluster.name.as_str().to_owned(),
        })
        .collect();

    let mut f = Formatter::new();
    f.list("kafka_clusters", &items)?;
    f.finish()
}

/// A Kafka cluster row: `[name]` and its age, with brokers and properties as details.
#[derive(Serialize)]
struct KafkaClusterItem {
    name: String,
    brokers: Option<String>,
    properties: usize,
    created_at: String,
    #[serde(skip)]
    created: DateTime<Local>,
}

impl ListItem for KafkaClusterItem {
    const HEADERS: &'static [&'static str] = &["cluster", "created"];

    fn columns(&self) -> Vec<Field> {
        vec![
            Field::new(format!("[{}]", self.name)),
            Field::new(short_ago(self.created)),
        ]
    }

    fn details(&self) -> Vec<String> {
        vec![format!(
            "brokers {} · {} properties",
            self.brokers.as_deref().unwrap_or("-"),
            self.properties
        )]
    }
}
