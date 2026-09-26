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
use tracing::debug;

use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::commands::kafkaclusters::utils as kc_shared;
use crate::commands::subscriptions::kafka_cluster_from_source;
use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::util::properties::REDACTION_PLACEHOLDER;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Subscription ID
    id: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let sub = client.get_subscription(&opts.id).await?.into_body().await?;

    let mut summary = vec![
        ("id", Field::new(sub.id.to_string())),
        ("source", Field::new(sub.source.as_str())),
        ("sink", Field::new(sub.sink.as_str())),
    ];

    // Best-effort cluster resolution. Failures are logged at debug only — we
    // never want describe to fail because the cluster lookup tripped.
    if let Some(cluster_name) = kafka_cluster_from_source(&sub.source) {
        match client
            .get_kafka_cluster(&cluster_name, false)
            .await
            .map_err(anyhow::Error::from)
        {
            Ok(envelope) => match envelope.into_body().await {
                Ok(cluster) => {
                    if let Some(brokers) = kc_shared::brokers_property(&cluster.properties) {
                        summary.push(("kafka_brokers", Field::new(brokers)));
                    }
                }
                Err(e) => debug!("could not load Kafka cluster {cluster_name}: {e}"),
            },
            Err(e) => debug!("could not request Kafka cluster {cluster_name}: {e}"),
        }
    }

    let mut f = Formatter::new();

    f.title("📜", "Subscription");
    f.detail("subscription", &summary);

    if !sub.options.is_empty() {
        let mut keys: Vec<&String> = sub.options.keys().collect();
        keys.sort();
        let rows: Vec<Vec<Field>> = keys
            .into_iter()
            .map(|k| {
                let v = &sub.options[k];
                let value = if v == REDACTION_PLACEHOLDER {
                    Field::styled(v.as_str(), Style::Warn)
                } else {
                    Field::new(v.as_str())
                };
                vec![Field::new(k.as_str()), value]
            })
            .collect();

        f.title("⚙️", "Options");
        f.table("options", &["key", "value"], &rows);
    }

    f.finish()
}
