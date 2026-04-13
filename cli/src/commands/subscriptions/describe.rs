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
use comfy_table::Table;
use tracing::debug;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::commands::kafkaclusters::utils as kc_shared;
use crate::commands::subscriptions::kafka_cluster_from_source;

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

    let mut summary = Table::new_styled();
    summary.add_kv_row("ID:", sub.id.to_string());
    summary.add_kv_row("Source:", &sub.source);
    summary.add_kv_row("Sink:", &sub.sink);

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
                        summary.add_kv_row("Kafka brokers:", brokers);
                    }
                }
                Err(e) => debug!("could not load Kafka cluster {cluster_name}: {e}"),
            },
            Err(e) => debug!("could not request Kafka cluster {cluster_name}: {e}"),
        }
    }

    c_title!("📜", "Subscription");
    c_println!("{summary}");

    if !sub.options.is_empty() {
        c_println!();
        c_title!("⚙️", "Options");
        c_println!("{}", kc_shared::render_properties_table(&sub.options));
    }

    Ok(())
}
