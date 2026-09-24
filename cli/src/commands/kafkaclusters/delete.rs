// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Result, bail};
use cling::prelude::*;

use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_success, c_warn};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Kafka cluster name
    name: String,

    /// Delete the cluster even if subscriptions still reference it. Those
    /// subscriptions will be orphaned and will stop consuming.
    #[clap(long)]
    force: bool,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let client = AdminClient::new(&env).await?;

    let cluster = client
        .get_kafka_cluster(&opts.name, true)
        .await?
        .into_body()
        .await?;

    let json = CliContext::get().json_output();
    let mut f = Formatter::new();
    f.detail(
        "kafka_cluster",
        &[
            ("name", Field::new(cluster.name.as_str())),
            ("subscriptions", Field::new(cluster.subscriptions.len())),
        ],
    );

    if !cluster.subscriptions.is_empty() && !opts.force {
        bail!(
            "Cluster {} has {} subscription(s) attached. Re-run with {} to orphan them.",
            Styled(Style::Info, &opts.name),
            cluster.subscriptions.len(),
            Styled(Style::Notice, "--force"),
        );
    }

    if !cluster.subscriptions.is_empty() {
        c_warn!(
            "{} subscription(s) will be orphaned and stop consuming.",
            cluster.subscriptions.len()
        );
    }

    if json {
        f.table(
            "changes",
            &["kafka_cluster", "change"],
            &[vec![Field::new(opts.name.as_str()), Field::new("delete")]],
        );
    }
    f.confirm(
        &opts.dry_run,
        &format!(
            "Are you sure you want to delete Kafka cluster {}?",
            opts.name
        ),
    )?;

    client
        .delete_kafka_cluster(&opts.name, opts.force)
        .await?
        .success_or_error()?;

    if !json {
        c_success!("Kafka cluster {} deleted", &opts.name);
    }
    f.next_step(
        "restate kafka-clusters list",
        "see the remaining Kafka clusters",
    );
    f.finish()
}
