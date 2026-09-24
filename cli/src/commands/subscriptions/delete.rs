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

use restate_cli_util::{CliContext, c_success};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_delete")]
#[clap(visible_alias = "rm", alias = "remove")]
pub struct Delete {
    /// Subscription ID
    id: String,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_delete(State(env): State<CliEnv>, opts: &Delete) -> Result<()> {
    let client = AdminClient::new(&env).await?;
    let sub = client.get_subscription(&opts.id).await?.into_body().await?;

    let json = CliContext::get().json_output();
    let mut f = Formatter::new();
    f.detail(
        "subscription",
        &[
            ("id", Field::new(sub.id.to_string())),
            ("source", Field::new(sub.source.as_str())),
            ("sink", Field::new(sub.sink.as_str())),
        ],
    );
    if json {
        f.table(
            "changes",
            &["subscription_id", "change"],
            &[vec![Field::new(sub.id.to_string()), Field::new("delete")]],
        );
    }
    f.confirm(&opts.dry_run, &format!("Delete subscription {}?", opts.id))?;

    client
        .delete_subscription(&opts.id)
        .await?
        .success_or_error()?;

    if !json {
        c_success!("Subscription {} deleted", &opts.id);
    }
    f.next_step(
        "restate subscriptions list",
        "see the remaining subscriptions",
    );
    f.finish()
}
