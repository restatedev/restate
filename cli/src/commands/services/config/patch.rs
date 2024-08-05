// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::Table;
use const_format::concatcp;
use restate_admin_rest_model::services::ModifyServiceRequest;
use restate_cli_util::c_println;
use restate_cli_util::ui::console::{confirm_or_exit, StyledTable};
use restate_serde_util::DurationString;

pub(super) const DURATION_EDIT_DESCRIPTION: &str = "Can be configured using the humantime format (https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) or the ISO8601.";
pub(super) const IDEMPOTENCY_RETENTION_EDIT_DESCRIPTION: &str = concatcp!(
    super::view::IDEMPOTENCY_RETENTION,
    "\n",
    DURATION_EDIT_DESCRIPTION
);
pub(super) const WORKFLOW_RETENTION_EDIT_DESCRIPTION: &str = concatcp!(
    super::view::WORKFLOW_RETENTION,
    "\n",
    DURATION_EDIT_DESCRIPTION
);

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_patch")]
pub struct Patch {
    #[clap(long, help = super::view::PUBLIC_DESCRIPTION)]
    public: Option<bool>,

    #[clap(long, alias = "idempotency_retention", help = IDEMPOTENCY_RETENTION_EDIT_DESCRIPTION)]
    idempotency_retention: Option<String>,

    #[clap(long, alias = "workflow_completion_retention", help = WORKFLOW_RETENTION_EDIT_DESCRIPTION)]
    workflow_completion_retention: Option<String>,

    /// Service name
    service: String,
}

pub async fn run_patch(State(env): State<CliEnv>, opts: &Patch) -> Result<()> {
    patch(&env, opts).await
}

async fn patch(env: &CliEnv, opts: &Patch) -> Result<()> {
    let admin_client = AdminClient::new(env).await?;
    let modify_request = ModifyServiceRequest {
        public: opts.public,
        idempotency_retention: opts
            .idempotency_retention
            .as_ref()
            .map(|s| {
                DurationString::parse_duration(s).context("Cannot parse idempotency_retention")
            })
            .transpose()?,
        workflow_completion_retention: opts
            .workflow_completion_retention
            .as_ref()
            .map(|s| {
                DurationString::parse_duration(s)
                    .context("Cannot parse workflow_completion_retention")
            })
            .transpose()?,
    };

    apply_service_configuration_patch(opts.service.clone(), admin_client, modify_request).await
}

pub(super) async fn apply_service_configuration_patch(
    service_name: String,
    admin_client: AdminClient,
    modify_request: ModifyServiceRequest,
) -> Result<()> {
    // Check if any change was made
    if modify_request.public.is_none()
        && modify_request.workflow_completion_retention.is_none()
        && modify_request.idempotency_retention.is_none()
    {
        c_println!("No changes requested");
        return Ok(());
    }

    // Print requested changes, ask for confirmation
    let mut table = Table::new_styled();
    if let Some(public) = &modify_request.public {
        table.add_kv_row("Public:", public);
    }
    if let Some(idempotency_retention) = &modify_request.idempotency_retention {
        table.add_kv_row(
            "Idempotent requests retention:",
            humantime::Duration::from(*idempotency_retention),
        );
    }
    if let Some(workflow_completion_retention) = &modify_request.workflow_completion_retention {
        table.add_kv_row(
            "Workflow retention time:",
            humantime::Duration::from(*workflow_completion_retention),
        );
    }
    c_println!("{table}");
    confirm_or_exit("Are you sure you want to apply these changes?")?;

    let _ = admin_client
        .patch_service(&service_name, modify_request)
        .await?
        .into_body()
        .await?;

    Ok(())
}
