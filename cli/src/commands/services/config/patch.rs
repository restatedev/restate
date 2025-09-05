// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use const_format::concatcp;

use restate_admin_rest_model::services::ModifyServiceRequest;
use restate_cli_util::c_println;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_time_util::{DurationExt, FriendlyDuration};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};

pub(super) const DURATION_EDIT_DESCRIPTION: &str = "Can be configured using a human friendly \
    duration format (e.g. 5d 1h 30m 15s) or ISO8601.";
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
pub(super) const JOURNAL_RETENTION_EDIT_DESCRIPTION: &str = concatcp!(
    super::view::JOURNAL_RETENTION,
    "\n",
    DURATION_EDIT_DESCRIPTION
);
pub(super) const INACTIVITY_TIMEOUT_EDIT_DESCRIPTION: &str = concatcp!(
    super::view::INACTIVITY_TIMEOUT,
    "\n",
    DURATION_EDIT_DESCRIPTION
);
pub(super) const ABORT_TIMEOUT_EDIT_DESCRIPTION: &str =
    concatcp!(super::view::ABORT_TIMEOUT, "\n", DURATION_EDIT_DESCRIPTION);

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_patch")]
pub struct Patch {
    #[clap(long, help = super::view::PUBLIC_DESCRIPTION)]
    public: Option<bool>,

    #[clap(long, alias = "idempotency_retention", help = IDEMPOTENCY_RETENTION_EDIT_DESCRIPTION)]
    idempotency_retention: Option<FriendlyDuration>,

    #[clap(long, alias = "workflow_completion_retention", help = WORKFLOW_RETENTION_EDIT_DESCRIPTION)]
    workflow_completion_retention: Option<FriendlyDuration>,

    #[clap(long, alias = "journal_retention", help = JOURNAL_RETENTION_EDIT_DESCRIPTION)]
    journal_retention: Option<FriendlyDuration>,

    #[clap(long, alias = "inactivity_timeout", help = INACTIVITY_TIMEOUT_EDIT_DESCRIPTION)]
    inactivity_timeout: Option<FriendlyDuration>,

    #[clap(long, alias = "abort_timeout", help = ABORT_TIMEOUT_EDIT_DESCRIPTION)]
    abort_timeout: Option<FriendlyDuration>,

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
        idempotency_retention: opts.idempotency_retention.map(FriendlyDuration::to_std),
        workflow_completion_retention: opts
            .workflow_completion_retention
            .map(FriendlyDuration::to_std),
        journal_retention: opts.journal_retention.map(FriendlyDuration::to_std),
        inactivity_timeout: opts.inactivity_timeout.map(FriendlyDuration::to_std),
        abort_timeout: opts.abort_timeout.map(FriendlyDuration::to_std),
    };

    apply_service_configuration_patch(&opts.service, admin_client, modify_request).await
}

pub(super) async fn apply_service_configuration_patch(
    service_name: &str,
    admin_client: AdminClient,
    modify_request: ModifyServiceRequest,
) -> Result<()> {
    // Check if any change was made
    if modify_request.public.is_none()
        && modify_request.workflow_completion_retention.is_none()
        && modify_request.idempotency_retention.is_none()
        && modify_request.inactivity_timeout.is_none()
        && modify_request.journal_retention.is_none()
        && modify_request.abort_timeout.is_none()
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
            idempotency_retention.friendly().to_days_span(),
        );
    }
    if let Some(workflow_completion_retention) = &modify_request.workflow_completion_retention {
        table.add_kv_row(
            "Workflow retention:",
            workflow_completion_retention.friendly().to_days_span(),
        );
    }
    if let Some(journal_retention) = &modify_request.journal_retention {
        table.add_kv_row(
            "Journal retention:",
            journal_retention.friendly().to_days_span(),
        );
    }
    if let Some(inactivity_timeout) = &modify_request.inactivity_timeout {
        table.add_kv_row(
            "Inactivity timeout:",
            inactivity_timeout.friendly().to_days_span(),
        );
    }
    if let Some(abort_timeout) = &modify_request.abort_timeout {
        table.add_kv_row("Abort timeout:", abort_timeout.friendly().to_days_span());
    }
    c_println!("{table}");
    confirm_or_exit("Are you sure you want to apply these changes?")?;

    let _ = admin_client
        .patch_service(service_name, modify_request)
        .await?
        .into_body()
        .await?;

    Ok(())
}
