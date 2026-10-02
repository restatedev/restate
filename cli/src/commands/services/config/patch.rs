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
use const_format::concatcp;

use restate_admin_rest_model::services::ModifyServiceRequest;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println, c_success};
use restate_util_time::{DurationExt, FriendlyDuration};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{DryRun, Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter};

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

/// Change a service's configuration from the command line (non-interactive)
///
/// Only the options passed are changed. Durations take a human friendly format (e.g.
/// `5d 1h 30m 15s`) or ISO8601. Registering a new deployment of the service resets these settings to what the service
/// code defines, or to the server defaults.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_patch")]
#[command(after_help = after_help!(
    examples: [
        "restate services config patch Greeter --public false --dry-run",
        "restate services config patch Greeter --idempotency-retention 1d --journal-retention 1d --yes",
    ],
    learn_more: "https://docs.restate.dev/services/configuration",
))]
pub struct Patch {
    #[clap(
        long,
        help = "true: callable through the ingress; false: only from other Restate services"
    )]
    public: Option<bool>,

    #[clap(
        long,
        alias = "idempotency_retention",
        help = "How long the result and idempotency key of a completed invocation are kept",
        long_help = IDEMPOTENCY_RETENTION_EDIT_DESCRIPTION
    )]
    idempotency_retention: Option<FriendlyDuration>,

    #[clap(
        long,
        alias = "workflow_completion_retention",
        help = "How long a completed workflow's result, state and promises are kept",
        long_help = WORKFLOW_RETENTION_EDIT_DESCRIPTION
    )]
    workflow_completion_retention: Option<FriendlyDuration>,

    #[clap(
        long,
        alias = "journal_retention",
        help = "How long the journal of a completed invocation is kept",
        long_help = JOURNAL_RETENTION_EDIT_DESCRIPTION
    )]
    journal_retention: Option<FriendlyDuration>,

    #[clap(
        long,
        alias = "inactivity_timeout",
        help = "How long an invocation can make no progress before Restate asks it to suspend",
        long_help = INACTIVITY_TIMEOUT_EDIT_DESCRIPTION
    )]
    inactivity_timeout: Option<FriendlyDuration>,

    #[clap(
        long,
        alias = "abort_timeout",
        help = "How long to wait, after asking to suspend, before aborting the invocation",
        long_help = ABORT_TIMEOUT_EDIT_DESCRIPTION
    )]
    abort_timeout: Option<FriendlyDuration>,

    /// Service name
    service: String,

    #[clap(flatten)]
    dry_run: DryRun,
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

    apply_service_configuration_patch(&opts.service, admin_client, modify_request, &opts.dry_run)
        .await
}

pub(super) async fn apply_service_configuration_patch(
    service_name: &str,
    admin_client: AdminClient,
    modify_request: ModifyServiceRequest,
    dry_run: &DryRun,
) -> Result<()> {
    // (machine key, human label, new value, human display) of every requested change.
    let duration_change = |key, label, value: &Option<std::time::Duration>| {
        value.map(|d| {
            let display = d.friendly().to_days_span().to_string();
            (key, label, Field::new(display.clone()), display)
        })
    };
    let changes: Vec<(&str, &str, Field, String)> = [
        modify_request
            .public
            .map(|public| ("public", "Public:", Field::new(public), public.to_string())),
        duration_change(
            "idempotency_retention",
            "Idempotent requests retention:",
            &modify_request.idempotency_retention,
        ),
        duration_change(
            "workflow_completion_retention",
            "Workflow retention:",
            &modify_request.workflow_completion_retention,
        ),
        duration_change(
            "journal_retention",
            "Journal retention:",
            &modify_request.journal_retention,
        ),
        duration_change(
            "inactivity_timeout",
            "Inactivity timeout:",
            &modify_request.inactivity_timeout,
        ),
        duration_change(
            "abort_timeout",
            "Abort timeout:",
            &modify_request.abort_timeout,
        ),
    ]
    .into_iter()
    .flatten()
    .collect();

    let json = CliContext::get().json_output();
    let mut f = Formatter::new();
    if json {
        let rows: Vec<Vec<Field>> = changes
            .iter()
            .map(|(key, _, value, _)| {
                vec![
                    Field::new(service_name),
                    Field::new("update"),
                    Field::new(*key),
                    value.clone(),
                ]
            })
            .collect();
        f.table(
            "changes",
            &["service", "change", "field", "new_value"],
            &rows,
            IfEmpty::Nothing,
        );
    }
    if changes.is_empty() {
        if !json {
            c_println!("No changes requested");
        }
        return f.finish();
    }

    // Print requested changes, ask for confirmation
    if !json {
        let mut table = Table::new_styled();
        for (_, label, _, display) in &changes {
            table.add_kv_row(label, display);
        }
        c_println!("{table}");
    }
    f.confirm(dry_run, "Are you sure you want to apply these changes?")?;

    let _ = admin_client
        .patch_service(service_name, modify_request)
        .await?
        .into_body()
        .await?;

    if !json {
        c_success!("Service {service_name} configuration updated");
    }
    f.next_step(
        &format!("restate services config view {service_name}"),
        "see the updated service configuration",
        IncludeFormatting::Yes,
    );
    f.finish()
}
