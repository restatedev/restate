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
use crossterm::style::Stylize;
use indoc::indoc;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_tip};
use restate_time_util::DurationExt;
use restate_types::invocation::ServiceType;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};

// TODO we could infer this text from the OpenAPI docs!
pub(super) const PUBLIC_DESCRIPTION: &str = indoc! {
    "Whether the service is publicly available or not.
    If true, the service can be invoked through the ingress.
    If false, the service can be invoked only from another Restate service."
};
pub(super) const IDEMPOTENCY_RETENTION: &str = indoc! {
    "The retention duration of idempotent requests for this service.
    The retention period starts once the invocation completes (with either success or failure).
    After the retention period, the invocation response and the idempotency key will be forgotten."
};
pub(super) const WORKFLOW_RETENTION: &str = indoc! {
    "The retention duration of workflows.
    The retention period starts once the invocation completes (with either success or failure).
    After the retention period, the invocation response together with the workflow state and promises will be forgotten."
};
pub(super) const JOURNAL_RETENTION: &str = indoc! {
    "The journal retention.
    The retention period starts once the invocation completes (with either success or failure).

    In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time."
};
pub(super) const INACTIVITY_TIMEOUT: &str = indoc! {
    "This timer guards against stalled service/handler invocations. Once it expires,
    Restate triggers a graceful termination by asking the service invocation to
    suspend (which preserves intermediate progress).

    The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    the request to suspend.

    This overrides the default inactivity timeout set in invoker options."
};
pub(super) const ABORT_TIMEOUT: &str = indoc! {
    "This timer guards against stalled service/handler invocations that are supposed to terminate.
    The abort timeout is started after the 'inactivity timeout' has expired and the
    service/handler invocation has been asked to gracefully terminate.
    Once the timer expires, it will abort the service/handler invocation.

    This timer potentially **interrupts** user code. If the user code needs longer to
    gracefully terminate, then this value needs to be set accordingly.

    This overrides the default abort timeout set in invoker options."
};
pub(super) const ENABLE_LAZY_STATE: &str = indoc! {
    "If true, lazy state will be enabled for all invocations to this service.
    This is relevant only for Workflows and Virtual Objects."
};
pub(super) const RETRY_POLICY: &str = indoc! {
    "Retry policy to use for transient errors. The next retry interval is calculated as
    initial_interval * (exponentiation_factor ^ attempt), capped at max_interval.

    Max attempts: Maximum number of retry attempts before giving up (infinite if unset).
    On max attempts: What to do when max attempts are reached (Pause or Kill)."
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
pub struct View {
    /// Service name
    service: String,
}

pub async fn run_view(State(env): State<CliEnv>, opts: &View) -> Result<()> {
    view(&env, opts).await
}

async fn view(env: &CliEnv, opts: &View) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let service = client.get_service(&opts.service).await?.into_body().await?;

    let mut table = Table::new_styled();
    table.add_kv_row("Name:", &service.name);
    table.add_kv_row("Service type:", format!("{:?}", service.ty));
    c_println!("{table}");
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row("Public:", service.public);
    c_println!("{table}");
    c_tip!("{}", PUBLIC_DESCRIPTION);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row(
        "Idempotent requests retention:",
        service.idempotency_retention.friendly(),
    );
    c_println!("{table}");
    c_tip!("{}", IDEMPOTENCY_RETENTION);
    c_println!();

    if service.ty == ServiceType::Workflow {
        let mut table = Table::new_styled();
        table.add_kv_row(
            "Workflow retention time:",
            service
                .workflow_completion_retention
                .expect("Workflows must have a well defined retention")
                .friendly(),
        );
        c_println!("{table}");
        c_tip!("{}", WORKFLOW_RETENTION);
        c_println!();
    }

    let mut table = Table::new_styled();
    table.add_kv_row(
        "Journal retention:",
        service
            .journal_retention
            .map(|d| d.friendly().to_string())
            .unwrap_or_else(|| "<UNSET>".to_string()),
    );
    c_println!("{table}");
    c_tip!("{}", JOURNAL_RETENTION);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row("Inactivity timeout:", service.inactivity_timeout.friendly());
    c_println!("{table}");
    c_tip!("{}", INACTIVITY_TIMEOUT);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row("Abort timeout:", service.abort_timeout.friendly());
    c_println!("{table}");
    c_tip!("{}", ABORT_TIMEOUT);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row("Enable lazy state:", service.enable_lazy_state);
    c_println!("{table}");
    c_tip!("{}", ENABLE_LAZY_STATE);
    c_println!();

    let mut table = Table::new_styled();
    table.add_row(vec!["Retry Policy:".bold()]);
    table.add_kv_row(
        "  Max attempts:",
        service
            .retry_policy
            .max_attempts
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<UNSET>".to_string()),
    );
    table.add_kv_row(
        "  On max attempts:",
        format!("{:?}", service.retry_policy.on_max_attempts),
    );
    table.add_kv_row(
        "  Initial interval:",
        service.retry_policy.initial_interval.friendly(),
    );
    table.add_kv_row(
        "  Exponentiation factor:",
        service.retry_policy.exponentiation_factor,
    );
    table.add_kv_row(
        "  Max interval:",
        service
            .retry_policy
            .max_interval
            .map(|d| d.friendly().to_string())
            .unwrap_or_else(|| "<UNSET>".to_string()),
    );
    c_println!("{table}");
    c_tip!("{}", RETRY_POLICY);
    c_println!();

    // Show handler overrides
    for (handler_name, handler) in service.handlers {
        let has_overrides = handler.idempotency_retention.is_some()
            || handler.journal_retention.is_some()
            || handler.inactivity_timeout.is_some()
            || handler.abort_timeout.is_some()
            || handler.enable_lazy_state.is_some()
            || handler.public != service.public
            || !is_retry_policy_empty(&handler.retry_policy);

        if has_overrides {
            let mut table = Table::new_styled();
            table.add_row(vec![format!(
                "Handler '{}' overrides:",
                handler_name.bold()
            )]);

            if let Some(idempotency_retention) = handler.idempotency_retention {
                table.add_kv_row(
                    "  Idempotent requests retention:",
                    idempotency_retention.friendly(),
                );
            }

            if let Some(journal_retention) = handler.journal_retention {
                table.add_kv_row("  Journal retention:", journal_retention.friendly());
            }

            if let Some(inactivity_timeout) = handler.inactivity_timeout {
                table.add_kv_row("  Inactivity timeout:", inactivity_timeout.friendly());
            }

            if let Some(abort_timeout) = handler.abort_timeout {
                table.add_kv_row("  Abort timeout:", abort_timeout.friendly());
            }

            if let Some(enable_lazy_state) = handler.enable_lazy_state {
                table.add_kv_row("  Enable lazy state:", enable_lazy_state);
            }

            if handler.public != service.public {
                table.add_kv_row("  Public:", handler.public);
            }

            c_println!("{table}");

            // Show retry policy overrides if any
            if !is_retry_policy_empty(&handler.retry_policy) {
                let mut table = Table::new_styled();
                table.add_row(vec!["  Retry Policy:".bold()]);

                if let Some(max_attempts) = handler.retry_policy.max_attempts {
                    table.add_kv_row("    Max attempts:", max_attempts);
                }

                if let Some(on_max_attempts) = &handler.retry_policy.on_max_attempts {
                    table.add_kv_row("    On max attempts:", format!("{:?}", on_max_attempts));
                }

                if let Some(initial_interval) = handler.retry_policy.initial_interval {
                    table.add_kv_row("    Initial interval:", initial_interval.friendly());
                }

                if let Some(exponentiation_factor) = handler.retry_policy.exponentiation_factor {
                    table.add_kv_row("    Exponentiation factor:", exponentiation_factor);
                }

                if let Some(max_interval) = handler.retry_policy.max_interval {
                    table.add_kv_row("    Max interval:", max_interval.friendly());
                }

                c_println!("{table}");
            }

            c_println!();
        }
    }

    Ok(())
}

fn is_retry_policy_empty(
    retry_policy: &restate_types::schema::service::HandlerRetryPolicyMetadata,
) -> bool {
    retry_policy.initial_interval.is_none()
        && retry_policy.exponentiation_factor.is_none()
        && retry_policy.max_attempts.is_none()
        && retry_policy.max_interval.is_none()
        && retry_policy.on_max_attempts.is_none()
}
