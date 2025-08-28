// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use anyhow::Result;
use cling::prelude::*;
use comfy_table::Table;
use indoc::indoc;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_tip};
use restate_serde_util::DurationString;
use restate_types::invocation::ServiceType;

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
        DurationString::display(service.idempotency_retention),
    );
    c_println!("{table}");
    c_tip!("{}", IDEMPOTENCY_RETENTION);
    c_println!();

    if service.ty == ServiceType::Workflow {
        let mut table = Table::new_styled();
        table.add_kv_row(
            "Workflow retention time:",
            DurationString::display(
                service
                    .workflow_completion_retention
                    .expect("Workflows must have a well defined retention"),
            ),
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
            .map(DurationString::display)
            .unwrap_or_else(|| "<UNSET>".to_string()),
    );
    c_println!("{table}");
    c_tip!("{}", JOURNAL_RETENTION);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row(
        "Inactivity timeout:",
        DurationString::display(service.inactivity_timeout),
    );
    c_println!("{table}");
    c_tip!("{}", INACTIVITY_TIMEOUT);
    c_println!();

    let mut table = Table::new_styled();
    table.add_kv_row(
        "Abort timeout:",
        DurationString::display(service.abort_timeout),
    );
    c_println!("{table}");
    c_tip!("{}", ABORT_TIMEOUT);
    c_println!();

    Ok(())
}
