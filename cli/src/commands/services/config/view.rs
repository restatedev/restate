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
use anyhow::Result;
use cling::prelude::*;
use comfy_table::Table;
use indoc::indoc;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_tip};
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
    table.add_kv_row("Service type:", &format!("{:?}", service.ty));
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
        service.idempotency_retention,
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
                .expect("Workflows must have a well defined retention"),
        );
        c_println!("{table}");
        c_tip!("{}", WORKFLOW_RETENTION);
        c_println!();
    }

    Ok(())
}
