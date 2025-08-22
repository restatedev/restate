// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;
use comfy_table::{Cell, Table};
use indicatif::ProgressBar;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_println, c_title};

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::{
    add_deployment_to_kv_table, render_active_invocations, render_deployment_type,
    render_deployment_url,
};
use crate::ui::service_handlers::create_service_handlers_table;

use anyhow::Result;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// service name
    name: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let service = client.get_service(&opts.name).await?.into_body().await?;

    let mut table = Table::new_styled();
    table.add_kv_row("Name:", &service.name);
    table.add_kv_row("Service type:", format!("{:?}", service.ty));
    table.add_kv_row("Revision:", service.revision);
    table.add_kv_row("Public:", service.public);
    table.add_kv_row("Deployment ID:", service.deployment_id);

    let deployment = client
        .get_deployment(&service.deployment_id.to_string())
        .await?
        .into_body()
        .await?;
    let (_, deployment, _) = Deployment::from_detailed_deployment_response(deployment);
    add_deployment_to_kv_table(&deployment, &mut table);

    c_title!("📜", "Service Information");
    c_println!("{}", table);

    // Methods
    c_println!();
    c_title!("🔌", "Handlers");
    let table = create_service_handlers_table(service.handlers.values());
    c_println!("{}", table);

    // Printing other existing endpoints with previous revisions. We currently don't
    // have an API to get endpoints by service name so we get everything and filter
    // locally in this case.
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Retrieving information about older deployments");

    let service_name = service.name;
    let latest_rev = service.revision;
    let mut other_deployments: Vec<_> = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments
        .into_iter()
        .filter_map(|e| {
            let (other_deployment_id, other_deployment, other_deployment_services) = Deployment::from_deployment_response(e);

            // endpoints that serve the same service.
            let service_match: Vec<_> = other_deployment_services
                .iter()
                .filter(|s| s.name == service_name && s.revision != latest_rev)
                .collect();
            // we should see either one or zero matches, more than one means that an endpoint is
            // hosting multiple revisions of the _the same_ service which indicates that something
            // is so wrong!
            if service_match.len() > 1 {
                progress.finish_and_clear();
                panic!(
                    "Deployment {other_deployment_id} is hosting multiple revisions of the same service {service_name}!"
                );
            }

            service_match.first().map(|service_match| {
                (
                    other_deployment_id,
                    other_deployment,
                    service_match.revision,
                )
            })
        })
        .collect();

    if other_deployments.is_empty() {
        return Ok(());
    }

    let sql_client = crate::clients::DataFusionHttpClient::from(client);
    // We have older deployments for this service, let's grab
    let mut table = Table::new_styled();
    let headers = vec![
        "ADDRESS",
        "TYPE",
        "SERVICE-REVISION",
        "ACTIVE-INVOCATIONS",
        "DEPLOYMENT-ID",
    ];
    table.set_styled_header(headers);
    // sort other_endpoints by revision in descending order
    other_deployments.sort_by(|(_, _, rev1), (_, _, rev2)| rev2.cmp(rev1));

    for (deployment_id, deployment_metadata, rev) in other_deployments {
        let active_inv = count_deployment_active_inv(&sql_client, &deployment_id).await?;

        table.add_row(vec![
            Cell::new(render_deployment_url(&deployment_metadata)),
            Cell::new(render_deployment_type(&deployment_metadata)),
            Cell::new(rev),
            render_active_invocations(active_inv),
            Cell::new(deployment_id),
        ]);
    }

    progress.finish_and_clear();

    c_println!();
    c_title!("👵", "Older Revisions");
    c_println!("{}", table);

    Ok(())
}
