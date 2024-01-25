// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use crate::c_title;
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::{MetaClientInterface, MetasClient};
use crate::console::c_println;
use crate::ui::console::StyledTable;
use crate::ui::deployments::{
    add_deployment_to_kv_table, render_active_invocations, render_deployment_type,
    render_deployment_url,
};
use crate::ui::service_methods::create_service_methods_table;
use crate::ui::watcher::Watch;

use anyhow::Result;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// Service name
    name: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = MetasClient::new(env)?;
    let svc = client.get_service(&opts.name).await?.into_body().await?;

    let mut table = Table::new_styled(&env.ui_config);
    table.add_kv_row("Name:", &svc.name);
    table.add_kv_row(
        "Flavor (Instance Type):",
        &format!("{:?}", svc.instance_type),
    );
    table.add_kv_row("Revision:", svc.revision);
    table.add_kv_row("Public:", svc.public);
    table.add_kv_row("Deployment ID:", svc.deployment_id);

    let deployment = client
        .get_deployment(&svc.deployment_id)
        .await?
        .into_body()
        .await?;
    add_deployment_to_kv_table(&deployment.deployment, &mut table);

    c_title!("📜", "Service Information");
    c_println!("{}", table);

    // Methods
    c_println!();
    c_title!("🔌", "Methods");
    let table = create_service_methods_table(&env.ui_config, &svc.methods);
    c_println!("{}", table);

    // Printing other existing endpoints with previous revisions. We currently don't
    // have an API to get endpoints by service name so we get everything and filter
    // locally in this case.
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Retrieving information about older deployments");

    let svc_name = svc.name;
    let latest_rev = svc.revision;
    let mut other_deployments: Vec<_> = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments
        .into_iter()
        .filter_map(|e| {
            // endpoints that serve the same service.
            let svc_match: Vec<_> = e
                .services
                .iter()
                .filter(|s| s.name == svc_name && s.revision != latest_rev)
                .collect();
            // we should see either one or zero matches, more than one means that an endpoint is
            // hosting multiple revisions of the _the same_ service which indicates that something
            // is so wrong!
            if svc_match.len() > 1 {
                progress.finish_and_clear();
                panic!(
                    "Deployment {} is hosting multiple revisions of the same service {}!",
                    e.id, svc_name
                );
            }

            svc_match
                .first()
                .map(|svc_match| (e.id, e.deployment, svc_match.revision))
        })
        .collect();

    if other_deployments.is_empty() {
        return Ok(());
    }

    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    // We have older deployments for this service, let's grab
    let mut table = Table::new_styled(&env.ui_config);
    let headers = vec![
        "ADDRESS",
        "TYPE",
        "SERVICE REVISION",
        "ACTIVE INVOCATIONS",
        "DEPLOYMENT ID",
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
