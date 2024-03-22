// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::c_error;
use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::MetaClientInterface;
use crate::console::c_println;
use crate::ui::console::{Styled, StyledTable};
use crate::ui::deployments::{
    calculate_deployment_status, render_active_invocations, render_deployment_status,
    render_deployment_type, render_deployment_url, DeploymentStatus,
};
use crate::ui::stylesheet::Style;
use crate::ui::watcher::Watch;

use restate_meta_rest_model::deployments::{ComponentNameRevPair, Deployment, DeploymentResponse};

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};
use restate_meta_rest_model::components::ComponentMetadata;
use restate_types::identifiers::DeploymentId;

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    //// Show additional columns
    #[clap(long)]
    extra: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, list_opts: &List) -> Result<()> {
    let client = crate::clients::MetasClient::new(env)?;
    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    // To know the latest version of every service.
    let components = client.get_components().await?.into_body().await?.components;

    let deployments = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments;

    if deployments.is_empty() {
        c_error!(
            "No deployments were found! Did you forget to register your deployment with 'restate dep register'?"
        );
        return Ok(());
    }
    // For each deployment, we need to calculate the status and # of invocations.
    let mut latest_components: HashMap<String, ComponentMetadata> = HashMap::new();
    for component in components {
        latest_components.insert(component.name.clone(), component);
    }
    //
    let mut table = Table::new_styled(&env.ui_config);
    let mut header = vec![
        "DEPLOYMENT",
        "TYPE",
        "STATUS",
        "ACTIVE INVOCATIONS",
        "ID",
        "CREATED AT",
    ];
    if list_opts.extra {
        header.push("COMPONENTS");
    }
    table.set_styled_header(header);

    let mut enriched_deployments: Vec<(DeploymentResponse, DeploymentStatus, i64)> =
        Vec::with_capacity(deployments.len());

    for deployment in deployments {
        // calculate status and counters.
        let active_inv = count_deployment_active_inv(&sql_client, &deployment.id).await?;
        let status = calculate_deployment_status(
            &deployment.id,
            &deployment.components,
            active_inv,
            &latest_components,
        );
        enriched_deployments.push((deployment, status, active_inv));
    }
    // Sort by active, draining, then drained.
    enriched_deployments.sort_unstable_by_key(|(_, status, _)| match status {
        DeploymentStatus::Active => 0,
        DeploymentStatus::Draining => 1,
        DeploymentStatus::Drained => 2,
    });

    for (deployment, status, active_inv) in enriched_deployments {
        let mut row = vec![
            Cell::new(render_deployment_url(&deployment.deployment)),
            Cell::new(render_deployment_type(&deployment.deployment)),
            render_deployment_status(status),
            render_active_invocations(active_inv),
            Cell::new(deployment.id),
            Cell::new(match &deployment.deployment {
                Deployment::Http { created_at, .. } => created_at,
                Deployment::Lambda { created_at, .. } => created_at,
            }),
        ];
        if list_opts.extra {
            row.push(render_components(
                &deployment.id,
                &deployment.components,
                &latest_components,
            ));
        }

        table.add_row(row);
    }

    c_println!("{}", table);

    Ok(())
}

fn render_components(
    deployment_id: &DeploymentId,
    components: &[ComponentNameRevPair],
    latest_components: &HashMap<String, ComponentMetadata>,
) -> Cell {
    use std::fmt::Write as FmtWrite;

    let mut out = String::new();
    for component in components {
        if let Some(latest_component) = latest_components.get(&component.name) {
            let style = if &latest_component.deployment_id == deployment_id {
                // We are hosting the latest revision of this service.
                Style::Success
            } else {
                Style::Normal
            };
            writeln!(
                &mut out,
                "- {} [{}]",
                &component.name,
                Styled(style, component.revision)
            )
            .unwrap();
        } else {
            // We couldn't find that service in latest_services? that's odd. We
            // highlight this with bright red to highlight the issue.
            writeln!(
                &mut out,
                "- {} [{}]",
                Styled(Style::Danger, &component.name),
                component.revision
            )
            .unwrap();
        }
    }
    Cell::new(out)
}
