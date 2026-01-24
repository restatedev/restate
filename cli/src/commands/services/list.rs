// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::Table;

use restate_admin_rest_model::deployments::DeploymentResponse;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_error, c_println};
use restate_types::identifiers::DeploymentId;
use restate_types::schema::service::HandlerMetadata;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClientInterface, Deployment};
use crate::ui::deployments::{render_deployment_type, render_deployment_url};
use crate::ui::service_handlers::{icon_for_is_public, icon_for_service_type};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Show only publicly accessible services
    #[clap(long)]
    public_only: bool,

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
    let client = crate::clients::AdminClient::new(env).await?;
    let defs = client.get_services().await?.into_body().await?;

    if defs.services.is_empty() {
        c_error!(
            "No services were found! Services are added by registering deployments with 'restate dep register'"
        );
        return Ok(());
    }

    let deployments = client.get_deployments().await?.into_body().await?;

    let mut deployment_cache: HashMap<DeploymentId, DeploymentResponse> = HashMap::new();

    // Caching endpoints
    for deployment in deployments.deployments {
        deployment_cache.insert(deployment.id(), deployment);
    }

    let mut table = Table::new_styled();
    let mut header = vec![
        "",
        "NAME",
        "REVISION",
        "FLAVOR",
        "DEPLOYMENT-TYPE",
        "DEPLOYMENT-ID",
    ];
    if list_opts.extra {
        header.push("ENDPOINT");
        header.push("METHODS");
    }
    table.set_styled_header(header);

    for svc in defs.services {
        if list_opts.public_only && !svc.public {
            // Skip non-public services if users chooses to.
            continue;
        }

        let public = icon_for_is_public(svc.public);
        let flavor = icon_for_service_type(&svc.ty);

        let deployment = deployment_cache
            .get(&svc.deployment_id)
            .with_context(|| format!("Deployment {} was not found!", svc.deployment_id))?;

        let (deployment_id, deployment, _) =
            Deployment::from_deployment_response(deployment.clone());

        let mut row = vec![
            public.to_string(),
            svc.name,
            svc.revision.to_string(),
            flavor.to_string(),
            render_deployment_type(&deployment),
            deployment_id.to_string(),
        ];
        if list_opts.extra {
            row.push(render_deployment_url(&deployment));
            row.push(render_methods(svc.handlers.into_values().collect()));
        }

        table.add_row(row);
    }
    c_println!("{}", table);
    Ok(())
}

fn render_methods(methods: Vec<HandlerMetadata>) -> String {
    use std::fmt::Write as FmtWrite;

    let mut out = String::new();
    for method in methods {
        writeln!(&mut out, "{}", method.name).unwrap();
    }
    out
}
