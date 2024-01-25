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

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};

use restate_meta_rest_model::deployments::ServiceNameRevPair;
use restate_meta_rest_model::services::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{MetaClientInterface, MetasClient};
use crate::ui::console::{Styled, StyledTable};
use crate::ui::deployments::{
    add_deployment_to_kv_table, calculate_deployment_status, render_active_invocations,
    render_deployment_status,
};
use crate::ui::service_methods::icon_for_service_flavor;
use crate::ui::stylesheet::Style;
use crate::ui::watcher::Watch;
use crate::{c_eprintln, c_indent_table, c_indentln, c_println, c_title};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    // TODO: Support inference of endpoint or ID, but this require the deployment
    // ID to follow a more constrained format
    /// Deployment ID
    deployment_id: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = MetasClient::new(env)?;

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    // To know the latest version of every service.
    let services = client.get_services().await?.into_body().await?.services;
    for svc in services {
        latest_services.insert(svc.name.clone(), svc);
    }

    let deployment = client
        .get_deployment(&opts.deployment_id)
        .await?
        .into_body()
        .await?;

    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment.id).await?;
    let total_active_inv = active_inv.iter().map(|x| x.inv_count).sum();

    let svc_rev_pairs: Vec<_> = deployment
        .services
        .iter()
        .map(|s| ServiceNameRevPair {
            name: s.name.clone(),
            revision: s.revision,
        })
        .collect();

    let status = calculate_deployment_status(
        &deployment.id,
        &svc_rev_pairs,
        total_active_inv,
        &latest_services,
    );

    let mut table = Table::new_styled(&env.ui_config);
    table.add_kv_row("ID:", deployment.id);

    add_deployment_to_kv_table(&deployment.deployment, &mut table);
    table.add_kv_row("Status:", render_deployment_status(status));
    table.add_kv_row("Invocations:", render_active_invocations(total_active_inv));

    c_title!("📜", "Deployment Information");
    c_println!("{}", table);

    // Services and methods.
    c_println!();

    c_title!("🤖", "Services");
    for svc in deployment.services {
        let Some(latest_svc) = latest_services.get(&svc.name) else {
            // if we can't find this service in the latest set of service, something is off. A
            // deployment cannot remove services defined by other deployment, so we should warn that
            // this is happening.
            c_eprintln!(
                "Service {} is not found in the latest set of services. This is unexpected.",
                svc.name
            );
            continue;
        };

        c_indentln!(1, "- {}", Styled(Style::Info, &svc.name));
        c_indentln!(
            2,
            "Type: {:?} {}",
            svc.instance_type,
            icon_for_service_flavor(&svc.instance_type),
        );

        let latest_revision_message = if svc.revision == latest_svc.revision {
            // We are latest.
            format!("[{}]", Styled(Style::Success, "Latest"))
        } else {
            // Not latest
            format!(
                "[Latest {} is in deployment ID {}]",
                Styled(Style::Success, latest_svc.revision),
                latest_svc.deployment_id
            )
        };
        c_indentln!(2, "Revision: {} {}", svc.revision, latest_revision_message);
        let mut methods_table = Table::new_styled(&env.ui_config);
        methods_table.set_styled_header(vec![
            "METHOD",
            "INPUT TYPE",
            "OUTPUT TYPE",
            "ACTIVE INVOCATIONS",
        ]);

        for method in &svc.methods {
            // how many inv pinned on this deployment+service+method.
            let active_inv = active_inv
                .iter()
                .filter(|x| x.service == svc.name && x.method == method.name)
                .map(|x| x.inv_count)
                .next()
                .unwrap_or(0);

            methods_table.add_row(vec![
                Cell::new(&method.name),
                Cell::new(&method.input_type),
                Cell::new(&method.output_type),
                render_active_invocations(active_inv),
            ]);
        }
        c_indent_table!(2, methods_table);
        c_println!();
    }

    Ok(())
}
