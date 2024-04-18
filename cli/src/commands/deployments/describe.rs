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
use restate_meta_rest_model::components::ComponentMetadata;
use restate_meta_rest_model::deployments::ComponentNameRevPair;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{MetaClientInterface, MetasClient};
use crate::ui::console::{Styled, StyledTable};
use crate::ui::deployments::{
    add_deployment_to_kv_table, calculate_deployment_status, render_active_invocations,
    render_deployment_status,
};
use crate::ui::service_handlers::icon_for_service_type;
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

    let mut latest_services: HashMap<String, ComponentMetadata> = HashMap::new();
    // To know the latest version of every service.
    let services = client.get_components().await?.into_body().await?.components;
    for service in services {
        latest_services.insert(service.name.clone(), service);
    }

    let deployment = client
        .get_deployment(&opts.deployment_id)
        .await?
        .into_body()
        .await?;

    let sql_client = crate::clients::DataFusionHttpClient::new(env)?;
    let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment.id).await?;
    let total_active_inv = active_inv.iter().map(|x| x.inv_count).sum();

    let service_rev_pairs: Vec<_> = deployment
        .components
        .iter()
        .map(|s| ComponentNameRevPair {
            name: s.name.clone(),
            revision: s.revision,
        })
        .collect();

    let status = calculate_deployment_status(
        &deployment.id,
        &service_rev_pairs,
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
    for service in deployment.components {
        let Some(latest_service) = latest_services.get(&service.name) else {
            // if we can't find this service in the latest set of services, something is off. A
            // deployment cannot remove services defined by other deployment, so we should warn that
            // this is happening.
            c_eprintln!(
                "Service {} is not found in the latest set of services. This is unexpected.",
                service.name
            );
            continue;
        };

        c_indentln!(1, "- {}", Styled(Style::Info, &service.name));
        c_indentln!(
            2,
            "Type: {:?} {}",
            service.ty,
            icon_for_service_type(&service.ty),
        );

        let latest_revision_message = if service.revision == latest_service.revision {
            // We are latest.
            format!("[{}]", Styled(Style::Success, "Latest"))
        } else {
            // Not latest
            format!(
                "[Latest {} is in deployment ID {}]",
                Styled(Style::Success, latest_service.revision),
                latest_service.deployment_id
            )
        };
        c_indentln!(
            2,
            "Revision: {} {}",
            service.revision,
            latest_revision_message
        );
        let mut methods_table = Table::new_styled(&env.ui_config);
        methods_table.set_styled_header(vec![
            "HANDLER",
            "INPUT TYPE",
            "OUTPUT TYPE",
            "ACTIVE INVOCATIONS",
        ]);

        for handler in &service.handlers {
            // how many inv pinned on this deployment+service+method.
            let active_inv = active_inv
                .iter()
                .filter(|x| x.service == service.name && x.handler == handler.name)
                .map(|x| x.inv_count)
                .next()
                .unwrap_or(0);

            methods_table.add_row(vec![
                Cell::new(&handler.name),
                Cell::new(&handler.input_description),
                Cell::new(&handler.output_description),
                render_active_invocations(active_inv),
            ]);
        }
        c_indent_table!(2, methods_table);
        c_println!();
    }

    Ok(())
}
