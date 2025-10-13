// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_eprintln, c_indent_table, c_indentln, c_println, c_title};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::{
    add_deployment_to_kv_table, calculate_deployment_status, render_active_invocations,
    render_deployment_status,
};
use crate::ui::service_handlers::icon_for_service_type;

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

    /// Show draining status and invocation statistics per deployment
    #[clap(long)]
    extra: bool,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    // To know the latest version of every service.
    let services = client.get_services().await?.into_body().await?.services;
    for service in services {
        latest_services.insert(service.name.clone(), service);
    }

    let deployment = client
        .get_deployment(&opts.deployment_id)
        .await?
        .into_body()
        .await?;

    let (deployment_id, deployment, services) =
        Deployment::from_detailed_deployment_response(deployment);

    let mut table = Table::new_styled();
    table.add_kv_row("ID:", deployment_id);

    add_deployment_to_kv_table(&deployment, &mut table);

    let active_inv = if opts.extra {
        let sql_client = crate::clients::DataFusionHttpClient::from(client);
        let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment_id).await?;
        Some(active_inv)
    } else {
        None
    };

    if opts.extra {
        let total_active_inv = active_inv.iter().flatten().map(|x| x.inv_count).sum();

        let service_rev_pairs: Vec<_> = services
            .iter()
            .map(|s| ServiceNameRevPair {
                name: s.name.clone(),
                revision: s.revision,
            })
            .collect();

        let status = calculate_deployment_status(
            &deployment_id,
            &service_rev_pairs,
            total_active_inv,
            &latest_services,
        );

        table.add_kv_row("Status:", render_deployment_status(status));
        table.add_kv_row("Invocations:", render_active_invocations(total_active_inv));
    }

    c_title!("📜", "Deployment Information");
    c_println!("{}", table);

    // Services and methods.
    c_println!();

    c_title!("🤖", "Services");
    let mut methods_header = vec!["HANDLER", "INPUT", "OUTPUT"];
    if opts.extra {
        methods_header.push("ACTIVE-INVOCATIONS");
    }

    for service in services {
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
        let mut methods_table = Table::new_styled();

        methods_table.set_styled_header(methods_header.clone());

        for handler in service.handlers.values() {
            let mut row = vec![
                Cell::new(&handler.name),
                Cell::new(&handler.input_description),
                Cell::new(&handler.output_description),
            ];

            if opts.extra {
                // how many inv pinned on this deployment+service+method.
                let active_inv = active_inv
                    .iter()
                    .flatten()
                    .filter(|x| x.service == service.name && x.handler == handler.name)
                    .map(|x| x.inv_count)
                    .next()
                    .unwrap_or(0);

                row.push(render_active_invocations(active_inv));
            }

            methods_table.add_row(row);
        }
        c_indent_table!(2, methods_table);
        c_println!();
    }

    Ok(())
}
