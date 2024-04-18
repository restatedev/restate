// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Result};
use cling::prelude::*;
use comfy_table::Table;
use indoc::indoc;
use restate_meta_rest_model::components::ComponentMetadata;
use restate_meta_rest_model::deployments::ComponentNameRevPair;
use std::collections::HashMap;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{MetaClientInterface, MetasClient};
use crate::console::c_println;
use crate::ui::console::{confirm_or_exit, Styled, StyledTable};
use crate::ui::deployments::{
    add_deployment_to_kv_table, calculate_deployment_status, render_active_invocations,
    render_deployment_status,
};
use crate::ui::service_handlers::icon_for_service_type;
use crate::ui::stylesheet::Style;
use crate::{c_eprintln, c_error, c_indentln, c_success};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "rm")]
#[cling(run = "run_remove")]
pub struct Remove {
    /// Force removal of a deployment if it's not drained. This is dangeous and will
    /// break in-flight invocations pinned to this deployment.
    #[clap(long)]
    force: bool,
    // TODO: Support inference of endpoint or ID, but this require the deployment
    // ID to follow a more constrained format
    /// Deployment ID
    deployment_id: String,
}

pub async fn run_remove(State(env): State<CliEnv>, opts: &Remove) -> Result<()> {
    // First get information about this deployment and inspect if it's drained or not.
    let client = MetasClient::new(&env)?;
    let sql_client = crate::clients::DataFusionHttpClient::new(&env)?;

    let deployment = client
        .get_deployment(&opts.deployment_id)
        .await?
        .into_body()
        .await?;

    let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment.id).await?;

    let mut latest_services: HashMap<String, ComponentMetadata> = HashMap::new();
    // To know the latest version of every service.
    let services = client.get_components().await?.into_body().await?.components;
    for service in services {
        latest_services.insert(service.name.clone(), service);
    }

    // sum inv_count in active_inv
    let total_active_inv = active_inv.iter().fold(0, |acc, x| acc + x.inv_count);

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
    c_println!("{}", table);
    c_println!("{}", Styled(Style::Info, "Services:"));
    for service in deployment.components {
        let Some(latest_component) = latest_services.get(&service.name) else {
            // if we can't find this service in the latest set of service, something is off. A
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
        let latest_revision_message = if service.revision == latest_component.revision {
            // We are latest.
            format!("[{}]", Styled(Style::Success, "Latest"))
        } else {
            // Not latest
            format!(
                "[Latest {} is in deployment ID {}]",
                Styled(Style::Success, latest_component.revision),
                latest_component.deployment_id
            )
        };
        c_indentln!(
            2,
            "Revision: {} {}",
            service.revision,
            latest_revision_message
        );
    }
    c_println!();

    // Now, if this is a drained deployment, it's safe to remove. If not, we ask the user to use
    // --force.
    let safe = match status {
        crate::ui::deployments::DeploymentStatus::Active => {
            // unsafe to remove, use --force
            c_error!(
                indoc! {
                    "Deployment is still {}. This means that it hosts the latest revision of some of
                       your services as indicated above. Removing this deployment will cause those
                       services to be unavailable and current or future invocations on them WILL fail.
                "
                },
                Styled(Style::Success, "Active"),
            );
            false
        }
        crate::ui::deployments::DeploymentStatus::Draining => {
            // unsafe to remove, use --force
            c_error!(
                indoc! {
                "Deployment is still {}. There are {} invocations that will break if you proceed
                    with this operation. Please make sure in-flight invocations are completed (deployment is Drained)
                    or killed/cancelled before continuing.
                "
                },
                Styled(Style::Warn, "Draining"),
                Styled(Style::Warn, total_active_inv)
            );
            false
        }
        crate::ui::deployments::DeploymentStatus::Drained => {
            // safe to remove
            c_success!("The deployment is fully drained and is safe to remove");
            true
        }
    };

    if !safe && !opts.force {
        bail!(
            "If you accept the risk of breaking in-flight invocations, you can use {} to \
                forcefully remove this deployment.",
            Styled(Style::Notice, "--force"),
        );
    }

    confirm_or_exit(&env, "Are you sure you want to remove this deployment?")?;

    let result = client
        .remove_deployment(
            &opts.deployment_id,
            //TODO: Use opts.force when the server implements the false + validation case!
            true,
        )
        .await?;
    let _ = result.success_or_error()?;

    c_println!();
    c_success!("Deployment {} removed successfully", &opts.deployment_id);
    Ok(())
}
