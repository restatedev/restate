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

use anyhow::{Result, bail};
use cling::prelude::*;
use indoc::indoc;

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::CliContext;
use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_eprintln, c_error, c_indentln, c_success};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::console::c_println;
use crate::ui::deployments::{
    active_invocations_field, calculate_deployment_status, deployment_info_fields,
    deployment_status_field,
};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};
use crate::ui::service_handlers::{service_type_label, service_type_machine};

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

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_remove(State(env): State<CliEnv>, opts: &Remove) -> Result<()> {
    // First get information about this deployment and inspect if it's drained or not.
    let client = AdminClient::new(&env).await?;
    let sql_client = crate::clients::DataFusionHttpClient::from(client.clone());

    let deployment = client
        .get_deployment(&opts.deployment_id)
        .await?
        .into_body()
        .await?;
    let (deployment_id, deployment, deployment_services) =
        Deployment::from_detailed_deployment_response(deployment);
    let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment_id).await?;

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    // To know the latest version of every service.
    for service in client.get_services().await?.into_body().await?.services {
        latest_services.insert(service.name.clone(), service);
    }

    // sum inv_count in active_inv
    let total_active_inv = active_inv.iter().fold(0, |acc, x| acc + x.inv_count);

    let service_rev_pairs: Vec<_> = deployment_services
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

    let json = CliContext::get().json_output();
    let mut deployment_fields = vec![("id".to_owned(), Field::new(deployment_id.to_string()))];
    deployment_fields.extend(deployment_info_fields(&deployment));
    deployment_fields.push(("status".to_owned(), deployment_status_field(status)));
    deployment_fields.push((
        "invocations".to_owned(),
        active_invocations_field(total_active_inv),
    ));
    let mut f = Formatter::new();
    f.detail("deployment", &deployment_fields);

    if !json {
        c_println!("{}", Styled(Style::Info, "Services:"));
    }
    let mut service_rows = Vec::new();
    for service in deployment_services {
        let Some(latest_service) = latest_services.get(&service.name) else {
            // if we can't find this service in the latest set of service, something is off. A
            // deployment cannot remove services defined by other deployment, so we should warn that
            // this is happening.
            c_eprintln!(
                "Service {} is not found in the latest set of services. This is unexpected.",
                service.name
            );
            continue;
        };
        if json {
            service_rows.push(vec![
                Field::new(service.name),
                Field::new(service_type_machine(&service.ty)),
                Field::new(service.revision),
                Field::new(latest_service.revision),
                Field::new(latest_service.deployment_id.to_string()),
            ]);
            continue;
        }
        c_indentln!(1, "- {}", Styled(Style::Info, &service.name));
        c_indentln!(2, "Type: {}", service_type_label(&service.ty));
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
    }
    if json {
        f.table(
            "services",
            &[
                "service",
                "service_type",
                "revision",
                "latest_revision",
                "latest_deployment_id",
            ],
            &service_rows,
        );
        f.table(
            "changes",
            &["deployment_id", "change"],
            &[vec![
                Field::new(deployment_id.to_string()),
                Field::new("delete"),
            ]],
        );
    } else {
        c_println!();
    }

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
            if !json {
                c_success!("The deployment is fully drained and is safe to remove");
            }
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

    f.confirm(
        &opts.dry_run,
        "Are you sure you want to remove this deployment?",
    )?;

    let result = client
        .remove_deployment(
            &opts.deployment_id,
            //TODO: Use opts.force when the server implements the false + validation case!
            true,
        )
        .await?;
    let _ = result.success_or_error()?;

    if !json {
        c_println!();
        c_success!("Deployment {} removed successfully", &opts.deployment_id);
    }
    f.next_step("restate deployments list", "see the remaining deployments");
    f.finish()
}
