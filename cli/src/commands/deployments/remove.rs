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

use anyhow::Result;
use cling::prelude::*;
use indoc::indoc;
use serde_json::json;

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::ui::console::Styled;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_eprintln, c_error, c_println, c_success};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::error::RestateCliError;
use crate::ui::deployments::{
    DeploymentStatus, active_invocations_field, calculate_deployment_status,
    deployment_info_fields, deployment_status_field, latest_service, service_item,
};
use crate::ui::fmt::{DryRun, Field, Formatter, IncludeFormatting, OutputFormatter};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "rm")]
#[cling(run = "run_remove")]
pub struct Remove {
    /// Force removal of a deployment if it's not drained. This is dangerous and will
    /// break in-flight invocations pinned to this deployment.
    #[clap(long)]
    force: bool,
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

    let mut deployment_fields = vec![("id".to_owned(), Field::new(deployment_id.to_string()))];
    deployment_fields.extend(deployment_info_fields(&deployment));
    deployment_fields.push(("status".to_owned(), deployment_status_field(status)));
    deployment_fields.push((
        "invocations".to_owned(),
        active_invocations_field(total_active_inv),
    ));
    let mut f = Formatter::new();
    f.title("📜", "Deployment Information");
    f.detail("deployment", &deployment_fields);

    f.title("🤖", "Services");
    let mut items = f.start_items("services");
    for service in &deployment_services {
        let Some(latest_service) = latest_service(&latest_services, service) else {
            continue;
        };
        let mut item = items.item();
        service_item(&mut item, service, latest_service);
        item.finish()?;
    }
    items.finish();
    // The plan, JSON-only: the human output already describes the deployment to remove.
    f.value(
        "changes",
        Field::with_display(
            json!([{"deployment_id": deployment_id.to_string(), "change": "delete"}]),
            "",
        ),
    );

    let json = CliContext::get().json_output();
    // Now, if this is a drained deployment, it's safe to remove. If not, we ask the user to use
    // --force.
    let safe = matches!(status, DeploymentStatus::Drained);
    if !json {
        c_println!();
        if safe {
            c_success!("The deployment is fully drained and is safe to remove");
        }
    }
    match status {
        DeploymentStatus::Active => {
            c_error!(
                indoc! {
                    "Deployment is still {}. This means that it hosts the latest revision of some of
                       your services as indicated above. Removing this deployment will cause those
                       services to be unavailable and current or future invocations on them WILL fail."
                },
                Styled(Style::Success, "Active"),
            );
        }
        DeploymentStatus::Draining => {
            c_error!(
                indoc! {
                "Deployment is still {}. There are {} invocations that will break if you proceed
                    with this operation. Please make sure in-flight invocations are completed (deployment is Drained)
                    or killed/cancelled before continuing."
                },
                Styled(Style::Warn, "Draining"),
                Styled(Style::Warn, total_active_inv)
            );
        }
        DeploymentStatus::Drained => {}
    }

    if !safe && !opts.force {
        // Keep the warning apart from the error below it.
        c_eprintln!();
        return Err(RestateCliError::bad_input(format!(
            "Deployment {deployment_id} is not drained: removing it can break in-flight invocations"
        ))
        .with_next_step(
            format!("restate deployments remove {deployment_id} --force"),
            "remove it anyway, if you accept that risk",
        )
        .into());
    }

    f.confirm(
        &opts.dry_run,
        "Are you sure you want to remove this deployment?",
    )?;

    let result = client
        .remove_deployment(
            &deployment_id.to_string(),
            //TODO: Use opts.force when the server implements the false + validation case!
            true,
        )
        .await?;
    let _ = result.success_or_error()?;

    if !json {
        c_println!();
        c_success!("Deployment {deployment_id} removed successfully");
    }
    f.next_step(
        "restate deployments list",
        "see the remaining deployments",
        IncludeFormatting::Yes,
    );
    f.finish()
}
