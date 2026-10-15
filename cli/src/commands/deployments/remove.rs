// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use cling::prelude::*;
use futures::future::try_join_all;
use reqwest::StatusCode;
use serde_json::json;

use restate_admin_rest_model::deployments::{
    BulkDeleteDeploymentsRequest, FailedDeploymentDeletion, ServiceNameRevPair,
};
use restate_admin_rest_model::invocations::BATCH_OPERATION_MAX_SIZE;
use restate_cli_util::{CliContext, c_eprintln, c_println, c_success};
use restate_types::identifiers::DeploymentId;
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient, Deployment};
use crate::error::RestateCliError;
use crate::ui::deployments::{
    DeploymentStatus, active_invocations_field, calculate_deployment_status,
    deployment_info_fields, deployment_status_field, latest_service, service_item,
};
use crate::ui::fmt::{DryRun, Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

/// Remove drained deployments
///
/// A deployment is drained when newer deployments serve the latest revision of all its
/// services, and none of its invocations are still in flight (invocations keep running on the
/// deployment they started on). Check with `restate deployments describe <id> --extra`.
/// Removing a deployment that isn't drained is refused unless --force. Several deployments
/// can be removed at once, confirming them together.
#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "rm")]
#[cling(run = "run_remove")]
#[command(after_help = after_help!(
    examples: [
        "restate deployments remove dp_14LsPzBd7Uuvbx8wA3wtKoD --dry-run",
        "restate deployments remove dp_14LsPzBd7Uuvbx8wA3wtKoD dp_11pXxBETJoA3YQ1jW8QnSuF",
        "restate deployments list --extra --json \\",
        "  | jq -r '.deployments[] | select(.status == \"Drained\") | .deployment_id' \\",
        "  | xargs restate deployments remove --yes",
    ],
    learn_more: "https://docs.restate.dev/services/versioning",
))]
pub struct Remove {
    /// Remove the deployments even if they're not drained. Their in-flight invocations will
    /// fail, and the services whose latest revision they serve become unavailable
    #[clap(long)]
    force: bool,
    /// Deployment ids (`dp_...`)
    #[clap(required = true, value_name = "DEPLOYMENT_ID")]
    deployment_ids: Vec<String>,

    #[clap(flatten)]
    dry_run: DryRun,
}

/// A deployment to remove, with what removing it would break.
struct Inspected {
    deployment_id: DeploymentId,
    deployment: Deployment,
    services: Vec<ServiceMetadata>,
    status: DeploymentStatus,
    total_active_inv: i64,
}

async fn inspect(
    client: &AdminClient,
    sql_client: &DataFusionHttpClient,
    latest_services: &HashMap<String, ServiceMetadata>,
    deployment_id: &str,
) -> Result<Inspected> {
    let deployment = client
        .get_deployment(deployment_id)
        .await?
        .into_body()
        .await?;
    let (deployment_id, deployment, services) =
        Deployment::from_detailed_deployment_response(deployment);
    let active_inv = count_deployment_active_inv_by_method(sql_client, &deployment_id).await?;
    let total_active_inv = active_inv.iter().map(|x| x.inv_count).sum();

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
        latest_services,
    );

    Ok(Inspected {
        deployment_id,
        deployment,
        services,
        status,
        total_active_inv,
    })
}

impl Inspected {
    /// Why removing this deployment is risky, if it isn't drained.
    fn risk(&self) -> Option<String> {
        let total_active_inv = self.total_active_inv;
        match self.status {
            DeploymentStatus::Active => Some(
                "Active. This means that it hosts the latest revision of some of your services as \
                 indicated above. Removing this deployment will cause those services to be \
                 unavailable and current or future invocations on them WILL fail."
                    .to_owned(),
            ),
            DeploymentStatus::Draining => Some(format!(
                "Draining. There are {total_active_inv} invocations that will break if you proceed \
                 with this operation. Please make sure in-flight invocations are completed \
                 (deployment is Drained) or killed/cancelled before continuing."
            )),
            DeploymentStatus::Drained => None,
        }
    }

    fn describe(
        &self,
        f: &mut impl OutputFormatter,
        latest_services: &HashMap<String, ServiceMetadata>,
    ) -> Result<()> {
        let mut deployment_fields = vec![(
            "deployment_id".to_owned(),
            Field::new(self.deployment_id.to_string()),
        )];
        deployment_fields.extend(deployment_info_fields(&self.deployment));
        deployment_fields.push(("status".to_owned(), deployment_status_field(self.status)));
        deployment_fields.push((
            "active_invocations".to_owned(),
            active_invocations_field(self.total_active_inv),
        ));
        f.detail("deployment", &deployment_fields);

        f.title("🤖", "Services");
        let mut items = f.start_items("services");
        for service in &self.services {
            let Some(latest_service) = latest_service(latest_services, service) else {
                continue;
            };
            let mut item = items.item();
            service_item(&mut item, service, latest_service);
            item.finish()?;
        }
        items.finish();
        Ok(())
    }
}

pub async fn run_remove(State(env): State<CliEnv>, opts: &Remove) -> Result<()> {
    // First get information about these deployments and inspect if they're drained or not.
    let client = AdminClient::new(&env).await?;
    let sql_client = DataFusionHttpClient::from(client.clone());

    let mut deployment_ids = opts.deployment_ids.clone();
    // Drop duplicates, keeping the order the user gave.
    let mut seen = HashSet::new();
    deployment_ids.retain(|id| seen.insert(id.clone()));

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    // To know the latest version of every service.
    for service in client.get_services().await?.into_body().await?.services {
        latest_services.insert(service.name.clone(), service);
    }

    let deployments = try_join_all(
        deployment_ids
            .iter()
            .map(|id| inspect(&client, &sql_client, &latest_services, id)),
    )
    .await?;

    let mut f = Formatter::new();
    if let [deployment] = &deployments[..] {
        f.title("📜", "Deployment Information");
        deployment.describe(&mut f, &latest_services)?;
    } else {
        f.title("📜", "Deployments");
        let mut items = f.start_items("deployments");
        for deployment in &deployments {
            let mut item = items.item();
            deployment.describe(&mut item, &latest_services)?;
            item.finish()?;
        }
        items.finish();
    }
    // The plan, JSON-only: the human output already describes the deployments to remove.
    let changes: Vec<_> = deployments
        .iter()
        .map(|d| json!({"deployment_id": d.deployment_id.to_string(), "change": "delete"}))
        .collect();
    f.value("changes", Field::with_display(changes, ""));

    let ids = deployments
        .iter()
        .map(|d| d.deployment_id.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    let json = CliContext::get().json_output();
    // Removing a deployment that isn't drained breaks invocations: refuse unless --force.
    let risks: Vec<_> = deployments
        .iter()
        .filter_map(|d| {
            d.risk()
                .map(|risk| format!("Deployment {} is still {risk}", d.deployment_id))
        })
        .collect();
    let force_step = format!("restate deployments remove {ids} --force");
    let force_description = "remove anyway, if you accept that risk";
    if !risks.is_empty() && !opts.force && !opts.dry_run.dry_run {
        if !json {
            // Keep the error apart from the services above it.
            c_eprintln!();
        }
        return Err(RestateCliError::bad_input(risks.join("\n\n"))
            .with_next_step(force_step, force_description)
            .into());
    }
    if risks.is_empty() {
        if !json {
            c_println!();
            if deployments.len() == 1 {
                c_success!("The deployment is fully drained and is safe to remove");
            } else {
                c_success!("The deployments are fully drained and are safe to remove");
            }
        }
    } else if opts.force {
        for risk in &risks {
            f.warning(risk);
        }
    } else {
        f.next_step(&force_step, force_description, IncludeFormatting::Yes);
        for risk in &risks {
            f.warning(&format!(
                "{risk} Without --force, removing it will be refused."
            ));
        }
    }

    let (prompt, removed) = if let [deployment] = &deployments[..] {
        (
            "Are you sure you want to remove this deployment?".to_owned(),
            format!(
                "Deployment {} removed successfully",
                deployment.deployment_id
            ),
        )
    } else {
        (
            format!(
                "Are you sure you want to remove these {} deployments?",
                deployments.len()
            ),
            format!("{} deployments removed successfully", deployments.len()),
        )
    };
    f.confirm(&opts.dry_run, &prompt)?;

    let deployment_ids: Vec<_> = deployments.iter().map(|d| d.deployment_id).collect();
    let failed = remove_deployments(&client, &deployment_ids).await?;
    if !failed.is_empty() {
        let not_removed = failed
            .iter()
            .map(|f| format!(" - {}: {}", f.deployment_id, f.error))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(RestateCliError::not_found(format!(
            "Removed {} of {} deployments. Not removed:\n{not_removed}",
            deployment_ids.len() - failed.len(),
            deployment_ids.len()
        ))
        .into());
    }

    f.outcome(
        "result",
        Field::with_display("removed", removed),
        Outcome::Success,
    );
    f.next_step(
        "restate deployments list",
        "see the remaining deployments",
        IncludeFormatting::Yes,
    );
    f.finish()
}

/// Removes the deployments, in chunks the server accepts, and returns the ones that couldn't be.
async fn remove_deployments(
    client: &AdminClient,
    deployment_ids: &[DeploymentId],
) -> Result<Vec<FailedDeploymentDeletion>> {
    //TODO: Use opts.force when the server implements the false + validation case!
    let force = true;
    if let [deployment_id] = deployment_ids {
        client
            .remove_deployment(&deployment_id.to_string(), force)
            .await?
            .into_success_status()
            .await?;
        return Ok(vec![]);
    }

    let mut failed = vec![];
    for (i, chunk) in deployment_ids.chunks(BATCH_OPERATION_MAX_SIZE).enumerate() {
        let response = client
            .bulk_remove_deployments(BulkDeleteDeploymentsRequest {
                deployment_ids: chunk.to_vec(),
                force,
            })
            .await?;
        if response.status_code() == StatusCode::METHOD_NOT_ALLOWED {
            // Servers without the bulk delete API: remove the deployments one by one.
            for deployment_id in &deployment_ids[i * BATCH_OPERATION_MAX_SIZE..] {
                client
                    .remove_deployment(&deployment_id.to_string(), force)
                    .await?
                    .into_success_status()
                    .await?;
            }
            break;
        }
        failed.extend(response.into_body().await?.failed);
    }
    Ok(failed)
}
