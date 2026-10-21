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
use std::sync::Arc;

use anyhow::Result;
use cling::prelude::*;
use futures::{StreamExt, TryStreamExt, stream};
use http::StatusCode;
use itertools::Itertools;
use serde_json::json;

use restate_admin_rest_model::deployments::{
    BulkDeleteDeploymentsRequest, DeploymentDeletionFailureReason, FailedDeploymentDeletion,
    ServiceNameRevPair,
};
use restate_admin_rest_model::invocations::BATCH_OPERATION_MAX_SIZE;
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_eprintln, c_println, c_success, exit};
use restate_types::identifiers::DeploymentId;
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_active_inv_by_deployment;
use crate::clients::{AdminClient, AdminClientInterface, DataFusionHttpClient, Deployment};
use crate::error::RestateCliError;
use crate::ui::deployments::{
    DeploymentStatus, active_invocations_field, calculate_deployment_status,
    deployment_info_fields, deployment_status_field, latest_service, service_item,
};
use crate::ui::fmt::{
    DryRun, Field, Formatter, IfEmpty, IncludeFormatting, Outcome, OutputFormatter,
};

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
    deployment_ids: Vec<DeploymentId>,

    #[clap(flatten)]
    dry_run: DryRun,
}

/// How many deployments are inspected concurrently, each with a request to the server.
const MAX_CONCURRENT_INSPECTIONS: usize = 16;

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
    latest_services: &HashMap<String, ServiceMetadata>,
    deployment_id: DeploymentId,
    total_active_inv: i64,
) -> Result<Inspected> {
    let deployment = client
        .get_deployment(deployment_id.to_string())
        .await?
        .into_body()
        .await?;
    let (deployment_id, deployment, services) =
        Deployment::from_detailed_deployment_response(deployment);
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
        let Self {
            deployment_id,
            status,
            total_active_inv,
            ..
        } = self;
        match status {
            DeploymentStatus::Active => Some(format!(
                "Deployment {deployment_id} is still Active. This means that it hosts the latest \
                 revision of some of your services as indicated above. Removing this deployment \
                 will cause those services to be unavailable and current or future invocations on \
                 them WILL fail."
            )),
            DeploymentStatus::Draining => Some(format!(
                "Deployment {deployment_id} is still Draining. There are {total_active_inv} \
                 invocations that will break if you proceed with this operation. Please make sure \
                 in-flight invocations are completed (deployment is Drained) or killed/cancelled \
                 before continuing."
            )),
            DeploymentStatus::Drained => None,
        }
    }

    fn describe(
        &self,
        f: &mut impl OutputFormatter,
        latest_services: &HashMap<String, ServiceMetadata>,
    ) -> Result<()> {
        let mut deployment_fields =
            vec![("deployment_id".to_owned(), Field::new(self.deployment_id))];
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

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    // To know the latest version of every service.
    for service in client.get_services().await?.into_body().await?.services {
        latest_services.insert(service.name.clone(), service);
    }

    let deployment_ids: Vec<_> = opts.deployment_ids.iter().copied().unique().collect();
    // One scan of the invocation status table for all deployments, rather than one each.
    let mut active_inv = HashMap::new();
    for chunk in deployment_ids.chunks(BATCH_OPERATION_MAX_SIZE) {
        active_inv.extend(count_active_inv_by_deployment(&sql_client, chunk).await?);
    }
    let inspections = deployment_ids.iter().copied().map(|id| {
        let total_active_inv = active_inv.get(&id).copied().unwrap_or_default();
        inspect(&client, &latest_services, id, total_active_inv)
    });
    let deployments: Vec<_> = stream::iter(inspections)
        .buffered(MAX_CONCURRENT_INSPECTIONS)
        .try_collect()
        .await?;

    let mut f = Formatter::new();
    f.title("📜", "Deployments");
    let mut items = f.start_items("deployments");
    for deployment in &deployments {
        let mut item = items.item();
        deployment.describe(&mut item, &latest_services)?;
        item.finish()?;
    }
    items.finish();
    // The plan, JSON-only: the human output already describes the deployments to remove.
    let changes: Vec<_> = deployments
        .iter()
        .map(|d| json!({"deployment_id": d.deployment_id, "change": "delete"}))
        .collect();
    f.value("changes", Field::with_display(changes, ""));

    let ids = deployments.iter().map(|d| d.deployment_id).join(" ");
    let json = CliContext::get().json_output();
    // Removing a deployment that isn't drained breaks invocations: refuse unless --force.
    let risks: Vec<_> = deployments.iter().filter_map(Inspected::risk).collect();
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
            c_success!("The deployments are fully drained and are safe to remove");
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

    f.confirm(
        &opts.dry_run,
        "Are you sure you want to remove these deployments?",
    )?;

    //TODO: Use opts.force when the server implements the false + validation case!
    let failed = remove_deployments(&client, &deployments, true).await;

    // One row per deployment: `removed`, or why it wasn't.
    let failures: HashMap<_, _> = failed.iter().map(|f| (f.deployment_id, f)).collect();
    let rows = deployments
        .iter()
        .map(|d| match failures.get(&d.deployment_id) {
            None => [
                Field::new(d.deployment_id),
                Field::styled("removed", Style::Success),
                Field::new(None::<&str>),
            ],
            Some(failure) => [
                Field::new(d.deployment_id),
                Field::styled(failure.outcome, Style::Danger),
                Field::new(failure.error.as_deref()),
            ],
        });
    f.table(
        "results",
        &["deployment_id", "outcome", "error"],
        rows,
        IfEmpty::Nothing,
    );

    if failed.is_empty() {
        f.outcome(
            "result",
            Field::with_display(
                "removed",
                format!("Deployments removed: {}", deployments.len()),
            ),
            Outcome::Success,
        );
        f.next_step(
            "restate deployments list",
            "see the remaining deployments",
            IncludeFormatting::Yes,
        );
        return f.finish();
    }
    f.outcome(
        "result",
        Field::with_display(
            "partially_removed",
            format!(
                "Deployments removed: {} of {}, see the errors above",
                deployments.len() - failed.len(),
                deployments.len()
            ),
        ),
        Outcome::Failure,
    );
    f.finish()?;
    // The output already describes the failures: only signal them through the exit code.
    Err(exit::AlreadyReported {
        code: exit::GENERIC_ERROR,
    }
    .into())
}

/// A deployment that wasn't removed.
#[derive(Debug, PartialEq)]
struct NotRemoved {
    deployment_id: DeploymentId,
    /// The server's reason (e.g. `not_found`), `unknown` when the request failed, so it may or may
    /// not have been applied, or `not_attempted` after an earlier request failed.
    outcome: &'static str,
    error: Option<Arc<str>>,
}

impl From<FailedDeploymentDeletion> for NotRemoved {
    fn from(failure: FailedDeploymentDeletion) -> Self {
        Self {
            deployment_id: failure.deployment_id,
            outcome: match failure.reason {
                DeploymentDeletionFailureReason::NotFound => "not_found",
                DeploymentDeletionFailureReason::Unknown => "failed",
            },
            error: Some(failure.error.into()),
        }
    }
}

/// Records the deployments of a failed request, and the ones not attempted after it.
fn give_up(
    not_removed: &mut Vec<NotRemoved>,
    unknown: impl IntoIterator<Item = DeploymentId>,
    not_attempted: impl IntoIterator<Item = DeploymentId>,
    error: &anyhow::Error,
) {
    let error: Arc<str> = RestateCliError::from(error).message().into();
    not_removed.extend(
        unknown
            .into_iter()
            .map(|deployment_id| NotRemoved {
                deployment_id,
                outcome: "unknown",
                error: Some(Arc::clone(&error)),
            })
            .chain(not_attempted.into_iter().map(|deployment_id| NotRemoved {
                deployment_id,
                outcome: "not_attempted",
                error: None,
            })),
    );
}

/// Removes the deployments, in chunks the server accepts, and returns the ones that weren't. A
/// failed request stops the removal, so the chunks before it are still reported.
async fn remove_deployments(
    client: &AdminClient,
    deployments: &[Inspected],
    force: bool,
) -> Vec<NotRemoved> {
    fn ids(deployments: &[Inspected]) -> impl Iterator<Item = DeploymentId> + '_ {
        deployments.iter().map(|d| d.deployment_id)
    }
    let mut not_removed = vec![];
    for (i, chunk) in deployments.chunks(BATCH_OPERATION_MAX_SIZE).enumerate() {
        let remaining = &deployments[i * BATCH_OPERATION_MAX_SIZE..];
        match bulk_remove(client, chunk, force).await {
            Ok(Some(failed)) => not_removed.extend(failed.into_iter().map(NotRemoved::from)),
            Ok(None) => {
                // Servers before the bulk delete API: remove the deployments one by one.
                for (j, deployment) in remaining.iter().enumerate() {
                    if let Err(err) =
                        remove_deployment(client, deployment.deployment_id, force).await
                    {
                        give_up(
                            &mut not_removed,
                            [deployment.deployment_id],
                            ids(&remaining[j + 1..]),
                            &err,
                        );
                        break;
                    }
                }
                break;
            }
            Err(err) => {
                give_up(
                    &mut not_removed,
                    ids(chunk),
                    ids(&remaining[chunk.len()..]),
                    &err,
                );
                break;
            }
        }
    }
    not_removed
}

/// Removes the deployments with the bulk API, and returns the ones that weren't, or `None` when
/// the server doesn't have the bulk API.
async fn bulk_remove(
    client: &AdminClient,
    deployments: &[Inspected],
    force: bool,
) -> Result<Option<Vec<FailedDeploymentDeletion>>> {
    let response = client
        .bulk_remove_deployments(BulkDeleteDeploymentsRequest {
            deployment_ids: deployments.iter().map(|d| d.deployment_id).collect(),
            force,
        })
        .await?;
    if response.status_code() == StatusCode::METHOD_NOT_ALLOWED {
        return Ok(None);
    }
    Ok(Some(response.into_body().await?.failed))
}

async fn remove_deployment(
    client: &AdminClient,
    deployment_id: DeploymentId,
    force: bool,
) -> Result<()> {
    client
        .remove_deployment(&deployment_id.to_string(), force)
        .await?
        .into_success_status()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn give_up_marks_the_failed_request_and_the_rest() {
        let [failed, also_failed, skipped] = std::array::from_fn(|_| DeploymentId::new());
        let mut not_removed = vec![NotRemoved::from(FailedDeploymentDeletion {
            deployment_id: DeploymentId::new(),
            reason: DeploymentDeletionFailureReason::NotFound,
            error: "missing".to_owned(),
        })];
        give_up(
            &mut not_removed,
            [failed, also_failed],
            [skipped],
            &anyhow::anyhow!("connection reset"),
        );

        let error: Option<Arc<str>> = Some("connection reset".into());
        assert_eq!(not_removed[0].outcome, "not_found");
        assert_eq!(
            not_removed[1..],
            [
                NotRemoved {
                    deployment_id: failed,
                    outcome: "unknown",
                    error: error.clone(),
                },
                NotRemoved {
                    deployment_id: also_failed,
                    outcome: "unknown",
                    error,
                },
                NotRemoved {
                    deployment_id: skipped,
                    outcome: "not_attempted",
                    error: None,
                },
            ]
        );
    }
}
