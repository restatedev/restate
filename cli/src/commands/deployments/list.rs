// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::HashMap;

use anyhow::Result;
use cling::prelude::*;
use serde::Serialize;

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::ui::watcher::Watch;
use restate_types::identifiers::{DeploymentId, ServiceRevision};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::{AdminClientInterface, Deployment};
use crate::ui::datetime::DateTimeExt;
use crate::ui::deployments::{
    DeploymentStatus, calculate_deployment_status, render_deployment_url, render_transport_protocol,
};
use crate::ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, ListItem, OutputFormatter};

/// List the registered deployments, with their endpoint and services
#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
#[command(after_help = after_help!(
    examples: [
        "restate deployments list --extra --json",
        "restate deployments list --extra --json \\",
        "  | jq -r '.deployments[] | select(.status == \"Drained\") | .deployment_id'",
    ],
    learn_more: "https://docs.restate.dev/foundations/services#deployments-endpoints-and-versions",
))]
pub struct List {
    /// Also show each deployment's status (active, draining or drained) and number of active
    /// invocations
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
    let sql_client = crate::clients::DataFusionHttpClient::from(client.clone());
    // To know the latest version of every service.
    let services = client.get_services().await?.into_body().await?.services;

    let deployments = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments;

    // For each deployment, we need to calculate the status and # of invocations.
    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    for service in services {
        latest_services.insert(service.name.clone(), service);
    }

    let mut enriched_deployments: Vec<EnrichedDeployment> = Vec::with_capacity(deployments.len());

    for deployment in deployments {
        let (deployment_id, deployment, services) =
            Deployment::from_deployment_response(deployment);

        let mut enriched_deployment = EnrichedDeployment {
            deployment_id,
            deployment,
            services,
            active_invocations: None,
            status: None,
        };
        // calculate status and counters.
        if list_opts.extra {
            let active_inv = count_deployment_active_inv(&sql_client, &deployment_id).await?;

            enriched_deployment.active_invocations = Some(active_inv);

            enriched_deployment.status = calculate_deployment_status(
                &deployment_id,
                &enriched_deployment.services,
                active_inv,
                &latest_services,
            )
            .into();
        }

        enriched_deployments.push(enriched_deployment);
    }

    // Sort by active, draining, drained, then newest by creation time within the same status.
    enriched_deployments.sort_unstable_by_key(|endriched_deployment| {
        let order = match endriched_deployment.status {
            Some(DeploymentStatus::Active) => 0,
            Some(DeploymentStatus::Draining) => 1,
            Some(DeploymentStatus::Drained) => 2,
            None => 3,
        };
        (order, Reverse(endriched_deployment.deployment.created_at()))
    });

    let items: Vec<DeploymentListItem> = enriched_deployments
        .into_iter()
        .map(|d| DeploymentListItem::new(d, &latest_services))
        .collect();

    let mut f = Formatter::new();
    f.list(
        "deployments",
        &items,
        IfEmpty::Say(
            "No deployments were found! Did you forget to register your deployment with 'restate dep register'?",
        ),
    )?;
    if let Some(item) = items.first() {
        f.next_step(
            &format!("restate deployments describe {}", item.deployment_id),
            "see the deployment's services and endpoint details",
            IncludeFormatting::Yes,
        );
    }
    f.finish()
}

struct EnrichedDeployment {
    deployment_id: DeploymentId,
    deployment: Deployment,
    services: Vec<ServiceNameRevPair>,
    status: Option<DeploymentStatus>,
    active_invocations: Option<i64>,
}

#[derive(Serialize)]
struct DeploymentListItem {
    deployment_id: DeploymentId,
    endpoint: String,
    /// e.g. `HTTP/2.0` or `AWS Lambda`.
    transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<DeploymentStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_invocations: Option<i64>,
    created_at: String,
    services: Vec<ServiceListEntry>,
}

#[derive(Serialize)]
struct ServiceListEntry {
    name: String,
    revision: ServiceRevision,
    /// Whether this deployment serves the latest revision of the service.
    latest: bool,
}

impl DeploymentListItem {
    fn new(d: EnrichedDeployment, latest_services: &HashMap<String, ServiceMetadata>) -> Self {
        let mut services: Vec<_> = d
            .services
            .into_iter()
            .map(|svc| ServiceListEntry {
                latest: latest_services
                    .get(&svc.name)
                    .is_some_and(|latest| latest.deployment_id == d.deployment_id),
                name: svc.name,
                revision: svc.revision,
            })
            .collect();
        services.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            deployment_id: d.deployment_id,
            endpoint: render_deployment_url(&d.deployment),
            transport: render_transport_protocol(&d.deployment),
            status: d.status,
            active_invocations: d.active_invocations,
            created_at: d.deployment.created_at().iso(),
            services,
        }
    }
}

/// `deployments list` item: id and endpoint (with the HTTP version), then the services and
/// (with `--extra`, which fetches them) the status and active invocations.
impl ListItem for DeploymentListItem {
    const HEADERS: &'static [&'static str] = &["deployment_id", "endpoint"];

    fn columns(&self) -> Vec<Field> {
        let endpoint = match self.transport.strip_prefix("HTTP/") {
            // e.g. `HTTP/2.0` -> `HTTP 2`, `HTTP/1.1` -> `HTTP 1.1`
            Some(version) => {
                let version = version.strip_suffix(".0").unwrap_or(version);
                format!("{} (HTTP {version})", self.endpoint)
            }
            None => self.endpoint.clone(),
        };
        vec![
            Field::new(self.deployment_id.to_string()),
            Field::new(endpoint),
        ]
    }

    fn details(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if !self.services.is_empty() {
            let services = self
                .services
                .iter()
                .map(|svc| {
                    let superseded = if svc.latest { "" } else { " (superseded)" };
                    format!("{}@{}{superseded}", svc.name, svc.revision)
                })
                .collect::<Vec<_>>();
            lines.push(format!("services {}", services.join(", ")));
        }
        if let (Some(status), Some(invocations)) = (self.status, self.active_invocations) {
            lines.push(format!(
                "status {status:?} · {invocations} active invocations"
            ));
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_headers_are_json_keys() {
        crate::ui::fmt::assert_headers_are_json_keys(&DeploymentListItem {
            deployment_id: DeploymentId::new(),
            endpoint: "http://localhost:9080/".to_owned(),
            transport: "HTTP/2.0".to_owned(),
            status: None,
            active_invocations: None,
            created_at: String::new(),
            services: Vec::new(),
        });
    }
}
