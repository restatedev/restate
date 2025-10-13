// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use comfy_table::{Cell, Row, Table};

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::c_error;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::{AdminClientInterface, Deployment};
use crate::console::c_println;
use crate::ui::datetime::DateTimeExt;
use crate::ui::deployments::{
    DeploymentStatus, calculate_deployment_status, render_active_invocations,
    render_deployment_status, render_deployment_type, render_deployment_url,
};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
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
    let sql_client = crate::clients::DataFusionHttpClient::from(client.clone());
    // To know the latest version of every service.
    let services = client.get_services().await?.into_body().await?.services;

    let deployments = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments;

    if deployments.is_empty() {
        c_error!(
            "No deployments were found! Did you forget to register your deployment with 'restate dep register'?"
        );
        return Ok(());
    }
    // For each deployment, we need to calculate the status and # of invocations.
    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    for service in services {
        latest_services.insert(service.name.clone(), service);
    }

    let mut table = Table::new_styled();

    table.set_styled_header(DeploymentRow::headers(list_opts.extra));

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

    for EnrichedDeployment {
        deployment_id,
        deployment,
        services,
        status,
        active_invocations,
    } in enriched_deployments
    {
        let mut row = DeploymentRow::default();
        row.with_id(deployment_id)
            .with_url(render_deployment_url(&deployment))
            .with_type(render_deployment_type(&deployment))
            .with_created_at(match &deployment {
                Deployment::Http { created_at, .. } => created_at.display(),
                Deployment::Lambda { created_at, .. } => created_at.display(),
            })
            .with_services(render_services(&deployment_id, &services, &latest_services));

        if let Some(status) = status {
            row.with_status(render_deployment_status(status));
        }

        if let Some(active_inv) = active_invocations {
            row.with_active_invocations(render_active_invocations(active_inv));
        }

        table.add_row(row.into_row(list_opts.extra));
    }

    c_println!("{}", table);

    Ok(())
}

fn render_services(
    deployment_id: &DeploymentId,
    services: &[ServiceNameRevPair],
    latest_services: &HashMap<String, ServiceMetadata>,
) -> Cell {
    use std::fmt::Write as FmtWrite;

    let mut out = String::new();
    for service in services {
        if !out.is_empty() {
            writeln!(&mut out).unwrap();
        }

        if let Some(latest_service) = latest_services.get(&service.name) {
            let style = if &latest_service.deployment_id == deployment_id {
                // We are hosting the latest revision of this service.
                Style::Success
            } else {
                Style::Normal
            };
            write!(
                &mut out,
                "- {} [{}]",
                &service.name,
                Styled(style, service.revision)
            )
            .unwrap();
        } else {
            // We couldn't find that service in latest_services? that's odd. We
            // highlight this with bright red to highlight the issue.
            write!(
                &mut out,
                "- {} [{}]",
                Styled(Style::Danger, &service.name),
                service.revision
            )
            .unwrap();
        }
    }
    Cell::new(out)
}

struct EnrichedDeployment {
    deployment_id: DeploymentId,
    deployment: Deployment,
    services: Vec<ServiceNameRevPair>,
    status: Option<DeploymentStatus>,
    active_invocations: Option<i64>,
}

#[derive(Default)]
struct DeploymentRow {
    url: Option<Cell>,
    deployment_type: Option<Cell>,
    status: Option<Cell>,
    active_invocations: Option<Cell>,
    id: Option<Cell>,
    created_at: Option<Cell>,
    services: Option<Cell>,
}

impl DeploymentRow {
    fn headers(extra: bool) -> Vec<&'static str> {
        if extra {
            vec![
                "DEPLOYMENT",
                "TYPE",
                "STATUS",
                "ACTIVE-INVOCATIONS",
                "ID",
                "CREATED-AT",
                "SERVICES",
            ]
        } else {
            vec!["DEPLOYMENT", "TYPE", "ID", "CREATED-AT"]
        }
    }

    fn with_id<T: Into<Cell>>(&mut self, id: T) -> &mut Self {
        self.id = Some(id.into());
        self
    }

    fn with_url<T: Into<Cell>>(&mut self, url: T) -> &mut Self {
        self.url = Some(url.into());
        self
    }

    fn with_type<T: Into<Cell>>(&mut self, deployment_type: T) -> &mut Self {
        self.deployment_type = Some(deployment_type.into());
        self
    }

    fn with_status<T: Into<Cell>>(&mut self, status: T) -> &mut Self {
        self.status = Some(status.into());
        self
    }

    fn with_active_invocations<T: Into<Cell>>(&mut self, active_invocations: T) -> &mut Self {
        self.active_invocations = Some(active_invocations.into());
        self
    }

    fn with_created_at<T: Into<Cell>>(&mut self, created_at: T) -> &mut Self {
        self.created_at = Some(created_at.into());
        self
    }

    fn with_services<T: Into<Cell>>(&mut self, services: T) -> &mut Self {
        self.services = Some(services.into());
        self
    }

    fn into_row(self, extra: bool) -> Row {
        if extra {
            vec![
                self.url.expect("is set"),
                self.deployment_type.expect("is set"),
                self.status.expect("is set"),
                self.active_invocations.expect("is set"),
                self.id.expect("is set"),
                self.created_at.expect("is set"),
                self.services.expect("is set"),
            ]
        } else {
            vec![
                self.url.expect("is set"),
                self.deployment_type.expect("is set"),
                self.id.expect("is set"),
                self.created_at.expect("is set"),
                self.services.expect("is set"),
            ]
        }
        .into()
    }
}
