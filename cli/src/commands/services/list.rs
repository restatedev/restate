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

use anyhow::{Context, Result};
use cling::prelude::*;
use serde::Serialize;

use restate_admin_rest_model::deployments::DeploymentResponse;
use restate_cli_util::CliContext;
use restate_cli_util::c_error;
use restate_cli_util::ui::watcher::Watch;
use restate_types::identifiers::{DeploymentId, ServiceRevision};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClientInterface, Deployment};
use crate::ui::deployments::render_deployment_url;
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};
use crate::ui::service_handlers::{service_type_label, service_type_machine, visibility_label};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Show only publicly accessible services
    #[clap(long)]
    public_only: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_list(State(env): State<CliEnv>, opts: &List) -> Result<()> {
    opts.watch.run(|| list(&env, opts)).await
}

async fn list(env: &CliEnv, list_opts: &List) -> Result<()> {
    let client = crate::clients::AdminClient::new(env).await?;
    let defs = client.get_services().await?.into_body().await?;

    if defs.services.is_empty() && !CliContext::get().json_output() {
        c_error!(
            "No services were found! Services are added by registering deployments with 'restate dep register'"
        );
        return Ok(());
    }

    let deployments = client.get_deployments().await?.into_body().await?;

    let mut deployment_cache: HashMap<DeploymentId, DeploymentResponse> = HashMap::new();

    // Caching endpoints
    for deployment in deployments.deployments {
        deployment_cache.insert(deployment.id(), deployment);
    }

    let mut items = Vec::new();
    for svc in defs.services {
        if list_opts.public_only && !svc.public {
            // Skip non-public services if users chooses to.
            continue;
        }
        let deployment = deployment_cache
            .get(&svc.deployment_id)
            .with_context(|| format!("Deployment {} was not found!", svc.deployment_id))?;
        let (_, deployment, _) = Deployment::from_deployment_response(deployment.clone());

        let mut handlers: Vec<String> = svc.handlers.into_values().map(|h| h.name).collect();
        handlers.sort();
        items.push(ServiceListItem {
            revision: svc.revision,
            service_type: service_type_machine(&svc.ty),
            deployment_id: svc.deployment_id.to_string(),
            endpoint: render_deployment_url(&deployment),
            handlers,
            visibility: visibility_label(svc.public),
            service_type_label: service_type_label(&svc.ty),
            name: svc.name,
        });
    }

    let mut f = Formatter::new();
    f.list("services", &items)?;
    if let Some(ServiceListItem {
        name,
        deployment_id,
        ..
    }) = items.first()
    {
        f.next_step(
            &format!("restate services describe {name}"),
            "see the service's handlers",
        );
        f.next_step(
            &format!("restate deployments describe {deployment_id}"),
            "see the deployment serving it",
        );
    }
    f.finish()
}

#[derive(Serialize)]
struct ServiceListItem {
    name: String,
    revision: ServiceRevision,
    service_type: &'static str,
    deployment_id: String,
    endpoint: String,
    handlers: Vec<String>,
    visibility: &'static str,
    #[serde(skip)]
    service_type_label: &'static str,
}

impl ListItem for ServiceListItem {
    const HEADERS: &'static [&'static str] = &["service", "type"];

    fn columns(&self) -> Vec<Field> {
        vec![
            Field::new(self.name.as_str()),
            Field::new(self.service_type_label),
        ]
    }

    fn details(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "deployment {} · {}",
            self.deployment_id, self.endpoint
        )];
        if !self.handlers.is_empty() {
            lines.push(format!("handlers {}", self.handlers.join(", ")));
        }
        lines
    }
}
