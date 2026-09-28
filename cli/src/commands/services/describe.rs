// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;
use indicatif::ProgressBar;
use itertools::Itertools;

use restate_cli_util::ui::watcher::Watch;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::{
    active_invocations_field, deployment_info_fields, render_deployment_type, render_deployment_url,
};
use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::ui::service_handlers::{service_type_field, visibility_label, write_service_handlers};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    /// service name
    name: String,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_describe(State(env): State<CliEnv>, opts: &Describe) -> Result<()> {
    opts.watch.run(|| describe(&env, opts)).await
}

async fn describe(env: &CliEnv, opts: &Describe) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let service = client.get_service(&opts.name).await?.into_body().await?;

    let deployment = client
        .get_deployment(&service.deployment_id.to_string())
        .await?
        .into_body()
        .await?;
    let (_, deployment, _) = Deployment::from_detailed_deployment_response(deployment);

    let mut f = Formatter::new();
    f.next_step(
        &format!("restate services status {}", service.name),
        "see the service's invocation activity per handler",
    );
    f.next_step(
        &format!("restate invocations list --service {}", service.name),
        "list the service's active invocations",
    );

    f.title("📜", "Service Information");
    f.detail(
        "service",
        [
            ("name", Field::new(service.name.clone())),
            ("service_type", service_type_field(&service.ty)),
            ("revision", Field::new(service.revision)),
            ("visibility", Field::new(visibility_label(service.public))),
        ],
    );
    f.title("📜", "Deployment Information");
    f.detail(
        "deployment",
        vec![
            vec![(
                "deployment_id".to_string(),
                Field::new(service.deployment_id.to_string()),
            )],
            deployment_info_fields(&deployment),
        ]
        .into_iter()
        .concat(),
    );

    // Handlers
    f.title("🔌", "Handlers");
    write_service_handlers(&mut f, service.handlers.values());

    // Printing other existing endpoints with previous revisions. We currently don't
    // have an API to get endpoints by service name so we get everything and filter
    // locally in this case.
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Retrieving information about older deployments");

    let service_name = service.name;
    let latest_rev = service.revision;
    let mut other_deployments: Vec<_> = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments
        .into_iter()
        .filter_map(|e| {
            let (other_deployment_id, other_deployment, other_deployment_services) = Deployment::from_deployment_response(e);

            // endpoints that serve the same service.
            let service_match: Vec<_> = other_deployment_services
                .iter()
                .filter(|s| s.name == service_name && s.revision != latest_rev)
                .collect();
            // we should see either one or zero matches, more than one means that an endpoint is
            // hosting multiple revisions of the _the same_ service which indicates that something
            // is so wrong!
            if service_match.len() > 1 {
                progress.finish_and_clear();
                panic!(
                    "Deployment {other_deployment_id} is hosting multiple revisions of the same service {service_name}!"
                );
            }

            service_match.first().map(|service_match| {
                (
                    other_deployment_id,
                    other_deployment,
                    service_match.revision,
                )
            })
        })
        .collect();

    if other_deployments.is_empty() {
        progress.finish_and_clear();
        return f.finish();
    }

    let sql_client = crate::clients::DataFusionHttpClient::from(client);
    // sort other_endpoints by revision in descending order
    other_deployments.sort_by(|(_, _, rev1), (_, _, rev2)| rev2.cmp(rev1));

    let mut rows = Vec::with_capacity(other_deployments.len());
    for (deployment_id, deployment_metadata, rev) in other_deployments {
        let active_inv = count_deployment_active_inv(&sql_client, &deployment_id).await?;

        rows.push(vec![
            Field::new(render_deployment_url(&deployment_metadata)),
            Field::new(render_deployment_type(&deployment_metadata)),
            Field::new(rev),
            active_invocations_field(active_inv),
            Field::new(deployment_id.to_string()),
        ]);
    }

    progress.finish_and_clear();

    f.title("👵", "Older Revisions");
    f.table(
        "older_revisions",
        &[
            "address",
            "type",
            "service_revision",
            "active_invocations",
            "deployment_id",
        ],
        &rows,
    );

    f.finish()
}
