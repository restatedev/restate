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

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::ui::watcher::Watch;
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::{
    active_invocations_field, calculate_deployment_status, deployment_info_fields,
    deployment_status_field, latest_service, service_item,
};
use crate::ui::fmt::{Field, Formatter, IfEmpty, OutputFormatter};
use crate::ui::service_handlers::handler_description;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
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

    let active_inv = if opts.extra {
        let sql_client = crate::clients::DataFusionHttpClient::from(client);
        let active_inv = count_deployment_active_inv_by_method(&sql_client, &deployment_id).await?;
        Some(active_inv)
    } else {
        None
    };

    // Deployment header fields.
    let mut deployment_fields: Vec<(String, Field)> = vec![(
        "deployment_id".to_owned(),
        Field::new(deployment_id.to_string()),
    )];
    deployment_fields.extend(deployment_info_fields(&deployment));

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

        deployment_fields.push(("status".to_owned(), deployment_status_field(status)));
        deployment_fields.push((
            "active_invocations".to_owned(),
            active_invocations_field(total_active_inv),
        ));
    }

    let mut f = Formatter::new();
    f.title("📜", "Deployment Information");
    f.detail("deployment", &deployment_fields);

    f.title("🤖", "Services");
    let mut items = f.start_items("services");
    for service in &services {
        let Some(latest_service) = latest_service(&latest_services, service) else {
            continue;
        };
        let mut item = items.item();
        service_item(&mut item, service, latest_service);

        let mut handlers: Vec<_> = service.handlers.values().collect();
        handlers.sort_by(|a, b| a.name.cmp(&b.name));
        let with_description = handlers.iter().any(|h| handler_description(h).is_some());
        let mut headers = vec!["handler", "input", "output"];
        if opts.extra {
            headers.push("active_invocations");
        }
        if with_description {
            headers.push("description");
        }
        let rows = handlers.into_iter().map(|handler| {
            let mut row = vec![
                Field::new(handler.name.as_str()),
                Field::new(handler.input_description.as_str()),
                Field::new(handler.output_description.as_str()),
            ];
            if opts.extra {
                // How many invocations are pinned on this deployment+service+handler.
                let count = active_inv
                    .iter()
                    .flatten()
                    .find(|x| x.service == service.name && x.handler == handler.name)
                    .map_or(0, |x| x.inv_count);
                row.push(active_invocations_field(count));
            }
            if with_description {
                row.push(Field::new(handler_description(handler)));
            }
            row
        });
        item.table("handlers", &headers, rows, IfEmpty::Nothing);
        item.finish()?;
    }
    items.finish();

    f.finish()
}
