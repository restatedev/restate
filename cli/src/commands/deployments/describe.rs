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
use comfy_table::{Cell, Table};
use serde_json::{Value, json};

use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::CliContext;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{c_eprintln, c_indent_table, c_println, c_title};
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_deployment_active_inv_by_method;
use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::{
    active_invocations_field, calculate_deployment_status, deployment_info_fields,
    deployment_status_field,
};
use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::ui::service_handlers::{handler_description, service_type_label, service_type_machine};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
#[clap(visible_alias = "get")]
pub struct Describe {
    // TODO: Support inference of endpoint or ID, but this require the deployment
    // ID to follow a more constrained format
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
    let mut deployment_fields: Vec<(String, Field)> =
        vec![("id".to_owned(), Field::new(deployment_id.to_string()))];
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
            "invocations".to_owned(),
            active_invocations_field(total_active_inv),
        ));
    }

    let mut f = Formatter::new();
    f.title("📜", "Deployment Information");
    f.detail("deployment", &deployment_fields);

    if CliContext::get().json_output() {
        // Services carry nested handlers, which the flat formatter can't model, so
        // emit them as a nested JSON value.
        f.value(
            "services",
            Field::json(services_json(
                &services,
                &latest_services,
                active_inv.as_deref(),
                opts.extra,
            )),
        );
        return f.finish();
    }

    // Human: rich, indented per-service rendering.
    c_println!();
    c_title!("🤖", "Services");
    let mut methods_header = vec!["HANDLER", "INPUT", "OUTPUT"];
    if opts.extra {
        methods_header.push("ACTIVE-INVOCATIONS");
    }

    for service in services {
        let Some(latest_service) = latest_services.get(&service.name) else {
            // if we can't find this service in the latest set of services, something is off. A
            // deployment cannot remove services defined by other deployment, so we should warn that
            // this is happening.
            c_eprintln!(
                "Service {} is not found in the latest set of services. This is unexpected.",
                service.name
            );
            continue;
        };

        // Indented like the key/value rows of the detail table above.
        c_println!(" - {}", Styled(Style::Info, &service.name));
        c_println!("   Type: {}", service_type_label(&service.ty));

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
        c_println!(
            "   Revision: {} {}",
            service.revision,
            latest_revision_message
        );
        let mut methods_table = Table::new_styled();

        let mut handlers: Vec<_> = service.handlers.values().collect();
        handlers.sort_by(|a, b| a.name.cmp(&b.name));
        let with_description = handlers.iter().any(|h| handler_description(h).is_some());
        let mut header = methods_header.clone();
        if with_description {
            header.push("DESCRIPTION");
        }
        methods_table.set_styled_header(header);

        for handler in handlers {
            let mut row = vec![
                Cell::new(&handler.name),
                Cell::new(&handler.input_description),
                Cell::new(&handler.output_description),
            ];

            if opts.extra {
                // how many inv pinned on this deployment+service+method.
                let active_inv = active_inv
                    .iter()
                    .flatten()
                    .filter(|x| x.service == service.name && x.handler == handler.name)
                    .map(|x| x.inv_count)
                    .next()
                    .unwrap_or(0);

                row.push(crate::ui::deployments::render_active_invocations(
                    active_inv,
                ));
            }
            if with_description {
                row.push(Cell::new(handler_description(handler).unwrap_or_default()));
            }

            methods_table.add_row(row);
        }
        c_indent_table!(1, methods_table);
        c_println!();
    }

    f.finish()
}

/// Nested `services` value for `--json`: an array of services, each with its handlers.
fn services_json(
    services: &[ServiceMetadata],
    latest_services: &HashMap<String, ServiceMetadata>,
    active_inv: Option<&[crate::clients::datafusion_helpers::ServiceHandlerUsage]>,
    extra: bool,
) -> Value {
    let mut out = Vec::with_capacity(services.len());
    for service in services {
        let Some(latest_service) = latest_services.get(&service.name) else {
            continue;
        };
        let is_latest = service.revision == latest_service.revision;

        let mut service_handlers: Vec<_> = service.handlers.values().collect();
        service_handlers.sort_by(|a, b| a.name.cmp(&b.name));
        let handlers: Vec<Value> = service_handlers
            .into_iter()
            .map(|handler| {
                let mut handler_json = json!({
                    "handler": handler.name,
                    "input": handler.input_description,
                    "output": handler.output_description,
                    "description": handler_description(handler),
                });
                if extra {
                    let count = active_inv
                        .into_iter()
                        .flatten()
                        .filter(|x| x.service == service.name && x.handler == handler.name)
                        .map(|x| x.inv_count)
                        .next()
                        .unwrap_or(0);
                    handler_json["active_invocations"] = json!(count);
                }
                handler_json
            })
            .collect();

        let mut service_json = json!({
            "name": service.name,
            "service_type": service_type_machine(&service.ty),
            "revision": service.revision,
            "latest": is_latest,
            "handlers": handlers,
        });
        if !is_latest {
            service_json["latest_revision"] = json!(latest_service.revision);
            service_json["latest_deployment_id"] = json!(latest_service.deployment_id.to_string());
        }
        out.push(service_json);
    }
    Value::Array(out)
}
