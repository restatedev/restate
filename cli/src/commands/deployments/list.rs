// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::count_endpoint_active_inv;
use crate::clients::MetaClientInterface;
use crate::console::c_println;
use crate::ui::console::{Styled, StyledTable};
use crate::ui::endpoints::{
    calculate_endpoint_status, render_active_invocations, render_endpoint_status,
    render_endpoint_type, render_endpoint_url, EndpointStatus,
};
use crate::ui::stylesheet::Style;

use restate_meta_rest_model::endpoints::{
    ServiceEndpoint, ServiceEndpointResponse, ServiceNameRevPair,
};
use restate_meta_rest_model::services::ServiceMetadata;

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    //// Show additional columns
    #[clap(long)]
    extra: bool,
}

pub async fn run_list(State(env): State<CliEnv>, list_opts: &List) -> Result<()> {
    let client = crate::clients::MetasClient::new(&env)?;
    let sql_client = crate::clients::DataFusionHttpClient::new(&env)?;
    // To know the latest version of every service.
    let services = client.get_services().await?.into_body().await?.services;

    let endpoints = client.get_endpoints().await?.into_body().await?.endpoints;
    // For each endpoint, we need to calculate the status and # of invocations.

    let mut latest_services: HashMap<String, ServiceMetadata> = HashMap::new();
    for svc in services {
        latest_services.insert(svc.name.clone(), svc);
    }
    //
    let mut table = Table::new_styled(&env.ui_config);
    let mut header = vec![
        "ENDPOINT",
        "TYPE",
        "STATUS",
        "# OF INVOCATIONS",
        "ID",
        "CREATED AT",
    ];
    if list_opts.extra {
        header.push("SERVICES");
    }
    table.set_styled_header(header);

    let mut enriched_endpoints: Vec<(ServiceEndpointResponse, EndpointStatus, i64)> =
        Vec::with_capacity(endpoints.len());

    for endpoint in endpoints {
        // calculate status and counters.
        let active_inv = count_endpoint_active_inv(&sql_client, &endpoint.id).await?;
        let status = calculate_endpoint_status(
            &endpoint.id,
            &endpoint.services,
            active_inv,
            &latest_services,
        );
        enriched_endpoints.push((endpoint, status, active_inv));
    }
    // Sort by active, draining, then drained.
    enriched_endpoints.sort_unstable_by_key(|(_, status, _)| match status {
        EndpointStatus::Active => 0,
        EndpointStatus::Draining => 1,
        EndpointStatus::Drained => 2,
    });

    for (endpoint, status, active_inv) in enriched_endpoints {
        let mut row = vec![
            Cell::new(render_endpoint_url(&endpoint.service_endpoint)),
            Cell::new(render_endpoint_type(&endpoint.service_endpoint)),
            render_endpoint_status(status),
            render_active_invocations(active_inv),
            Cell::new(&endpoint.id),
            Cell::new(match &endpoint.service_endpoint {
                ServiceEndpoint::Http { created_at, .. } => created_at,
                ServiceEndpoint::Lambda { created_at, .. } => created_at,
            }),
        ];
        if list_opts.extra {
            row.push(render_services(
                &endpoint.id,
                &endpoint.services,
                &latest_services,
            ));
            // Include services + revision +
        }

        table.add_row(row);
    }

    c_println!("{}", table);

    Ok(())
}

fn render_services(
    endpoint_id: &str,
    services: &[ServiceNameRevPair],
    latest_services: &HashMap<String, ServiceMetadata>,
) -> Cell {
    use std::fmt::Write as FmtWrite;

    let mut out = String::new();
    for svc in services {
        if let Some(latest_svc) = latest_services.get(&svc.name) {
            let style = if latest_svc.endpoint_id == endpoint_id {
                // We are hosting the latest revision of this service.
                Style::Success
            } else {
                Style::Normal
            };
            writeln!(
                &mut out,
                "- {} [{}]",
                &svc.name,
                Styled(style, svc.revision)
            )
            .unwrap();
        } else {
            // We couldn't find that service in latest_services? that's odd. We
            // highlight this with bright red to highlight the issue.
            writeln!(
                &mut out,
                "- {} [{}]",
                Styled(Style::Danger, &svc.name),
                svc.revision
            )
            .unwrap();
        }
    }
    Cell::new(out)
}
