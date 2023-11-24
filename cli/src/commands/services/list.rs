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
use crate::console::{c_println, Icon};
use crate::meta_client::MetaClientInterface;
use crate::ui::console::StyledTable;
use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::{Attribute, Cell, Table};
use restate_meta::rest_api::endpoints::{ServiceEndpoint, ServiceEndpointResponse};
use restate_schema_api::endpoint::ProtocolType;
use restate_schema_api::service::{InstanceType, MethodMetadata};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Show only publicly accessible services
    #[clap(long)]
    public_only: bool,

    ////Show additional columns
    #[clap(long)]
    extra: bool,
}

pub async fn run_list(State(env): State<CliEnv>, list_opts: &List) -> Result<()> {
    let client = crate::meta_client::MetaClient::new(&env)?;
    let defs = client.get_services().await?.into_body().await?;

    let endpoints = client.get_endpoints().await?.into_body().await?;

    let mut endpoint_cache: HashMap<String, ServiceEndpointResponse> = HashMap::new();

    // Caching endpoints
    for endpoint in endpoints.endpoints {
        endpoint_cache.insert(endpoint.id.to_string(), endpoint);
    }

    let mut table = Table::new_styled(&env.ui_config);
    let mut header = vec![
        Cell::new(""),
        Cell::new("NAME").add_attribute(Attribute::Bold),
        Cell::new("REV").add_attribute(Attribute::Bold),
        Cell::new("FLAVOR").add_attribute(Attribute::Bold),
        Cell::new("ENDPOINT").add_attribute(Attribute::Bold),
    ];
    if list_opts.extra {
        header.push(Cell::new("ADDRESS").add_attribute(Attribute::Bold));
        header.push(Cell::new("METHODS").add_attribute(Attribute::Bold));
    }
    table.set_header(header);

    for svc in defs.services {
        if list_opts.public_only && !svc.public {
            // Skip non-public services if users chooses to.
            continue;
        }

        let public = if svc.public {
            Icon("🌎", "[public]")
        } else {
            Icon("🔒", "[private]")
        };

        let flavor = match svc.instance_type {
            InstanceType::Unkeyed => Icon("", ""),
            InstanceType::Keyed => Icon("⬅️ 🚶🚶🚶", "keyed"),
            InstanceType::Singleton => Icon("👑", "singleton"),
        };

        let endpoint = endpoint_cache
            .get(&svc.endpoint_id)
            .with_context(|| format!("endpoint {} not found!", svc.endpoint_id))?;

        let mut row = vec![
            public.to_string(),
            svc.name,
            svc.revision.to_string(),
            flavor.to_string(),
            render_endpoint_type(endpoint), //   render_methods(svc.methods),
        ];
        if list_opts.extra {
            row.push(render_endpoint_url(endpoint));
            row.push(render_methods(svc.methods));
        }

        table.add_row(row);
    }
    c_println!("{}", table);
    Ok(())
}

fn render_methods(methods: Vec<MethodMetadata>) -> String {
    use std::fmt::Write as FmtWrite;

    let mut out = String::new();
    for method in methods {
        writeln!(&mut out, "- {}", method.name).unwrap();
    }
    out
}

fn render_endpoint_type(endpoint: &ServiceEndpointResponse) -> String {
    match &endpoint.service_endpoint {
        ServiceEndpoint::Http { protocol_type, .. } => {
            format!(
                "HTTP {}",
                if protocol_type == &ProtocolType::BidiStream {
                    "2"
                } else {
                    "1"
                }
            )
        }
        ServiceEndpoint::Lambda { .. } => "AWS Lambda".to_string(),
    }
}

fn render_endpoint_url(endpoint: &ServiceEndpointResponse) -> String {
    match &endpoint.service_endpoint {
        ServiceEndpoint::Http { uri, .. } => uri.to_string(),
        ServiceEndpoint::Lambda { arn, .. } => arn.to_string(),
    }
}
