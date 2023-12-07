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
use crate::clients::MetaClientInterface;
use crate::console::c_println;
use crate::ui::console::StyledTable;
use crate::ui::endpoints::{render_endpoint_type, render_endpoint_url};
use crate::ui::service_methods::{icon_for_is_public, icon_for_service_flavor};

use restate_meta_rest_model::endpoints::ServiceEndpointResponse;
use restate_meta_rest_model::services::MethodMetadata;

use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::Table;

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "ls")]
#[cling(run = "run_list")]
pub struct List {
    /// Show only publicly accessible services
    #[clap(long)]
    public_only: bool,

    //// Show additional columns
    #[clap(long)]
    extra: bool,
}

pub async fn run_list(State(env): State<CliEnv>, list_opts: &List) -> Result<()> {
    let client = crate::clients::MetasClient::new(&env)?;
    let defs = client.get_services().await?.into_body().await?;

    let endpoints = client.get_endpoints().await?.into_body().await?;

    let mut endpoint_cache: HashMap<String, ServiceEndpointResponse> = HashMap::new();

    // Caching endpoints
    for endpoint in endpoints.endpoints {
        endpoint_cache.insert(endpoint.id.to_string(), endpoint);
    }

    let mut table = Table::new_styled(&env.ui_config);
    let mut header = vec!["", "NAME", "REV", "FLAVOR", "DEPLOYMENT TYPE"];
    if list_opts.extra {
        header.push("ADDRESS");
        header.push("METHODS");
    }
    table.set_styled_header(header);

    for svc in defs.services {
        if list_opts.public_only && !svc.public {
            // Skip non-public services if users chooses to.
            continue;
        }

        let public = icon_for_is_public(svc.public);
        let flavor = icon_for_service_flavor(&svc.instance_type);

        let endpoint = endpoint_cache
            .get(&svc.endpoint_id)
            .with_context(|| format!("endpoint {} not found!", svc.endpoint_id))?;

        let mut row = vec![
            public.to_string(),
            svc.name,
            svc.revision.to_string(),
            flavor.to_string(),
            render_endpoint_type(&endpoint.service_endpoint),
        ];
        if list_opts.extra {
            row.push(render_endpoint_url(&endpoint.service_endpoint));
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
