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

use cling::prelude::*;
use comfy_table::Table;

use crate::cli_env::CliEnv;
use crate::console::c_println;
use crate::meta_client::{MetaClient, MetaClientInterface};
use crate::ui::console::{Styled, StyledTable};
use crate::ui::stylesheet::Style;

use restate_meta_rest_model::endpoints::{ProtocolType, ServiceEndpoint, ServiceEndpointResponse};

use anyhow::Result;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_describe")]
pub struct Describe {
    /// Service name
    name: String,
}

pub async fn run_describe(State(env): State<CliEnv>, describe_opts: &Describe) -> Result<()> {
    let client = MetaClient::new(&env)?;
    let svc = client
        .get_service(&describe_opts.name)
        .await?
        .into_body()
        .await?;

    let mut table = Table::new_styled(&env.ui_config);
    table.add_kv_row("Name:", svc.name);
    table.add_kv_row(
        "Flavor (Instance Type):",
        &format!("{:?}", svc.instance_type),
    );
    table.add_kv_row("Revision:", svc.revision);
    table.add_kv_row("Public:", svc.public);
    table.add_kv_row("Endpoint Id:", &svc.endpoint_id);

    let endpoint = client
        .get_endpoint(&svc.endpoint_id)
        .await?
        .into_body()
        .await?;
    add_endpoint(&endpoint, &mut table);

    c_println!("{}", Styled(Style::Info, "Service Information"));
    c_println!("{}", table);

    // Methods
    c_println!();
    c_println!("{}", Styled(Style::Info, "Methods"));
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["NAME", "INPUT TYPE", "OUTPUT TYPE", "KEY FIELD INEDX"]);

    for method in svc.methods {
        table.add_row(vec![
            &method.name,
            &method.input_type,
            &method.output_type,
            &method
                .key_field_number
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        ]);
    }
    c_println!("{}", table);

    Ok(())
}

fn add_endpoint(endpoint: &ServiceEndpointResponse, table: &mut Table) {
    let additional_headers = match &endpoint.service_endpoint {
        ServiceEndpoint::Http {
            uri,
            protocol_type,
            additional_headers,
        } => {
            table.add_kv_row("Endpoint Type:", "HTTP");
            table.add_kv_row("Endpoint URL:", uri);
            let protocol_type = match protocol_type {
                ProtocolType::RequestResponse => "Request/Response",
                ProtocolType::BidiStream => "Streaming",
            }
            .to_string();
            table.add_kv_row("Endpoint Protocol:", protocol_type);
            additional_headers.clone()
        }
        ServiceEndpoint::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
        } => {
            table.add_kv_row("Endpoint Type:", "AWS Lambda");
            table.add_kv_row("Endpoint ARN:", arn);
            table.add_kv_row("Endpoint Protocol:", "Request/Response");
            table.add_kv_row_if(
                || assume_role_arn.is_some(),
                "Endpoint Assume Role ARN:",
                assume_role_arn.as_ref().unwrap(),
            );
            additional_headers.clone()
        }
    };

    let additional_headers: HashMap<http::HeaderName, http::HeaderValue> =
        additional_headers.into();

    for (header, value) in additional_headers.iter() {
        table.add_kv_row(
            "Endpoint Additional Header:",
            &format!("{}: {}", header, value.to_str().unwrap_or("<BINARY>")),
        );
    }
}
