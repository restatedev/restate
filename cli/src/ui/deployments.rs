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

use comfy_table::{Cell, Color, Table};
use restate_meta_rest_model::components::ComponentMetadata;
use restate_meta_rest_model::deployments::{ComponentNameRevPair, Deployment, ProtocolType};
use restate_types::identifiers::DeploymentId;

use super::console::StyledTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentStatus {
    /// An active endpoint is an endpoint that has the latest revision of one or more services.
    Active,
    /// A draining endpoint is an endpoint that has all of its services replaced
    /// by higher revisions on other endpoints, but it still has pinned invocations.
    Draining,
    /// A draining endpoint is an endpoint that has all of its services replaced
    /// by higher revisions on other endpoints, and it has NO pinned invocations.
    Drained,
}

pub fn render_deployment_url(deployment: &Deployment) -> String {
    match deployment {
        Deployment::Http { uri, .. } => uri.to_string(),
        Deployment::Lambda { arn, .. } => arn.to_string(),
    }
}

pub fn render_deployment_type(deployment: &Deployment) -> String {
    match deployment {
        Deployment::Http { protocol_type, .. } => {
            format!(
                "HTTP {}",
                if protocol_type == &ProtocolType::BidiStream {
                    "2"
                } else {
                    "1"
                }
            )
        }
        Deployment::Lambda { .. } => "AWS Lambda".to_string(),
    }
}

pub fn calculate_deployment_status(
    deployment_id: &DeploymentId,
    owned_services: &[ComponentNameRevPair],
    active_inv: i64,
    latest_services: &HashMap<String, ComponentMetadata>,
) -> DeploymentStatus {
    let mut status = DeploymentStatus::Draining;

    for svc in owned_services {
        if let Some(latest_svc) = latest_services.get(&svc.name) {
            if &latest_svc.deployment_id == deployment_id {
                status = DeploymentStatus::Active;
                break;
            }
        } else {
            // We couldn't find that service in latest_services? that's odd but
            // we'll ignore and err on the side of assuming it's an active endpoint.
            status = DeploymentStatus::Active;
        }
    }

    if status == DeploymentStatus::Draining && active_inv == 0 {
        status = DeploymentStatus::Drained;
    }

    status
}

pub fn render_deployment_status(status: DeploymentStatus) -> Cell {
    let color = match status {
        DeploymentStatus::Active => Color::Green,
        DeploymentStatus::Draining => Color::Yellow,
        DeploymentStatus::Drained => Color::Grey,
    };
    Cell::new(format!("{:?}", status)).fg(color)
}

pub fn render_active_invocations(active_inv: i64) -> Cell {
    if active_inv > 0 {
        Cell::new(active_inv).fg(comfy_table::Color::Yellow)
    } else {
        Cell::new(active_inv).fg(comfy_table::Color::Grey)
    }
}

pub fn add_deployment_to_kv_table(deployment: &Deployment, table: &mut Table) {
    table.add_kv_row("Deployment Type:", render_deployment_type(deployment));
    let (additional_headers, created_at) = match &deployment {
        Deployment::Http {
            uri,
            protocol_type,
            additional_headers,
            created_at,
        } => {
            let protocol_type = match protocol_type {
                ProtocolType::RequestResponse => "Request/Response",
                ProtocolType::BidiStream => "Streaming",
            }
            .to_string();
            table.add_kv_row("Protocol Style:", protocol_type);

            table.add_kv_row("Endpoint:", uri);
            (additional_headers.clone(), created_at)
        }
        Deployment::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
            created_at,
        } => {
            table.add_kv_row("Protocol Style:", "Request/Response");
            table.add_kv_row_if(
                || assume_role_arn.is_some(),
                "Deployment Assume Role ARN:",
                assume_role_arn.as_ref().unwrap(),
            );

            table.add_kv_row("Endpoint:", arn);
            (additional_headers.clone(), created_at)
        }
    };

    let additional_headers: HashMap<http::HeaderName, http::HeaderValue> =
        additional_headers.into();

    table.add_kv_row("Created at:", created_at);
    for (header, value) in additional_headers.iter() {
        table.add_kv_row(
            "Deployment Additional Header:",
            &format!("{}: {}", header, value.to_str().unwrap_or("<BINARY>")),
        );
    }
}
