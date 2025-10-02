// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use comfy_table::{Cell, Color, Table};
use std::collections::HashMap;

use crate::clients::Deployment;
use restate_admin_rest_model::deployments::ServiceNameRevPair;
use restate_cli_util::ui::console::StyledTable;
use restate_types::deployment;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::deployment::ProtocolType;
use restate_types::schema::service::ServiceMetadata;

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
        Deployment::Http { .. } => "HTTP".to_string(),
        Deployment::Lambda { .. } => "Lambda".to_string(),
    }
}

pub fn render_transport_protocol(deployment: &Deployment) -> String {
    match deployment {
        Deployment::Http { http_version, .. } => {
            format!("{http_version:?}")
        }
        Deployment::Lambda { .. } => "AWS Lambda".to_string(),
    }
}

pub fn calculate_deployment_status(
    deployment_id: &DeploymentId,
    owned_services: &[ServiceNameRevPair],
    active_inv: i64,
    latest_services: &HashMap<String, ServiceMetadata>,
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
    Cell::new(format!("{status:?}")).fg(color)
}

pub fn render_active_invocations(active_inv: i64) -> Cell {
    if active_inv > 0 {
        Cell::new(active_inv).fg(comfy_table::Color::Yellow)
    } else {
        Cell::new(active_inv).fg(comfy_table::Color::Grey)
    }
}

pub fn add_deployment_to_kv_table(deployment: &Deployment, table: &mut Table) {
    let (additional_headers, metadata, created_at, min_protocol_version, max_protocol_version) =
        match &deployment {
            Deployment::Http {
                uri,
                protocol_type,
                http_version: _,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                metadata,
                ..
            } => {
                table.add_kv_row("Transport:", render_transport_protocol(deployment));
                table.add_kv_row("Protocol Style:", format!("{protocol_type}"));
                table.add_kv_row("Endpoint:", uri);
                (
                    additional_headers.clone(),
                    metadata.clone(),
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                )
            }
            Deployment::Lambda {
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                metadata,
                ..
            } => {
                table.add_kv_row("Transport:", "AWS Lambda");
                table.add_kv_row(
                    "Protocol Style:",
                    format!("{}", ProtocolType::RequestResponse),
                );
                table.add_kv_row_if(
                    || assume_role_arn.is_some(),
                    "Deployment Assume Role ARN:",
                    || assume_role_arn.as_ref().unwrap(),
                );

                table.add_kv_row("Endpoint:", arn);
                (
                    additional_headers.clone(),
                    metadata.clone(),
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                )
            }
        };

    let additional_headers: HashMap<http::HeaderName, http::HeaderValue> =
        additional_headers.into();

    table.add_kv_row("Created at:", created_at);
    for (header, value) in additional_headers.iter() {
        table.add_kv_row(
            "Deployment Additional Header:",
            format!("{}: {}", header, value.to_str().unwrap_or("<BINARY>")),
        );
    }

    if min_protocol_version == max_protocol_version {
        table.add_kv_row("Protocol:", min_protocol_version);
    } else {
        table.add_kv_row(
            "Protocol:",
            format!("[{min_protocol_version}, {max_protocol_version}]"),
        );
    }

    // Additional metadata is printed nicely when possible
    for (key, value) in metadata.iter() {
        match deployment::metadata::MetadataKey::try_from(key.as_str()) {
            Ok(k) => {
                table.add_kv_row(&format!("{k}:"), value);
            }
            Err(k) => {
                table.add_kv_row(&format!("{k}:"), value);
            }
        }
    }
}
