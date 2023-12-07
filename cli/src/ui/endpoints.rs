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

use comfy_table::{Cell, Color};
use restate_meta_rest_model::endpoints::{ProtocolType, ServiceEndpoint, ServiceNameRevPair};
use restate_meta_rest_model::services::ServiceMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointStatus {
    /// An active endpoint is an endpoint that has the latest revision of one or more services.
    Active,
    /// A draining endpoint is an endpoint that has all of its services replaced
    /// by higher revisions on other endpoints, but it still has pinned invocations.
    Draining,
    /// A draining endpoint is an endpoint that has all of its services replaced
    /// by higher revisions on other endpoints, and it has NO pinned invocations.
    Drained,
}

pub fn render_endpoint_url(svc_endpoint: &ServiceEndpoint) -> String {
    match svc_endpoint {
        ServiceEndpoint::Http { uri, .. } => uri.to_string(),
        ServiceEndpoint::Lambda { arn, .. } => arn.to_string(),
    }
}

pub fn render_endpoint_type(svc_endpoint: &ServiceEndpoint) -> String {
    match svc_endpoint {
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

pub fn calculate_endpoint_status(
    endpoint_id: &str,
    owned_services: &[ServiceNameRevPair],
    active_inv: i64,
    latest_services: &HashMap<String, ServiceMetadata>,
) -> EndpointStatus {
    let mut status = EndpointStatus::Draining;

    for svc in owned_services {
        if let Some(latest_svc) = latest_services.get(&svc.name) {
            if latest_svc.endpoint_id == endpoint_id {
                status = EndpointStatus::Active;
                break;
            }
        } else {
            // We couldn't find that service in latest_services? that's odd but
            // we'll ignore and err on the side of assuming it's an active endpoint.
            status = EndpointStatus::Active;
        }
    }

    if status == EndpointStatus::Draining && active_inv == 0 {
        status = EndpointStatus::Drained;
    }

    status
}

pub fn render_endpoint_status(status: EndpointStatus) -> Cell {
    let color = match status {
        EndpointStatus::Active => Color::Green,
        EndpointStatus::Draining => Color::Yellow,
        EndpointStatus::Drained => Color::Grey,
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
