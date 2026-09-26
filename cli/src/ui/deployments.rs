// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use comfy_table::Cell;
use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::clients::Deployment;
use crate::ui::datetime::DateTimeExt;
use crate::ui::fmt::Field;
use restate_admin_rest_model::deployments::{HttpAuth, ServiceNameRevPair};
use restate_cli_util::ui::stylesheet::Style;
use restate_types::deployment;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::deployment::ProtocolType;
use restate_types::schema::service::ServiceMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
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

pub fn render_active_invocations(active_inv: i64) -> Cell {
    if active_inv > 0 {
        Cell::new(active_inv).fg(comfy_table::Color::Yellow)
    } else {
        Cell::new(active_inv).fg(comfy_table::Color::Grey)
    }
}

/// Deployment status as a [`Field`] for the output formatter:
/// the machine value is the status name, styled for human output.
pub fn deployment_status_field(status: DeploymentStatus) -> Field {
    let style = match status {
        DeploymentStatus::Active => Style::Success,
        DeploymentStatus::Draining => Style::Warn,
        DeploymentStatus::Drained => Style::Notice,
    };
    Field::styled(format!("{status:?}"), style)
}

/// [`Field`] variant of [`render_active_invocations`]: a native number, styled.
pub fn active_invocations_field(active_inv: i64) -> Field {
    let style = if active_inv > 0 {
        Style::Warn
    } else {
        Style::Notice
    };
    Field::styled(active_inv, style)
}

/// Deployment details as `(machine_key, Field)` pairs for a formatter `detail`
/// section. Yields structured fields so
/// `--json` produces a clean object.
pub fn deployment_info_fields(deployment: &Deployment) -> Vec<(String, Field)> {
    let mut rows: Vec<(String, Field)> = Vec::new();

    let (
        additional_headers,
        metadata,
        created_at,
        min_protocol_version,
        max_protocol_version,
        sdk_version,
    ) = match deployment {
        Deployment::Http {
            uri,
            protocol_type,
            additional_headers,
            created_at,
            min_protocol_version,
            max_protocol_version,
            metadata,
            sdk_version,
            auth,
            ..
        } => {
            rows.push((
                "transport".to_owned(),
                Field::new(render_transport_protocol(deployment)),
            ));
            rows.push((
                "protocol_style".to_owned(),
                Field::new(format!("{protocol_type}")),
            ));
            rows.push(("endpoint".to_owned(), Field::new(uri.to_string())));
            if let Some(HttpAuth::GoogleIdToken(token_auth)) = auth {
                let impersonation = token_auth
                    .impersonate_service_account
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "(ambient ADC)".to_owned());
                let audience = token_auth
                    .audience
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| {
                        "(not set - re-register with --force to refresh)".to_owned()
                    });
                rows.push((
                    "authentication".to_owned(),
                    Field::new("Google OIDC ID token"),
                ));
                rows.push(("impersonation".to_owned(), Field::new(impersonation)));
                rows.push(("audience".to_owned(), Field::new(audience)));
                if let Some(provider) = &token_auth.workload_identity_provider {
                    rows.push((
                        "workload_identity_provider".to_owned(),
                        Field::new(provider.to_string()),
                    ));
                }
            }
            (
                additional_headers.clone(),
                metadata.clone(),
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
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
            sdk_version,
            ..
        } => {
            rows.push(("transport".to_owned(), Field::new("AWS Lambda")));
            rows.push((
                "protocol_style".to_owned(),
                Field::new(format!("{}", ProtocolType::RequestResponse)),
            ));
            if let Some(assume_role_arn) = assume_role_arn {
                rows.push((
                    "deployment_assume_role_arn".to_owned(),
                    Field::new(assume_role_arn.to_string()),
                ));
            }
            rows.push(("endpoint".to_owned(), Field::new(arn.to_string())));
            (
                additional_headers.clone(),
                metadata.clone(),
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
            )
        }
    };

    let additional_headers: HashMap<http::HeaderName, http::HeaderValue> =
        additional_headers.into();

    rows.push((
        "sdk".to_owned(),
        Field::new(
            sdk_version
                .as_ref()
                .map(|v| AsRef::<str>::as_ref(v).to_owned())
                .unwrap_or_else(|| "unknown".to_owned()),
        ),
    ));
    rows.push((
        "created_at".to_owned(),
        Field::with_display(created_at.iso(), created_at.display()),
    ));

    if !additional_headers.is_empty() {
        let obj: Map<String, Value> = additional_headers
            .iter()
            .map(|(header, value)| {
                (
                    header.to_string(),
                    Value::from(value.to_str().unwrap_or("<BINARY>")),
                )
            })
            .collect();
        let human = additional_headers
            .iter()
            .map(|(header, value)| format!("{}: {}", header, value.to_str().unwrap_or("<BINARY>")))
            .collect::<Vec<_>>()
            .join("\n");
        rows.push((
            "additional_headers".to_owned(),
            Field::with_display(Value::Object(obj), human),
        ));
    }

    if min_protocol_version == max_protocol_version {
        rows.push(("protocol".to_owned(), Field::new(*min_protocol_version)));
    } else {
        let range = Value::Array(vec![
            Value::from(*min_protocol_version),
            Value::from(*max_protocol_version),
        ]);
        rows.push((
            "protocol".to_owned(),
            Field::with_display(
                range,
                format!("[{min_protocol_version}, {max_protocol_version}]"),
            ),
        ));
    }

    for (key, value) in metadata.iter() {
        let key = match deployment::metadata::MetadataKey::try_from(key.as_str()) {
            Ok(k) => k.to_string(),
            Err(k) => k.to_string(),
        };
        rows.push((key, Field::new(value.to_string())));
    }

    rows
}
