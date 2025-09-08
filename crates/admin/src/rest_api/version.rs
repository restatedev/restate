// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use okapi_operation::*;
use restate_admin_rest_model::version::{AdminApiVersion, VersionInformation};
use restate_types::config::Configuration;

/// Min/max supported admin api versions by the server
pub const MIN_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V2;
pub const MAX_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V2;

/// Version information endpoint
#[openapi(
    summary = "Admin version information",
    description = "Obtain admin version information.",
    operation_id = "version",
    tags = "version"
)]
pub async fn version() -> Json<VersionInformation> {
    Json(VersionInformation {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        min_admin_api_version: MIN_ADMIN_API_VERSION.as_repr(),
        max_admin_api_version: MAX_ADMIN_API_VERSION.as_repr(),
        ingress_endpoint: Configuration::pinned()
            .ingress
            .advertised_ingress_endpoint
            .clone(),
    })
}
