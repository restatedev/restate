// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use restate_admin_rest_model::version::{AdminApiVersion, VersionInformation};
use restate_core::TaskCenter;
use restate_types::config::Configuration;

/// Min/max supported admin api versions by the server
pub const MIN_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V2;
pub const MAX_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V3;

/// Get version information
///
/// Returns the server version, supported Admin API versions, and the advertised ingress endpoint.
#[utoipa::path(
    get,
    path = "/version",
    operation_id = "version",
    tag = "version",
    responses(
        (status = 200, description = "Server version information including supported API versions and ingress endpoint.", body = VersionInformation)
    )
)]
pub async fn version() -> Json<VersionInformation> {
    Json(VersionInformation {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        min_admin_api_version: MIN_ADMIN_API_VERSION.as_repr(),
        max_admin_api_version: MAX_ADMIN_API_VERSION.as_repr(),
        ingress_endpoint: Some(TaskCenter::with_current(|tc| {
            Configuration::pinned()
                .ingress
                .advertised_address(tc.address_book())
        })),
    })
}
