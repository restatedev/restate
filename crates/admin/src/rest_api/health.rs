// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::StatusCode;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    operation_id = "health",
    tag = "health",
    responses(
        (status = 200, description = "The Admin API is ready to accept requests."),
    )
)]
pub async fn health() -> StatusCode {
    StatusCode::OK
}
