// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::StatusCode;
use okapi_operation::*;

/// Health check endpoint
#[openapi(
    summary = "Health check",
    description = "Check REST API Health.",
    operation_id = "health",
    tags = "health",
    responses(
        ignore_return_type = true,
        response(status = "200", description = "OK", content = "okapi_operation::Empty"),
    )
)]
pub async fn health() -> StatusCode {
    StatusCode::OK
}
