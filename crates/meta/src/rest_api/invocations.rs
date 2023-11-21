// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::*;
use super::state::*;

use axum::extract::{Path, State};
use okapi_operation::*;
use restate_types::identifiers::InvocationId;
use std::sync::Arc;

/// Cancel/kill an invocation
#[openapi(
    summary = "Kill an invocation",
    description = "Kill the given invocation. When killing, consistency is not guaranteed for service instance state, in-flight invocation to other services, etc. Future releases will support graceful invocation cancellation.",
    operation_id = "cancel_invocation",
    tags = "invocation",
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    ))
)]
pub async fn cancel_invocation<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(invocation_id): Path<String>,
) -> Result<(), MetaApiError>
where
    W: restate_worker_api::Handle + Send,
    W::Future: Send,
{
    state
        .worker_handle()
        .kill_invocation(
            invocation_id
                .parse::<InvocationId>()
                .map_err(|e| MetaApiError::InvalidField("invocation_id", e.to_string()))?,
        )
        .await?;
    Ok(())
}
