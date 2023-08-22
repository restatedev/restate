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
use axum::extract::State;
use axum::Json;
use okapi_operation::*;
use restate_schema_api::key::json_conversion::Error;
use restate_schema_api::key::RestateKeyConverter;
use restate_types::identifiers;

use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

/// # Service invocation id
///
/// Identifier for a service invocation.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ServiceInvocationId {
    /// # Token representation
    ///
    /// Token representation of the service invocation identifier.
    /// This is the same representation used by the Restate CLI SQL interface.
    Token(String),
    /// # Structured representation
    ///
    /// Structured representation of the service invocation identifier.
    ///
    /// When providing the key, it must be non-empty for Keyed and Unkeyed services, and it must be empty for Singleton services.
    Structured {
        service: String,
        key: Option<serde_json::Value>,
        #[serde(alias = "id")]
        invocation_id: restate_serde_util::SerdeableUuid,
    },
}

impl ServiceInvocationId {
    fn into_service_invocation_id<K: RestateKeyConverter>(
        self,
        key_converter: &K,
    ) -> Result<identifiers::FullInvocationId, MetaApiError> {
        match self {
            ServiceInvocationId::Token(opaque_sid) => opaque_sid
                .parse::<identifiers::FullInvocationId>()
                .map_err(|e| MetaApiError::InvalidField("sid", e.to_string())),
            ServiceInvocationId::Structured {
                service,
                key,
                invocation_id,
            } => {
                // Convert the json key to restate key
                let restate_key = key_converter
                    .json_to_key(&service, key.unwrap_or(serde_json::Value::Null))
                    .map_err(|e| match e {
                        Error::NotFound => MetaApiError::ServiceNotFound(service.clone()),
                        e => MetaApiError::InvalidField("sid", e.to_string()),
                    })?;

                Ok(identifiers::FullInvocationId::new(
                    service,
                    restate_key,
                    Uuid::from(invocation_id),
                ))
            }
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CancelInvocationRequest {
    /// # Target identifier
    ///
    /// Identifier of the service invocation to cancel/kill.
    #[allow(dead_code)]
    sid: ServiceInvocationId,
}

/// Cancel/kill an invocation
#[openapi(
    summary = "Kill an invocation",
    description = "Kill the given invocation. When killing, consistency is not guaranteed for service instance state, in-flight invocation to other services, etc. Future releases will support graceful invocation cancellation.",
    operation_id = "cancel_invocation",
    tags = "invocation"
)]
pub async fn cancel_invocation<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    #[request_body(required = true)] Json(req): Json<CancelInvocationRequest>,
) -> Result<(), MetaApiError>
where
    S: RestateKeyConverter,
    W: restate_worker_api::Handle + Send,
    W::Future: Send,
{
    state
        .worker_handle()
        .kill_invocation(req.sid.into_service_invocation_id(state.schemas())?)
        .await?;
    Ok(())
}
