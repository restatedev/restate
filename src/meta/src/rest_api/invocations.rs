use super::error::*;
use super::state::*;
use axum::extract::State;
use axum::Json;
use okapi_operation::*;
use restate_service_metadata::MethodDescriptorRegistry;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

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
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CancelInvocationRequest {
    /// # Target identifier
    ///
    /// Identifier of the service invocation to cancel/kill.
    #[allow(dead_code)]
    id: ServiceInvocationId,
}

/// Cancel/kill an invocation
#[openapi(
    summary = "Kill an invocation",
    description = "Kill the given invocation. When killing, consistency is not guaranteed for service instance state, in-flight invocation to other services, etc. Future releases will support graceful invocation cancellation.",
    operation_id = "cancel_invocation",
    tags = "invocation"
)]
pub async fn cancel_invocation<S, M: MethodDescriptorRegistry>(
    State(_): State<Arc<RestEndpointState<S, M>>>,
    #[request_body(required = true)] Json(_): Json<CancelInvocationRequest>,
) -> Result<(), MetaApiError> {
    //
    Ok(())
}
