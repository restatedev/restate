use super::error::*;
use super::state::*;
use axum::extract::State;
use axum::Json;
use okapi_operation::*;
use restate_common::types;
use restate_service_key_extractor::json::RestateKeyConverter;
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
    fn into_service_invocation_id<M: MethodDescriptorRegistry, K: RestateKeyConverter>(
        self,
        method_descriptors: &M,
        key_converter: &K,
    ) -> Result<types::ServiceInvocationId, MetaApiError> {
        match self {
            ServiceInvocationId::Token(opaque_sid) => opaque_sid
                .parse::<types::ServiceInvocationId>()
                .map_err(|e| MetaApiError::InvalidField("sid", e.to_string())),
            ServiceInvocationId::Structured {
                service,
                key,
                invocation_id,
            } => {
                // TODO improve this with https://github.com/restatedev/restate/issues/43 in place
                let service_descriptor = method_descriptors
                    .list_methods(&service)
                    .map(|m| {
                        m.into_values()
                            .next()
                            .expect("A service descriptor must contain at least one method")
                            .parent_service()
                            .clone()
                    })
                    .ok_or_else(|| MetaApiError::ServiceNotFound(service.clone()))?;

                // Convert the json key to restate key
                let restate_key = key_converter
                    .json_to_key(service_descriptor, key.unwrap_or(serde_json::Value::Null))
                    .map_err(|e| MetaApiError::InvalidField("sid", e.to_string()))?;

                Ok(types::ServiceInvocationId::new(
                    service,
                    restate_key,
                    invocation_id,
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
pub async fn cancel_invocation<S, M: MethodDescriptorRegistry, K: RestateKeyConverter>(
    State(state): State<Arc<RestEndpointState<S, M, K>>>,
    #[request_body(required = true)] Json(req): Json<CancelInvocationRequest>,
) -> Result<(), MetaApiError> {
    state
        .worker_command_tx()
        .kill_invocation(req.sid.into_service_invocation_id(
            state.method_descriptor_registry(),
            state.key_converter(),
        )?)
        .await?;
    Ok(())
}
