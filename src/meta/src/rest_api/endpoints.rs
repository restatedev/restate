use super::error::*;
use super::state::*;

use std::sync::Arc;

use axum::extract::State;
use axum::http::Uri;
use axum::Json;
use hyper::http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct RegisterEndpointRequest {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub uri: Uri,
    pub additional_headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct RegisterEndpointResponse {
    services: Vec<String>,
}

/// Discover endpoint and return discovered endpoints.
pub async fn discover_endpoint(
    State(state): State<Arc<RestEndpointState>>,
    Json(payload): Json<RegisterEndpointRequest>,
) -> Result<Json<RegisterEndpointResponse>, MetaApiError> {
    // False positive with Bytes field
    #[allow(clippy::mutable_key_type)]
    let headers = payload
        .additional_headers
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let header_name = HeaderName::try_from(k)
                .map_err(|e| MetaApiError::InvalidField("additional_headers", e.to_string()))?;
            let header_value = HeaderValue::try_from(v)
                .map_err(|e| MetaApiError::InvalidField("additional_headers", e.to_string()))?;
            Ok((header_name, header_value))
        })
        .collect::<Result<HashMap<HeaderName, HeaderValue>, MetaApiError>>()?;

    let registration_result = state.meta_handle().register(payload.uri, headers).await;
    Ok(registration_result
        .map(|services| RegisterEndpointResponse { services })?
        .into())
}
