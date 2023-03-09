use super::data::*;

use std::sync::Arc;

use crate::service::MetaHandle;
use axum::extract::State;
use axum::Json;

#[derive(Clone)]
pub struct RestEndpointState {
    meta_handle: MetaHandle,
}

impl RestEndpointState {
    pub fn new(meta_handle: MetaHandle) -> Self {
        Self { meta_handle }
    }
}

pub async fn register_endpoint(
    State(state): State<Arc<RestEndpointState>>,
    Json(payload): Json<RegisterEndpointRequest>,
) -> Result<Json<RegisterEndpointResponse>, MetaApiError> {
    #[allow(clippy::let_unit_value)]
    let registration_result = state
        .meta_handle
        .register(payload.uri, payload.additional_headers.unwrap_or_default())
        .await;

    unimplemented!();
    // let result: Result<Vec<String>, ()> = Ok(vec!["my-str".to_string()]);
    // match result {
    //     Ok(services) => Ok(RegisterEndpointResponse::Done { services }.into()),
    //     Err(err) => Err(MetaApiError::ServiceRegistrationError),
    // }
}
