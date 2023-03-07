#![allow(dead_code)]

use std::collections::HashMap;

use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[derive(Debug, thiserror::Error)]
pub enum MetaApiError {
    #[error("Service registration error")]
    ServiceRegistrationError,
}

impl IntoResponse for MetaApiError {
    fn into_response(self) -> Response {
        todo!()
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RegisterEndpointRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub uri: Uri,
    pub additional_headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RegisterEndpointResponse {
    Done { services: Vec<String> },
    Error { message: String },
}
