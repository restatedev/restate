use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use codederror::CodedError;
use serde::Serialize;

use crate::service::MetaError;

/// This error is used by handlers to propagate API errors,
/// and later converted to a response through the IntoResponse implementation
#[derive(Debug, thiserror::Error)]
pub enum MetaApiError {
    #[error("The request field '{0}' is invalid. Reason: {1}")]
    InvalidField(&'static str, String),
    #[error("The requested service '{0}' does not exist")]
    ServiceNotFound(String),
    #[error("The requested method '{method_name}' on service '{service_name}' does not exist")]
    MethodNotFound {
        service_name: String,
        method_name: String,
    },
    #[error(transparent)]
    Meta(#[from] MetaError),
}

impl MetaApiError {}

/// To format the error response body.
#[derive(Debug, Serialize)]
struct ErrorDescriptionResponse {
    message: String,
}

impl IntoResponse for MetaApiError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            MetaApiError::InvalidField(_, _) => StatusCode::BAD_REQUEST,
            MetaApiError::ServiceNotFound(_) => StatusCode::NOT_FOUND,
            MetaApiError::MethodNotFound {
                service_name: _,
                method_name: _,
            } => StatusCode::NOT_FOUND,
            MetaApiError::Meta(MetaError::Discovery(desc_error)) if desc_error.is_user_error() => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ErrorDescriptionResponse {
            message: match &self {
                MetaApiError::Meta(m) => m.decorate().to_string(),
                e => e.to_string(),
            },
        });

        (status_code, body).into_response()
    }
}
