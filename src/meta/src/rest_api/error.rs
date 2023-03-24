use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::service::MetaError;

/// This error is used by handlers to propagate API errors,
/// and later converted to a response through the IntoResponse implementation
#[derive(Debug, thiserror::Error)]
pub enum MetaApiError {
    #[error("The request field '{0}' is invalid. Reason: {1}")]
    InvalidField(&'static str, String),
    #[error("The requested {0} '{1}' does not exist")]
    NotFound(&'static str, String),
    #[error(transparent)]
    Meta(#[from] MetaError),
}

/// To format the error response body.
#[derive(Debug, Serialize)]
struct ErrorDescriptionResponse {
    message: String,
}

impl IntoResponse for MetaApiError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            MetaApiError::InvalidField(_, _) => StatusCode::BAD_REQUEST,
            MetaApiError::NotFound(_, _) => StatusCode::NOT_FOUND,
            MetaApiError::Meta(MetaError::Discovery(desc_error)) if desc_error.is_user_error() => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ErrorDescriptionResponse {
            message: self.to_string(),
        });

        (status_code, body).into_response()
    }
}
