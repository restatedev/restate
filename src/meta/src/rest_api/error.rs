use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::service::MetaError;

/// This error is used by handlers to propagate API errors,
/// and later converted to a response through the IntoResponse implementation
#[derive(Debug, thiserror::Error)]
pub enum MetaApiError {
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
        // TODO we should probably return 400 for some errors
        let status_code = StatusCode::INTERNAL_SERVER_ERROR;
        let body = Json(ErrorDescriptionResponse {
            message: self.to_string(),
        });

        (status_code, body).into_response()
    }
}
