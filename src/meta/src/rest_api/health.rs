use axum::http::StatusCode;
use okapi_operation::*;

/// Health check endpoint
#[openapi(
    summary = "Health check",
    description = "Check REST API Health.",
    operation_id = "health",
    tags = "health",
    responses(
        ignore_return_type = true,
        response(status = "200", description = "OK", content = "okapi_operation::Empty"),
    )
)]
pub async fn health() -> StatusCode {
    StatusCode::OK
}
