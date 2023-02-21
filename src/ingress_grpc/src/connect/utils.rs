use http::header::CONTENT_TYPE;
use http::{Response, StatusCode};
use hyper::Body;
use serde::Serialize;
use tonic::{Code, Status};

pub(super) fn not_found() -> Response<Body> {
    code_response(Code::NotFound)
}

pub(super) fn method_not_allowed() -> Response<Body> {
    http_code_response(StatusCode::METHOD_NOT_ALLOWED)
}

pub(super) fn unsupported_media_type() -> Response<Body> {
    http_code_response(StatusCode::UNSUPPORTED_MEDIA_TYPE)
}

pub(super) fn internal_server_error() -> Response<Body> {
    code_response(Code::Internal)
}

pub(super) fn not_implemented() -> Response<Body> {
    code_response(Code::Unimplemented)
}

#[derive(Serialize)]
struct StatusJsonResponse<'a> {
    code: &'a str,
    message: &'a str,
}

impl<'a> From<&'a Code> for StatusJsonResponse<'a> {
    fn from(code: &'a Code) -> Self {
        Self {
            code: code_str(code),
            message: code.description(),
        }
    }
}

impl<'a> From<&'a Status> for StatusJsonResponse<'a> {
    fn from(status: &'a Status) -> Self {
        Self {
            code: code_str(&status.code()),
            message: status.message(),
        }
    }
}

pub(super) fn code_response(code: Code) -> Response<Body> {
    let status_response: StatusJsonResponse<'_> = (&code).into();

    Response::builder()
        .status(grpc_status_code_to_http_status_code(code))
        .header(CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&status_response).unwrap().into())
        .unwrap()
}

pub(super) fn status_response(status: Status) -> Response<Body> {
    let status_response: StatusJsonResponse<'_> = (&status).into();

    Response::builder()
        .status(grpc_status_code_to_http_status_code(status.code()))
        .header(CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&status_response).unwrap().into())
        .unwrap()
}

pub(super) fn http_code_response(status_code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status_code)
        .body(Body::empty())
        .unwrap()
}

fn grpc_status_code_to_http_status_code(code: Code) -> StatusCode {
    // This mapping comes from https://connect.build/docs/protocol/#error-codes
    match code {
        Code::Ok => StatusCode::OK,
        Code::Cancelled => StatusCode::REQUEST_TIMEOUT,
        Code::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        Code::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists => StatusCode::CONFLICT,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        Code::Aborted => StatusCode::CONFLICT,
        Code::OutOfRange => StatusCode::BAD_REQUEST,
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn code_str(code: &Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::Unauthenticated => "unauthenticated",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
    }
}
