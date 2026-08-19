// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::string;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Response, StatusCode, header};
use http_body_util::LengthLimitError;
use serde::Serialize;

use restate_types::errors::{GenericError, IdDecodeError, InvocationError};
use restate_types::identifiers::DeploymentId;
use restate_types::schema::invocation_target::InputValidationError;
use restate_util_string::RestrictedValueError;

use super::APPLICATION_JSON;
use crate::RequestDispatcherError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("not found")]
    NotFound,
    #[error("service '{0}' not found, make sure to register the service before calling it.")]
    ServiceNotFound(String),
    #[error(
        "the service '{0}' exists, but the handler '{1}' was not found, check that the handler exists in the latest registered service version."
    )]
    ServiceHandlerNotFound(String, String),
    #[error(
        "the service {0} is exposed by the deprecated deployment {1}. Upgrade the SDK used by {0}."
    )]
    DeploymentDeprecated(String, DeploymentId),
    #[error("invocation not found")]
    InvocationNotFound,
    #[error(
        "bad path, expected either /:service-name/:handler or /:object-name/:object-key/:handler"
    )]
    BadServicePath,
    #[error(
        "bad path, expected either /restate/awakeables/:id/resolve or /restate/awakeables/:id/reject"
    )]
    BadAwakeablesPath,
    #[error(
        "bad path, expected either /restate/invocation/:invocation_id/output or /restate/invocation/:invocation_id/attach or /restate/invocation/:invocation_target/:idempotency_key/output or /restate/invocation/:invocation_target/:idempotency_key/attach"
    )]
    BadInvocationPath,
    #[error(
        "bad path, expected either /restate/workflow/:workflow_name/:workflow_key/output or /restate/workflow/:workflow_name/:workflow_key/attach"
    )]
    BadWorkflowPath,
    #[error("bad path: {0}")]
    BadPath(String),
    #[error(
        "bad path, expected /restate/call/:service/:handler, /restate/send/:service/:handler, /restate/scope/:scope/call/:service/:handler, /restate/attach/:invocation_id, /restate/output/:invocation_id, or /restate/lookup"
    )]
    BadRestateApiPath,
    #[error("limit-key requires a scope to be set")]
    LimitKeyWithoutScope,
    #[error("invalid limit-key: {0}")]
    InvalidLimitKey(String),
    #[error("scoped invocations require vqueues to be enabled")]
    ScopeRequiresVQueues,
    #[error("scope is not supported for Virtual Object targets")]
    ScopedVirtualObjectNotSupported,
    #[error("bad header {0}: {1:?}")]
    BadHeader(header::HeaderName, #[source] header::ToStrError),
    #[error("bad delay query parameter, must be a ISO8601 duration: {0}")]
    BadDelayDuration(String),
    #[error("bad path, cannot decode key: {0:?}")]
    UrlDecodingError(string::FromUtf8Error),
    #[error("the invoked service is not public")]
    PrivateService,
    #[error("cannot read body: {0:?}")]
    Body(GenericError),
    #[error("too many requests, the ingress is overloaded. Retry later.")]
    TooManyRequests,
    #[error("the invocation exists but has not completed yet")]
    NotReady,
    #[error("method not allowed")]
    MethodNotAllowed,
    #[error(
        "cannot get output for the given invocation. You can get output only for invocations created with an idempotency key, or for workflow methods."
    )]
    UnsupportedGetOutput,
    #[error("invocation error: {0:?}")]
    Invocation(InvocationError),
    #[error("input validation error: {0}")]
    InputValidation(#[from] InputValidationError),
    #[error(
        "cannot use the delay query parameter with calls. The delay is supported only with sends"
    )]
    UnsupportedDelay,
    #[error(
        "cannot use the idempotency key with workflow handlers. The handler invocation will already be idempotent by the workflow key itself."
    )]
    UnsupportedIdempotencyKey,
    #[error("bad awakeable id '{0}': {1}")]
    BadAwakeableId(String, IdDecodeError),
    #[error("bad invocation id '{0}': {1}")]
    BadInvocationId(String, IdDecodeError),
    #[error(
        "internal routing error: {0}. \
        If the request has an 'idempotency-key', it can be safely retried, \
        otherwise submitting it again may result in executing the invocation twice."
    )]
    InvocationDispatcherError(RequestDispatcherError),
    #[error(
        "internal routing error: {0}. \
        You may safely retry the request."
    )]
    GenericReadDispatcherError(RequestDispatcherError),
    #[error(
        "internal routing error: {0}. \
        Submitting the request again may result in executing the operation twice."
    )]
    GenericWriteDispatcherError(RequestDispatcherError),
    #[error("bad scope value: {0}")]
    BadScopeValue(RestrictedValueError),
}

/// Header signaling whether an error response was generated by the ingress
/// ([`ErrorSource::Ingress`]) or is the terminal outcome of the invocation
/// ([`ErrorSource::Invocation`]).
pub(crate) const X_RESTATE_ERROR_SOURCE: HeaderName =
    HeaderName::from_static("x-restate-error-source");

/// Discriminates the origin of an error response, so that ingress clients can tell a
/// user-controlled invocation terminal error apart from a Restate/ingress generated error.
#[derive(Debug, Clone, Copy, Serialize, strum::IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ErrorSource {
    /// Error generated by the ingress (or Restate itself) while handling the request, e.g. bad
    /// request, service not found, overload, or a routing failure.
    /// The HTTP status code is Restate-controlled and follows the usual HTTP semantics.
    Ingress,
    /// The error is the terminal outcome of the durable invocation.
    /// The HTTP status code is user/SDK-controlled.
    Invocation,
}

// IMPORTANT! If you touch this, please update the `RestateError` schema in
// crates/types/src/schema/metadata/openapi.rs too
#[derive(Debug, Serialize)]
struct ErrorResponse {
    #[serde(flatten)]
    inner: ErrorResponseBody,
    /// Discriminates ingress-generated errors from invocation terminal outcomes.
    source: ErrorSource,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ErrorResponseBody {
    Invocation(
        // InvocationError has its own json representation, we simply use that
        InvocationError,
    ),
    Other {
        code: u16,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        message: HandlerError,
    },
}

impl HandlerError {
    pub(crate) fn status_code(&self) -> StatusCode {
        match self {
            // 4xx range
            HandlerError::NotFound
            | HandlerError::ServiceNotFound(_)
            | HandlerError::ServiceHandlerNotFound(_, _)
            | HandlerError::InvocationNotFound => StatusCode::NOT_FOUND,
            HandlerError::BadServicePath
            | HandlerError::PrivateService
            | HandlerError::UrlDecodingError(_)
            | HandlerError::BadDelayDuration(_)
            | HandlerError::BadAwakeablesPath
            | HandlerError::UnsupportedDelay
            | HandlerError::BadHeader(_, _)
            | HandlerError::BadAwakeableId(_, _)
            | HandlerError::BadInvocationPath
            | HandlerError::BadInvocationId(_, _)
            | HandlerError::BadWorkflowPath
            | HandlerError::InputValidation(_)
            | HandlerError::UnsupportedIdempotencyKey
            | HandlerError::UnsupportedGetOutput
            | HandlerError::DeploymentDeprecated(_, _)
            | HandlerError::BadRestateApiPath
            | HandlerError::LimitKeyWithoutScope
            | HandlerError::InvalidLimitKey(_)
            | HandlerError::BadScopeValue(_)
            | HandlerError::BadPath(_)
            | HandlerError::ScopeRequiresVQueues
            | HandlerError::ScopedVirtualObjectNotSupported => StatusCode::BAD_REQUEST,
            HandlerError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            HandlerError::NotReady => StatusCode::from_u16(470).unwrap(),
            HandlerError::Body(inner) if inner.downcast_ref::<LengthLimitError>().is_some() => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            HandlerError::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,

            // 5xx range
            HandlerError::InvocationDispatcherError(_)
            | HandlerError::GenericWriteDispatcherError(_)
            | HandlerError::GenericReadDispatcherError(_) => StatusCode::SERVICE_UNAVAILABLE,
            HandlerError::Body(_) => StatusCode::INTERNAL_SERVER_ERROR,

            // Invocation errors
            HandlerError::Invocation(e) => {
                StatusCode::from_u16(e.code().into()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// Whether this error was generated by the ingress or is the terminal outcome of the
    /// invocation. Surfaced to clients through the [`X_RESTATE_ERROR_SOURCE`] header and the
    /// `source` body field so they can implement safe retries.
    pub(crate) fn source(&self) -> ErrorSource {
        match self {
            HandlerError::Invocation(_) => ErrorSource::Invocation,
            _ => ErrorSource::Ingress,
        }
    }

    pub(crate) fn fill_builder<B: http_body::Body + Default + From<Bytes>>(
        self,
        res_builder: http::response::Builder,
    ) -> Response<B> {
        let status_code = self.status_code();
        let source = self.source();

        let error_response = ErrorResponse {
            source,
            inner: match self {
                HandlerError::Invocation(e) => ErrorResponseBody::Invocation(e),
                e => ErrorResponseBody::Other {
                    code: status_code.as_u16(),
                    message: e,
                },
            },
        };

        res_builder
            .status(status_code)
            .header(
                X_RESTATE_ERROR_SOURCE,
                HeaderValue::from_static(source.into()),
            )
            .header(http::header::CONTENT_TYPE, APPLICATION_JSON)
            .body(
                Bytes::from(
                    serde_json::to_vec(&error_response)
                        .expect("Serializing ErrorResponse should not fail"),
                )
                .into(),
            )
            .unwrap()
    }

    pub(crate) fn into_response<B: http_body::Body + Default + From<Bytes>>(self) -> Response<B> {
        self.fill_builder(http::response::Builder::new())
    }
}
