// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::APPLICATION_JSON;

use bytes::Bytes;
use http::{header, Response, StatusCode};
use restate_schema_api::invocation_target::InputValidationError;
use restate_types::errors::{IdDecodeError, InvocationError};
use serde::Serialize;
use std::string;

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("not found")]
    NotFound,
    #[error(
        "bad path, expected either /:service-name/:handler or /:object-name/:object-key/:handler"
    )]
    BadServicePath,
    #[error(
        "bad path, expected either /restate/awakeables/:id/resolve or /restate/awakeables/:id/reject"
    )]
    BadAwakeablesPath,
    #[error(
        "bad path, expected either /restate/invocation/:id/output or /restate/invocation/:id/attach"
    )]
    BadInvocationPath,
    #[error(
    "bad path, expected either /restate/workflow/:workflow_name/:workflow_key/output or /restate/workflow/:workflow_name/:workflow_key/attach"
    )]
    BadWorkflowPath,
    #[error("not implemented")]
    NotImplemented,
    #[error("bad header {0}: {1:?}")]
    BadHeader(header::HeaderName, #[source] header::ToStrError),
    #[error("bad delay query parameter, must be a ISO8601 duration: {0}")]
    BadDelayDuration(String),
    #[error("bad path, cannot decode key: {0:?}")]
    UrlDecodingError(string::FromUtf8Error),
    #[error("the invoked service is not public")]
    PrivateService,
    #[error("cannot read body: {0:?}")]
    Body(anyhow::Error),
    #[error("unavailable")]
    Unavailable,
    #[error("not ready")]
    NotReady,
    #[error("method not allowed")]
    MethodNotAllowed,
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
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ErrorResponse {
    Invocation(
        // InvocationError has its own json representation, we simply use that
        InvocationError,
    ),
    Other {
        // This will simply write the error using the Display trait
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        message: HandlerError,
    },
}

impl HandlerError {
    pub(crate) fn fill_builder<B: http_body::Body + Default + From<Bytes>>(
        self,
        res_builder: http::response::Builder,
    ) -> Response<B> {
        let status_code = match &self {
            HandlerError::NotFound => StatusCode::NOT_FOUND,
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
            | HandlerError::UnsupportedIdempotencyKey => StatusCode::BAD_REQUEST,
            HandlerError::Body(_) => StatusCode::INTERNAL_SERVER_ERROR,
            HandlerError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            HandlerError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            HandlerError::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            HandlerError::Invocation(e) => {
                StatusCode::from_u16(e.code().into()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            HandlerError::NotReady => StatusCode::from_u16(470).unwrap(),
        };

        let error_response = match self {
            HandlerError::Invocation(e) => ErrorResponse::Invocation(e),
            e => ErrorResponse::Other { message: e },
        };

        res_builder
            .status(status_code)
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
