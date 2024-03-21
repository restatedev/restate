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
use restate_types::errors::InvocationError;
use serde::Serialize;
use std::string;

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("not found")]
    NotFound,
    #[error(
        "bad path, expected either /:service-name/:handler or /:object-name/:object-key/:handler"
    )]
    BadComponentPath,
    #[error(
        "bad path, expected either /restate/awakeables/:id/resolve or /restate/awakeables/:id/reject"
    )]
    BadAwakeablesPath,
    #[error("not implemented")]
    NotImplemented,
    #[error("bad header {0}: {1:?}")]
    BadHeader(header::HeaderName, #[source] header::ToStrError),
    #[error("bad path, cannot decode key: {0:?}")]
    UrlDecodingError(string::FromUtf8Error),
    #[error("the invoked component is not public")]
    PrivateComponent,
    #[error("bad idempotency header: {0:?}")]
    BadIdempotency(anyhow::Error),
    #[error("cannot read body: {0:?}")]
    Body(anyhow::Error),
    #[error("unavailable")]
    Unavailable,
    #[error("method not allowed")]
    MethodNotAllowed,
    #[error("using the idempotency key and send together is not yet supported")]
    SendAndIdempotencyKey,
    #[error("invocation error: {0:?}")]
    Invocation(InvocationError),
    #[error("input validation error: {0}")]
    InputValidation(#[from] InputValidationError),
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
            HandlerError::BadComponentPath => StatusCode::BAD_REQUEST,
            HandlerError::PrivateComponent => StatusCode::BAD_REQUEST,
            HandlerError::BadIdempotency(_) => StatusCode::BAD_REQUEST,
            HandlerError::Body(_) => StatusCode::INTERNAL_SERVER_ERROR,
            HandlerError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            HandlerError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            HandlerError::UrlDecodingError(_) => StatusCode::BAD_REQUEST,
            HandlerError::SendAndIdempotencyKey => StatusCode::NOT_IMPLEMENTED,
            HandlerError::BadAwakeablesPath => StatusCode::BAD_REQUEST,
            HandlerError::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            HandlerError::Invocation(e) => {
                StatusCode::from_u16(e.code().into()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            HandlerError::BadHeader(_, _) => StatusCode::BAD_REQUEST,
            HandlerError::InputValidation(_) => StatusCode::BAD_REQUEST,
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
