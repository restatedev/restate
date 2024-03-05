// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{Response, StatusCode};
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
}

impl HandlerError {
    pub(crate) fn into_response<B: http_body::Body + Default>(self) -> Response<B> {
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
        };

        Response::builder()
            .status(status_code)
            .body(Default::default())
            .unwrap()
    }
}
