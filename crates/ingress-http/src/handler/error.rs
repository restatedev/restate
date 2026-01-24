// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::APPLICATION_JSON;

use crate::RequestDispatcherError;
use bytes::Bytes;
use http::{Response, StatusCode, header};
use restate_types::errors::{IdDecodeError, InvocationError};
use restate_types::identifiers::DeploymentId;
use restate_types::schema::invocation_target::InputValidationError;
use serde::Serialize;
use std::string;

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
        "internal routing error: {0}. The ingress was not able to acknowledge the invocation submission, and will not retry because the request is missing an 'idempotency-key'. Please note that the request may have been correctly submitted and executed."
    )]
    DispatcherError(#[from] RequestDispatcherError),
}

// IMPORTANT! If you touch this, please update crates/types/src/schema/openapi.rs too
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
            | HandlerError::DeploymentDeprecated(_, _) => StatusCode::BAD_REQUEST,
            HandlerError::DispatcherError(_) => {
                // TODO add more distinctions between different dispatcher errors (unavailable, etc)
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
