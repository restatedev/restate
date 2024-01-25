// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use codederror::{Code, CodedError};
use okapi_operation::anyhow::Error;
use okapi_operation::okapi::map;
use okapi_operation::okapi::openapi3::Responses;
use okapi_operation::{okapi, Components, ToMediaTypes, ToResponses};
use restate_meta::Error as MetaError;
use restate_schema_impl::SchemasUpdateError;
use restate_types::identifiers::DeploymentId;
use schemars::JsonSchema;
use serde::Serialize;

/// This error is used by handlers to propagate API errors,
/// and later converted to a response through the IntoResponse implementation
#[derive(Debug, thiserror::Error)]
pub enum MetaApiError {
    #[error("The request field '{0}' is invalid. Reason: {1}")]
    InvalidField(&'static str, String),
    #[error("The requested deployment '{0}' does not exist")]
    DeploymentNotFound(DeploymentId),
    #[error("The requested service '{0}' does not exist")]
    ServiceNotFound(String),
    #[error("The requested method '{method_name}' on service '{service_name}' does not exist")]
    MethodNotFound {
        service_name: String,
        method_name: String,
    },
    #[error("The requested subscription '{0}' does not exist")]
    SubscriptionNotFound(String),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Worker(#[from] restate_worker_api::Error),
}

/// # Error description response
///
/// Error details of the response
#[derive(Debug, Serialize, JsonSchema)]
struct ErrorDescriptionResponse {
    message: String,
    /// # Restate code
    ///
    /// Restate error code describing this error
    restate_code: Option<&'static str>,
}

impl IntoResponse for MetaApiError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            MetaApiError::ServiceNotFound(_)
            | MetaApiError::MethodNotFound { .. }
            | MetaApiError::DeploymentNotFound(_)
            | MetaApiError::SubscriptionNotFound(_) => StatusCode::NOT_FOUND,
            MetaApiError::Meta(MetaError::SchemaRegistry(SchemasUpdateError::BadDescriptor(_))) => {
                StatusCode::BAD_REQUEST
            }
            MetaApiError::Meta(MetaError::SchemaRegistry(
                SchemasUpdateError::OverrideDeployment(_),
            ))
            | MetaApiError::Meta(MetaError::SchemaRegistry(
                SchemasUpdateError::IncompatibleServiceChange(_),
            )) => StatusCode::CONFLICT,
            MetaApiError::Meta(MetaError::SchemaRegistry(SchemasUpdateError::UnknownService(
                _,
            ))) => StatusCode::NOT_FOUND,
            MetaApiError::Meta(MetaError::SchemaRegistry(
                SchemasUpdateError::UnknownDeployment(_),
            )) => StatusCode::NOT_FOUND,
            MetaApiError::Meta(MetaError::SchemaRegistry(
                SchemasUpdateError::ModifyInternalService(_),
            )) => StatusCode::FORBIDDEN,
            MetaApiError::InvalidField(_, _) => StatusCode::BAD_REQUEST,
            MetaApiError::Worker(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(match &self {
            MetaApiError::Meta(m) => ErrorDescriptionResponse {
                message: m.decorate().to_string(),
                restate_code: m.code().map(Code::code),
            },
            e => ErrorDescriptionResponse {
                message: e.to_string(),
                restate_code: None,
            },
        });

        (status_code, body).into_response()
    }
}

impl ToResponses for MetaApiError {
    fn generate(components: &mut Components) -> Result<Responses, Error> {
        let error_media_type =
            <Json<ErrorDescriptionResponse> as ToMediaTypes>::generate(components)?;
        Ok(Responses {
            responses: map! {
                "400".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type.clone(), ..Default::default() }
                ),
                "403".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type.clone(), ..Default::default() }
                ),
                "404".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type.clone(), ..Default::default() }
                ),
                "409".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type.clone(), ..Default::default() }
                ),
                "500".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type.clone(), ..Default::default() }
                ),
                "503".into() => okapi::openapi3::RefOr::Object(
                    okapi::openapi3::Response { content: error_media_type, ..Default::default() }
                )
            },
            ..Default::default()
        })
    }
}
