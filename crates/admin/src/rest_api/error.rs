// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::schema_registry::SchemaRegistryError;
use assert2::let_assert;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use codederror::{Code, CodedError};
use okapi_operation::okapi::map;
use okapi_operation::okapi::openapi3::{RefOr, Responses};
use okapi_operation::{Components, ToMediaTypes, ToResponses, okapi};
use restate_core::ShutdownError;
use restate_types::identifiers::{DeploymentId, SubscriptionId};
use restate_types::invocation::ServiceType;
use restate_types::schema::updater;
use schemars::JsonSchema;
use serde::Serialize;
use std::ops::RangeInclusive;

// --- Few helpers to define Admin API errors.

/// Macro to generate an Admin API Error enum with the given variants.
///
/// All the errors should implement both axum IntoResponse and okapi_operation ToResponses (see macro below).
///
/// Example usage:
///
/// ```rust,ignore
/// generate_meta_api_error!(CancelInvocationError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, InvocationWasAlreadyCompletedError]);
/// ```
#[macro_export]
macro_rules! generate_meta_api_error {
    // Entry point of the macro
    ($enum_name:ident: [$($variant:ident),* $(,)?]) => {
        // Generate the error enum with transparent variants
        #[derive(Debug, thiserror::Error)]
        #[allow(clippy::enum_variant_names)]
        pub enum $enum_name {
            $(
                #[error(transparent)]
                $variant(#[from] $variant),
            )*
        }

        // Generate IntoResponse implementation
        impl axum::response::IntoResponse for $enum_name {
            fn into_response(self) -> axum::response::Response {
                match self {
                    $(
                        $enum_name::$variant(err) => err.into_response(),
                    )*
                }
            }
        }

        // Generate ToResponses implementation
        impl okapi_operation::ToResponses for $enum_name {
            fn generate(components: &mut okapi_operation::Components) -> Result<okapi_operation::okapi::openapi3::Responses, okapi_operation::anyhow::Error> {
                // Collect responses from all variants
                let responses = vec![
                    $(
                        <$variant as okapi_operation::ToResponses>::generate(components)?,
                    )*
                ];

                // Fold the responses into one
                $crate::rest_api::error::merge_error_responses(responses)
            }
        }
    };
}

pub(crate) fn merge_error_responses(responses: Vec<Responses>) -> Result<Responses, anyhow::Error> {
    let mut result_responses = Responses::default();
    for t_responses in responses {
        assert!(
            t_responses.default.is_none(),
            "Errors should not define a default response"
        );
        for (status, response) in t_responses.responses {
            let_assert!(RefOr::Object(new_response) = response);
            result_responses
                .responses
                .entry(status)
                .and_modify(|res| {
                    let_assert!(RefOr::Object(old_response) = res);
                    old_response.description =
                        format!("{}\n{}", old_response.description, new_response.description);
                })
                .or_insert_with(|| RefOr::Object(new_response));
        }
    }

    Ok(result_responses)
}

/// Macro to implement both axum IntoResponse and okapi_operation ToResponses,
/// such that the error can be used both as value from axum handlers and to auto generate the OpenAPI documentation.
///
/// Example usage:
///
/// ```rust,ignore
/// #[derive(Debug, thiserror::Error)]
// #[error("Error message returned in the HTTP API.")]
// pub(crate) struct MyError;
// impl_meta_api_error!(MyError: HTTP_STATUS_CODE "Error description rendered in the OpenAPI.");
/// ```
macro_rules! impl_meta_api_error {
    ($error_name:ident: $status_code:ident $description:literal) => {
        impl IntoResponse for $error_name {
            fn into_response(self) -> Response {
                (
                    StatusCode::$status_code,
                    Json(ErrorDescriptionResponse {
                        message: self.to_string(),
                        restate_code: None,
                    })
                ).into_response()
            }
        }

        impl ToResponses for $error_name {
            fn generate(components: &mut Components) -> Result<Responses, anyhow::Error> {
                let error_media_type =
                    <Json<ErrorDescriptionResponse> as ToMediaTypes>::generate(components)?;
                Ok(Responses {
                    responses: map! {
                        StatusCode::$status_code.to_string() => okapi::openapi3::RefOr::Object(
                            okapi::openapi3::Response { content: error_media_type.clone(), description: $description.to_owned(), ..Default::default() }
                        ),
                    },
                    ..Default::default()
                })
            }
        }
    };
    ($error_name:ident: $status_code:ident) => {
        impl_meta_api_error!($error_name: $status_code "");
    };
}

// --- Common Admin API errors.

#[derive(Debug, thiserror::Error)]
#[error("The request field '{0}' is invalid. Reason: {1}")]
pub(crate) struct InvalidFieldError(pub(crate) &'static str, pub(crate) String);
impl_meta_api_error!(InvalidFieldError: BAD_REQUEST);

#[derive(Debug, thiserror::Error)]
#[error("The requested invocation '{0}' does not exist")]
pub(crate) struct InvocationNotFoundError(pub(crate) String);
impl_meta_api_error!(InvocationNotFoundError: NOT_FOUND);

#[derive(Debug, thiserror::Error)]
#[error("Error when routing the request internally. Reason: {0}")]
pub(crate) struct InvocationClientError(
    #[from] pub(crate) restate_types::invocation::client::InvocationClientError,
);
impl_meta_api_error!(InvocationClientError: SERVICE_UNAVAILABLE "Error when routing the request within restate.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' was already completed.")]
pub(crate) struct InvocationWasAlreadyCompletedError(pub(crate) String);
impl_meta_api_error!(InvocationWasAlreadyCompletedError: CONFLICT "The invocation was already completed, so it cannot be cancelled nor killed. You can instead purge the invocation, in order for restate to forget it.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' is not yet completed.")]
pub(crate) struct PurgeInvocationNotCompletedError(pub(crate) String);
impl_meta_api_error!(PurgeInvocationNotCompletedError: CONFLICT "The invocation is not yet completed. An invocation can be purged only when completed.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' is still running.")]
pub(crate) struct RestartAsNewInvocationStillRunningError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationStillRunningError: CONFLICT "The invocation is still running. An invocation can be restarted only when completed.");

#[derive(Debug, thiserror::Error)]
#[error(
    "Restarting the invocation '{0}' is not supported. Restarting workflows is not supported, and restarting invocations created using the old service protocol."
)]
pub(crate) struct RestartAsNewInvocationUnsupportedError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationUnsupportedError: UNPROCESSABLE_ENTITY "Restarting the invocation is not supported. Restarting workflows is not supported, and restarting invocations created using the old service protocol.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The invocation '{0}' cannot be restarted because the input is not available. This indicates that the journal was already purged, or not retained at all."
)]
pub(crate) struct RestartAsNewInvocationMissingInputError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationMissingInputError: GONE "The invocation cannot be restarted because the input is not available. In order to restart an invocation, the journal must be available in order to read the input again. Journal can be retained after completion by enabling journal retention.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' cannot be restarted because it's not running yet.")]
pub(crate) struct RestartAsNewInvocationNotStartedError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationNotStartedError: TOO_EARLY "The invocation cannot be restarted because it's not running yet, meaning it might have been scheduled or inboxed.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' is completed, cannot be resumed.")]
pub(crate) struct ResumeInvocationCompletedError(pub(crate) String);
impl_meta_api_error!(ResumeInvocationCompletedError: CONFLICT "The invocation is completed. An invocation can be resumed only when running, paused or suspended.");

#[derive(Debug, thiserror::Error)]
#[error("The invocation '{0}' is either inboxed or scheduled, cannot be resumed.")]
pub(crate) struct ResumeInvocationNotStartedError(pub(crate) String);
impl_meta_api_error!(ResumeInvocationNotStartedError: TOO_EARLY "The invocation is either inboxed or scheduled. An invocation can be resumed only when running, paused or suspended.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The invocation '{0}' is still running or the deployment id is not pinned yet, deployment id cannot be changed."
)]
pub(crate) struct ResumeInvocationCannotChangeDeploymentIdError(pub(crate) String);
impl_meta_api_error!(ResumeInvocationCannotChangeDeploymentIdError: CONFLICT "The invocation is still running or the deployment id is not pinned yet, deployment id cannot be changed. The deployment id can be changed only if the invocation is paused or suspended, and a deployment id is already pinned.");

#[derive(Debug, thiserror::Error)]
#[error("The given deployment was not found when trying to resume the invocation '{0}'.")]
pub(crate) struct ResumeInvocationDeploymentNotFoundError(pub(crate) String);
impl_meta_api_error!(ResumeInvocationDeploymentNotFoundError: BAD_REQUEST "The given deployment was not found.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The invocation '{invocation_id}' is running on protocol version '{pinned_protocol_version}', while the chosen deployment '{deployment_id}' supports the range {supported_protocol_versions:?}."
)]
pub(crate) struct ResumeInvocationIncompatibleDeploymentIdError {
    pub(crate) invocation_id: String,
    pub(crate) pinned_protocol_version: i32,
    pub(crate) deployment_id: String,
    pub(crate) supported_protocol_versions: RangeInclusive<i32>,
}
impl_meta_api_error!(ResumeInvocationIncompatibleDeploymentIdError: BAD_REQUEST "The selected deployment id to resume the invocation doesn't support the currently pinned service protocol version.");

#[derive(Debug, thiserror::Error)]
#[error("The given index is out of range of currently stored journal for invocation '{0}'.")]
pub(crate) struct RestartAsNewInvocationJournalIndexOutOfRangeError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationJournalIndexOutOfRangeError: BAD_REQUEST "The given journal index is out of range.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The prefix of the journal '{0}' up to 'from' (included) contains some Commands without respective Completions."
)]
pub(crate) struct RestartAsNewInvocationJournalCopyRangeInvalidError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationJournalCopyRangeInvalidError: BAD_REQUEST "The given journal prefix contains some Commands without respective Completions.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The invocation '{0}' is still running or the deployment id is not pinned yet, deployment id cannot be changed."
)]
pub(crate) struct RestartAsNewInvocationCannotChangeDeploymentIdError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationCannotChangeDeploymentIdError: CONFLICT "The invocation is still running or the deployment id is not pinned yet, deployment id cannot be changed. The deployment id can be changed only if the invocation is paused or suspended, and a deployment id is already pinned.");

#[derive(Debug, thiserror::Error)]
#[error("The given deployment was not found when trying to restart as new the invocation '{0}'.")]
pub(crate) struct RestartAsNewInvocationDeploymentNotFoundError(pub(crate) String);
impl_meta_api_error!(RestartAsNewInvocationDeploymentNotFoundError: BAD_REQUEST "The given deployment was not found.");

#[derive(Debug, thiserror::Error)]
#[error(
    "The invocation '{invocation_id}' is running on protocol version '{pinned_protocol_version}', while the chosen deployment '{deployment_id}' supports the range {supported_protocol_versions:?}."
)]
pub(crate) struct RestartAsNewInvocationIncompatibleDeploymentIdError {
    pub(crate) invocation_id: String,
    pub(crate) pinned_protocol_version: i32,
    pub(crate) deployment_id: String,
    pub(crate) supported_protocol_versions: RangeInclusive<i32>,
}
impl_meta_api_error!(RestartAsNewInvocationIncompatibleDeploymentIdError: BAD_REQUEST "The selected deployment id to restart as new the invocation doesn't support the currently pinned service protocol version.");

// --- Old Meta API errors. Please don't use these anymore.

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
    #[error("The requested handler '{handler_name}' on service '{service_name}' does not exist")]
    HandlerNotFound {
        service_name: String,
        handler_name: String,
    },
    #[error("The requested subscription '{0}' does not exist")]
    SubscriptionNotFound(SubscriptionId),
    #[error("Cannot {0} for service type {1}")]
    UnsupportedOperation(&'static str, ServiceType),
    #[error(transparent)]
    Schema(#[from] updater::SchemaError),
    #[error(transparent)]
    Discovery(#[from] restate_service_protocol::discovery::DiscoveryError),
    #[error("Internal server error: {0}")]
    Internal(String),
    #[error("Conflict: {0}")]
    Conflict(String),
}

/// # Error description response
///
/// Error details of the response
#[derive(Debug, Serialize, JsonSchema)]
pub(crate) struct ErrorDescriptionResponse {
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
            | MetaApiError::HandlerNotFound { .. }
            | MetaApiError::DeploymentNotFound(_)
            | MetaApiError::SubscriptionNotFound(_) => StatusCode::NOT_FOUND,
            MetaApiError::InvalidField(_, _) | MetaApiError::UnsupportedOperation(_, _) => {
                StatusCode::BAD_REQUEST
            }
            MetaApiError::Schema(schema_error) => match schema_error {
                updater::SchemaError::NotFound(_) => StatusCode::NOT_FOUND,
                updater::SchemaError::Service(updater::ServiceError::DifferentType { .. })
                | updater::SchemaError::Service(updater::ServiceError::RemovedHandlers {
                    ..
                }) => StatusCode::CONFLICT,
                updater::SchemaError::Service(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::BAD_REQUEST,
            },
            MetaApiError::Conflict(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(match &self {
            MetaApiError::Schema(m) => ErrorDescriptionResponse {
                message: m.decorate().to_string(),
                restate_code: m.code().map(Code::code),
            },
            MetaApiError::Discovery(err) => ErrorDescriptionResponse {
                message: err.decorate().to_string(),
                restate_code: err.code().map(Code::code),
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
    fn generate(components: &mut Components) -> Result<Responses, anyhow::Error> {
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

impl From<SchemaRegistryError> for MetaApiError {
    fn from(value: SchemaRegistryError) -> Self {
        match value {
            SchemaRegistryError::Schema(err) => MetaApiError::Schema(err),
            SchemaRegistryError::Internal(msg) => MetaApiError::Internal(msg),
            SchemaRegistryError::Shutdown(err) => MetaApiError::Internal(err.to_string()),
            SchemaRegistryError::Discovery(err) => MetaApiError::Discovery(err),
            e @ SchemaRegistryError::UpdateDeployment { .. } => {
                MetaApiError::Conflict(e.to_string())
            }
        }
    }
}

impl From<ShutdownError> for MetaApiError {
    fn from(value: ShutdownError) -> Self {
        MetaApiError::Internal(value.to_string())
    }
}

pub(crate) struct GenericRestError {
    status_code: StatusCode,
    error_message: String,
}

impl GenericRestError {
    pub fn new(status_code: StatusCode, error_message: impl Into<String>) -> Self {
        Self {
            status_code,
            error_message: error_message.into(),
        }
    }
}

impl IntoResponse for GenericRestError {
    fn into_response(self) -> Response {
        (self.status_code, self.error_message).into_response()
    }
}

impl ToResponses for GenericRestError {
    fn generate(components: &mut Components) -> Result<Responses, anyhow::Error> {
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
