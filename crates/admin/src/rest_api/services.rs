// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::*;
use crate::state::AdminServiceState;

use restate_meta_rest_model::services::*;
use restate_pb::grpc::reflection::FileDescriptorResponse;
use restate_schema_api::service::ServiceMetadataResolver;

use axum::extract::{Path, State};
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use http::StatusCode;
use okapi_operation::okapi::openapi3::MediaType;
use okapi_operation::okapi::Map;
use okapi_operation::*;
use prost::Message;
use restate_types::identifiers::ServiceId;
use restate_types::state_mut::ExternalStateMutation;

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<W>(
    State(state): State<AdminServiceState<W>>,
) -> Result<Json<ListServicesResponse>, MetaApiError> {
    Ok(ListServicesResponse {
        services: state.schemas().list_services(),
    }
    .into())
}

/// Get a service
#[openapi(
    summary = "Get service",
    description = "Get a registered service.",
    operation_id = "get_service",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn get_service<W>(
    State(state): State<AdminServiceState<W>>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

/// Modify a service
#[openapi(
    summary = "Modify a service",
    description = "Modify a registered service.",
    operation_id = "modify_service",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn modify_service<W>(
    State(state): State<AdminServiceState<W>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceRequest { public }): Json<
        ModifyServiceRequest,
    >,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    state
        .meta_handle()
        .modify_service(service_name.clone(), public)
        .await?;

    state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

/// Modify a service state
#[openapi(
    summary = "Modify a service state",
    description = "Modify service state",
    operation_id = "modify_service_state",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(
            status = "202",
            description = "Accepted",
            content = "okapi_operation::Empty",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn modify_service_state<W>(
    State(state): State<AdminServiceState<W>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceStateRequest {
        version,
        service_key,
        new_state,
    }): Json<ModifyServiceStateRequest>,
) -> Result<StatusCode, MetaApiError>
where
    W: restate_worker_api::Handle + Clone + Send,
{
    let service_id = ServiceId::new(service_name, service_key);

    let new_state = new_state
        .into_iter()
        .map(|(k, v)| (Bytes::from(k), v))
        .collect();

    state
        .worker_handle()
        .external_state_mutation(ExternalStateMutation {
            service_id,
            version,
            state: new_state,
        })
        .await?;

    Ok(StatusCode::ACCEPTED)
}

/// List service descriptors
#[openapi(
    summary = "List service descriptors",
    description = "List file descriptors for the service.",
    operation_id = "list_service_descriptors",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(status = "200", description = "OK", content = "okapi_operation::Empty"),
        from_type = "MetaApiError",
    )
)]
pub async fn list_service_descriptors<W>(
    State(state): State<AdminServiceState<W>>,
    Path(service_name): Path<String>,
) -> Result<Proto<FileDescriptorResponse>, MetaApiError> {
    state
        .schemas()
        .descriptors(&service_name)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
        .map(|descriptors| {
            FileDescriptorResponse {
                file_descriptor_proto: descriptors,
            }
            .into()
        })
}

pub struct Proto<T>(pub T);

impl<T> From<T> for Proto<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: Message> IntoResponse for Proto<T> {
    fn into_response(self) -> Response {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )],
            self.0.encode_to_vec(),
        )
            .into_response()
    }
}

impl<T> ToMediaTypes for Proto<T> {
    fn generate(_components: &mut Components) -> Result<Map<String, MediaType>, anyhow::Error> {
        Ok(okapi::map! {
            "application/x-protobuf".into() => {
                MediaType { ..Default::default() }
            }
        })
    }
}

impl_to_responses_for_wrapper!(Proto<T>);
