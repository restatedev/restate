// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use okapi_operation::okapi::openapi3::MediaType;
use okapi_operation::okapi::Map;
use okapi_operation::*;
use prost::Message;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use restate_pb::grpc::reflection::FileDescriptorResponse;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};

use super::error::*;
use super::state::*;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListServicesResponse {
    pub services: Vec<ServiceMetadata>,
}

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
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
pub async fn get_service<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ModifyServiceRequest {
    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    pub public: bool,
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
pub async fn modify_service<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
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
pub async fn list_service_descriptors<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
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
