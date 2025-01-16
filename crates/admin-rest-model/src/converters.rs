// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::schema::service::{HandlerMetadata, HandlerMetadataType, ServiceMetadata};

use crate::{
    deployments::{DetailedDeploymentResponse, RegisterDeploymentResponse},
    services::ListServicesResponse,
    version::AdminApiVersion,
};

pub fn convert_register_deployment_response(
    version: AdminApiVersion,
    resp: RegisterDeploymentResponse,
) -> RegisterDeploymentResponse {
    match version {
        AdminApiVersion::V1 => convert_register_deployment_response_v1(resp),
        _ => resp,
    }
}

fn convert_register_deployment_response_v1(
    resp: RegisterDeploymentResponse,
) -> RegisterDeploymentResponse {
    RegisterDeploymentResponse {
        services: resp
            .services
            .into_iter()
            .map(convert_service_metadata_v1)
            .collect(),
        ..resp
    }
}

pub fn convert_detailed_deployment_response(
    version: AdminApiVersion,
    resp: DetailedDeploymentResponse,
) -> DetailedDeploymentResponse {
    match version {
        AdminApiVersion::V1 => convert_detailed_deployment_response_v1(resp),
        _ => resp,
    }
}

fn convert_detailed_deployment_response_v1(
    resp: DetailedDeploymentResponse,
) -> DetailedDeploymentResponse {
    DetailedDeploymentResponse {
        services: resp
            .services
            .into_iter()
            .map(convert_service_metadata_v1)
            .collect(),
        ..resp
    }
}

pub fn convert_list_services_response(
    version: AdminApiVersion,
    resp: ListServicesResponse,
) -> ListServicesResponse {
    match version {
        AdminApiVersion::V1 => convert_list_services_response_v1(resp),
        _ => resp,
    }
}

fn convert_list_services_response_v1(resp: ListServicesResponse) -> ListServicesResponse {
    ListServicesResponse {
        services: resp
            .services
            .into_iter()
            .map(convert_service_metadata_v1)
            .collect(),
    }
}

pub fn convert_service_metadata(
    version: AdminApiVersion,
    service_metadata: ServiceMetadata,
) -> ServiceMetadata {
    match version {
        AdminApiVersion::V1 => convert_service_metadata_v1(service_metadata),
        _ => service_metadata,
    }
}

fn convert_service_metadata_v1(service_metadata: ServiceMetadata) -> ServiceMetadata {
    ServiceMetadata {
        handlers: service_metadata
            .handlers
            .into_iter()
            .map(convert_handler_metadata_v1)
            .collect(),
        ..service_metadata
    }
}

fn convert_handler_metadata_v1(handler_metadata: HandlerMetadata) -> HandlerMetadata {
    HandlerMetadata {
        // pre 1.2, we provided Shared instead of None when the handler is in a Service
        ty: handler_metadata.ty.or(Some(HandlerMetadataType::Shared)),
        ..handler_metadata
    }
}
