// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;
use bytes::Bytes;

use crate::schemas_impl::ServiceLocation;
use restate_schema_api::endpoint::{EndpointMetadata, EndpointMetadataResolver};
use restate_types::identifiers::{EndpointId, ServiceRevision};

impl EndpointMetadataResolver for Schemas {
    fn resolve_latest_endpoint_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<EndpointMetadata> {
        let schemas = self.0.load();
        let service = schemas.services.get(service_name.as_ref())?;
        match &service.location {
            ServiceLocation::BuiltIn { .. } => None,
            ServiceLocation::ServiceEndpoint {
                latest_endpoint, ..
            } => schemas
                .endpoints
                .get(latest_endpoint)
                .map(|schemas| schemas.metadata.clone()),
        }
    }

    fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata> {
        let schemas = self.0.load();
        schemas
            .endpoints
            .get(endpoint_id)
            .map(|schemas| schemas.metadata.clone())
    }

    fn get_endpoint_descriptor_pool(&self, endpoint_id: &EndpointId) -> Option<Bytes> {
        let schemas = self.0.load();
        schemas
            .endpoints
            .get(endpoint_id)
            .map(|schemas| schemas.descriptor_pool.encode_to_vec().into())
    }

    fn get_endpoint_and_services(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<(EndpointMetadata, Vec<(String, ServiceRevision)>)> {
        let schemas = self.0.load();
        schemas
            .endpoints
            .get(endpoint_id)
            .map(|schemas| (schemas.metadata.clone(), schemas.services.clone()))
    }

    fn get_endpoints(&self) -> Vec<(EndpointMetadata, Vec<(String, ServiceRevision)>)> {
        let schemas = self.0.load();
        schemas
            .endpoints
            .values()
            .map(|schemas| (schemas.metadata.clone(), schemas.services.clone()))
            .collect()
    }
}
