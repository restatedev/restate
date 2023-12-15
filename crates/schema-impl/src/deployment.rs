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
use restate_schema_api::deployment::{DeploymentMetadata, DeploymentMetadataResolver};
use restate_schema_api::service::ServiceMetadata;
use restate_types::identifiers::{DeploymentId, ServiceRevision};

impl DeploymentMetadataResolver for Schemas {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<DeploymentMetadata> {
        let schemas = self.0.load();
        let service = schemas.services.get(service_name.as_ref())?;
        match &service.location {
            ServiceLocation::BuiltIn { .. } => None,
            ServiceLocation::Deployment {
                latest_deployment, ..
            } => schemas
                .deployments
                .get(latest_deployment)
                .map(|schemas| schemas.metadata.clone()),
        }
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<DeploymentMetadata> {
        let schemas = self.0.load();
        schemas
            .deployments
            .get(deployment_id)
            .map(|schemas| schemas.metadata.clone())
    }

    fn get_deployment_descriptor_pool(&self, deployment_id: &DeploymentId) -> Option<Bytes> {
        let schemas = self.0.load();
        schemas
            .deployments
            .get(deployment_id)
            .map(|schemas| schemas.descriptor_pool.encode_to_vec().into())
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(DeploymentMetadata, Vec<ServiceMetadata>)> {
        let schemas = self.0.load();
        schemas
            .deployments
            .get(deployment_id)
            .map(|schemas| (schemas.metadata.clone(), schemas.services.clone()))
    }

    fn get_deployments(&self) -> Vec<(DeploymentMetadata, Vec<(String, ServiceRevision)>)> {
        let schemas = self.0.load();
        schemas
            .deployments
            .values()
            .map(|schemas| {
                (
                    schemas.metadata.clone(),
                    schemas
                        .services
                        .iter()
                        .map(|s| (s.name.clone(), s.revision))
                        .collect(),
                )
            })
            .collect()
    }
}
