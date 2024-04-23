// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Schema, UpdateableSchema};
use restate_schema_api::deployment::{Deployment, DeploymentMetadata, DeploymentResolver};
use restate_schema_api::service::ServiceMetadata;
use restate_types::identifiers::{DeploymentId, ServiceRevision};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeploymentSchemas {
    pub metadata: DeploymentMetadata,

    // We need to store ServiceMetadata here only for queries
    // We could optimize the memory impact of this by reading these info from disk
    pub services: Vec<ServiceMetadata>,
}

impl DeploymentResolver for Schema {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        let service = self.services.get(service_name.as_ref())?;
        self.deployments
            .get(&service.location.latest_deployment)
            .map(|schemas| Deployment {
                id: service.location.latest_deployment,
                metadata: schemas.metadata.clone(),
            })
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.deployments
            .get(deployment_id)
            .map(|schemas| Deployment {
                id: *deployment_id,
                metadata: schemas.metadata.clone(),
            })
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.deployments.get(deployment_id).map(|schemas| {
            (
                Deployment {
                    id: *deployment_id,
                    metadata: schemas.metadata.clone(),
                },
                schemas.services.clone(),
            )
        })
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.deployments
            .iter()
            .map(|(deployment_id, schemas)| {
                (
                    Deployment {
                        id: *deployment_id,
                        metadata: schemas.metadata.clone(),
                    },
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

impl DeploymentResolver for UpdateableSchema {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.0
            .load()
            .resolve_latest_deployment_for_service(service_name)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.0.load().get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.0.load().get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.0.load().get_deployments()
    }
}
