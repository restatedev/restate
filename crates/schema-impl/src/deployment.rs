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

use crate::schemas_impl::ComponentLocation;
use restate_schema_api::component::ComponentMetadata;
use restate_schema_api::deployment::{Deployment, DeploymentResolver};
use restate_types::identifiers::{ComponentRevision, DeploymentId};

impl DeploymentResolver for Schemas {
    fn resolve_latest_deployment_for_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        let schemas = self.0.load();
        let component = schemas.components.get(component_name.as_ref())?;
        match &component.location {
            ComponentLocation::BuiltIn { .. } => None,
            ComponentLocation::Deployment {
                latest_deployment, ..
            } => schemas
                .deployments
                .get(latest_deployment)
                .map(|schemas| Deployment {
                    id: *latest_deployment,
                    metadata: schemas.metadata.clone(),
                }),
        }
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        let schemas = self.0.load();
        schemas
            .deployments
            .get(deployment_id)
            .map(|schemas| Deployment {
                id: *deployment_id,
                metadata: schemas.metadata.clone(),
            })
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ComponentMetadata>)> {
        let schemas = self.0.load();
        schemas.deployments.get(deployment_id).map(|schemas| {
            (
                Deployment {
                    id: *deployment_id,
                    metadata: schemas.metadata.clone(),
                },
                schemas.components.clone(),
            )
        })
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ComponentRevision)>)> {
        let schemas = self.0.load();
        schemas
            .deployments
            .iter()
            .map(|(deployment_id, schemas)| {
                (
                    Deployment {
                        id: *deployment_id,
                        metadata: schemas.metadata.clone(),
                    },
                    schemas
                        .components
                        .iter()
                        .map(|s| (s.name.clone(), s.revision))
                        .collect(),
                )
            })
            .collect()
    }
}
