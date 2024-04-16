// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use std::time::Duration;

use restate_schema_api::component::ComponentMetadataResolver;
use restate_schema_api::invocation_target::InvocationTargetMetadata;
use restate_types::invocation::ComponentType;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HandlerSchemas {
    pub target_meta: InvocationTargetMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComponentSchemas {
    pub revision: ComponentRevision,
    pub handlers: HashMap<String, HandlerSchemas>,
    pub ty: ComponentType,
    pub location: ComponentLocation,
    pub idempotency_retention: Duration,
}

impl ComponentSchemas {
    pub fn as_component_metadata(&self, name: String) -> ComponentMetadata {
        ComponentMetadata {
            name,
            handlers: self
                .handlers
                .iter()
                .map(|(h_name, h_schemas)| HandlerMetadata {
                    name: h_name.clone(),
                    ty: h_schemas.target_meta.handler_ty,
                    input_description: h_schemas.target_meta.input_rules.to_string(),
                    output_description: h_schemas.target_meta.output_rules.to_string(),
                })
                .collect(),
            ty: self.ty,
            deployment_id: self.location.latest_deployment,
            revision: self.revision,
            public: self.location.public,
            idempotency_retention: self.idempotency_retention.into(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComponentLocation {
    pub latest_deployment: DeploymentId,
    pub public: bool,
}

impl ComponentMetadataResolver for Schema {
    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        let name = component_name.as_ref();
        self.use_component_schema(name, |component_schemas| {
            component_schemas.as_component_metadata(name.to_owned())
        })
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.use_component_schema(component_name.as_ref(), |component_schemas| {
            component_schemas.ty
        })
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        self.components
            .iter()
            .map(|(component_name, component_schemas)| {
                component_schemas.as_component_metadata(component_name.clone())
            })
            .collect()
    }
}

impl ComponentMetadataResolver for UpdateableSchema {
    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        self.0.load().resolve_latest_component(component_name)
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.0.load().resolve_latest_component_type(component_name)
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        self.0.load().list_components()
    }
}
