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

use restate_schema_api::invocation_target::InvocationTargetMetadata;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_types::invocation::ServiceType;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HandlerSchemas {
    pub target_meta: InvocationTargetMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceSchemas {
    pub revision: ServiceRevision,
    pub handlers: HashMap<String, HandlerSchemas>,
    pub ty: ServiceType,
    pub location: ServiceLocation,
    pub idempotency_retention: Duration,
    pub workflow_completion_retention: Option<Duration>,
}

impl ServiceSchemas {
    pub fn as_service_metadata(&self, name: String) -> ServiceMetadata {
        ServiceMetadata {
            name,
            handlers: self
                .handlers
                .iter()
                .map(|(h_name, h_schemas)| HandlerMetadata {
                    name: h_name.clone(),
                    ty: h_schemas.target_meta.target_ty.into(),
                    input_description: h_schemas.target_meta.input_rules.to_string(),
                    output_description: h_schemas.target_meta.output_rules.to_string(),
                })
                .collect(),
            ty: self.ty,
            deployment_id: self.location.latest_deployment,
            revision: self.revision,
            public: self.location.public,
            idempotency_retention: self.idempotency_retention.into(),
            workflow_completion_retention: self.workflow_completion_retention.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceLocation {
    pub latest_deployment: DeploymentId,
    pub public: bool,
}

impl ServiceMetadataResolver for Schema {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        let name = service_name.as_ref();
        self.use_service_schema(name, |service_schemas| {
            service_schemas.as_service_metadata(name.to_owned())
        })
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| service_schemas.ty)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.services
            .iter()
            .map(|(service_name, service_schemas)| {
                service_schemas.as_service_metadata(service_name.clone())
            })
            .collect()
    }
}

impl ServiceMetadataResolver for UpdateableSchema {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.load().resolve_latest_service(service_name)
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.0.load().resolve_latest_service_type(service_name)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.load().list_services()
    }
}
