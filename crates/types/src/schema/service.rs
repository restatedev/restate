// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;

use super::invocation_target::InvocationTargetMetadata;
use super::Schema;
use crate::identifiers::{DeploymentId, ServiceRevision};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ServiceMetadata {
    /// # Name
    ///
    /// Fully qualified name of the service
    pub name: String,

    pub handlers: Vec<HandlerMetadata>,

    pub ty: ServiceType,

    /// # Deployment Id
    ///
    /// Deployment exposing the latest revision of the service.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub deployment_id: DeploymentId,

    /// # Revision
    ///
    /// Latest revision of the service.
    pub revision: ServiceRevision,

    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    pub public: bool,

    /// # Idempotency retention
    ///
    /// The retention duration of idempotent requests for this service.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub idempotency_retention: humantime::Duration,

    /// # Workflow completion retention
    ///
    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub workflow_completion_retention: Option<humantime::Duration>,
}

// This type is used only for exposing the handler metadata, and not internally. See [ServiceAndHandlerType].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HandlerMetadataType {
    Exclusive,
    Shared,
    Workflow,
}

impl From<InvocationTargetType> for HandlerMetadataType {
    fn from(value: InvocationTargetType) -> Self {
        match value {
            InvocationTargetType::Service => HandlerMetadataType::Shared,
            InvocationTargetType::VirtualObject(h_ty) => match h_ty {
                VirtualObjectHandlerType::Exclusive => HandlerMetadataType::Exclusive,
                VirtualObjectHandlerType::Shared => HandlerMetadataType::Shared,
            },
            InvocationTargetType::Workflow(h_ty) => match h_ty {
                WorkflowHandlerType::Workflow => HandlerMetadataType::Workflow,
                WorkflowHandlerType::Shared => HandlerMetadataType::Shared,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct HandlerMetadata {
    pub name: String,

    pub ty: HandlerMetadataType,

    // # Human readable input description
    //
    // If empty, no schema was provided by the user at discovery time.
    pub input_description: String,

    // # Human readable output description
    //
    // If empty, no schema was provided by the user at discovery time.
    pub output_description: String,
}

/// This API will return services registered by the user.
pub trait ServiceMetadataResolver {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata>;

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType>;

    fn list_services(&self) -> Vec<ServiceMetadata>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[cfg(feature = "test-util")]
#[allow(dead_code)]
pub mod test_util {
    use super::*;

    use std::collections::HashMap;

    #[derive(Debug, Default, Clone)]
    pub struct MockServiceMetadataResolver(HashMap<String, ServiceMetadata>);

    impl MockServiceMetadataResolver {
        pub fn add(&mut self, service_metadata: ServiceMetadata) {
            self.0
                .insert(service_metadata.name.clone(), service_metadata);
        }
    }

    impl ServiceMetadataResolver for MockServiceMetadataResolver {
        fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
            self.0.get(service_name.as_ref()).cloned()
        }

        fn resolve_latest_service_type(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceType> {
            self.0.get(service_name.as_ref()).map(|c| c.ty)
        }

        fn list_services(&self) -> Vec<ServiceMetadata> {
            self.0.values().cloned().collect()
        }
    }

    impl ServiceMetadata {
        pub fn mock_service(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| HandlerMetadata {
                        name: s.as_ref().to_string(),
                        ty: HandlerMetadataType::Shared,
                        input_description: "any".to_string(),
                        output_description: "any".to_string(),
                    })
                    .collect(),
                ty: ServiceType::Service,
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: std::time::Duration::from_secs(60).into(),
                workflow_completion_retention: None,
            }
        }

        pub fn mock_virtual_object(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| HandlerMetadata {
                        name: s.as_ref().to_string(),
                        ty: HandlerMetadataType::Exclusive,
                        input_description: "any".to_string(),
                        output_description: "any".to_string(),
                    })
                    .collect(),
                ty: ServiceType::VirtualObject,
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: std::time::Duration::from_secs(60).into(),
                workflow_completion_retention: None,
            }
        }
    }
}
