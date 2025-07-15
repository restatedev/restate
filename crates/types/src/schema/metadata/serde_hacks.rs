// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::identifiers::DeploymentId;
use crate::schema::deployment::DeploymentMetadata;
use crate::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;

mod v1_data_model {
    use super::*;

    #[serde_as]
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct DeploymentSchemas {
        pub metadata: DeploymentMetadata,

        #[serde_as(as = "restate_serde_util::MapAsVec")]
        pub services: HashMap<String, ServiceMetadata>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ServiceLocation {
        pub latest_deployment: DeploymentId,
        pub public: bool,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ServiceSchemas {
        pub revision: crate::identifiers::ServiceRevision,
        pub handlers: HashMap<String, HandlerSchemas>,
        pub ty: ServiceType,
        pub location: ServiceLocation,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub idempotency_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub workflow_completion_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub journal_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub inactivity_timeout: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub abort_timeout: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub enable_lazy_state: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub documentation: Option<String>,
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        pub metadata: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HandlerSchemas {
        pub target_meta: InvocationTargetMetadata,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub idempotency_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub workflow_completion_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub journal_retention: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub inactivity_timeout: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub abort_timeout: Option<Duration>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub enable_lazy_state: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub documentation: Option<String>,
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        pub metadata: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(
        from = "serde_hacks::InvocationTargetMetadata",
        into = "serde_hacks::InvocationTargetMetadata"
    )]
    pub struct InvocationTargetMetadata {
        pub public: bool,
        pub completion_retention: Duration,
        pub journal_retention: Duration,
        pub target_ty: InvocationTargetType,
        pub input_rules: InputRules,
        pub output_rules: OutputRules,
    }

    mod serde_hacks {
        use super::*;

        /// Some good old serde hacks here to make sure we can parse the old data structure.
        /// See the tests for more details on the old data structure and the various cases.
        /// Revisit this for Restate 1.5
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct InvocationTargetMetadata {
            pub public: bool,

            #[serde(default)]
            pub completion_retention: Option<Duration>,
            /// This is unused at this point, we just write it for backward compatibility.
            #[serde(default)]
            pub idempotency_retention: Duration,
            #[serde(default, skip_serializing_if = "Duration::is_zero")]
            pub journal_retention: Duration,

            pub target_ty: InvocationTargetType,
            pub input_rules: InputRules,
            pub output_rules: OutputRules,
        }

        impl From<super::InvocationTargetMetadata> for InvocationTargetMetadata {
            fn from(
                super::InvocationTargetMetadata {
                    public,
                    completion_retention,
                    journal_retention,
                    target_ty,
                    input_rules,
                    output_rules,
                }: super::InvocationTargetMetadata,
            ) -> Self {
                // This is the 1.3 business logic we need to please here.
                // Note that completion_retention was set only for Workflow methods anyway
                //   if has_idempotency_key {
                //     Some(cmp::max(
                //       self.completion_retention.unwrap_or_default(),
                //       self.idempotency_retention,
                //     ))
                //   } else {
                //     self.completion_retention
                //   }
                let completion_retention_old =
                    if target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) {
                        Some(completion_retention)
                    } else {
                        None
                    };
                Self {
                    public,
                    completion_retention: completion_retention_old,
                    // Just always set it, won't hurt and doesn't violate the code below.
                    idempotency_retention: completion_retention,
                    journal_retention,
                    target_ty,
                    input_rules,
                    output_rules,
                }
            }
        }

        impl From<InvocationTargetMetadata> for super::InvocationTargetMetadata {
            fn from(
                InvocationTargetMetadata {
                    public,
                    completion_retention,
                    idempotency_retention,
                    journal_retention,
                    target_ty,
                    input_rules,
                    output_rules,
                }: InvocationTargetMetadata,
            ) -> Self {
                Self {
                    public,
                    // In the old data structure, either completion_retention was filled and used, or idempotency retention was filled.
                    // So just try to pick first the one, then the other
                    completion_retention: completion_retention.unwrap_or(idempotency_retention),
                    journal_retention,
                    target_ty,
                    input_rules,
                    output_rules,
                }
            }
        }
    }
}

/// The schema information
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    // --- Old data structure
    #[serde(default, skip_serializing_if = "Option::is_none")]
    services: Option<HashMap<String, v1_data_model::ServiceSchemas>>,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments: Option<HashMap<DeploymentId, v1_data_model::DeploymentSchemas>>,

    // --- New data structure

    // Registered deployments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments_v2: Option<Vec<Deployment>>,

    // --- Same in old and new schema data structure
    /// This gets bumped on each update.
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl From<super::Schema> for Schema {
    fn from(
        super::Schema {
            version,
            deployments,
            subscriptions,
            ..
        }: super::Schema,
    ) -> Self {
        Self {
            services: None,
            deployments: None,
            deployments_v2: Some(deployments.into_values().collect()),
            version,
            subscriptions,
        }
    }
}

impl From<Schema> for super::Schema {
    fn from(
        Schema {
            services,
            deployments,
            deployments_v2,
            version,
            subscriptions,
        }: Schema,
    ) -> Self {
        fn build_active_service_revision_index(
            deployments: &Vec<Deployment>,
        ) -> HashMap<String, ActiveServiceRevision> {
            let mut active_service_revisions = HashMap::new();
            for deployment in deployments {
                for service in deployment.services.values() {
                    active_service_revisions
                        .entry(service.name.clone())
                        .and_modify(|registered_service_revision: &mut ActiveServiceRevision| {
                            if registered_service_revision.service_revision.revision
                                < service.revision
                            {
                                *registered_service_revision = ActiveServiceRevision {
                                    deployment_id: deployment.id,
                                    service_revision: Arc::clone(service),
                                }
                            }
                        })
                        .or_insert(ActiveServiceRevision {
                            deployment_id: deployment.id,
                            service_revision: Arc::clone(service),
                        });
                }
            }
            active_service_revisions
        }

        if let Some(deployments_v2) = deployments_v2 {
            Self {
                version,
                active_service_revisions: build_active_service_revision_index(&deployments_v2),
                deployments: deployments_v2
                    .into_iter()
                    .map(|deployment| (deployment.id, deployment))
                    .collect(),
                subscriptions,
            }
        } else if let (Some(services), Some(deployments)) = (services, deployments) {
            let conversions::V2Schemas { deployments } = conversions::V1Schemas {
                services,
                deployments,
            }
            .into_v2();

            Self {
                version,
                active_service_revisions: build_active_service_revision_index(&deployments),
                deployments: deployments
                    .into_iter()
                    .map(|deployment| (deployment.id, deployment))
                    .collect(),
                subscriptions,
            }
        } else {
            panic!(
                "Unexpected situation where neither v1 data structure nor v2 data structure is used!"
            )
        }
    }
}

mod conversions {
    use super::*;
    use crate::identifiers::DeploymentId;
    use crate::invocation::{
        InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
    };
    use crate::schema::deployment::DeploymentMetadata;
    use crate::schema::invocation_target::{
        DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION,
    };
    use crate::schema::service::{HandlerMetadata, HandlerMetadataType, ServiceMetadata};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    pub struct V1Schemas {
        pub services: HashMap<String, v1_data_model::ServiceSchemas>,
        pub deployments: HashMap<DeploymentId, v1_data_model::DeploymentSchemas>,
    }

    impl V1Schemas {
        pub fn into_v2(self) -> V2Schemas {
            let V1Schemas {
                services,
                deployments,
            } = self;

            let mut v2_deployments = Vec::with_capacity(deployments.len());
            for (deployment_id, deployment) in deployments {
                let mut v2_services = HashMap::with_capacity(deployment.services.len());

                for (service_name, service) in deployment.services {
                    let service_schemas = services.get(&service_name);
                    let mut v2_handlers = HashMap::with_capacity(service.handlers.len());

                    for (handler_name, handler) in service.handlers {
                        let input_rules = service_schemas
                            .and_then(|service| service.handlers.get(&handler_name))
                            .map(|hs| hs.target_meta.input_rules.clone())
                            .unwrap_or_default();
                        let output_rules = service_schemas
                            .and_then(|service| service.handlers.get(&handler_name))
                            .map(|hs| hs.target_meta.output_rules.clone())
                            .unwrap_or_default();
                        let handler = Handler {
                            name: handler_name.clone(),
                            target_ty: to_target_ty(service.ty, handler.ty),
                            input_rules,
                            output_rules,
                            public: None,
                            idempotency_retention: handler.idempotency_retention,
                            workflow_completion_retention: handler.workflow_completion_retention,
                            journal_retention: handler.journal_retention,
                            inactivity_timeout: handler.inactivity_timeout,
                            abort_timeout: handler.abort_timeout,
                            documentation: handler.documentation,
                            enable_lazy_state: handler.enable_lazy_state,
                            metadata: handler.metadata,
                        };
                        v2_handlers.insert(handler_name, handler);
                    }

                    let service_revision = ServiceRevision {
                        name: service_name.clone(),
                        handlers: v2_handlers,
                        ty: service.ty,
                        documentation: service.documentation,
                        metadata: service.metadata,
                        revision: service.revision,
                        public: service.public,
                        idempotency_retention: service.idempotency_retention,
                        workflow_completion_retention: service.workflow_completion_retention,
                        journal_retention: service.journal_retention,
                        inactivity_timeout: service.inactivity_timeout,
                        abort_timeout: service.abort_timeout,
                        enable_lazy_state: service.enable_lazy_state,
                        service_openapi_cache: Arc::new(Default::default()),
                    };

                    v2_services.insert(service_name, Arc::new(service_revision));
                }
                let v2_deployment = Deployment {
                    id: deployment_id,
                    ty: deployment.metadata.ty,
                    delivery_options: deployment.metadata.delivery_options,
                    supported_protocol_versions: deployment.metadata.supported_protocol_versions,
                    sdk_version: deployment.metadata.sdk_version,
                    created_at: deployment.metadata.created_at,
                    services: v2_services,
                };
                v2_deployments.push(v2_deployment);
            }

            V2Schemas {
                deployments: v2_deployments,
            }
        }
    }

    #[allow(dead_code)]
    fn to_target_ty(
        ty: ServiceType,
        handler_ty: Option<HandlerMetadataType>,
    ) -> InvocationTargetType {
        match (ty, handler_ty) {
            (ServiceType::Service, _) => InvocationTargetType::Service,
            (ServiceType::VirtualObject, None | Some(HandlerMetadataType::Exclusive)) => {
                InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
            }
            (ServiceType::VirtualObject, Some(HandlerMetadataType::Shared)) => {
                InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Shared)
            }
            (ServiceType::Workflow, None | Some(HandlerMetadataType::Workflow)) => {
                InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
            }
            (ServiceType::Workflow, Some(HandlerMetadataType::Shared)) => {
                InvocationTargetType::Workflow(WorkflowHandlerType::Shared)
            }
            (st, ht) => panic!(
                "Unexpected combination service type {st} and handler type {ht:?}, this should have been checked at discovery time."
            ),
        }
    }

    pub struct V2Schemas {
        pub deployments: Vec<Deployment>,
    }

    impl V2Schemas {
        #[allow(dead_code)]
        pub fn into_v1(self) -> V1Schemas {
            let V2Schemas { deployments } = self;

            let mut v1_deployments = HashMap::with_capacity(deployments.len());
            let mut current_service_revision_to_deployment = HashMap::new();

            for deployment in &deployments {
                let deployment_id = deployment.id;
                let mut v1_services_metadata = HashMap::with_capacity(deployment.services.len());

                for (service_name, service) in &deployment.services {
                    let mut v1_handlers = HashMap::with_capacity(service.handlers.len());

                    for (handler_name, handler) in &service.handlers {
                        v1_handlers.insert(
                            handler_name.clone(),
                            HandlerMetadata {
                                name: handler_name.clone(),
                                ty: handler.target_ty.into(),
                                documentation: handler.documentation.clone(),
                                metadata: handler.metadata.clone(),
                                idempotency_retention: handler.idempotency_retention,
                                workflow_completion_retention: handler
                                    .workflow_completion_retention,
                                journal_retention: handler.journal_retention,
                                inactivity_timeout: handler.inactivity_timeout,
                                abort_timeout: handler.abort_timeout,
                                enable_lazy_state: handler.enable_lazy_state,
                                public: handler.public.unwrap_or(service.public),
                                input_description: handler.input_rules.to_string(),
                                output_description: handler.output_rules.to_string(),
                                input_json_schema: handler.input_rules.json_schema(),
                                output_json_schema: handler.output_rules.json_schema(),
                            },
                        );
                    }

                    v1_services_metadata.insert(
                        service_name.clone(),
                        ServiceMetadata {
                            name: service_name.clone(),
                            handlers: v1_handlers,
                            ty: service.ty,
                            documentation: service.documentation.clone(),
                            metadata: service.metadata.clone(),
                            deployment_id,
                            revision: service.revision,
                            public: service.public,
                            idempotency_retention: service.idempotency_retention,
                            workflow_completion_retention: service.workflow_completion_retention,
                            journal_retention: service.journal_retention,
                            inactivity_timeout: service.inactivity_timeout,
                            abort_timeout: service.abort_timeout,
                            enable_lazy_state: service.enable_lazy_state,
                        },
                    );
                    current_service_revision_to_deployment
                        .entry(service_name.clone())
                        .and_modify(|(revision, dep_id)| {
                            if *revision < service.revision {
                                *revision = service.revision;
                                *dep_id = deployment_id;
                            }
                        })
                        .or_insert((service.revision, deployment_id));
                }
                v1_deployments.insert(
                    deployment_id,
                    v1_data_model::DeploymentSchemas {
                        metadata: DeploymentMetadata {
                            ty: deployment.ty.clone(),
                            delivery_options: deployment.delivery_options.clone(),
                            supported_protocol_versions: deployment
                                .supported_protocol_versions
                                .clone(),
                            sdk_version: deployment.sdk_version.clone(),
                            created_at: deployment.created_at,
                        },
                        services: v1_services_metadata,
                    },
                );
            }

            let mut v1_services =
                HashMap::with_capacity(current_service_revision_to_deployment.len());
            let deployments: HashMap<_, _> = deployments.into_iter().map(|d| (d.id, d)).collect();
            for (service_name, (current_service_revision, deployment_id)) in
                current_service_revision_to_deployment
            {
                if let Some(deployment) = deployments.get(&deployment_id) {
                    // I built this index above, those values must be present!
                    let service = deployment.services.get(&service_name).unwrap();

                    let mut v1_handler_schemas = HashMap::with_capacity(service.handlers.len());
                    for (handler_name, handler) in &service.handlers {
                        // Copy-pasted from updater.rs
                        // In the final version, this stuff lives only there.

                        // Defaulting and overriding
                        let completion_retention = if handler.target_ty
                            == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                        {
                            handler
                                .workflow_completion_retention
                                .or(service.workflow_completion_retention)
                                .unwrap_or(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
                        } else {
                            handler
                                .idempotency_retention
                                .or(service.idempotency_retention)
                                .unwrap_or(DEFAULT_IDEMPOTENCY_RETENTION)
                        };
                        let journal_retention = handler
                            .journal_retention
                            .or(service.journal_retention)
                            .unwrap_or(Duration::ZERO);

                        v1_handler_schemas.insert(
                            handler_name.clone(),
                            v1_data_model::HandlerSchemas {
                                target_meta: v1_data_model::InvocationTargetMetadata {
                                    public: handler.public.unwrap_or(service.public),
                                    completion_retention,
                                    journal_retention,
                                    target_ty: handler.target_ty,
                                    input_rules: handler.input_rules.clone(),
                                    output_rules: handler.output_rules.clone(),
                                },
                                idempotency_retention: handler.idempotency_retention,
                                workflow_completion_retention: handler
                                    .workflow_completion_retention,
                                journal_retention: handler.journal_retention,
                                inactivity_timeout: handler.inactivity_timeout,
                                abort_timeout: handler.abort_timeout,
                                enable_lazy_state: handler.enable_lazy_state,
                                documentation: handler.documentation.clone(),
                                metadata: handler.metadata.clone(),
                            },
                        );
                    }

                    v1_services.insert(
                        service_name,
                        v1_data_model::ServiceSchemas {
                            revision: current_service_revision,
                            handlers: v1_handler_schemas,
                            ty: service.ty,
                            location: v1_data_model::ServiceLocation {
                                latest_deployment: deployment_id,
                                public: service.public,
                            },
                            idempotency_retention: service.idempotency_retention,
                            workflow_completion_retention: service.workflow_completion_retention,
                            journal_retention: service.journal_retention,
                            inactivity_timeout: service.inactivity_timeout,
                            abort_timeout: service.abort_timeout,
                            enable_lazy_state: service.enable_lazy_state,
                            documentation: service.documentation.clone(),
                            metadata: service.metadata.clone(),
                        },
                    );
                } else {
                    // This should not happen unless there is a bug in the conversions
                    panic!(
                        "Constraint violation. There is no deployment for the given current service {service_name} revision {current_service_revision}. This should not happen"
                    );
                }
            }

            V1Schemas {
                services: v1_services,
                deployments: v1_deployments,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::v1_data_model::*;
        use super::*;

        use crate::Version;
        use crate::schema::deployment::{DeploymentType, ProtocolType};
        use crate::schema::invocation_target::InvocationTargetMetadata;
        use crate::storage::StorageCodec;
        use crate::time::MillisSinceEpoch;
        use googletest::prelude::*;

        #[serde_as]
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct OldSchema {
            /// This gets bumped on each update.
            pub version: Version,

            pub services: HashMap<String, ServiceSchemas>,
            #[serde_as(as = "serde_with::Seq<(_, _)>")]
            pub deployments: HashMap<DeploymentId, DeploymentSchemas>,

            #[serde_as(as = "serde_with::Seq<(_, _)>")]
            pub subscriptions: HashMap<SubscriptionId, Subscription>,
        }

        mod storage {
            use crate::flexbuffers_storage_encode_decode;

            use super::OldSchema;

            flexbuffers_storage_encode_decode!(OldSchema);
        }

        #[test]
        fn v2_to_v1_to_schema() {
            let dp_1 = DeploymentId::new();
            let dp_2 = DeploymentId::new();

            let v2_schemas = V2Schemas {
                deployments: vec![
                    Deployment {
                        id: dp_1,
                        ty: DeploymentType::Http {
                            address: "http://localhost:9080/".parse().unwrap(),
                            protocol_type: ProtocolType::BidiStream,
                            http_version: http::Version::HTTP_2,
                        },
                        delivery_options: Default::default(),
                        supported_protocol_versions: 5..=5,
                        sdk_version: None,
                        created_at: MillisSinceEpoch::now(),
                        services: HashMap::from([
                            (
                                "Greeter".to_owned(),
                                Arc::new(ServiceRevision {
                                    name: "Greeter".to_string(),
                                    ty: ServiceType::Service,
                                    documentation: None,
                                    metadata: Default::default(),
                                    revision: 1,
                                    public: true,
                                    idempotency_retention: None,
                                    workflow_completion_retention: None,
                                    journal_retention: None,
                                    inactivity_timeout: None,
                                    abort_timeout: None,
                                    enable_lazy_state: None,
                                    service_openapi_cache: Arc::new(Default::default()),
                                    handlers: HashMap::from([(
                                        "greet".to_owned(),
                                        Handler {
                                            name: "greet".to_owned(),
                                            target_ty: InvocationTargetType::Service,
                                            input_rules: Default::default(),
                                            output_rules: Default::default(),
                                            public: None,
                                            idempotency_retention: None,
                                            workflow_completion_retention: None,
                                            journal_retention: None,
                                            inactivity_timeout: None,
                                            abort_timeout: None,
                                            documentation: None,
                                            enable_lazy_state: None,
                                            metadata: Default::default(),
                                        },
                                    )]),
                                }),
                            ),
                            (
                                "AnotherGreeter".to_owned(),
                                Arc::new(ServiceRevision {
                                    name: "AnotherGreeter".to_string(),
                                    ty: ServiceType::VirtualObject,
                                    documentation: None,
                                    metadata: Default::default(),
                                    revision: 1,
                                    public: true,
                                    idempotency_retention: None,
                                    workflow_completion_retention: None,
                                    journal_retention: None,
                                    inactivity_timeout: None,
                                    abort_timeout: None,
                                    enable_lazy_state: None,
                                    service_openapi_cache: Arc::new(Default::default()),
                                    handlers: HashMap::from([
                                        (
                                            "greet".to_owned(),
                                            Handler {
                                                name: "greet".to_owned(),
                                                target_ty: InvocationTargetType::VirtualObject(
                                                    VirtualObjectHandlerType::Exclusive,
                                                ),
                                                input_rules: Default::default(),
                                                output_rules: Default::default(),
                                                public: None,
                                                idempotency_retention: None,
                                                workflow_completion_retention: None,
                                                journal_retention: None,
                                                inactivity_timeout: None,
                                                abort_timeout: None,
                                                documentation: None,
                                                enable_lazy_state: None,
                                                metadata: Default::default(),
                                            },
                                        ),
                                        (
                                            "another_greet".to_owned(),
                                            Handler {
                                                name: "another_greet".to_owned(),
                                                target_ty: InvocationTargetType::VirtualObject(
                                                    VirtualObjectHandlerType::Shared,
                                                ),
                                                input_rules: Default::default(),
                                                output_rules: Default::default(),
                                                public: None,
                                                idempotency_retention: None,
                                                workflow_completion_retention: None,
                                                journal_retention: Some(Duration::from_secs(60)),
                                                inactivity_timeout: None,
                                                abort_timeout: None,
                                                documentation: None,
                                                enable_lazy_state: None,
                                                metadata: Default::default(),
                                            },
                                        ),
                                    ]),
                                }),
                            ),
                        ]),
                    },
                    Deployment {
                        id: dp_2,
                        ty: DeploymentType::Http {
                            address: "http://localhost:9081/".parse().unwrap(),
                            protocol_type: ProtocolType::RequestResponse,
                            http_version: http::Version::HTTP_2,
                        },
                        delivery_options: Default::default(),
                        supported_protocol_versions: 5..=5,
                        sdk_version: None,
                        created_at: MillisSinceEpoch::now(),
                        services: HashMap::from([(
                            "Greeter".to_owned(),
                            Arc::new(ServiceRevision {
                                name: "Greeter".to_string(),
                                ty: ServiceType::Service,
                                documentation: None,
                                metadata: Default::default(),
                                revision: 2,
                                public: false,
                                idempotency_retention: None,
                                workflow_completion_retention: None,
                                journal_retention: None,
                                inactivity_timeout: None,
                                abort_timeout: None,
                                enable_lazy_state: None,
                                service_openapi_cache: Arc::new(Default::default()),
                                handlers: HashMap::from([(
                                    "greet".to_owned(),
                                    Handler {
                                        name: "greet".to_owned(),
                                        target_ty: InvocationTargetType::Service,
                                        input_rules: Default::default(),
                                        output_rules: Default::default(),
                                        public: None,
                                        idempotency_retention: None,
                                        workflow_completion_retention: None,
                                        journal_retention: None,
                                        inactivity_timeout: None,
                                        abort_timeout: None,
                                        documentation: None,
                                        enable_lazy_state: None,
                                        metadata: Default::default(),
                                    },
                                )]),
                            }),
                        )]),
                    },
                ],
            };

            let v1_schemas = v2_schemas.into_v1();
            let mut buf = bytes::BytesMut::default();
            StorageCodec::encode(
                &OldSchema {
                    version: Version::from(1),
                    services: v1_schemas.services,
                    deployments: v1_schemas.deployments,
                    subscriptions: Default::default(),
                },
                &mut buf,
            )
            .unwrap();
            let schema: crate::schema::Schema = StorageCodec::decode(&mut buf.freeze()).unwrap();

            assert_that!(
                schema.assert_service_handler("Greeter", "greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(false),
                    target_ty: eq(InvocationTargetType::Service),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::ZERO),
                })
            );
            assert_that!(
                schema.assert_service("Greeter"),
                pat!(ServiceMetadata {
                    public: eq(false),
                    deployment_id: eq(dp_2),
                    revision: eq(2)
                })
            );

            assert_that!(
                schema.assert_service_handler("AnotherGreeter", "greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(true),
                    target_ty: eq(InvocationTargetType::VirtualObject(
                        VirtualObjectHandlerType::Exclusive
                    )),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::ZERO),
                })
            );
            assert_that!(
                schema.assert_service_handler("AnotherGreeter", "another_greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(true),
                    target_ty: eq(InvocationTargetType::VirtualObject(
                        VirtualObjectHandlerType::Shared
                    )),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::from_secs(60)),
                })
            );
            assert_that!(
                schema.assert_service("AnotherGreeter"),
                pat!(ServiceMetadata {
                    public: eq(true),
                    deployment_id: eq(dp_1),
                    revision: eq(1)
                })
            );
        }

        #[test]
        fn old_to_new() {
            let dp_1 = DeploymentId::new();
            let dp_2 = DeploymentId::new();

            let old_schema = OldSchema {
                version: Version::from(1),
                deployments: HashMap::from([
                    (
                        dp_1,
                        DeploymentSchemas {
                            metadata: DeploymentMetadata {
                                ty: DeploymentType::Http {
                                    address: "http://localhost:9080/".parse().unwrap(),
                                    protocol_type: ProtocolType::BidiStream,
                                    http_version: http::Version::HTTP_2,
                                },
                                delivery_options: Default::default(),
                                supported_protocol_versions: 5..=5,
                                sdk_version: None,
                                created_at: MillisSinceEpoch::now(),
                            },
                            services: HashMap::from([
                                (
                                    "Greeter".to_owned(),
                                    ServiceMetadata {
                                        name: "Greeter".to_string(),
                                        ty: ServiceType::Service,
                                        documentation: None,
                                        metadata: Default::default(),
                                        deployment_id: dp_1,
                                        revision: 1,
                                        public: true,
                                        idempotency_retention: None,
                                        workflow_completion_retention: None,
                                        journal_retention: None,
                                        inactivity_timeout: None,
                                        abort_timeout: None,
                                        enable_lazy_state: None,
                                        handlers: HashMap::from([(
                                            "greet".to_owned(),
                                            HandlerMetadata {
                                                name: "".to_string(),
                                                idempotency_retention: None,
                                                workflow_completion_retention: None,
                                                journal_retention: None,
                                                inactivity_timeout: None,
                                                abort_timeout: None,
                                                documentation: None,
                                                enable_lazy_state: None,
                                                public: true,
                                                input_description: "".to_string(),
                                                output_description: "".to_string(),
                                                input_json_schema: None,
                                                metadata: Default::default(),
                                                ty: None,
                                                output_json_schema: None,
                                            },
                                        )]),
                                    },
                                ),
                                (
                                    "AnotherGreeter".to_owned(),
                                    ServiceMetadata {
                                        name: "AnotherGreeter".to_string(),
                                        ty: ServiceType::VirtualObject,
                                        documentation: None,
                                        metadata: Default::default(),
                                        deployment_id: dp_1,
                                        revision: 1,
                                        idempotency_retention: None,
                                        workflow_completion_retention: None,
                                        journal_retention: None,
                                        inactivity_timeout: None,
                                        abort_timeout: None,
                                        enable_lazy_state: None,
                                        public: true,
                                        handlers: HashMap::from([
                                            (
                                                "greet".to_owned(),
                                                HandlerMetadata {
                                                    name: "greet".to_string(),
                                                    idempotency_retention: None,
                                                    workflow_completion_retention: None,
                                                    journal_retention: None,
                                                    inactivity_timeout: None,
                                                    abort_timeout: None,
                                                    documentation: None,
                                                    enable_lazy_state: None,
                                                    public: true,
                                                    input_description: "".to_string(),
                                                    output_description: "".to_string(),
                                                    input_json_schema: None,
                                                    metadata: Default::default(),
                                                    ty: Some(HandlerMetadataType::Exclusive),
                                                    output_json_schema: None,
                                                },
                                            ),
                                            (
                                                "another_greet".to_owned(),
                                                HandlerMetadata {
                                                    name: "another_greet".to_string(),
                                                    idempotency_retention: None,
                                                    workflow_completion_retention: None,
                                                    journal_retention: Some(Duration::from_secs(
                                                        60,
                                                    )),
                                                    inactivity_timeout: None,
                                                    abort_timeout: None,
                                                    documentation: None,
                                                    enable_lazy_state: None,
                                                    public: true,
                                                    input_description: "".to_string(),
                                                    output_description: "".to_string(),
                                                    input_json_schema: None,
                                                    metadata: Default::default(),
                                                    ty: Some(HandlerMetadataType::Shared),
                                                    output_json_schema: None,
                                                },
                                            ),
                                        ]),
                                    },
                                ),
                            ]),
                        },
                    ),
                    (
                        dp_2,
                        DeploymentSchemas {
                            metadata: DeploymentMetadata {
                                ty: DeploymentType::Http {
                                    address: "http://localhost:9081/".parse().unwrap(),
                                    protocol_type: ProtocolType::RequestResponse,
                                    http_version: http::Version::HTTP_2,
                                },
                                delivery_options: Default::default(),
                                supported_protocol_versions: 5..=5,
                                sdk_version: None,
                                created_at: MillisSinceEpoch::now(),
                            },
                            services: HashMap::from([(
                                "Greeter".to_owned(),
                                ServiceMetadata {
                                    name: "Greeter".to_string(),
                                    ty: ServiceType::Service,
                                    documentation: None,
                                    metadata: Default::default(),
                                    deployment_id: dp_2,
                                    revision: 2,
                                    public: false,
                                    idempotency_retention: None,
                                    workflow_completion_retention: None,
                                    journal_retention: Some(Duration::from_millis(500)),
                                    inactivity_timeout: None,
                                    abort_timeout: None,
                                    enable_lazy_state: None,
                                    handlers: HashMap::from([(
                                        "greet".to_owned(),
                                        HandlerMetadata {
                                            name: "greet".to_owned(),
                                            input_description: "".to_string(),
                                            output_description: "".to_string(),
                                            input_json_schema: None,
                                            idempotency_retention: None,
                                            workflow_completion_retention: None,
                                            journal_retention: None,
                                            inactivity_timeout: None,
                                            abort_timeout: None,
                                            documentation: None,
                                            enable_lazy_state: None,
                                            metadata: Default::default(),
                                            ty: None,
                                            output_json_schema: None,
                                            public: true,
                                        },
                                    )]),
                                },
                            )]),
                        },
                    ),
                ]),
                services: HashMap::from([
                    (
                        "Greeter".to_owned(),
                        ServiceSchemas {
                            ty: ServiceType::Service,
                            location: ServiceLocation {
                                latest_deployment: dp_2,
                                public: false,
                            },
                            documentation: None,
                            metadata: Default::default(),
                            revision: 1,
                            idempotency_retention: None,
                            workflow_completion_retention: None,
                            journal_retention: Some(Duration::from_millis(500)),
                            inactivity_timeout: None,
                            abort_timeout: None,
                            enable_lazy_state: None,
                            handlers: HashMap::from([(
                                "greet".to_owned(),
                                HandlerSchemas {
                                    target_meta: v1_data_model::InvocationTargetMetadata {
                                        public: false,
                                        completion_retention: Default::default(),
                                        journal_retention: Duration::from_millis(500),
                                        target_ty: InvocationTargetType::Service,
                                        input_rules: Default::default(),
                                        output_rules: Default::default(),
                                    },
                                    idempotency_retention: None,
                                    workflow_completion_retention: None,
                                    journal_retention: None,
                                    inactivity_timeout: None,
                                    abort_timeout: None,
                                    documentation: None,
                                    enable_lazy_state: None,
                                    metadata: Default::default(),
                                },
                            )]),
                        },
                    ),
                    (
                        "AnotherGreeter".to_owned(),
                        ServiceSchemas {
                            ty: ServiceType::VirtualObject,
                            documentation: None,
                            metadata: Default::default(),
                            revision: 1,
                            location: ServiceLocation {
                                latest_deployment: dp_1,
                                public: true,
                            },
                            idempotency_retention: None,
                            workflow_completion_retention: None,
                            journal_retention: None,
                            inactivity_timeout: None,
                            abort_timeout: None,
                            enable_lazy_state: None,
                            handlers: HashMap::from([
                                (
                                    "greet".to_owned(),
                                    HandlerSchemas {
                                        target_meta: v1_data_model::InvocationTargetMetadata {
                                            public: true,
                                            completion_retention: Default::default(),
                                            journal_retention: Default::default(),
                                            target_ty: InvocationTargetType::VirtualObject(
                                                VirtualObjectHandlerType::Exclusive,
                                            ),
                                            input_rules: Default::default(),
                                            output_rules: Default::default(),
                                        },
                                        idempotency_retention: None,
                                        workflow_completion_retention: None,
                                        journal_retention: None,
                                        inactivity_timeout: None,
                                        abort_timeout: None,
                                        documentation: None,
                                        enable_lazy_state: None,
                                        metadata: Default::default(),
                                    },
                                ),
                                (
                                    "another_greet".to_owned(),
                                    HandlerSchemas {
                                        target_meta: v1_data_model::InvocationTargetMetadata {
                                            public: true,
                                            completion_retention: Default::default(),
                                            journal_retention: Default::default(),
                                            target_ty: InvocationTargetType::VirtualObject(
                                                VirtualObjectHandlerType::Shared,
                                            ),
                                            input_rules: Default::default(),
                                            output_rules: Default::default(),
                                        },
                                        idempotency_retention: None,
                                        workflow_completion_retention: None,
                                        journal_retention: Some(Duration::from_secs(60)),
                                        inactivity_timeout: None,
                                        abort_timeout: None,
                                        documentation: None,
                                        enable_lazy_state: None,
                                        metadata: Default::default(),
                                    },
                                ),
                            ]),
                        },
                    ),
                ]),
                subscriptions: Default::default(),
            };
            let mut buf = bytes::BytesMut::default();
            StorageCodec::encode(&old_schema, &mut buf).unwrap();
            let schema: crate::schema::Schema = StorageCodec::decode(&mut buf).unwrap();

            assert_that!(
                schema.assert_service_handler("Greeter", "greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(false),
                    target_ty: eq(InvocationTargetType::Service),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::from_millis(500)),
                })
            );
            assert_that!(
                schema.assert_service("Greeter"),
                pat!(ServiceMetadata {
                    public: eq(false),
                    deployment_id: eq(dp_2),
                    revision: eq(2),
                    journal_retention: some(eq(Duration::from_millis(500))),
                })
            );

            assert_that!(
                schema.assert_service_handler("AnotherGreeter", "greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(true),
                    target_ty: eq(InvocationTargetType::VirtualObject(
                        VirtualObjectHandlerType::Exclusive
                    )),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::ZERO),
                })
            );
            assert_that!(
                schema.assert_service_handler("AnotherGreeter", "another_greet"),
                pat!(InvocationTargetMetadata {
                    public: eq(true),
                    target_ty: eq(InvocationTargetType::VirtualObject(
                        VirtualObjectHandlerType::Shared
                    )),
                    completion_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                    journal_retention: eq(Duration::from_secs(60)),
                })
            );
            assert_that!(
                schema.assert_service("AnotherGreeter"),
                pat!(ServiceMetadata {
                    public: eq(true),
                    deployment_id: eq(dp_1),
                    revision: eq(1)
                })
            );
        }
    }
}
