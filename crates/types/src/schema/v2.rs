// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers;
use crate::identifiers::DeploymentId;
use crate::invocation::{InvocationTargetType, ServiceType};
use crate::schema::deployment::{DeliveryOptions, DeploymentType};
use crate::schema::invocation_target::{InputRules, OutputRules};
use crate::schema::openapi::ServiceOpenAPI;
use crate::time::MillisSinceEpoch;
use arc_swap::ArcSwapOption;
use restate_serde_util::MapAsVecItem;
use serde_with::serde_as;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Deployment {
    pub id: DeploymentId,
    pub ty: DeploymentType,
    pub delivery_options: DeliveryOptions,
    pub supported_protocol_versions: RangeInclusive<i32>,
    /// Declared SDK during discovery
    pub sdk_version: Option<String>,
    pub created_at: MillisSinceEpoch,

    #[serde_as(as = "restate_serde_util::MapAsVec")]
    pub services: HashMap<String, ServiceRevision>,
}

impl MapAsVecItem for Deployment {
    type Key = DeploymentId;

    fn key(&self) -> Self::Key {
        self.id
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceRevision {
    /// # Name
    ///
    /// Fully qualified name of the service
    pub name: String,

    #[serde_as(as = "restate_serde_util::MapAsVec")]
    pub handlers: HashMap<String, Handler>,

    pub ty: ServiceType,

    /// # Documentation
    ///
    /// Documentation of the service, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// # Metadata
    ///
    /// Additional service metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// # Revision
    ///
    /// Latest revision of the service.
    pub revision: identifiers::ServiceRevision,

    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    pub public: bool,

    /// # Idempotency retention
    ///
    /// The retention duration of idempotent requests for this service.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub idempotency_retention: Option<Duration>,

    /// # Workflow completion retention
    ///
    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub workflow_completion_retention: Option<Duration>,

    /// # Journal retention
    ///
    /// The journal retention. When set, this applies to all requests to all handlers of this service.
    ///
    /// In case the request has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the request targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub journal_retention: Option<Duration>,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// This overrides the default inactivity timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub inactivity_timeout: Option<Duration>,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// This overrides the default abort timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub abort_timeout: Option<Duration>,

    /// # Enable lazy state
    ///
    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub enable_lazy_state: Option<bool>,

    /// This is a cache for the computed value of ServiceOpenAPI
    #[serde(skip)]
    pub service_openapi_cache: Arc<ArcSwapOption<ServiceOpenAPI>>,
}

impl MapAsVecItem for ServiceRevision {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Handler {
    pub name: String,
    pub target_ty: InvocationTargetType,
    pub input_rules: InputRules,
    pub output_rules: OutputRules,
    /// Override of public for this handler. If unspecified, the `public` from `service` is used instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_completion_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactivity_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub enable_lazy_state: Option<bool>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

impl MapAsVecItem for Handler {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

pub mod conversions {
    use super::{Deployment, Handler, ServiceRevision};
    use crate::identifiers::DeploymentId;
    use crate::invocation::{
        InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
    };
    use crate::schema::deployment::{DeploymentMetadata, DeploymentSchemas};
    use crate::schema::invocation_target::{
        DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION,
        InvocationTargetMetadata,
    };
    use crate::schema::service::{
        HandlerMetadata, HandlerMetadataType, HandlerSchemas, ServiceLocation, ServiceMetadata,
        ServiceSchemas,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    pub struct V1Schemas {
        pub services: HashMap<String, ServiceSchemas>,
        pub deployments: HashMap<DeploymentId, DeploymentSchemas>,
    }

    impl V1Schemas {
        #[allow(dead_code)]
        pub fn into_v2(self) -> V2Schemas {
            let V1Schemas {
                services,
                deployments,
            } = self;

            let mut v2_deployments = HashMap::with_capacity(deployments.len());
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

                    v2_services.insert(service_name, service_revision);
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
                v2_deployments.insert(deployment_id, v2_deployment);
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
        pub deployments: HashMap<DeploymentId, Deployment>,
    }

    impl V2Schemas {
        pub fn into_v1(self) -> V1Schemas {
            let V2Schemas { deployments } = self;

            let mut v1_deployments = HashMap::with_capacity(deployments.len());
            let mut current_service_revision_to_deployment = HashMap::new();

            for (deployment_id, deployment) in &deployments {
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
                            deployment_id: *deployment_id,
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
                                *dep_id = *deployment_id;
                            }
                        })
                        .or_insert((service.revision, *deployment_id));
                }
                v1_deployments.insert(
                    *deployment_id,
                    DeploymentSchemas {
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
                        // TODO enable this in Restate 1.4
                        // let journal_retention =
                        //     handler.journal_retention.or(service_level_journal_retention).unwrap_or(
                        //      if target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) {
                        //         DEFAULT_WORKFLOW_COMPLETION_RETENTION
                        //      } else {
                        //         Duration::ZERO
                        //      }
                        //     );
                        let journal_retention = handler
                            .journal_retention
                            .or(service.journal_retention)
                            .unwrap_or(Duration::ZERO);

                        v1_handler_schemas.insert(
                            handler_name.clone(),
                            HandlerSchemas {
                                target_meta: InvocationTargetMetadata {
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
                        ServiceSchemas {
                            revision: current_service_revision,
                            handlers: v1_handler_schemas,
                            ty: service.ty,
                            location: ServiceLocation {
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
                            service_openapi_cache: Arc::new(Default::default()),
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
        use super::*;

        use crate::Version;
        use crate::schema::Schema;
        use crate::schema::deployment::{DeploymentType, ProtocolType};
        use crate::time::MillisSinceEpoch;
        use googletest::prelude::*;

        #[test]
        fn v2_to_v1() {
            let dp_1 = DeploymentId::new();
            let dp_2 = DeploymentId::new();

            let v2_schemas = V2Schemas {
                deployments: HashMap::from([
                    (
                        dp_1,
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
                                    ServiceRevision {
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
                                    },
                                ),
                                (
                                    "AnotherGreeter".to_owned(),
                                    ServiceRevision {
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
                                                    journal_retention: Some(Duration::from_secs(
                                                        60,
                                                    )),
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
                        },
                    ),
                    (
                        dp_2,
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
                                ServiceRevision {
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
                                },
                            )]),
                        },
                    ),
                ]),
            };

            let v1_schemas = v2_schemas.into_v1();
            let schema = Schema {
                version: Version::from(1),
                services: v1_schemas.services,
                deployments: v1_schemas.deployments,
                subscriptions: Default::default(),
            };

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
    }
}
