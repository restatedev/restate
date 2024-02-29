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

use crate::service::map_to_service_metadata;
use anyhow::anyhow;
use prost_reflect::{DescriptorPool, Kind, MethodDescriptor, ServiceDescriptor};
use proto_symbol::ProtoSymbols;
use restate_schema_api::service::InstanceType;
use restate_schema_api::subscription::{
    EventReceiverServiceInstanceType, FieldRemapType, InputEventRemap, Sink, Source,
};
use restate_types::identifiers::{ComponentRevision, DeploymentId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tracing::{debug, info, warn};

mod component;
pub(crate) mod deployment;
mod service;
mod subscription;

impl Schemas {
    pub(crate) fn use_service_schema<F, R>(&self, service_name: impl AsRef<str>, f: F) -> Option<R>
    where
        F: FnOnce(&ServiceSchemas) -> R,
    {
        let guard = self.0.load();
        guard.services.get(service_name.as_ref()).map(f)
    }

    pub(crate) fn use_component_schema<F, R>(
        &self,
        component_name: impl AsRef<str>,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&ComponentSchemas) -> R,
    {
        let guard = self.0.load();
        guard.components.get(component_name.as_ref()).map(f)
    }
}

/// This struct contains the actual data held by Schemas.
#[derive(Debug, Clone)]
pub(crate) struct SchemasInner {
    pub(crate) components: HashMap<String, ComponentSchemas>,

    pub(crate) services: HashMap<String, ServiceSchemas>,
    pub(crate) deployments: HashMap<DeploymentId, DeploymentSchemas>,
    pub(crate) subscriptions: HashMap<SubscriptionId, Subscription>,
    pub(crate) proto_symbols: ProtoSymbols,
}

impl SchemasInner {
    pub fn apply_updates(
        &mut self,
        updates: impl IntoIterator<Item = SchemasUpdateCommand>,
    ) -> Result<(), SchemasUpdateError> {
        for cmd in updates {
            match cmd {
                SchemasUpdateCommand::OldInsertDeployment {
                    deployment_id,
                    metadata,
                    services,
                    descriptor_pool,
                } => {
                    self.apply_old_insert_deployment(
                        deployment_id,
                        metadata,
                        services,
                        descriptor_pool,
                    )?;
                }
                SchemasUpdateCommand::RemoveDeployment { deployment_id } => {
                    self.apply_remove_deployment(deployment_id)?;
                }
                SchemasUpdateCommand::RemoveService { name, revision } => {
                    self.apply_remove_service(name, revision)?;
                }
                SchemasUpdateCommand::ModifyService { name, public } => {
                    self.apply_modify_service(name, public)?;
                }
                SchemasUpdateCommand::AddSubscription(sub) => {
                    self.apply_add_subscription(sub)?;
                }
                SchemasUpdateCommand::RemoveSubscription(sub_id) => {
                    self.apply_remove_subscription(sub_id)?;
                }
                SchemasUpdateCommand::InsertDeployment {
                    metadata,
                    deployment_id,
                } => {
                    self.apply_insert_deployment(deployment_id, metadata)?;
                }
                SchemasUpdateCommand::InsertComponent(InsertComponentUpdateCommand {
                    name,
                    revision,
                    ty,
                    deployment_id,
                    handlers,
                }) => {
                    self.apply_insert_component(name, revision, ty, deployment_id, handlers)?;
                }
                SchemasUpdateCommand::RemoveComponent { name, revision } => {
                    self.apply_remove_component(name, revision)?;
                }
                SchemasUpdateCommand::ModifyComponent { name, public } => {
                    self.apply_modify_component(name, public)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HandlerSchemas {
    input_schema: Option<Bytes>,
    output_schema: Option<Bytes>,
}

impl HandlerSchemas {
    pub(crate) fn schema_to_description(_schema: Bytes) -> String {
        // TODO to implement
        "any".to_string()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ComponentSchemas {
    pub(crate) revision: ComponentRevision,
    pub(crate) handlers: HashMap<String, HandlerSchemas>,
    pub(crate) ty: ComponentType,
    pub(crate) location: ServiceLocation,
}

impl ComponentSchemas {
    pub(crate) fn compute_handlers(
        handlers: Vec<DiscoveredHandlerMetadata>,
    ) -> HashMap<String, HandlerSchemas> {
        handlers
            .into_iter()
            .map(|m| {
                (
                    m.name,
                    HandlerSchemas {
                        input_schema: m.input_schema,
                        output_schema: m.output_schema,
                    },
                )
            })
            .collect()
    }

    pub(crate) fn as_component_metadata(&self, name: String) -> Option<ComponentMetadata> {
        match &self.location {
            ServiceLocation::BuiltIn { .. } => None,
            ServiceLocation::Deployment {
                latest_deployment,
                public,
            } => Some(ComponentMetadata {
                name,
                handlers: self
                    .handlers
                    .iter()
                    .map(|(h_name, h_schemas)| HandlerMetadata {
                        name: h_name.clone(),
                        input_description: h_schemas
                            .input_schema
                            .clone()
                            .map(HandlerSchemas::schema_to_description),
                        output_description: h_schemas
                            .output_schema
                            .clone()
                            .map(HandlerSchemas::schema_to_description),
                    })
                    .collect(),
                ty: self.ty,
                deployment_id: *latest_deployment,
                revision: self.revision,
                public: *public,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MethodSchemas {
    descriptor: MethodDescriptor,
    input_fields_annotations: HashMap<FieldAnnotation, u32>,
}

impl MethodSchemas {
    pub(crate) fn new(
        descriptor: MethodDescriptor,
        input_fields_annotations: HashMap<FieldAnnotation, u32>,
    ) -> Self {
        Self {
            descriptor,
            input_fields_annotations,
        }
    }

    pub(crate) fn descriptor(&self) -> &MethodDescriptor {
        &self.descriptor
    }

    pub(crate) fn input_field_annotated(&self, annotation: FieldAnnotation) -> Option<u32> {
        self.input_fields_annotations.get(&annotation).cloned()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ServiceSchemas {
    pub(crate) revision: ComponentRevision,
    pub(crate) methods: HashMap<String, MethodSchemas>,
    pub(crate) instance_type: InstanceTypeMetadata,
    pub(crate) location: ServiceLocation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum InstanceTypeMetadata {
    Keyed {
        key_structure: KeyStructure,
        service_methods_key_field_root_number: HashMap<String, u32>,
    },
    Unkeyed,
    Singleton,
    Unsupported,
    #[allow(dead_code)]
    Custom {
        // If method is missing, it means there's no key, hence a random key will be generated
        structure_per_method: HashMap<String, (u32, KeyStructure)>,
    },
}

impl InstanceTypeMetadata {
    pub(crate) fn keyed_with_scalar_key<'a>(
        methods: impl IntoIterator<Item = (&'a str, u32)>,
    ) -> Self {
        Self::Keyed {
            key_structure: KeyStructure::Scalar,
            service_methods_key_field_root_number: methods
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    pub(crate) fn from_discovered_metadata(
        instance_type: DiscoveredInstanceType,
        methods: &HashMap<String, DiscoveredMethodMetadata>,
    ) -> Self {
        match instance_type.clone() {
            DiscoveredInstanceType::Keyed(key_structure) => InstanceTypeMetadata::Keyed {
                key_structure,
                service_methods_key_field_root_number: methods
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            *v.input_fields_annotations
                                .get(&FieldAnnotation::Key)
                                .expect("At this point there must be a field annotated with key"),
                        )
                    })
                    .collect(),
            },
            DiscoveredInstanceType::Unkeyed => InstanceTypeMetadata::Unkeyed,
            DiscoveredInstanceType::Singleton => InstanceTypeMetadata::Singleton,
        }
    }
}

impl TryFrom<&InstanceTypeMetadata> for InstanceType {
    type Error = ();

    fn try_from(value: &InstanceTypeMetadata) -> Result<Self, Self::Error> {
        match value {
            InstanceTypeMetadata::Keyed { .. } => Ok(InstanceType::Keyed),
            InstanceTypeMetadata::Unkeyed => Ok(InstanceType::Unkeyed),
            InstanceTypeMetadata::Singleton => Ok(InstanceType::Singleton),
            _ => Err(()),
        }
    }
}

impl ServiceSchemas {
    pub(crate) fn new(
        revision: ComponentRevision,
        methods: HashMap<String, MethodSchemas>,
        instance_type: InstanceTypeMetadata,
        latest_deployment: DeploymentId,
    ) -> Self {
        Self {
            revision,
            methods,
            instance_type,
            location: ServiceLocation::Deployment {
                latest_deployment,
                public: true,
            },
        }
    }

    fn new_built_in(
        svc_desc: &ServiceDescriptor,
        instance_type: InstanceTypeMetadata,
        ingress_available: bool,
    ) -> Self {
        Self {
            revision: 0,
            methods: svc_desc
                .methods()
                .map(|descriptor| {
                    (
                        descriptor.name().to_string(),
                        MethodSchemas {
                            descriptor,
                            input_fields_annotations: Default::default(),
                        },
                    )
                })
                .collect(),
            instance_type,
            location: ServiceLocation::BuiltIn { ingress_available },
        }
    }

    pub(crate) fn compute_service_methods(
        svc_desc: &ServiceDescriptor,
        method_meta: &HashMap<String, DiscoveredMethodMetadata>,
    ) -> HashMap<String, MethodSchemas> {
        svc_desc
            .methods()
            .flat_map(|descriptor| {
                let method_name = descriptor.name().to_string();
                method_meta.get(&method_name).map(|metadata| {
                    (
                        method_name,
                        MethodSchemas::new(descriptor, metadata.input_fields_annotations.clone()),
                    )
                })
            })
            .collect()
    }

    fn service_descriptor(&self) -> &ServiceDescriptor {
        self.methods
            .values()
            .next()
            .expect("A service should have at least one method")
            .descriptor
            .parent_service()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ServiceLocation {
    BuiltIn {
        // Available at the ingress
        ingress_available: bool,
    },
    Deployment {
        // None if this is a built-in service
        latest_deployment: DeploymentId,
        public: bool,
    },
}

impl ServiceLocation {
    pub(crate) fn is_ingress_available(&self) -> bool {
        match self {
            ServiceLocation::BuiltIn {
                ingress_available, ..
            } => *ingress_available,
            ServiceLocation::Deployment { public, .. } => *public,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeploymentSchemas {
    pub(crate) metadata: DeploymentMetadata,

    // We need to store ServiceSchemas and DescriptorPool here only for queries
    // We could optimize the memory impact of this by reading these info from disk
    pub(crate) services: Vec<ServiceMetadata>,
    pub(crate) descriptor_pool: DescriptorPool,
    pub(crate) components: Vec<ComponentMetadata>,
}

impl Default for SchemasInner {
    fn default() -> Self {
        const INGRESS_DEPLOYMENT_ID: DeploymentId = DeploymentId::from_parts(0, 0);

        let mut inner = Self {
            components: Default::default(),
            services: Default::default(),
            deployments: Default::default(),
            subscriptions: Default::default(),
            proto_symbols: Default::default(),
        };

        enum Visibility {
            Public,
            IngressAvailable,
            Internal,
        }

        // Register built-in services
        let mut register_built_in = |svc_name: &'static str,
                                     service_instance_type: InstanceTypeMetadata,
                                     visibility: Visibility| {
            inner.services.insert(
                svc_name.to_string(),
                ServiceSchemas::new_built_in(
                    &restate_pb::get_service(svc_name),
                    service_instance_type,
                    matches!(
                        visibility,
                        Visibility::Public | Visibility::IngressAvailable
                    ),
                ),
            );
            if matches!(visibility, Visibility::Public) {
                inner
                    .proto_symbols
                    .add_service(&INGRESS_DEPLOYMENT_ID, &restate_pb::get_service(svc_name))
            }
        };
        register_built_in(
            restate_pb::REFLECTION_SERVICE_NAME,
            InstanceTypeMetadata::Unsupported,
            Visibility::Public,
        );
        register_built_in(
            restate_pb::REFLECTION_SERVICE_NAME_V1ALPHA,
            InstanceTypeMetadata::Unsupported,
            Visibility::Public,
        );
        register_built_in(
            restate_pb::HEALTH_SERVICE_NAME,
            InstanceTypeMetadata::Unsupported,
            Visibility::Public,
        );
        register_built_in(
            restate_pb::INGRESS_SERVICE_NAME,
            InstanceTypeMetadata::Unkeyed,
            Visibility::Public,
        );
        register_built_in(
            restate_pb::AWAKEABLES_SERVICE_NAME,
            InstanceTypeMetadata::Unkeyed,
            Visibility::Public,
        );
        register_built_in(
            restate_pb::PROXY_SERVICE_NAME,
            // Key must be manually provided when invoking the proxy service
            InstanceTypeMetadata::Unsupported,
            Visibility::Internal,
        );
        register_built_in(
            restate_pb::REMOTE_CONTEXT_SERVICE_NAME,
            InstanceTypeMetadata::keyed_with_scalar_key([
                ("Start", 1),
                ("Send", 1),
                ("Recv", 1),
                ("GetResult", 1),
                ("Cleanup", 1),
            ]),
            Visibility::IngressAvailable,
        );
        register_built_in(
            restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME,
            InstanceTypeMetadata::Unsupported,
            Visibility::Internal,
        );

        inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_list_service_should_not_contain_remote_context() {
        let schemas = Schemas::default();
        assert!(
            !restate_schema_api::proto_symbol::ProtoSymbolResolver::list_services(&schemas)
                .contains(&"dev.restate.internal.RemoteContext".to_string())
        );
    }
}
