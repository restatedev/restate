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

use anyhow::anyhow;
use restate_schema_api::subscription::{Sink, Source};
use restate_types::identifiers::{ComponentRevision, DeploymentId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tracing::{debug, info, warn};

mod component;
pub(crate) mod deployment;
mod subscription;

impl Schemas {
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

    pub(crate) deployments: HashMap<DeploymentId, DeploymentSchemas>,
    pub(crate) subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl SchemasInner {
    pub fn apply_updates(
        &mut self,
        updates: impl IntoIterator<Item = SchemasUpdateCommand>,
    ) -> Result<(), SchemasUpdateError> {
        for cmd in updates {
            match cmd {
                SchemasUpdateCommand::RemoveDeployment { deployment_id } => {
                    self.apply_remove_deployment(deployment_id)?;
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
    pub(crate) location: ComponentLocation,
}

impl ComponentSchemas {
    fn new_built_in<'a>(
        ty: ComponentType,
        ingress_available: bool,
        handlers: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        Self {
            revision: 0,
            handlers: Self::compute_handlers(
                handlers
                    .into_iter()
                    .map(|s| DiscoveredHandlerMetadata {
                        name: s.to_owned(),
                        input_schema: None,
                        output_schema: None,
                    })
                    .collect(),
            ),
            location: ComponentLocation::BuiltIn { ingress_available },
            ty,
        }
    }

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
            ComponentLocation::BuiltIn { .. } => None,
            ComponentLocation::Deployment {
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
pub(crate) enum ComponentLocation {
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

impl ComponentLocation {
    pub(crate) fn is_ingress_available(&self) -> bool {
        match self {
            ComponentLocation::BuiltIn {
                ingress_available, ..
            } => *ingress_available,
            ComponentLocation::Deployment { public, .. } => *public,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeploymentSchemas {
    pub(crate) metadata: DeploymentMetadata,

    // We need to store ComponentMetadata here only for queries
    // We could optimize the memory impact of this by reading these info from disk
    pub(crate) components: Vec<ComponentMetadata>,
}

impl Default for SchemasInner {
    fn default() -> Self {
        let mut inner = Self {
            components: Default::default(),
            deployments: Default::default(),
            subscriptions: Default::default(),
        };

        #[allow(dead_code)]
        enum Visibility {
            Public,
            IngressAvailable,
            Internal,
        }

        // Register built-in services
        let mut register_built_in = |component_name: &'static str,
                                     ty: ComponentType,
                                     visibility: Visibility,
                                     handlers: Vec<&str>| {
            inner.components.insert(
                component_name.to_string(),
                ComponentSchemas::new_built_in(
                    ty,
                    matches!(
                        visibility,
                        Visibility::Public | Visibility::IngressAvailable
                    ),
                    handlers,
                ),
            );
        };
        register_built_in(
            restate_pb::PROXY_SERVICE_NAME,
            ComponentType::Service,
            Visibility::Internal,
            vec![restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME],
        );
        register_built_in(
            restate_pb::REMOTE_CONTEXT_SERVICE_NAME,
            ComponentType::VirtualObject,
            Visibility::Internal,
            vec!["Start", "Send", "Recv", "GetResult", "Cleanup"],
        );
        register_built_in(
            restate_pb::PROXY_SERVICE_NAME,
            ComponentType::Service,
            Visibility::Internal,
            vec![restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME],
        );
        register_built_in(
            restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME,
            ComponentType::Service,
            Visibility::Internal,
            vec![
                restate_pb::IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME,
                restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_TIMER_METHOD_NAME,
                restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_RESPONSE_METHOD_NAME,
            ],
        );

        inner
    }
}
