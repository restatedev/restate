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

use restate_schema_api::invocation_target::InvocationTargetMetadata;
use restate_schema_api::subscription::{Sink, Source};
use restate_types::identifiers::{ComponentRevision, DeploymentId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tracing::{debug, info, warn};

pub(crate) mod component;
pub(crate) mod deployment;
pub(crate) mod subscription;

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
#[derive(Debug, Clone, Default)]
pub(crate) struct SchemasInner {
    pub(crate) components: HashMap<String, ComponentSchemas>,

    pub(crate) deployments: HashMap<DeploymentId, DeploymentSchemas>,
    pub(crate) subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl SchemasInner {
    pub fn apply_updates(&mut self, updates: impl IntoIterator<Item = SchemasUpdateCommand>) {
        for cmd in updates {
            match cmd {
                SchemasUpdateCommand::RemoveDeployment { deployment_id } => {
                    self.apply_remove_deployment(deployment_id);
                }
                SchemasUpdateCommand::AddSubscription(sub) => {
                    self.apply_add_subscription(sub);
                }
                SchemasUpdateCommand::RemoveSubscription(sub_id) => {
                    self.apply_remove_subscription(sub_id);
                }
                SchemasUpdateCommand::InsertDeployment {
                    metadata,
                    deployment_id,
                } => {
                    self.apply_insert_deployment(deployment_id, metadata);
                }
                SchemasUpdateCommand::InsertComponent(InsertComponentUpdateCommand {
                    name,
                    revision,
                    ty,
                    deployment_id,
                    handlers,
                }) => {
                    self.apply_insert_component(name, revision, ty, deployment_id, handlers);
                }
                SchemasUpdateCommand::RemoveComponent { name, revision } => {
                    self.apply_remove_component(name, revision);
                }
                SchemasUpdateCommand::ModifyComponent { name, public } => {
                    self.apply_modify_component(name, public);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HandlerSchemas {
    pub(crate) target_meta: InvocationTargetMetadata,
}

#[derive(Debug, Clone)]
pub(crate) struct ComponentSchemas {
    pub(crate) revision: ComponentRevision,
    pub(crate) handlers: HashMap<String, HandlerSchemas>,
    pub(crate) ty: ComponentType,
    pub(crate) location: ComponentLocation,
}

impl ComponentSchemas {
    pub(crate) fn compute_handlers(
        component_ty: ComponentType,
        handlers: Vec<DiscoveredHandlerMetadata>,
    ) -> HashMap<String, HandlerSchemas> {
        handlers
            .into_iter()
            .map(|handler| {
                (
                    handler.name,
                    HandlerSchemas {
                        target_meta: InvocationTargetMetadata {
                            public: true,
                            component_ty,
                            handler_ty: handler.ty,
                            input_rules: handler.input,
                            output_rules: handler.output,
                        },
                    },
                )
            })
            .collect()
    }

    pub(crate) fn as_component_metadata(&self, name: String) -> ComponentMetadata {
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
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ComponentLocation {
    pub(crate) latest_deployment: DeploymentId,
    pub(crate) public: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DeploymentSchemas {
    pub(crate) metadata: DeploymentMetadata,

    // We need to store ComponentMetadata here only for queries
    // We could optimize the memory impact of this by reading these info from disk
    pub(crate) components: Vec<ComponentMetadata>,
}
