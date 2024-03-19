// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use bytes::Bytes;
use http::Uri;
use restate_schema_api::component::{ComponentMetadata, ComponentType, HandlerMetadata};
use restate_schema_api::deployment::DeploymentMetadata;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_service_protocol::discovery::schema;
use restate_types::identifiers::{ComponentRevision, DeploymentId, SubscriptionId};
use schemas_impl::{HandlerSchemas, SchemasInner};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

mod component;
mod deployment;
mod error;
mod schemas_impl;
mod subscriptions;

pub use error::*;

// --- Update commands data structure

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DiscoveredHandlerMetadata {
    name: String,
    input_schema: Option<Bytes>,
    output_schema: Option<Bytes>,
}

impl DiscoveredHandlerMetadata {
    pub fn as_handler_metadata(&self) -> HandlerMetadata {
        HandlerMetadata {
            name: self.name.clone(),
            input_description: self
                .clone()
                .input_schema
                .map(HandlerSchemas::schema_to_description),
            output_description: self
                .clone()
                .output_schema
                .map(HandlerSchemas::schema_to_description),
        }
    }
}

/// Insert (or replace) component
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertComponentUpdateCommand {
    pub name: String,
    pub revision: ComponentRevision,
    pub ty: ComponentType,
    pub deployment_id: DeploymentId,
    pub handlers: Vec<DiscoveredHandlerMetadata>,
}

impl InsertComponentUpdateCommand {
    pub fn as_component_metadata(&self) -> ComponentMetadata {
        ComponentMetadata {
            name: self.name.clone(),
            handlers: self
                .handlers
                .iter()
                .map(DiscoveredHandlerMetadata::as_handler_metadata)
                .collect(),
            ty: self.ty,
            deployment_id: self.deployment_id,
            revision: self.revision,
            public: true,
        }
    }
}

/// Represents an update command to update the [`Schemas`] object. See [`Schemas::apply_updates`] for more info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemasUpdateCommand {
    /// Insert (or replace) deployment
    InsertDeployment {
        deployment_id: DeploymentId,
        metadata: DeploymentMetadata,
    },
    InsertComponent(InsertComponentUpdateCommand),
    RemoveDeployment {
        deployment_id: DeploymentId,
    },
    RemoveComponent {
        name: String,
        revision: ComponentRevision,
    },
    ModifyComponent {
        name: String,
        public: bool,
    },
    AddSubscription(Subscription),
    RemoveSubscription(SubscriptionId),
}

/// The schema registry
#[derive(Debug, Default, Clone)]
pub struct Schemas(Arc<ArcSwap<SchemasInner>>);

impl Schemas {
    /// Compute the commands to update the schema registry with a new deployment.
    /// This method doesn't execute any in-memory update of the registry.
    /// To update the registry, compute the commands and apply them with [`Self::apply_updates`].
    ///
    /// If `allow_override` is true, this method compares the deployment registered services with the given `components`,
    /// and generates remove commands for services no longer available at that deployment.
    ///
    /// If `deployment_id` is set, it's assumed that:
    ///   - This ID should be used if it's a new deployment
    ///   - or if conflicting/existing deployment exists, it must match.
    /// Otherwise, a new deployment id is generated for this deployment, or the existing
    /// deployment_id will be reused.
    ///
    /// IMPORTANT: It is not safe to consecutively call this method several times and apply the updates all-together with a single [`Self::apply_updates`],
    /// as one change set might depend on the previously computed change set.
    pub fn compute_new_deployment(
        &self,
        deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        components: Vec<schema::Component>,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, Error> {
        let debug_id = deployment_id
            .as_ref()
            .map(|d| d.to_string())
            .unwrap_or_else(|| deployment_metadata.address_display().to_string());
        self.0
            .load()
            .compute_new_deployment(deployment_id, deployment_metadata, components, force)
            .map_err(|e| Error::new("add new deployment", debug_id, e))
    }

    pub fn compute_modify_component(
        &self,
        component_name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, Error> {
        self.0
            .load()
            .compute_modify_component_updates(component_name.clone(), public)
            .map_err(|e| Error::new("modify component", component_name, e))
    }

    pub fn compute_remove_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<Vec<SchemasUpdateCommand>, Error> {
        self.0
            .load()
            .compute_remove_deployment(deployment_id)
            .map_err(|e| Error::new("remove deployment", deployment_id, e))
    }

    // Returns the [`Subscription`] id together with the update command
    pub fn compute_add_subscription<V: SubscriptionValidator>(
        &self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: &V,
    ) -> Result<(Subscription, SchemasUpdateCommand), Error> {
        let debug_id = id
            .as_ref()
            .map(|d| d.to_string())
            .unwrap_or_else(|| format!("{} to {}", source, sink));
        self.0
            .load()
            .compute_add_subscription(id, source, sink, metadata, validator)
            .map_err(|e| Error::new("add subscription", debug_id, e))
    }

    pub fn compute_remove_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<SchemasUpdateCommand, Error> {
        self.0
            .load()
            .compute_remove_subscription(id)
            .map_err(|e| Error::new("remove subscription", id, e))
    }

    /// Apply the updates to the schema registry.
    /// This method will update the internal pointer to the in-memory schema registry,
    /// propagating the changes to every component consuming it.
    ///
    /// IMPORTANT: This method is not thread safe! This method should be called only by a single thread.
    pub fn apply_updates(
        &self,
        updates: impl IntoIterator<Item = SchemasUpdateCommand>,
    ) -> Result<(), Error> {
        let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
        schemas_inner.apply_updates(updates);
        self.0.store(Arc::new(schemas_inner));

        Ok(())
    }

    /// Overwrites the existing schema registry with the provided schema updates
    /// This method will update the internal pointer to the in-memory schema registry,
    /// propagating the changes to every component consuming it.
    ///
    /// IMPORTANT: This method is not thread safe! This method should be called only by a single thread.
    pub fn overwrite(
        &self,
        updates: impl IntoIterator<Item = SchemasUpdateCommand>,
    ) -> Result<(), Error> {
        let mut schemas_inner = SchemasInner::default();
        schemas_inner.apply_updates(updates);
        self.0.store(Arc::new(schemas_inner));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_schema_api::component::ComponentMetadataResolver;
    use restate_test_util::assert_eq;

    impl Schemas {
        #[track_caller]
        pub(crate) fn assert_component_handler(&self, component_name: &str, handler_name: &str) {
            assert!(self
                .resolve_latest_component_handler(component_name, handler_name)
                .is_some());
        }

        #[track_caller]
        pub(crate) fn assert_component_revision(
            &self,
            component_name: &str,
            revision: ComponentRevision,
        ) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .revision,
                revision
            );
        }

        #[track_caller]
        pub(crate) fn assert_component_deployment(
            &self,
            component_name: &str,
            deployment_id: DeploymentId,
        ) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .deployment_id,
                deployment_id
            );
        }

        #[track_caller]
        pub(crate) fn assert_component(&self, component_name: &str) -> ComponentMetadata {
            self.resolve_latest_component(component_name).unwrap()
        }
    }
}
