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
use http::Uri;
use prost_reflect::{DescriptorPool, ServiceDescriptor};
use restate_schema_api::deployment::DeploymentMetadata;
use restate_schema_api::service::ServiceMetadata;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_types::identifiers::{DeploymentId, ServiceRevision, SubscriptionId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

mod deployment;
mod json;
mod json_key_conversion;
mod key_expansion;
mod key_extraction;
mod proto_symbol;
mod schemas_impl;
mod service;
mod subscriptions;

use self::schemas_impl::deployment::{BadDescriptorError, IncompatibleServiceChangeError};
use self::schemas_impl::InstanceTypeMetadata;
use self::schemas_impl::ServiceSchemas;

// --- Update commands data structure

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldAnnotation {
    Key,
    EventPayload,
    EventMetadata,
}

/// This structure provides the directives to the key parser to parse nested messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyStructure {
    Scalar,
    Nested(std::collections::BTreeMap<u32, KeyStructure>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoveredInstanceType {
    Keyed(KeyStructure),
    Unkeyed,
    Singleton,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DiscoveredMethodMetadata {
    input_fields_annotations: HashMap<FieldAnnotation, u32>,
}

/// Insert (or replace) service
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertServiceUpdateCommand {
    pub name: String,
    pub revision: ServiceRevision,
    pub instance_type: DiscoveredInstanceType,
    pub methods: HashMap<String, DiscoveredMethodMetadata>,
}

impl InsertServiceUpdateCommand {
    pub fn as_service_metadata(
        &self,
        latest_deployment_id: DeploymentId,
        service_descriptor: &ServiceDescriptor,
    ) -> Option<ServiceMetadata> {
        let schemas = ServiceSchemas::new(
            self.revision,
            ServiceSchemas::compute_service_methods(service_descriptor, &self.methods),
            InstanceTypeMetadata::from_discovered_metadata(
                self.instance_type.clone(),
                &self.methods,
            ),
            latest_deployment_id,
        );
        service::map_to_service_metadata(&self.name, &schemas)
    }
}

/// Represents an update command to update the [`Schemas`] object. See [`Schemas::apply_updates`] for more info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemasUpdateCommand {
    /// Insert (or replace) deployment
    InsertDeployment {
        deployment_id: DeploymentId,
        metadata: DeploymentMetadata,
        services: Vec<InsertServiceUpdateCommand>,
        #[serde(with = "descriptor_pool_serde")]
        descriptor_pool: DescriptorPool,
    },
    RemoveDeployment {
        deployment_id: DeploymentId,
    },
    /// Remove only if the revision is matching
    RemoveService {
        name: String,
        revision: ServiceRevision,
    },
    ModifyService {
        name: String,
        public: bool,
    },
    AddSubscription(Subscription),
    RemoveSubscription(SubscriptionId),
}

mod descriptor_pool_serde {
    use bytes::Bytes;
    use prost_reflect::DescriptorPool;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(descriptor_pool: &DescriptorPool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&descriptor_pool.encode_to_vec())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DescriptorPool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b = Bytes::deserialize(deserializer)?;
        DescriptorPool::decode(b).map_err(serde::de::Error::custom)
    }
}

/// The schema registry
#[derive(Debug, Default, Clone)]
pub struct Schemas(Arc<ArcSwap<schemas_impl::SchemasInner>>);

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(unknown)]
pub enum SchemasUpdateError {
    #[error("a deployment with the same id {0} already exists in the registry")]
    #[code(restate_errors::META0004)]
    OverrideDeployment(DeploymentId),
    #[error(
        "existing deployment id is different from requested (requested = {requested}, existing = {existing})"
    )]
    #[code(restate_errors::META0004)]
    IncorrectDeploymentId {
        requested: DeploymentId,
        existing: DeploymentId,
    },
    #[error("missing service {0} in descriptor")]
    #[code(restate_errors::META0005)]
    MissingServiceInDescriptor(String),
    #[error("service {0} does not exist in the registry")]
    #[code(restate_errors::META0005)]
    UnknownService(String),
    #[error("cannot insert/modify service {0} as it's a reserved name")]
    #[code(restate_errors::META0005)]
    ModifyInternalService(String),
    #[error("unknown deployment id {0}")]
    UnknownDeployment(DeploymentId),
    #[error("unknown subscription id {0}")]
    UnknownSubscription(SubscriptionId),
    #[error("invalid subscription: {0}")]
    #[code(restate_errors::META0009)]
    InvalidSubscription(anyhow::Error),
    #[error("a subscription with the same id {0} already exists in the registry")]
    OverrideSubscription(SubscriptionId),
    #[error(transparent)]
    IncompatibleServiceChange(
        #[from]
        #[code]
        IncompatibleServiceChangeError,
    ),
    #[error(transparent)]
    BadDescriptor(
        #[from]
        #[code]
        BadDescriptorError,
    ),
}

impl Schemas {
    /// Compute the commands to update the schema registry with a new deployment.
    /// This method doesn't execute any in-memory update of the registry.
    /// To update the registry, compute the commands and apply them with [`Self::apply_updates`].
    ///
    /// If `allow_override` is true, this method compares the deployment registered services with the given `services`,
    /// and generates remove commands for services no longer available at that deployment.
    ///
    /// If `deployment_id` is set, it's assumed that:
    ///   - This ID should be used if it's is a new deployment
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
        services: Vec<String>,
        descriptor_pool: DescriptorPool,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        self.0.load().compute_new_deployment(
            deployment_id,
            deployment_metadata,
            services,
            descriptor_pool,
            force,
        )
    }

    pub fn compute_modify_service(
        &self,
        service_name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, SchemasUpdateError> {
        self.0
            .load()
            .compute_modify_service_updates(service_name, public)
    }

    pub fn compute_remove_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        self.0.load().compute_remove_deployment(deployment_id)
    }

    // Returns the [`Subscription`] id together with the update command
    pub fn compute_add_subscription<V: SubscriptionValidator>(
        &self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: V,
    ) -> Result<(Subscription, SchemasUpdateCommand), SchemasUpdateError> {
        self.0
            .load()
            .compute_add_subscription(id, source, sink, metadata, validator)
    }

    pub fn compute_remove_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<SchemasUpdateCommand, SchemasUpdateError> {
        self.0.load().compute_remove_subscription(id)
    }

    /// Apply the updates to the schema registry.
    /// This method will update the internal pointer to the in-memory schema registry,
    /// propagating the changes to every component consuming it.
    ///
    /// IMPORTANT: This method is not thread safe! This method should be called only by a single thread.
    pub fn apply_updates(
        &self,
        updates: impl IntoIterator<Item = SchemasUpdateCommand>,
    ) -> Result<(), SchemasUpdateError> {
        let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
        for cmd in updates {
            match cmd {
                SchemasUpdateCommand::InsertDeployment {
                    deployment_id,
                    metadata,
                    services,
                    descriptor_pool,
                } => {
                    schemas_inner.apply_insert_deployment(
                        deployment_id,
                        metadata,
                        services,
                        descriptor_pool,
                    )?;
                }
                SchemasUpdateCommand::RemoveDeployment { deployment_id } => {
                    schemas_inner.apply_remove_deployment(deployment_id)?;
                }
                SchemasUpdateCommand::RemoveService { name, revision } => {
                    schemas_inner.apply_remove_service(name, revision)?;
                }
                SchemasUpdateCommand::ModifyService { name, public } => {
                    schemas_inner.apply_modify_service(name, public)?;
                }
                SchemasUpdateCommand::AddSubscription(sub) => {
                    schemas_inner.apply_add_subscription(sub)?;
                }
                SchemasUpdateCommand::RemoveSubscription(sub_id) => {
                    schemas_inner.apply_remove_subscription(sub_id)?;
                }
            }
        }
        self.0.store(Arc::new(schemas_inner));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_schema_api::deployment::DeploymentResolver;
    use restate_schema_api::service::ServiceMetadataResolver;
    use restate_test_util::assert_eq;

    impl Schemas {
        pub(crate) fn add_mock_service(&self, service_name: &str, service_schemas: ServiceSchemas) {
            let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
            schemas_inner
                .services
                .insert(service_name.to_string(), service_schemas);
            self.0.store(Arc::new(schemas_inner));
        }

        #[track_caller]
        pub(crate) fn assert_service_revision(&self, svc_name: &str, revision: ServiceRevision) {
            assert_eq!(
                self.resolve_latest_service_metadata(svc_name)
                    .unwrap()
                    .revision,
                revision
            );
        }

        #[track_caller]
        pub(crate) fn assert_resolves_deployment(
            &self,
            svc_name: &str,
            deployment_id: DeploymentId,
        ) {
            assert_eq!(
                self.resolve_latest_deployment_for_service(svc_name)
                    .unwrap()
                    .id,
                deployment_id
            );
        }
    }

    #[macro_export]
    macro_rules! load_mock_descriptor {
        ($name:ident, $path:literal) => {
            static $name: once_cell::sync::Lazy<DescriptorPool> =
                once_cell::sync::Lazy::new(|| {
                    DescriptorPool::decode(
                        include_bytes!(concat!(env!("OUT_DIR"), "/pb/", $path, "/descriptor.bin"))
                            .as_ref(),
                    )
                    .expect("The built-in descriptor pool should be valid")
                });
        };
    }
}
