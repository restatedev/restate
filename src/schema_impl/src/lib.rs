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
use prost_reflect::DescriptorPool;
use restate_schema_api::endpoint::EndpointMetadata;
use restate_schema_api::key;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_types::identifiers::{EndpointId, ServiceRevision};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

mod endpoint;
mod json;
mod json_key_conversion;
mod key_expansion;
mod key_extraction;
mod proto_symbol;
mod schemas_impl;
mod service;
mod subscriptions;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRegistrationRequest {
    name: String,
    instance_type: key::ServiceInstanceType,
}

impl ServiceRegistrationRequest {
    pub fn new(name: String, instance_type: key::ServiceInstanceType) -> Self {
        Self {
            name,
            instance_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_type(&self) -> &key::ServiceInstanceType {
        &self.instance_type
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(unknown)]
pub enum RegistrationError {
    #[error("an endpoint with the same id {0} already exists in the registry")]
    #[code(restate_errors::META0004)]
    OverrideEndpoint(EndpointId),
    #[error("detected a new service {0} revision with a service instance type different from the previous revision")]
    #[code(restate_errors::META0006)]
    DifferentServiceInstanceType(String),
    #[error("missing expected field {0} in descriptor")]
    MissingFieldInDescriptor(&'static str),
    #[error("missing service {0} in descriptor")]
    #[code(restate_errors::META0005)]
    MissingServiceInDescriptor(String),
    #[error("service {0} does not exist in the registry")]
    #[code(restate_errors::META0005)]
    UnknownService(String),
    #[error("cannot insert/modify service {0} as it's a reserved name")]
    #[code(restate_errors::META0005)]
    ModifyInternalService(String),
    #[error("unknown endpoint id {0}")]
    UnknownEndpoint(EndpointId),
    #[error("unknown subscription id {0}")]
    UnknownSubscription(String),
    #[error("invalid subscription: {0}")]
    // TODO add error code to describe how to set sink and source
    InvalidSubscription(anyhow::Error),
    #[error("a subscription with the same id {0} already exists in the registry")]
    OverrideSubscription(EndpointId),
}

/// Insert (or replace) service
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertServiceUpdateCommand {
    pub name: String,
    pub revision: ServiceRevision,
    pub instance_type: key::ServiceInstanceType,
}

/// Represents an update command to update the [`Schemas`] object. See [`Schemas::apply_updates`] for more info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemasUpdateCommand {
    /// Insert (or replace) endpoint
    InsertEndpoint {
        metadata: EndpointMetadata,
        services: Vec<InsertServiceUpdateCommand>,
        #[serde(with = "descriptor_pool_serde")]
        descriptor_pool: DescriptorPool,
    },
    RemoveEndpoint {
        endpoint_id: EndpointId,
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
    RemoveSubscription(String),
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

impl Schemas {
    /// Compute the commands to update the schema registry with a new endpoint.
    /// This method doesn't execute any in-memory update of the registry.
    /// To update the registry, compute the commands and apply them with [`Self::apply_updates`].
    ///
    /// If `allow_override` is true, this method compares the endpoint registered services with the given `services`,
    /// and generates remove commands for services no longer available at that endpoint.
    ///
    /// IMPORTANT: It is not safe to consecutively call this method several times and apply the updates all-together with a single [`Self::apply_updates`],
    /// as one change set might depend on the previously computed change set.
    pub fn compute_new_endpoint_updates(
        &self,
        endpoint_metadata: EndpointMetadata,
        services: Vec<ServiceRegistrationRequest>,
        descriptor_pool: DescriptorPool,
        allow_overwrite: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
        self.0.load().compute_new_endpoint_updates(
            endpoint_metadata,
            services,
            descriptor_pool,
            allow_overwrite,
        )
    }

    pub fn compute_modify_service_updates(
        &self,
        service_name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, RegistrationError> {
        self.0
            .load()
            .compute_modify_service_updates(service_name, public)
    }

    pub fn compute_remove_endpoint(
        &self,
        endpoint_id: EndpointId,
    ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
        self.0.load().compute_remove_endpoint(endpoint_id)
    }

    // Returns the [`Subscription`] id together with the update command
    pub fn compute_add_subscription<V: SubscriptionValidator>(
        &self,
        id: Option<String>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: V,
    ) -> Result<(Subscription, SchemasUpdateCommand), RegistrationError> {
        self.0
            .load()
            .compute_add_subscription(id, source, sink, metadata, validator)
    }

    pub fn compute_remove_subscription(
        &self,
        id: String,
    ) -> Result<SchemasUpdateCommand, RegistrationError> {
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
    ) -> Result<(), RegistrationError> {
        let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
        for cmd in updates {
            schemas_inner.apply_update(cmd)?;
        }
        self.0.store(Arc::new(schemas_inner));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Schemas {
        pub(crate) fn add_mock_service(
            &self,
            service_name: &str,
            service_schemas: schemas_impl::ServiceSchemas,
        ) {
            let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
            schemas_inner
                .services
                .insert(service_name.to_string(), service_schemas);
            self.0.store(Arc::new(schemas_inner));
        }
    }
}
