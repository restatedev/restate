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
use restate_schema_api::key::ServiceInstanceType;
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
mod service;
mod subscriptions;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRegistrationRequest {
    name: String,
    instance_type: ServiceInstanceType,
}

impl ServiceRegistrationRequest {
    pub fn new(name: String, instance_type: ServiceInstanceType) -> Self {
        Self {
            name,
            instance_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_type(&self) -> &ServiceInstanceType {
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
    pub instance_type: ServiceInstanceType,
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
    ) -> Result<(String, SchemasUpdateCommand), RegistrationError> {
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

pub(crate) mod schemas_impl {
    use super::*;

    use anyhow::anyhow;
    use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};
    use proto_symbol::ProtoSymbols;
    use restate_types::identifiers::{EndpointId, ServiceRevision};
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use tracing::{debug, info, warn};

    const RESTATE_SERVICE_NAME_PREFIX: &str = "dev.restate.";
    const GRPC_SERVICE_NAME_PREFIX: &str = "grpc.";

    impl Schemas {
        pub(crate) fn use_service_schema<F, R>(
            &self,
            service_name: impl AsRef<str>,
            f: F,
        ) -> Option<R>
        where
            F: FnOnce(&ServiceSchemas) -> R,
        {
            let guard = self.0.load();
            guard.services.get(service_name.as_ref()).map(f)
        }
    }

    /// This struct contains the actual data held by Schemas.
    #[derive(Debug, Clone)]
    pub(crate) struct SchemasInner {
        pub(crate) services: HashMap<String, ServiceSchemas>,
        pub(crate) endpoints: HashMap<EndpointId, EndpointSchemas>,
        pub(crate) subscriptions: HashMap<String, Subscription>,
        pub(crate) proto_symbols: ProtoSymbols,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ServiceSchemas {
        pub(crate) revision: ServiceRevision,
        pub(crate) methods: HashMap<String, MethodDescriptor>,
        pub(crate) instance_type: ServiceInstanceType,
        pub(crate) location: ServiceLocation,
    }

    impl ServiceSchemas {
        fn new(
            revision: ServiceRevision,
            svc_desc: &ServiceDescriptor,
            instance_type: ServiceInstanceType,
            latest_endpoint: EndpointId,
        ) -> Self {
            Self {
                revision,
                methods: Self::compute_service_methods(svc_desc),
                instance_type,
                location: ServiceLocation::ServiceEndpoint {
                    latest_endpoint,
                    public: true,
                },
            }
        }

        fn new_ingress_only(svc_desc: &ServiceDescriptor) -> Self {
            Self {
                revision: 0,
                methods: Self::compute_service_methods(svc_desc),
                instance_type: ServiceInstanceType::Singleton,
                location: ServiceLocation::IngressOnly,
            }
        }

        fn compute_service_methods(
            svc_desc: &ServiceDescriptor,
        ) -> HashMap<String, MethodDescriptor> {
            svc_desc
                .methods()
                .map(|method_desc| (method_desc.name().to_string(), method_desc))
                .collect()
        }

        fn service_descriptor(&self) -> &ServiceDescriptor {
            self.methods
                .values()
                .next()
                .expect("A service should have at least one method")
                .parent_service()
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) enum ServiceLocation {
        IngressOnly,
        ServiceEndpoint {
            // None if this is a built-in service
            latest_endpoint: EndpointId,
            public: bool,
        },
    }

    impl ServiceLocation {
        pub(crate) fn is_ingress_available(&self) -> bool {
            match self {
                ServiceLocation::IngressOnly => true,
                ServiceLocation::ServiceEndpoint { public, .. } => *public,
            }
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct EndpointSchemas {
        pub(crate) metadata: EndpointMetadata,
        pub(crate) services: Vec<(String, ServiceRevision)>,
    }

    impl Default for SchemasInner {
        fn default() -> Self {
            let mut inner = Self {
                services: Default::default(),
                endpoints: Default::default(),
                subscriptions: Default::default(),
                proto_symbols: Default::default(),
            };

            // Insert built-in services
            inner.services.insert(
                restate_pb::REFLECTION_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(&restate_pb::get_service(
                    restate_pb::REFLECTION_SERVICE_NAME,
                )),
            );
            inner.services.insert(
                restate_pb::INGRESS_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(&restate_pb::get_service(
                    restate_pb::INGRESS_SERVICE_NAME,
                )),
            );
            inner.services.insert(
                restate_pb::AWAKEABLES_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(&restate_pb::get_service(
                    restate_pb::AWAKEABLES_SERVICE_NAME,
                )),
            );
            inner.services.insert(
                restate_pb::HEALTH_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(&restate_pb::get_service(
                    restate_pb::HEALTH_SERVICE_NAME,
                )),
            );

            inner
        }
    }

    impl SchemasInner {
        pub(crate) fn compute_new_endpoint_updates(
            &self,
            endpoint_metadata: EndpointMetadata,
            services: Vec<ServiceRegistrationRequest>,
            descriptor_pool: DescriptorPool,
            allow_overwrite: bool,
        ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
            let endpoint_id = endpoint_metadata.id();

            let mut result_commands = Vec::with_capacity(1 + services.len());

            if let Some(existing_endpoint) = self.endpoints.get(&endpoint_id) {
                if allow_overwrite {
                    // If we need to overwrite the endpoint we need to remove old services
                    for (svc_name, revision) in &existing_endpoint.services {
                        warn!(
                            restate.service_endpoint.id = %endpoint_id,
                            restate.service_endpoint.url = %endpoint_metadata.address(),
                            "Going to remove service {} due to a forced service endpoint update",
                            svc_name
                        );
                        result_commands.push(SchemasUpdateCommand::RemoveService {
                            name: svc_name.to_string(),
                            revision: *revision,
                        });
                    }
                } else {
                    return Err(RegistrationError::OverrideEndpoint(endpoint_id));
                }
            }

            // Compute service revision numbers
            let mut computed_revisions = HashMap::with_capacity(services.len());
            for service_meta in &services {
                check_is_reserved(&service_meta.name)?;

                // For the time being when updating we overwrite existing data
                let revision = if let Some(service_schemas) = self.services.get(service_meta.name())
                {
                    // Check instance type
                    if service_schemas.instance_type != service_meta.instance_type {
                        if allow_overwrite {
                            warn!(
                                restate.service_endpoint.id = %endpoint_id,
                                restate.service_endpoint.url = %endpoint_metadata.address(),
                                "Going to overwrite service instance type {} due to a forced service endpoint update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                                service_meta.name(),
                                service_schemas.instance_type,
                                service_meta.instance_type
                            );
                        } else {
                            return Err(RegistrationError::DifferentServiceInstanceType(
                                service_meta.name.clone(),
                            ));
                        }
                    }

                    service_schemas.revision.wrapping_add(1)
                } else {
                    1
                };
                computed_revisions.insert(service_meta.name().to_string(), revision);
            }

            // Create the InsertEndpoint command
            result_commands.push(SchemasUpdateCommand::InsertEndpoint {
                metadata: endpoint_metadata,
                services: services
                    .into_iter()
                    .map(|service_meta| {
                        let revision = *computed_revisions.get(service_meta.name()).unwrap();

                        InsertServiceUpdateCommand {
                            name: service_meta.name,
                            revision,
                            instance_type: service_meta.instance_type,
                        }
                    })
                    .collect(),
                descriptor_pool,
            });

            Ok(result_commands)
        }

        pub(crate) fn compute_modify_service_updates(
            &self,
            name: String,
            public: bool,
        ) -> Result<SchemasUpdateCommand, RegistrationError> {
            check_is_reserved(&name)?;
            if !self.services.contains_key(&name) {
                return Err(RegistrationError::UnknownService(name));
            }

            Ok(SchemasUpdateCommand::ModifyService { name, public })
        }

        pub(crate) fn compute_remove_endpoint(
            &self,
            endpoint_id: EndpointId,
        ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
            if !self.endpoints.contains_key(&endpoint_id) {
                return Err(RegistrationError::UnknownEndpoint(endpoint_id));
            }
            let endpoint_schemas = self.endpoints.get(&endpoint_id).unwrap();

            let mut commands = Vec::with_capacity(1 + endpoint_schemas.services.len());
            for (name, revision) in endpoint_schemas.services.clone() {
                commands.push(SchemasUpdateCommand::RemoveService { name, revision });
            }
            commands.push(SchemasUpdateCommand::RemoveEndpoint { endpoint_id });

            Ok(commands)
        }

        pub(crate) fn compute_add_subscription<V: SubscriptionValidator>(
            &self,
            id: Option<String>,
            source: Uri,
            sink: Uri,
            metadata: Option<HashMap<String, String>>,
            validator: V,
        ) -> Result<(String, SchemasUpdateCommand), RegistrationError> {
            // We could generate a more human readable uuid here by taking the source and sink, and adding an incremental number in case of collision.
            let id = id.unwrap_or_else(|| uuid::Uuid::now_v7().as_simple().to_string());

            if self.subscriptions.contains_key(&id) {
                return Err(RegistrationError::OverrideSubscription(id));
            }

            if source.scheme().is_none() || source.authority().is_none() {
                return Err(RegistrationError::InvalidSubscription(anyhow!(
                    "source URI must have a scheme and an authority segment, was {}",
                    source
                )));
            }
            if sink.scheme().is_none() || sink.authority().is_none() {
                return Err(RegistrationError::InvalidSubscription(anyhow!(
                    "sink URI must have a schema and an authority segment, was {}",
                    sink
                )));
            }

            let subscription = validator
                .validate(Subscription::new(
                    id.clone(),
                    source,
                    sink,
                    metadata.unwrap_or_default(),
                ))
                .map_err(|e| RegistrationError::InvalidSubscription(e.into()))?;

            Ok((id, SchemasUpdateCommand::AddSubscription(subscription)))
        }

        pub(crate) fn compute_remove_subscription(
            &self,
            id: String,
        ) -> Result<SchemasUpdateCommand, RegistrationError> {
            if !self.subscriptions.contains_key(&id) {
                return Err(RegistrationError::UnknownSubscription(id));
            }

            Ok(SchemasUpdateCommand::RemoveSubscription(id))
        }

        pub(crate) fn apply_update(
            &mut self,
            update_cmd: SchemasUpdateCommand,
        ) -> Result<(), RegistrationError> {
            match update_cmd {
                SchemasUpdateCommand::InsertEndpoint {
                    metadata,
                    services,
                    descriptor_pool,
                } => {
                    let endpoint_id = metadata.id();
                    let endpoint_address = metadata.address().clone();
                    info!(
                        restate.service_endpoint.id = %endpoint_id,
                        restate.service_endpoint.url = %endpoint_address,
                        "Registering endpoint"
                    );

                    let mut endpoint_services = vec![];

                    for InsertServiceUpdateCommand {
                        name,
                        revision,
                        instance_type,
                    } in services
                    {
                        let endpoint_address = metadata.address().clone();

                        info!(
                            rpc.service = name,
                            restate.service_endpoint.url = %endpoint_address,
                            "Registering service"
                        );
                        let service_descriptor =
                            descriptor_pool.get_service_by_name(&name).ok_or_else(|| {
                                RegistrationError::MissingServiceInDescriptor(name.clone())
                            })?;

                        if tracing::enabled!(tracing::Level::DEBUG) {
                            service_descriptor.methods().for_each(|method| {
                                debug!(
                                    rpc.service = name,
                                    rpc.method = method.name(),
                                    "Registering method"
                                )
                            });
                        }

                        // We need to retain the `public` field from previous registrations
                        self.services
                            .entry(name.clone())
                            .and_modify(|service_schemas| {
                                info!(rpc.service = name, "Overwriting existing service schemas");

                                service_schemas.revision = revision;
                                service_schemas.instance_type = instance_type.clone();
                                service_schemas.methods =
                                    ServiceSchemas::compute_service_methods(&service_descriptor);
                                if let ServiceLocation::ServiceEndpoint {
                                    latest_endpoint, ..
                                } = &mut service_schemas.location
                                {
                                    *latest_endpoint = endpoint_id.clone();
                                }

                                // We need to remove the service from the proto_symbols.
                                // We re-insert it later with the new endpoint id
                                self.proto_symbols.remove_service(&service_descriptor);
                            })
                            .or_insert_with(|| {
                                ServiceSchemas::new(
                                    revision,
                                    &service_descriptor,
                                    instance_type.clone(),
                                    endpoint_id.clone(),
                                )
                            });

                        self.proto_symbols
                            .add_service(&endpoint_id, &service_descriptor);

                        endpoint_services.push((name, revision));
                    }

                    self.endpoints.insert(
                        endpoint_id,
                        EndpointSchemas {
                            metadata,
                            services: endpoint_services,
                        },
                    );
                }
                SchemasUpdateCommand::RemoveEndpoint { endpoint_id } => {
                    self.endpoints.remove(&endpoint_id);
                }
                SchemasUpdateCommand::RemoveService { name, revision } => {
                    let entry = self.services.entry(name);
                    match entry {
                        Entry::Occupied(e) if e.get().revision == revision => {
                            let schemas = e.remove();
                            self.proto_symbols
                                .remove_service(schemas.service_descriptor());
                        }
                        _ => {}
                    }
                }
                SchemasUpdateCommand::ModifyService {
                    name,
                    public: new_public_value,
                } => {
                    let schemas = self
                        .services
                        .get_mut(&name)
                        .ok_or_else(|| RegistrationError::UnknownService(name.clone()))?;

                    // Update proto_symbols
                    if let ServiceLocation::ServiceEndpoint {
                        public: old_public_value,
                        latest_endpoint,
                    } = &schemas.location
                    {
                        match (*old_public_value, new_public_value) {
                            (true, false) => {
                                self.proto_symbols
                                    .remove_service(schemas.service_descriptor());
                            }
                            (false, true) => {
                                self.proto_symbols
                                    .add_service(latest_endpoint, schemas.service_descriptor());
                            }
                            _ => {}
                        }
                    }

                    // Update the public field
                    if let ServiceLocation::ServiceEndpoint {
                        public: old_public_value,
                        ..
                    } = &mut schemas.location
                    {
                        *old_public_value = new_public_value;
                    }
                }
                SchemasUpdateCommand::AddSubscription(sub) => {
                    self.subscriptions.insert(sub.id().to_string(), sub);
                }
                SchemasUpdateCommand::RemoveSubscription(sub_id) => {
                    self.subscriptions.remove(&sub_id);
                }
            }

            Ok(())
        }
    }

    fn check_is_reserved(svc_name: &str) -> Result<(), RegistrationError> {
        if svc_name.starts_with(GRPC_SERVICE_NAME_PREFIX)
            || svc_name.starts_with(RESTATE_SERVICE_NAME_PREFIX)
        {
            return Err(RegistrationError::ModifyInternalService(
                svc_name.to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use restate_pb::mocks;
        use restate_schema_api::endpoint::EndpointMetadataResolver;
        use restate_schema_api::service::ServiceMetadataResolver;
        use restate_test_util::{assert, assert_eq, let_assert, test};

        impl Schemas {
            fn assert_service_revision(&self, svc_name: &str, revision: ServiceRevision) {
                assert_eq!(
                    self.resolve_latest_service_metadata(svc_name)
                        .unwrap()
                        .revision,
                    revision
                );
            }

            fn assert_resolves_endpoint(&self, svc_name: &str, endpoint_id: EndpointId) {
                assert_eq!(
                    self.resolve_latest_endpoint_for_service(svc_name)
                        .unwrap()
                        .id(),
                    endpoint_id
                );
            }
        }

        #[test]
        fn register_new_endpoint_empty_registry() {
            let schemas = Schemas::default();

            let endpoint = EndpointMetadata::mock();
            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint.clone(),
                    vec![ServiceRegistrationRequest::new(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        ServiceInstanceType::Unkeyed,
                    )],
                    mocks::DESCRIPTOR_POOL.clone(),
                    false,
                )
                .unwrap();

            let_assert!(
                Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0)
            );
            assert_eq!(services.len(), 1);

            schemas.apply_updates(commands).unwrap();

            schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 1);
            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
        }

        #[test]
        fn register_new_endpoint_updating_old_service() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:8080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:8081");

            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint_1.clone(),
                    vec![ServiceRegistrationRequest::new(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        ServiceInstanceType::Unkeyed,
                    )],
                    mocks::DESCRIPTOR_POOL.clone(),
                    false,
                )
                .unwrap();

            let_assert!(
                Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0)
            );
            assert_eq!(services.len(), 1);

            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_1.id());

            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint_2.clone(),
                    vec![
                        ServiceRegistrationRequest::new(
                            mocks::GREETER_SERVICE_NAME.to_string(),
                            ServiceInstanceType::Unkeyed,
                        ),
                        ServiceRegistrationRequest::new(
                            mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                            ServiceInstanceType::Unkeyed,
                        ),
                    ],
                    mocks::DESCRIPTOR_POOL.clone(),
                    false,
                )
                .unwrap();

            let_assert!(
                Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0)
            );
            assert_eq!(services.len(), 2);

            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
            schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::ANOTHER_GREETER_SERVICE_NAME, 1);
        }

        #[test]
        fn register_new_endpoint_updating_old_service_fails_with_different_instance_type() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:8080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:8081");

            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint_updates(
                            endpoint_1.clone(),
                            vec![ServiceRegistrationRequest::new(
                                mocks::GREETER_SERVICE_NAME.to_string(),
                                ServiceInstanceType::Unkeyed,
                            )],
                            mocks::DESCRIPTOR_POOL.clone(),
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_1.id());

            let compute_result = schemas.compute_new_endpoint_updates(
                endpoint_2,
                vec![ServiceRegistrationRequest::new(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    ServiceInstanceType::Singleton,
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            );

            assert!(let Err(RegistrationError::DifferentServiceInstanceType(_)) = compute_result);
        }

        #[test]
        fn override_existing_endpoint() {
            let schemas = Schemas::default();

            let endpoint = EndpointMetadata::mock();
            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint.clone(),
                    vec![
                        ServiceRegistrationRequest::new(
                            mocks::GREETER_SERVICE_NAME.to_string(),
                            ServiceInstanceType::Unkeyed,
                        ),
                        ServiceRegistrationRequest::new(
                            mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                            ServiceInstanceType::Unkeyed,
                        ),
                    ],
                    mocks::DESCRIPTOR_POOL.clone(),
                    false,
                )
                .unwrap();
            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
            schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint.id());

            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint.clone(),
                    vec![ServiceRegistrationRequest::new(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        ServiceInstanceType::Unkeyed,
                    )],
                    mocks::DESCRIPTOR_POOL.clone(),
                    true,
                )
                .unwrap();
            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
            assert!(schemas
                .resolve_latest_endpoint_for_service(mocks::ANOTHER_GREETER_SERVICE_NAME)
                .is_none());
        }

        #[test]
        fn cannot_override_existing_endpoint() {
            let schemas = Schemas::default();

            let endpoint = EndpointMetadata::mock_with_uri("http://localhost:8080");
            let services = vec![ServiceRegistrationRequest::new(
                mocks::GREETER_SERVICE_NAME.to_string(),
                ServiceInstanceType::Unkeyed,
            )];

            let commands = schemas
                .compute_new_endpoint_updates(
                    endpoint.clone(),
                    services.clone(),
                    mocks::DESCRIPTOR_POOL.clone(),
                    false,
                )
                .unwrap();
            schemas.apply_updates(commands).unwrap();

            assert!(let Err(RegistrationError::OverrideEndpoint(_)) = schemas.compute_new_endpoint_updates(endpoint, services, mocks::DESCRIPTOR_POOL.clone(), false));
        }

        #[test]
        fn register_two_endpoints_then_remove_first() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:8080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:8081");

            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint_updates(
                            endpoint_1.clone(),
                            vec![
                                ServiceRegistrationRequest::new(
                                    mocks::GREETER_SERVICE_NAME.to_string(),
                                    ServiceInstanceType::Unkeyed,
                                ),
                                ServiceRegistrationRequest::new(
                                    mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                                    ServiceInstanceType::Unkeyed,
                                ),
                            ],
                            mocks::DESCRIPTOR_POOL.clone(),
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();
            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint_updates(
                            endpoint_2.clone(),
                            vec![ServiceRegistrationRequest::new(
                                mocks::GREETER_SERVICE_NAME.to_string(),
                                ServiceInstanceType::Unkeyed,
                            )],
                            mocks::DESCRIPTOR_POOL.clone(),
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
            schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint_1.id());
            schemas.assert_service_revision(mocks::ANOTHER_GREETER_SERVICE_NAME, 1);

            let commands = schemas.compute_remove_endpoint(endpoint_1.id()).unwrap();

            assert!(
                let Some(SchemasUpdateCommand::RemoveService { .. }) = commands.get(0)
            );
            assert!(
                let Some(SchemasUpdateCommand::RemoveService { .. }) = commands.get(1)
            );
            assert!(
                let Some(SchemasUpdateCommand::RemoveEndpoint { .. }) = commands.get(2)
            );

            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
            assert!(schemas
                .resolve_latest_endpoint_for_service(mocks::ANOTHER_GREETER_SERVICE_NAME)
                .is_none());
            assert!(schemas.get_endpoint(&endpoint_1.id()).is_none());
        }

        // Reproducer for issue where the service name is the same of the method name
        #[test]
        fn register_issue682() {
            let schemas = Schemas::default();
            let svc_name = "test.Issue682";

            let endpoint = EndpointMetadata::mock();
            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint_updates(
                            endpoint.clone(),
                            vec![ServiceRegistrationRequest::new(
                                svc_name.to_string(),
                                ServiceInstanceType::Unkeyed,
                            )],
                            mocks::DESCRIPTOR_POOL.clone(),
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();
            schemas.assert_service_revision(svc_name, 1);

            // Force the update. This should not panic.
            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint_updates(
                            endpoint,
                            vec![ServiceRegistrationRequest::new(
                                svc_name.to_string(),
                                ServiceInstanceType::Unkeyed,
                            )],
                            mocks::DESCRIPTOR_POOL.clone(),
                            true,
                        )
                        .unwrap(),
                )
                .unwrap();
            schemas.assert_service_revision(svc_name, 2);
        }
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
