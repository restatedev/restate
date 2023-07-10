use arc_swap::ArcSwap;
use prost_reflect::DescriptorPool;
use restate_schema_api::endpoint::EndpointMetadata;
use restate_schema_api::key::ServiceInstanceType;
use restate_types::identifiers::{EndpointId, ServiceRevision};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod endpoint;
mod json;
mod json_key_conversion;
mod key_expansion;
mod key_extraction;
mod proto_symbol;
mod service;

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
    #[error("missing expected field {0} in descriptor")]
    MissingFieldInDescriptor(&'static str),
    #[error("missing service {0} in descriptor")]
    #[code(restate_errors::META0005)]
    MissingServiceInDescriptor(String),
    #[error("unexpected endpoint id {0}")]
    #[code(restate_errors::META0005)]
    UnexpectedEndpointId(EndpointId),
}

/// Represents an update command to update the [`Schemas`] object. See [`Schemas::apply_updates`] for more info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemasUpdateCommand {
    /// Insert (or replace) service
    InsertEndpoint {
        metadata: EndpointMetadata,
        services: Vec<(String, ServiceRevision)>,
        #[serde(with = "descriptor_pool_serde")]
        descriptor_pool: DescriptorPool,
    },
    /// Insert (or replace) service
    InsertService {
        name: String,
        revision: ServiceRevision,
        endpoint_id: EndpointId,
        instance_type: ServiceInstanceType,
    },
    /// Remove only if the revision is matching
    RemoveService {
        name: String,
        revision: ServiceRevision,
    },
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
    use std::collections::hash_map::Entry;

    use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};
    use proto_symbol::ProtoSymbols;
    use restate_types::identifiers::{EndpointId, ServiceRevision};
    use std::collections::HashMap;
    use tracing::{debug, info};

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
            svc_desc: ServiceDescriptor,
            instance_type: ServiceInstanceType,
            latest_endpoint: EndpointId,
        ) -> Self {
            Self {
                revision,
                methods: svc_desc
                    .methods()
                    .map(|method_desc| (method_desc.name().to_string(), method_desc))
                    .collect(),
                instance_type,
                location: ServiceLocation::ServiceEndpoint { latest_endpoint },
            }
        }

        fn new_ingress_only(svc_desc: ServiceDescriptor) -> Self {
            Self {
                revision: 0,
                methods: svc_desc
                    .methods()
                    .map(|method_desc| (method_desc.name().to_string(), method_desc))
                    .collect(),
                instance_type: ServiceInstanceType::Singleton,
                location: ServiceLocation::IngressOnly,
            }
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) enum ServiceLocation {
        IngressOnly,
        ServiceEndpoint {
            // None if this is a built-in service
            latest_endpoint: EndpointId,
        },
    }

    #[derive(Debug, Clone)]
    pub(crate) struct EndpointSchemas {
        pub(crate) metadata: EndpointMetadata,
        pub(crate) services: Vec<(String, ServiceRevision)>,
        pub(crate) descriptor_pool: DescriptorPool,
    }

    impl Default for SchemasInner {
        fn default() -> Self {
            let mut inner = Self {
                services: Default::default(),
                endpoints: Default::default(),
                proto_symbols: Default::default(),
            };

            // Insert built-in services
            inner.services.insert(
                restate_pb::REFLECTION_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(
                    restate_pb::DESCRIPTOR_POOL
                        .get_service_by_name(restate_pb::REFLECTION_SERVICE_NAME)
                        .expect(
                            "The built-in descriptor pool should contain the reflection service",
                        ),
                ),
            );
            inner.services.insert(
                restate_pb::INGRESS_SERVICE_NAME.to_string(),
                ServiceSchemas::new_ingress_only(
                    restate_pb::DESCRIPTOR_POOL
                        .get_service_by_name(restate_pb::INGRESS_SERVICE_NAME)
                        .expect("The built-in descriptor pool should contain the ingress service"),
                ),
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
                    // If we need to override the endpoint we need to remove old services
                    for (svc_name, revision) in &existing_endpoint.services {
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
                // For the time being when updating we overwrite existing data
                let revision = if let Some(service_schemas) = self.services.get(service_meta.name())
                {
                    service_schemas.revision + 1
                } else {
                    1
                };
                computed_revisions.insert(service_meta.name().to_string(), revision);
            }

            // Register the endpoint first
            result_commands.push(SchemasUpdateCommand::InsertEndpoint {
                metadata: endpoint_metadata,
                services: computed_revisions
                    .iter()
                    .map(|(name, rev)| (name.to_string(), *rev))
                    .collect(),
                descriptor_pool,
            });

            // Register services
            for service_meta in services {
                let revision = *computed_revisions.get(service_meta.name()).unwrap();

                result_commands.push(SchemasUpdateCommand::InsertService {
                    name: service_meta.name,
                    revision,
                    endpoint_id: endpoint_id.clone(),
                    instance_type: service_meta.instance_type,
                })
            }

            Ok(result_commands)
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

                    self.endpoints.insert(
                        endpoint_id,
                        EndpointSchemas {
                            metadata,
                            services,
                            descriptor_pool,
                        },
                    );
                }
                SchemasUpdateCommand::InsertService {
                    name,
                    revision,
                    endpoint_id,
                    instance_type,
                } => {
                    let endpoint_schemas = self.endpoints.get(&endpoint_id).ok_or_else(|| {
                        RegistrationError::UnexpectedEndpointId(endpoint_id.clone())
                    })?;
                    let endpoint_address = endpoint_schemas.metadata.address().clone();

                    info!(
                        rpc.service = name,
                        restate.service_endpoint.url = %endpoint_address,
                        "Registering service"
                    );
                    let service_descriptor = endpoint_schemas
                        .descriptor_pool
                        .get_service_by_name(&name)
                        .ok_or_else(|| {
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

                    if self
                        .services
                        .insert(
                            name.clone(),
                            ServiceSchemas::new(
                                revision,
                                service_descriptor,
                                instance_type,
                                endpoint_id.clone(),
                            ),
                        )
                        .is_some()
                    {
                        info!(rpc.service = name, "Overwriting existing service schemas");
                    }

                    self.proto_symbols.register_new_service(
                        endpoint_id,
                        name,
                        endpoint_schemas.descriptor_pool.clone(),
                    )?;
                }
                SchemasUpdateCommand::RemoveService { name, revision } => {
                    let entry = self.services.entry(name);
                    match entry {
                        Entry::Occupied(e) if e.get().revision == revision => {
                            e.remove();
                        }
                        _ => {}
                    }
                }
            }

            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use restate_pb::mocks;
        use restate_schema_api::endpoint::EndpointMetadataResolver;
        use restate_schema_api::service::ServiceMetadataResolver;
        use restate_test_util::{assert, assert_eq, test};

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

            let endpoint = EndpointMetadata::mock_with_uri("http://localhost:8080");
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

            assert!(let Some(&SchemasUpdateCommand::InsertEndpoint { .. }) = commands.get(0));
            assert!(let Some(&SchemasUpdateCommand::InsertService { .. }) = commands.get(1));

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

            assert!(let Some(&SchemasUpdateCommand::InsertEndpoint { .. }) = commands.get(0));
            assert!(let Some(&SchemasUpdateCommand::InsertService { .. }) = commands.get(1));

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

            assert!(let Some(&SchemasUpdateCommand::InsertEndpoint { .. }) = commands.get(0));
            assert!(let Some(&SchemasUpdateCommand::InsertService { .. }) = commands.get(1));
            assert!(let Some(&SchemasUpdateCommand::InsertService { .. }) = commands.get(2));

            schemas.apply_updates(commands).unwrap();

            schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
            schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint_2.id());
            schemas.assert_service_revision(mocks::ANOTHER_GREETER_SERVICE_NAME, 1);
        }

        #[test]
        fn override_existing_endpoint() {
            let schemas = Schemas::default();

            let endpoint = EndpointMetadata::mock_with_uri("http://localhost:8080");
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
