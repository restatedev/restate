use arc_swap::ArcSwap;
use prost_reflect::DescriptorPool;
use restate_schema_api::endpoint::EndpointMetadata;
use restate_schema_api::key::ServiceInstanceType;
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

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("missing expected field {0} in descriptor")]
    MissingFieldInDescriptor(&'static str),
    #[error("missing service {0} in descriptor")]
    MissingServiceInDescriptor(String),
}

/// The schema registry
#[derive(Debug, Default, Clone)]
pub struct Schemas(Arc<ArcSwap<schemas_impl::SchemasInner>>);

impl Schemas {
    pub fn register_new_endpoint(
        &self,
        endpoint_metadata: EndpointMetadata,
        services: Vec<ServiceRegistrationRequest>,
        descriptor_pool: DescriptorPool,
    ) -> Result<(), RegistrationError> {
        let mut schemas_inner = schemas_impl::SchemasInner::clone(self.0.load().as_ref());
        schemas_inner.register_new_endpoint(endpoint_metadata, services, descriptor_pool)?;
        self.0.store(Arc::new(schemas_inner));

        Ok(())
    }
}

pub(crate) mod schemas_impl {
    use super::*;

    use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};
    use proto_symbol::ProtoSymbols;
    use restate_types::identifiers::EndpointId;
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
    ///
    /// When we'll need to distribute schemas across meta/worker, we can just ser/de this data structure and pass it around.
    /// See https://github.com/restatedev/restate/issues/91
    #[derive(Debug, Clone)]
    pub(crate) struct SchemasInner {
        pub(crate) services: HashMap<String, ServiceSchemas>,
        pub(crate) endpoints: HashMap<EndpointId, EndpointSchemas>,
        pub(crate) proto_symbols: ProtoSymbols,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ServiceSchemas {
        pub(crate) methods: HashMap<String, MethodDescriptor>,
        pub(crate) instance_type: ServiceInstanceType,
        pub(crate) location: ServiceLocation,
    }

    impl ServiceSchemas {
        fn new(
            svc_desc: ServiceDescriptor,
            instance_type: ServiceInstanceType,
            latest_endpoint: EndpointId,
        ) -> Self {
            Self {
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
        #[allow(dead_code)]
        pub(crate) services: Vec<String>,
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
        pub(crate) fn register_new_endpoint(
            &mut self,
            endpoint_metadata: EndpointMetadata,
            services: Vec<ServiceRegistrationRequest>,
            descriptor_pool: DescriptorPool,
        ) -> Result<(), RegistrationError> {
            let endpoint_id = endpoint_metadata.id();
            let endpoint_address = endpoint_metadata.address().clone();
            info!(
                restate.service_endpoint.id = %endpoint_id,
                restate.service_endpoint.url = %endpoint_address,
                "Registering endpoint"
            );

            self.proto_symbols.register_new_services(
                endpoint_id.clone(),
                services
                    .iter()
                    .map(|service_meta| service_meta.name().to_string())
                    .collect(),
                descriptor_pool.clone(),
            )?;
            self.endpoints.insert(
                endpoint_id.clone(),
                EndpointSchemas {
                    metadata: endpoint_metadata,
                    services: services
                        .iter()
                        .map(|service_meta| service_meta.name.clone())
                        .collect(),
                },
            );
            for service_meta in services {
                info!(
                    rpc.service = service_meta.name(),
                    restate.service_endpoint.url = %endpoint_address,
                    "Registering service"
                );
                let service_descriptor = descriptor_pool
                    .get_service_by_name(service_meta.name())
                    .expect(
                        "Service mut be available in the descriptor pool. This is a runtime bug.",
                    );

                if tracing::enabled!(tracing::Level::DEBUG) {
                    service_descriptor.methods().for_each(|method| {
                        debug!(
                            rpc.service = service_meta.name(),
                            rpc.method = method.name(),
                            "Registering method"
                        )
                    });
                }

                // For the time being when updating we overwrite existing data
                if self.services.remove(service_meta.name()).is_some() {
                    info!(
                        rpc.service = service_meta.name(),
                        "Overriding existing service schemas"
                    );
                }

                self.services.insert(
                    service_meta.name,
                    ServiceSchemas::new(
                        service_descriptor,
                        service_meta.instance_type,
                        endpoint_id.clone(),
                    ),
                );
            }

            Ok(())
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
