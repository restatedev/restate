use super::Schemas;

use crate::schemas_impl::{ServiceLocation, ServiceSchemas};
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};

impl ServiceMetadataResolver for Schemas {
    fn resolve_latest_service_metadata(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<ServiceMetadata> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            map_to_service_metadata(service_name.as_ref(), service_schemas)
        })
        .flatten()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        let schemas = self.0.load();
        schemas
            .services
            .iter()
            .filter_map(|(service_name, service_schemas)| {
                map_to_service_metadata(service_name, service_schemas)
            })
            .collect()
    }

    fn is_service_public(&self, service_name: impl AsRef<str>) -> Option<bool> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            service_schemas.location.is_ingress_available()
        })
    }
}

fn map_to_service_metadata(
    service_name: &str,
    service_schemas: &ServiceSchemas,
) -> Option<ServiceMetadata> {
    match &service_schemas.location {
        ServiceLocation::IngressOnly => None, // We filter out from this interface ingress only services
        ServiceLocation::ServiceEndpoint {
            latest_endpoint,
            public,
        } => Some(ServiceMetadata {
            name: service_name.to_string(),
            methods: service_schemas.methods.keys().cloned().collect(),
            instance_type: (&service_schemas.instance_type).into(),
            endpoint_id: latest_endpoint.clone(),
            revision: service_schemas.revision,
            public: *public,
        }),
    }
}
