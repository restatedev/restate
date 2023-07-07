use super::Schemas;

use crate::schemas_impl::ServiceLocation;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};

impl ServiceMetadataResolver for Schemas {
    fn resolve_latest_service_metadata(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<ServiceMetadata> {
        self.use_service_schema(
            service_name.as_ref(),
            |service_schemas| match &service_schemas.location {
                ServiceLocation::IngressOnly => None,
                ServiceLocation::ServiceEndpoint { latest_endpoint } => Some(ServiceMetadata {
                    name: service_name.as_ref().to_string(),
                    methods: service_schemas.methods.keys().cloned().collect(),
                    instance_type: (&service_schemas.instance_type).into(),
                    endpoint: latest_endpoint.clone(),
                }),
            },
        )
        .flatten()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        let schemas = self.0.load();
        schemas
            .services
            .iter()
            .filter_map(
                |(service_name, service_schemas)| match &service_schemas.location {
                    ServiceLocation::IngressOnly => None,
                    ServiceLocation::ServiceEndpoint { latest_endpoint } => Some(ServiceMetadata {
                        name: service_name.clone(),
                        methods: service_schemas.methods.keys().cloned().collect(),
                        instance_type: (&service_schemas.instance_type).into(),
                        endpoint: latest_endpoint.clone(),
                    }),
                },
            )
            .collect()
    }
}
