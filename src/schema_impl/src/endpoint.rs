use super::Schemas;

use restate_schema_api::endpoint::EndpointMetadataResolver;
use restate_types::service_endpoint::{EndpointId, EndpointMetadata};

impl EndpointMetadataResolver for Schemas {
    fn resolve_latest_endpoint_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<EndpointMetadata> {
        let schemas = self.0.load();
        let service = schemas.services.get(service_name.as_ref())?;
        schemas
            .endpoints
            .get(&service.latest_endpoint)
            .map(|schemas| schemas.metadata.clone())
    }

    fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata> {
        let schemas = self.0.load();
        schemas
            .endpoints
            .get(endpoint_id)
            .map(|schemas| schemas.metadata.clone())
    }
}
