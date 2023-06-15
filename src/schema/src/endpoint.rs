use super::Schemas;

// TODO we should move this type to restate_types
use restate_service_metadata::EndpointMetadata;

// TODO Perhaps to modify and move to restate_types
pub type EndpointId = String;

pub trait EndpointMetadataResolver {
    fn resolve_latest_endpoint_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<EndpointId>;

    fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata>;
}

impl EndpointMetadataResolver for Schemas {
    fn resolve_latest_endpoint_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<EndpointId> {
        todo!()
    }

    fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata> {
        todo!()
    }
}
