use super::Schemas;

use restate_types::service_endpoint::{EndpointId, EndpointMetadata};

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
