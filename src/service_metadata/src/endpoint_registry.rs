use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::EndpointMetadata;

/// Trait for resolving the service endpoint information
pub trait ServiceEndpointRegistry {
    /// Resolve for a given service name the [`EndpointMetadata`].
    fn resolve_endpoint(&self, service_name: impl AsRef<str>) -> Option<EndpointMetadata>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryServiceEndpointRegistry {
    registry: Arc<ArcSwap<HashMap<String, EndpointMetadata>>>,
}

impl InMemoryServiceEndpointRegistry {
    pub fn register_service_endpoint(
        &mut self,
        service_name: impl Into<String>,
        endpoint_metadata: EndpointMetadata,
    ) {
        let mut updated_registry = HashMap::clone(&self.registry.load());
        updated_registry.insert(service_name.into(), endpoint_metadata);

        self.registry.store(Arc::new(updated_registry));
    }
}

impl ServiceEndpointRegistry for InMemoryServiceEndpointRegistry {
    fn resolve_endpoint(&self, service_name: impl AsRef<str>) -> Option<EndpointMetadata> {
        self.registry.load().get(service_name.as_ref()).cloned()
    }
}
