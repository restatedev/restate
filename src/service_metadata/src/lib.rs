mod descriptors_registry;
mod endpoint_registry;

pub use descriptors_registry::{InMemoryMethodDescriptorRegistry, MethodDescriptorRegistry};
pub use endpoint_registry::{InMemoryServiceEndpointRegistry, ServiceEndpointRegistry};
