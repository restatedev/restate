mod descriptors_registry;

pub use descriptors_registry::InMemoryMethodDescriptorRegistry;

use prost_reflect::MethodDescriptor;

/// Trait to resolve the [`MethodDescriptor`] of given service method.
trait MethodDescriptorRegistry {
    fn resolve_method_descriptor(
        &self,
        svc_name: &str,
        method_name: &str,
    ) -> Option<MethodDescriptor>;
}
