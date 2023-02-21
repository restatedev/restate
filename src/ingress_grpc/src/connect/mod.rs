mod descriptors_registry;
mod content_type;

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

#[cfg(test)]
mod mocks {
        use super::*;

        pub mod pb {
            #![allow(warnings)]
            #![allow(clippy::all)]
            #![allow(unknown_lints)]
            include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
        }

        use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};

        static DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

        pub fn test_descriptor_pool() -> DescriptorPool {
            DescriptorPool::decode(DESCRIPTOR).unwrap()
        }

        pub fn test_descriptor_registry() -> InMemoryMethodDescriptorRegistry {
            let registry = InMemoryMethodDescriptorRegistry::default();
            registry.register(greeter_service_descriptor());
            registry
        }

        pub fn greeter_service_descriptor() -> ServiceDescriptor {
            test_descriptor_pool()
                .services()
                .find(|svc| svc.full_name() == "greeter.Greeter")
                .unwrap()
        }

        pub fn greeter_greet_method_descriptor() -> MethodDescriptor {
            greeter_service_descriptor()
                .methods()
                .find(|m| m.name() == "Greet")
                .unwrap()
        }

        pub fn greeter_get_count_method_descriptor() -> MethodDescriptor {
            greeter_service_descriptor()
                .methods()
                .find(|m| m.name() == "GetCount")
                .unwrap()
        }
}