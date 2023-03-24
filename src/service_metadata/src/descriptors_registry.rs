use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use prost_reflect::{MethodDescriptor, ServiceDescriptor};

/// Trait to resolve the [`MethodDescriptor`] of given service method.
pub trait MethodDescriptorRegistry {
    fn resolve_method_descriptor(
        &self,
        svc_name: &str,
        method_name: &str,
    ) -> Option<MethodDescriptor>;

    fn list_methods(&self, svc_name: &str) -> Option<HashMap<String, MethodDescriptor>>;
}

type ServiceMethods = HashMap<String, MethodDescriptor>;

#[derive(Clone, Default)]
pub struct InMemoryMethodDescriptorRegistry {
    services: Arc<ArcSwap<HashMap<String, ServiceMethods>>>,
}

impl InMemoryMethodDescriptorRegistry {
    pub fn register(&self, service_descriptor: ServiceDescriptor) {
        let methods = service_descriptor
            .methods()
            .map(|f| (f.name().to_string(), f))
            .collect();

        let services = self.services.load();

        let mut new_services = HashMap::clone(&services);
        new_services.insert(service_descriptor.full_name().to_string(), methods);

        self.services.store(Arc::new(new_services));
    }

    pub fn remove(&self, name: impl AsRef<str>) {
        let services = self.services.load();

        let name = name.as_ref();
        if services.contains_key(name) {
            let mut new_services = HashMap::clone(&services);
            new_services.remove(name);

            self.services.store(Arc::new(new_services));
        }
    }
}

impl MethodDescriptorRegistry for InMemoryMethodDescriptorRegistry {
    fn resolve_method_descriptor(
        &self,
        svc_name: &str,
        method_name: &str,
    ) -> Option<MethodDescriptor> {
        let services = self.services.load();

        services
            .get(svc_name)
            .and_then(|svc_methods| svc_methods.get(method_name))
            // We clone it in order to not holding the reference to self.services
            // MethodDescriptor just holds an Arc, so copying is cheap
            .cloned()
    }

    fn list_methods(&self, svc_name: &str) -> Option<HashMap<String, MethodDescriptor>> {
        self.services.load().get(svc_name).cloned()
    }
}
