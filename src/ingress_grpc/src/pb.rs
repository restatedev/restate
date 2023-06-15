use once_cell::sync::Lazy;
use prost_reflect::{DescriptorPool, MethodDescriptor};
use restate_service_metadata::MethodDescriptorRegistry;
use std::collections::HashMap;
use std::convert::AsRef;

pub(crate) mod grpc {
    pub(crate) mod reflection {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/grpc.reflection.v1alpha.rs"));
    }
}
pub(crate) mod restate {
    pub(crate) mod services {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.rs"));
    }
}

pub(crate) static DEV_RESTATE_DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

// TODO this is a temporary solution until we have a schema registry where we can distinguish between ingress only services
//  see https://github.com/restatedev/restate/issues/43#issuecomment-1597174972
#[derive(Clone)]
pub struct MethodDescriptorRegistryWithIngressService<MDR> {
    method_descriptor_registry: MDR,
}

impl<MDR> MethodDescriptorRegistryWithIngressService<MDR> {
    pub(crate) fn new(method_descriptor_registry: MDR) -> Self {
        Self {
            method_descriptor_registry,
        }
    }
}

impl<MDR: MethodDescriptorRegistry> MethodDescriptorRegistry
    for MethodDescriptorRegistryWithIngressService<MDR>
{
    fn resolve_method_descriptor(
        &self,
        svc_name: &str,
        method_name: &str,
    ) -> Option<MethodDescriptor> {
        if svc_name.starts_with("dev.restate") {
            return DEV_RESTATE_DESCRIPTOR_POOL
                .get_service_by_name(svc_name)
                .and_then(|s| s.methods().find(|m| m.name() == method_name));
        }
        self.method_descriptor_registry
            .resolve_method_descriptor(svc_name, method_name)
    }

    fn list_methods(&self, svc_name: &str) -> Option<HashMap<String, MethodDescriptor>> {
        if svc_name.starts_with("dev.restate") {
            return DEV_RESTATE_DESCRIPTOR_POOL
                .get_service_by_name(svc_name)
                .map(|s| s.methods().map(|m| (m.name().to_string(), m)).collect());
        }
        self.method_descriptor_registry.list_methods(svc_name)
    }
}
