mod command;
mod descriptors_registry;
mod handler;
mod protocol;
mod response_dispatcher;

pub(crate) use command::*;

pub use descriptors_registry::InMemoryMethodDescriptorRegistry;
pub use descriptors_registry::MethodDescriptorRegistry;
pub use response_dispatcher::*;

use bytes::Bytes;
use common::types::ServiceInvocationId;
use http_body::combinators::UnsyncBoxBody;
use opentelemetry::Context;
use prost_reflect::MethodDescriptor;
use tonic::Status;
use tower::BoxError;

#[derive(Debug)]
pub struct IngressRequestHeaders {
    service_name: String,
    method_name: String,
    tracing_context: Context,
}

impl IngressRequestHeaders {
    pub fn new(service_name: String, method_name: String, tracing_context: Context) -> Self {
        Self {
            service_name,
            method_name,
            tracing_context,
        }
    }
}

pub type IngressRequest = (IngressRequestHeaders, Bytes);
pub type IngressResponse = Bytes;
pub type IngressError = Status;
pub type IngressResult = Result<IngressResponse, IngressError>;

#[derive(Debug, Clone)]
pub struct IngressResponseMessage {
    pub service_invocation_id: ServiceInvocationId,
    pub result: IngressResult,
}

const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_all() {
        assert_send::<IngressRequest>();
        assert_send::<IngressResult>();
        assert_send::<IngressResponseMessage>();
    }
};

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use super::*;

    pub(super) mod pb {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
    }

    use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};

    static DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

    pub(super) fn test_descriptor_pool() -> DescriptorPool {
        DescriptorPool::decode(DESCRIPTOR).unwrap()
    }

    pub(super) fn test_descriptor_registry() -> InMemoryMethodDescriptorRegistry {
        let registry = InMemoryMethodDescriptorRegistry::default();
        registry.register(greeter_service_descriptor());
        registry
    }

    pub(super) fn greeter_service_descriptor() -> ServiceDescriptor {
        test_descriptor_pool()
            .services()
            .find(|svc| svc.full_name() == "greeter.Greeter")
            .unwrap()
    }

    pub(super) fn greeter_greet_method_descriptor() -> MethodDescriptor {
        greeter_service_descriptor()
            .methods()
            .find(|m| m.name() == "Greet")
            .unwrap()
    }

    pub(super) fn greeter_get_count_method_descriptor() -> MethodDescriptor {
        greeter_service_descriptor()
            .methods()
            .find(|m| m.name() == "GetCount")
            .unwrap()
    }
}
