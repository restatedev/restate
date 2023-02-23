mod command;
mod descriptors_registry;
mod response_dispatcher;

pub(crate) use command::*;

pub use descriptors_registry::InMemoryMethodDescriptorRegistry;
pub use descriptors_registry::MethodDescriptorRegistry;
pub use response_dispatcher::*;

use bytes::Bytes;
use common::types::ServiceInvocationId;
use opentelemetry::Context;
use prost_reflect::MethodDescriptor;
use tonic::Status;

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
