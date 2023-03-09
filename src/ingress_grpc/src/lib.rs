mod descriptors_registry;
mod dispatcher;
mod handler;
mod options;
mod protocol;
mod server;

pub use descriptors_registry::InMemoryMethodDescriptorRegistry;
pub use descriptors_registry::MethodDescriptorRegistry;
pub use dispatcher::IngressDispatcherLoop;
pub use options::Options;
pub use server::HyperServerIngress;
pub use server::StartSignal;

use bytes::Bytes;
use bytestring::ByteString;
use common::traits::KeyedMessage;
use common::types::{
    AckKind, ServiceInvocation, ServiceInvocationId, ServiceInvocationResponseSink, SpanRelation,
};
use common::utils::GenericError;
use futures_util::command::*;
use opentelemetry::Context;
use tokio::sync::mpsc;
use tonic::Status;

// --- Data model used by handlers and protocol

#[derive(Debug, Clone)]
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

type IngressResult = Result<IngressResponse, Status>;

pub type IngressRequest = (IngressRequestHeaders, Bytes);
pub type IngressResponse = Bytes;

#[derive(Debug, Clone)]
pub struct IngressError {
    code: i32,
    error_msg: ByteString,
}

impl IngressError {
    pub fn new(code: i32, error_msg: impl Into<ByteString>) -> Self {
        Self {
            code,
            error_msg: error_msg.into(),
        }
    }
}

impl From<IngressError> for Status {
    fn from(value: IngressError) -> Self {
        Status::new(value.code.into(), value.error_msg)
    }
}

// --- Input and output messages to interact with ingress

#[derive(Debug, Clone)]
pub struct IngressResponseMessage {
    pub service_invocation_id: ServiceInvocationId,
    pub result: Result<IngressResponse, IngressError>,
}

#[derive(Debug)]
pub enum IngressInput {
    Response(IngressResponseMessage),
    MessageAck(AckKind),
}

#[derive(Debug)]
pub struct IngressOutput(ServiceInvocation);

impl IngressOutput {
    pub fn new(service_invocation: ServiceInvocation) -> Self {
        Self(service_invocation)
    }

    pub fn into_inner(self) -> ServiceInvocation {
        self.0
    }
}

impl KeyedMessage for IngressOutput {
    type RoutingKey<'a> = &'a Bytes;

    fn routing_key(&self) -> &Bytes {
        &self.0.id.service_id.key
    }
}

// --- Channels

pub type DispatcherCommandSender = UnboundedCommandSender<ServiceInvocation, IngressResult>;
pub type IngressInputReceiver = mpsc::Receiver<IngressInput>;
pub type IngressInputSender = mpsc::Sender<IngressInput>;

// --- Traits

#[derive(Debug, thiserror::Error)]
pub enum ServiceInvocationFactoryError {
    #[error("service method '{service_name}/{method_name}' is unknown")]
    UnknownServiceMethod {
        service_name: String,
        method_name: String,
    },
    #[error("failed extracting the key from the request payload: {0}")]
    KeyExtraction(GenericError),
}

impl ServiceInvocationFactoryError {
    pub fn unknown_service_method(
        service_name: impl Into<String>,
        method_name: impl Into<String>,
    ) -> Self {
        ServiceInvocationFactoryError::UnknownServiceMethod {
            service_name: service_name.into(),
            method_name: method_name.into(),
        }
    }

    pub fn key_extraction_error(source: impl Into<GenericError>) -> Self {
        ServiceInvocationFactoryError::KeyExtraction(source.into())
    }
}

/// Trait to create a new [`ServiceInvocation`].
///
/// This trait can be used by ingresses and partition processors to
/// abstract the logic to perform key extraction and id generation.
pub trait ServiceInvocationFactory {
    /// Create a new service invocation.
    // TODO: Probably needs to be asynchronous: https://github.com/restatedev/restate/issues/91
    fn create(
        &self,
        service_name: &str,
        method_name: &str,
        request_payload: Bytes,
        response_sink: ServiceInvocationResponseSink,
        span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError>;
}

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
