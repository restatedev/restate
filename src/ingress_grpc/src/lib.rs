mod dispatcher;
mod handler;
mod options;
mod protocol;
mod server;

use bytes::Bytes;
use bytestring::ByteString;
use common::types::{
    AckKind, IngressId, MessageIndex, PeerId, ServiceInvocation, ServiceInvocationId,
    ServiceInvocationResponseSink, SpanRelation,
};
use common::utils::GenericError;
pub use dispatcher::{IngressDispatcherLoop, IngressDispatcherLoopError};
use futures_util::command::*;
use opentelemetry::Context;
pub use options::Options;
pub use server::{HyperServerIngress, StartSignal};
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
    pub ack_target: AckTarget,
}

#[derive(Debug, Clone)]
pub struct AckTarget {
    pub shuffle_target: PeerId,
    pub msg_index: MessageIndex,
}

impl AckTarget {
    pub fn new(shuffle_target: PeerId, msg_index: MessageIndex) -> Self {
        Self {
            shuffle_target,
            msg_index,
        }
    }

    fn acknowledge(self) -> AckResponse {
        AckResponse {
            shuffle_target: self.shuffle_target,
            kind: AckKind::Acknowledge(self.msg_index),
        }
    }
}

#[derive(Debug)]
pub enum IngressInput {
    Response(IngressResponseMessage),
    MessageAck(AckKind),
}

impl IngressInput {
    pub fn message_ack(ack_kind: AckKind) -> Self {
        IngressInput::MessageAck(ack_kind)
    }

    pub fn response(response: IngressResponseMessage) -> Self {
        IngressInput::Response(response)
    }
}

#[derive(Debug)]
pub enum IngressOutput {
    Invocation {
        service_invocation: ServiceInvocation,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    },
    Ack(AckResponse),
}

#[derive(Debug)]
pub struct AckResponse {
    pub shuffle_target: PeerId,
    pub kind: AckKind,
}

impl IngressOutput {
    pub fn service_invocation(
        service_invocation: ServiceInvocation,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    ) -> Self {
        Self::Invocation {
            service_invocation,
            ingress_id,
            msg_index,
        }
    }

    pub fn shuffle_ack(ack_response: AckResponse) -> Self {
        Self::Ack(ack_response)
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
    pub(super) mod pb {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
    }

    use prost_reflect::{DescriptorPool, MethodDescriptor, ServiceDescriptor};
    use service_metadata::InMemoryMethodDescriptorRegistry;

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
