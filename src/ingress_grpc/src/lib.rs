mod dispatcher;
mod handler;
mod options;
mod pb;
mod protocol;
mod reflection;
mod server;

pub use dispatcher::{IngressDispatcherLoop, IngressDispatcherLoopError};
pub use options::Options;
pub use pb::MethodDescriptorRegistryWithIngressService;
pub use reflection::{ReflectionRegistry, RegistrationError};
pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use opentelemetry::Context;
use restate_futures_util::command::*;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{IngressId, PeerId, ServiceInvocationId};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationResponseSink, SpanRelation};
use restate_types::message::{AckKind, MessageIndex};
use tokio::sync::mpsc;
use tonic::Status;
use tracing::Span;

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

type IngressResult = Result<IngressResponse, IngressError>;

pub type IngressRequest = (IngressRequestHeaders, Bytes);
pub type IngressResponse = Bytes;
pub type IngressError = InvocationError;

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
    MessageAck(MessageIndex),
}

impl IngressInput {
    pub fn message_ack(seq_number: MessageIndex) -> Self {
        IngressInput::MessageAck(seq_number)
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
    KeyExtraction(anyhow::Error),
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

    pub fn key_extraction_error(source: impl Into<anyhow::Error>) -> Self {
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
        response_sink: Option<ServiceInvocationResponseSink>,
        span_relation: SpanRelation,
    ) -> Result<(ServiceInvocation, Span), ServiceInvocationFactoryError>;
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
    use restate_service_metadata::InMemoryMethodDescriptorRegistry;

    static DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

    pub(super) fn test_descriptor_pool() -> DescriptorPool {
        DescriptorPool::decode(DESCRIPTOR).unwrap()
    }

    pub(super) fn test_descriptor_registry() -> InMemoryMethodDescriptorRegistry {
        let registry = InMemoryMethodDescriptorRegistry::default();
        registry.register(greeter_service_descriptor());
        registry.register(ingress_service_descriptor());
        registry
    }

    pub(super) fn ingress_service_descriptor() -> ServiceDescriptor {
        crate::pb::DEV_RESTATE_DESCRIPTOR_POOL
            .get_service_by_name("dev.restate.Ingress")
            .unwrap()
    }

    pub(super) fn ingress_invoke_method_descriptor() -> MethodDescriptor {
        ingress_service_descriptor()
            .methods()
            .find(|m| m.name() == "Invoke")
            .unwrap()
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
