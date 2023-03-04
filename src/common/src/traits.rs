use crate::types::{
    ServiceInvocation, ServiceInvocationFactoryError, ServiceInvocationResponseSink, SpanRelation,
};
use bytes::Bytes;
use std::hash;

/// Trait for messages that have a routing key
pub trait KeyedMessage {
    type RoutingKey<'a>: hash::Hash
    where
        Self: 'a;
    /// Returns a reference to the bytes of a keyed message
    fn routing_key(&self) -> Self::RoutingKey<'_>;
}

/// Trait to create a new [`ServiceInvocation`].
///
/// This trait can be used by ingresses and partition processors to
/// abstract the logic to perform key extraction and id generation.
pub trait ServiceInvocationFactory {
    /// Create a new service invocation.
    fn create(
        &self,
        service_name: &str,
        method_name: &str,
        request_payload: Bytes,
        response_sink: ServiceInvocationResponseSink,
        span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError>;
}
