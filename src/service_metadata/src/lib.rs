use common::retry_policy::RetryPolicy;
use hyper::header::{HeaderName, HeaderValue};
use hyper::Uri;
use std::collections::HashMap;

mod descriptors_registry;
mod endpoint_registry;

pub use descriptors_registry::{InMemoryMethodDescriptorRegistry, MethodDescriptorRegistry};
pub use endpoint_registry::{InMemoryServiceEndpointRegistry, ServiceEndpointRegistry};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone, Default)]
pub struct DeliveryOptions {
    additional_headers: HashMap<HeaderName, HeaderValue>,
    retry_policy: Option<RetryPolicy>,
}

impl DeliveryOptions {
    // false positive because of inner Bytes field of HeaderName
    #[allow(clippy::mutable_key_type)]
    pub fn new(
        additional_headers: HashMap<HeaderName, HeaderValue>,
        retry_policy: Option<RetryPolicy>,
    ) -> Self {
        Self {
            additional_headers,
            retry_policy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointMetadata {
    address: Uri,
    protocol_type: ProtocolType,
    delivery_options: DeliveryOptions,
}

impl EndpointMetadata {
    pub fn new(
        address: Uri,
        protocol_type: ProtocolType,
        delivery_options: DeliveryOptions,
    ) -> Self {
        Self {
            address,
            protocol_type,
            delivery_options,
        }
    }

    pub fn address(&self) -> &Uri {
        &self.address
    }

    pub fn protocol_type(&self) -> ProtocolType {
        self.protocol_type
    }

    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.delivery_options.retry_policy.as_ref()
    }

    // false positive because of inner Bytes field of HeaderName
    #[allow(clippy::mutable_key_type)]
    pub fn additional_headers(&self) -> &HashMap<HeaderName, HeaderValue> {
        &self.delivery_options.additional_headers
    }
}
