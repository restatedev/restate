use std::collections::HashMap;

use common::retry_policy::RetryPolicy;
use http::header::{HeaderName, HeaderValue};
use http::Uri;
use service_key_extractor::ServiceInstanceType;

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

    pub fn additional_headers(&self) -> &HashMap<HeaderName, HeaderValue> {
        &self.delivery_options.additional_headers
    }
}

#[derive(Debug, Clone)]
pub struct ServiceMetadata {
    name: String,
    instance_type: ServiceInstanceType,
}

impl ServiceMetadata {
    pub fn new(name: String, instance_type: ServiceInstanceType) -> Self {
        Self {
            name,
            instance_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_type(&self) -> &ServiceInstanceType {
        &self.instance_type
    }
}
