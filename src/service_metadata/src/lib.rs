use common::retry_policy::RetryPolicy;
use http::header::{HeaderName, HeaderValue};
use http::Uri;
use serde::Serialize;
use serde_with::serde_as;
use std::collections::HashMap;

mod descriptors_registry;
mod endpoint_registry;

pub use descriptors_registry::{InMemoryMethodDescriptorRegistry, MethodDescriptorRegistry};
pub use endpoint_registry::{InMemoryServiceEndpointRegistry, ServiceEndpointRegistry};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeliveryOptions {
    #[serde(with = "header_map")]
    additional_headers: HashMap<HeaderName, HeaderValue>,
    retry_policy: Option<RetryPolicy>,
}

mod header_map {
    use http::{HeaderName, HeaderValue};
    use serde::Serializer;
    use std::collections::HashMap;

    #[allow(clippy::mutable_key_type)]
    pub fn serialize<S: Serializer>(
        headers: &HashMap<HeaderName, HeaderValue>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        ser.collect_map(
            headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.to_str().unwrap())),
        )
    }
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

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct EndpointMetadata {
    #[serde_as(as = "serde_with::DisplayFromStr")]
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
