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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeliveryOptions {
    #[cfg_attr(
        feature = "serde",
        serde(
            with = "serde_with::As::<serde_with::TryFromInto<header_map_serde::HeaderMapSerde>>"
        )
    )]
    additional_headers: HashMap<HeaderName, HeaderValue>,
    retry_policy: Option<RetryPolicy>,
}

#[cfg(feature = "serde")]
mod header_map_serde {
    use super::*;

    use common::utils::GenericError;
    use http::header::ToStrError;

    // Proxy type to implement HashMap<HeaderName, HeaderValue> ser/de
    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    pub(super) struct HeaderMapSerde(HashMap<String, String>);

    impl TryFrom<HashMap<HeaderName, HeaderValue>> for HeaderMapSerde {
        type Error = ToStrError;

        fn try_from(value: HashMap<HeaderName, HeaderValue>) -> Result<Self, Self::Error> {
            Ok(HeaderMapSerde(
                value
                    .into_iter()
                    .map(|(k, v)| Ok((k.to_string(), v.to_str()?.to_string())))
                    .collect::<Result<HashMap<_, _>, _>>()?,
            ))
        }
    }
    impl TryFrom<HeaderMapSerde> for HashMap<HeaderName, HeaderValue> {
        type Error = GenericError;

        fn try_from(value: HeaderMapSerde) -> Result<Self, Self::Error> {
            value
                .0
                .into_iter()
                .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                .collect::<Result<HashMap<_, _>, GenericError>>()
        }
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

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde_with::serde_as)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EndpointMetadata {
    #[cfg_attr(
        feature = "serde",
        serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
    )]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
