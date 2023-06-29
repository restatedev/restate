//! Restate interacts with Service endpoints to process invocations. This module contains entities defining service endpoints.

use crate::retries::RetryPolicy;
use http::header::{HeaderName, HeaderValue};
use http::Uri;
use std::collections::HashMap;

pub type EndpointId = String;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
pub struct DeliveryOptions {
    #[cfg_attr(
        feature = "serde",
        serde(
            with = "serde_with::As::<serde_with::TryFromInto<header_map_serde::HeaderMapSerde>>"
        )
    )]
    #[cfg_attr(feature = "serde_schema", schemars(with = "HashMap<String, String>"))]
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

#[cfg(feature = "serde")]
mod header_map_serde {
    use super::*;

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
        type Error = anyhow::Error;

        fn try_from(value: HeaderMapSerde) -> Result<Self, Self::Error> {
            value
                .0
                .into_iter()
                .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                .collect::<Result<HashMap<_, _>, anyhow::Error>>()
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde_with::serde_as)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
pub struct EndpointMetadata {
    #[cfg_attr(
        feature = "serde",
        serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
    )]
    #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
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

    pub fn id(&self) -> EndpointId {
        use base64::Engine;

        // For the time being we generate this from the URI
        // We use only authority and path, as those uniquely identify the endpoint.
        let authority_and_path = format!(
            "{}{}",
            self.address.authority().expect("Must have authority"),
            self.address.path()
        );
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(authority_and_path.as_bytes())
    }
}
