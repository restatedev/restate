// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::pb::protocol;
use crate::{MAX_SERVICE_PROTOCOL_VERSION, MIN_SERVICE_PROTOCOL_VERSION};
use bytes::Bytes;
use codederror::CodedError;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::http::response::Parts as ResponseParts;
use hyper::http::uri::PathAndQuery;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{Body, HeaderMap, StatusCode};
use restate_errors::{META0003, META0012};
use restate_schema_api::deployment::ProtocolType;
use restate_schema_api::MAX_SERVICE_PROTOCOL_VERSION_VALUE;
use restate_service_client::{Endpoint, Parts, Request, ServiceClient, ServiceClientError};
use restate_types::retries::{RetryIter, RetryPolicy};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::RangeInclusive;
use tracing::warn;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

const DISCOVER_PATH: &str = "/discover";

pub mod schema {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]

    include!(concat!(env!("OUT_DIR"), "/deployment.rs"));

    impl From<ServiceType> for restate_types::invocation::ServiceType {
        fn from(value: ServiceType) -> Self {
            match value {
                ServiceType::VirtualObject => restate_types::invocation::ServiceType::VirtualObject,
                ServiceType::Service => restate_types::invocation::ServiceType::Service,
                ServiceType::Workflow => restate_types::invocation::ServiceType::Workflow,
            }
        }
    }
}

#[derive(Clone)]
pub struct DiscoverEndpoint(Endpoint, HashMap<HeaderName, HeaderValue>);

impl DiscoverEndpoint {
    pub fn new(address: Endpoint, additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        Self(address, additional_headers)
    }

    pub fn into_inner(self) -> (Endpoint, HashMap<HeaderName, HeaderValue>) {
        (self.0, self.1)
    }

    pub fn address(&self) -> &Endpoint {
        &self.0
    }

    fn request(&self) -> Request<Body> {
        let mut headers = HeaderMap::from_iter([(ACCEPT, APPLICATION_JSON)]);
        headers.extend(self.1.clone());
        let path = PathAndQuery::from_static(DISCOVER_PATH);
        Request::new(Parts::new(self.0.clone(), path, headers), Body::empty())
    }
}

#[derive(Debug)]
pub struct DiscoveredMetadata {
    pub protocol_type: ProtocolType,
    pub services: Vec<schema::Service>,
    // type is i32 because the generated ServiceProtocolVersion enum uses this as its representation
    // and we need to represent unknown later versions
    pub supported_protocol_versions: RangeInclusive<i32>,
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum DiscoveryError {
    // Errors most likely related to SDK bugs
    #[error("received a bad response from the SDK: {0}")]
    #[code(unknown)]
    BadResponse(Cow<'static, str>),
    #[error(
        "received a bad response from the SDK that cannot be decoded: {0}. Discovery response: {}",
        String::from_utf8_lossy(.1)
    )]
    #[code(unknown)]
    Decode(#[source] serde_json::Error, Bytes),

    // Network related retryable errors
    #[error("bad status code: {0}")]
    #[code(META0003)]
    BadStatusCode(u16),
    #[error("client error: {0}")]
    #[code(META0003)]
    Client(#[from] ServiceClientError),
    #[error("unsupported service protocol versions: [{min_version}, {max_version}]. Supported versions by this runtime are [{}, {}]", i32::from(MIN_SERVICE_PROTOCOL_VERSION), i32::from(MAX_SERVICE_PROTOCOL_VERSION))]
    #[code(META0012)]
    UnsupportedServiceProtocol { min_version: i32, max_version: i32 },
}

impl DiscoveryError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            DiscoveryError::BadStatusCode(status) => matches!(
                StatusCode::from_u16(*status).expect("should be valid status code"),
                StatusCode::REQUEST_TIMEOUT
                    | StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ),
            DiscoveryError::Client(client_error) => client_error.is_retryable(),
            DiscoveryError::BadResponse(_)
            | DiscoveryError::Decode(_, _)
            | DiscoveryError::UnsupportedServiceProtocol { .. } => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceDiscovery {
    retry_policy: RetryPolicy,
    client: ServiceClient,
}

impl ServiceDiscovery {
    pub fn new(retry_policy: RetryPolicy, client: ServiceClient) -> Self {
        Self {
            retry_policy,
            client,
        }
    }
}

impl ServiceDiscovery {
    pub async fn discover(
        &self,
        endpoint: &DiscoverEndpoint,
    ) -> Result<DiscoveredMetadata, DiscoveryError> {
        let retry_policy = self.retry_policy.clone().into_iter();
        let (mut parts, body) = Self::invoke_discovery_endpoint(
            &self.client,
            endpoint.address(),
            || endpoint.request(),
            retry_policy,
        )
        .await?;

        // Validate response parts.
        // No need to retry these: if the validation fails, they're sdk bugs.
        let content_type = parts.headers.remove(CONTENT_TYPE);
        match content_type {
            // False positive with Bytes field
            #[allow(clippy::borrow_interior_mutable_const)]
            Some(ct) if ct == APPLICATION_JSON => {}
            Some(ct) => {
                return Err(DiscoveryError::BadResponse(
                    format!("Bad content type header: {ct:?}").into(),
                ));
            }
            None => {
                return Err(DiscoveryError::BadResponse(format!("No content type header was specified. Expected '{APPLICATION_JSON:?}' content type.").into()))
            }
        }

        // Parse the response
        let response: schema::Endpoint =
            serde_json::from_slice(&body).map_err(|e| DiscoveryError::Decode(e, body))?;

        Self::create_discovered_metadata_from_endpoint_response(response)
    }

    fn create_discovered_metadata_from_endpoint_response(
        endpoint_response: schema::Endpoint,
    ) -> Result<DiscoveredMetadata, DiscoveryError> {
        let protocol_type = match endpoint_response.protocol_mode {
            Some(schema::ProtocolMode::BidiStream) => ProtocolType::BidiStream,
            Some(schema::ProtocolMode::RequestResponse) => ProtocolType::RequestResponse,
            None => {
                return Err(DiscoveryError::BadResponse("missing protocol mode".into()));
            }
        };

        // Sanity checks for the service protocol version
        if endpoint_response.min_protocol_version <= 0
            || endpoint_response.min_protocol_version > MAX_SERVICE_PROTOCOL_VERSION_VALUE as i64
        {
            return Err(DiscoveryError::BadResponse(
                format!(
                    "min protocol version must be in [1, {}]",
                    MAX_SERVICE_PROTOCOL_VERSION_VALUE
                )
                .into(),
            ));
        }

        if endpoint_response.max_protocol_version <= 0
            || endpoint_response.max_protocol_version > MAX_SERVICE_PROTOCOL_VERSION_VALUE as i64
        {
            return Err(DiscoveryError::BadResponse(
                format!(
                    "max protocol version must be in [1, {}]",
                    MAX_SERVICE_PROTOCOL_VERSION_VALUE
                )
                .into(),
            ));
        }

        if endpoint_response.min_protocol_version > endpoint_response.max_protocol_version {
            return Err(DiscoveryError::BadResponse(
                format!("Expected min protocol version to be <= max protocol version. Received min protocol version '{}', max protocol version '{}'", endpoint_response.min_protocol_version, endpoint_response.max_protocol_version).into(),
            ));
        }

        let min_version = endpoint_response.min_protocol_version as i32;
        let max_version = endpoint_response.max_protocol_version as i32;

        if !protocol::ServiceProtocolVersion::is_supported(min_version, max_version) {
            return Err(DiscoveryError::UnsupportedServiceProtocol {
                min_version,
                max_version,
            });
        }

        Ok(DiscoveredMetadata {
            protocol_type,
            services: endpoint_response.services,
            // we need to store the raw representation since the runtime might not know the latest
            // version yet.
            supported_protocol_versions: min_version..=max_version,
        })
    }

    async fn invoke_discovery_endpoint(
        client: &ServiceClient,
        address: impl Display,
        build_request: impl Fn() -> Request<Body>,
        mut retry_iter: RetryIter,
    ) -> Result<(ResponseParts, Bytes), DiscoveryError> {
        loop {
            let response_fut = client.call(build_request());
            let response = async {
                let (parts, body) = response_fut
                    .await
                    .map_err(Into::<DiscoveryError>::into)?
                    .into_parts();

                if !parts.status.is_success() {
                    return Err(DiscoveryError::BadStatusCode(parts.status.as_u16()));
                }

                Ok((
                    parts,
                    hyper::body::to_bytes(body).await.map_err(|err| {
                        DiscoveryError::Client(ServiceClientError::Http(err.into()))
                    })?,
                ))
            };

            let e = match response.await {
                Ok(response) => {
                    // Discovery succeeded
                    return Ok(response);
                }
                Err(e) => e,
            };

            // Discovery failed
            if e.is_retryable() {
                if let Some(next_retry_interval) = retry_iter.next() {
                    warn!(
                        "Error when discovering deployment at address '{}'. Retrying in {} seconds: {}",
                        address,
                        next_retry_interval.as_secs(),
                        e
                    );
                    tokio::time::sleep(next_retry_interval).await;
                    continue;
                }
            }

            return Err(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::discovery::schema::ProtocolMode;
    use crate::discovery::{schema, DiscoveryError, ServiceDiscovery};
    use crate::MAX_SERVICE_PROTOCOL_VERSION;

    #[test]
    fn fail_on_invalid_min_protocol_version_with_bad_response() {
        let response = schema::Endpoint {
            min_protocol_version: 0,
            max_protocol_version: 1,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(response),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_on_invalid_max_protocol_version_with_bad_response() {
        let response = schema::Endpoint {
            min_protocol_version: 1,
            max_protocol_version: i64::MAX,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(response),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_on_max_protocol_version_smaller_than_min_protocol_version_with_bad_response() {
        let response = schema::Endpoint {
            min_protocol_version: 10,
            max_protocol_version: 9,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(response),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_with_unsupported_protocol_version() {
        let unsupported_version = i32::from(MAX_SERVICE_PROTOCOL_VERSION) + 1;
        let response = schema::Endpoint {
            min_protocol_version: unsupported_version as i64,
            max_protocol_version: unsupported_version as i64,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(
            matches!(ServiceDiscovery::create_discovered_metadata_from_endpoint_response(response), Err(DiscoveryError::UnsupportedServiceProtocol { min_version, max_version }) if min_version == unsupported_version && max_version == unsupported_version )
        );
    }
}
