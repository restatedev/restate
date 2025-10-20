// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::LazyLock;

use bytes::Bytes;
use codederror::CodedError;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::response::Parts as ResponseParts;
use http::uri::{PathAndQuery, Scheme};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Version};
use http_body_util::BodyExt;
use http_body_util::Empty;
use itertools::Itertools;
use strum::IntoEnumIterator;
use tracing::{debug, warn};

use restate_errors::{META0003, META0012, META0013, META0014, META0015};
use restate_service_client::{Endpoint, Method, Parts, Request, ServiceClient, ServiceClientError};
use restate_types::deployment::DeploymentAddress;
use restate_types::endpoint_manifest;
use restate_types::errors::GenericError;
use restate_types::retries::{RetryIter, RetryPolicy};
use restate_types::schema::deployment::{EndpointLambdaCompression, ProtocolType};
use restate_types::schema::registry::{
    DeploymentConnectionParameters, DiscoveryClient, DiscoveryRequest, DiscoveryResponse,
};
use restate_types::service_discovery::{
    MAX_SERVICE_DISCOVERY_PROTOCOL_VERSION, MIN_SERVICE_DISCOVERY_PROTOCOL_VERSION,
    ServiceDiscoveryProtocolVersion,
};
use restate_types::service_protocol::{
    MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION, MAX_SERVICE_PROTOCOL_VERSION_VALUE,
    MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION, ServiceProtocolVersion,
};

// TODO(slinkydeveloper) move this code somewhere else!

#[allow(clippy::declare_interior_mutable_const)]
const X_RESTATE_SERVER: HeaderName = HeaderName::from_static("x-restate-server");

const SERVICE_DISCOVERY_PROTOCOL_V1_HEADER_VALUE: &str =
    "application/vnd.restate.endpointmanifest.v1+json";
const SERVICE_DISCOVERY_PROTOCOL_V2_HEADER_VALUE: &str =
    "application/vnd.restate.endpointmanifest.v2+json";
const SERVICE_DISCOVERY_PROTOCOL_V3_HEADER_VALUE: &str =
    "application/vnd.restate.endpointmanifest.v3+json";
const SERVICE_DISCOVERY_PROTOCOL_V4_HEADER_VALUE: &str =
    "application/vnd.restate.endpointmanifest.v4+json";
static SUPPORTED_SERVICE_DISCOVERY_PROTOCOL_VERSIONS: LazyLock<HeaderValue> = LazyLock::new(|| {
    let supported_versions = ServiceDiscoveryProtocolVersion::iter()
        .skip_while(|version| version < &MIN_SERVICE_DISCOVERY_PROTOCOL_VERSION)
        .take_while(|version| version <= &MAX_SERVICE_DISCOVERY_PROTOCOL_VERSION)
        .map(service_discovery_protocol_to_content_type)
        .join(", ");
    HeaderValue::from_str(&supported_versions)
        .expect("header value to contain only valid characters")
});

const DISCOVER_PATH: &str = "/discover";

fn service_discovery_protocol_to_content_type(
    version: ServiceDiscoveryProtocolVersion,
) -> &'static str {
    match version {
        ServiceDiscoveryProtocolVersion::Unspecified => {
            unreachable!("unspecified protocol version should never be used")
        }
        ServiceDiscoveryProtocolVersion::V1 => SERVICE_DISCOVERY_PROTOCOL_V1_HEADER_VALUE,
        ServiceDiscoveryProtocolVersion::V2 => SERVICE_DISCOVERY_PROTOCOL_V2_HEADER_VALUE,
        ServiceDiscoveryProtocolVersion::V3 => SERVICE_DISCOVERY_PROTOCOL_V3_HEADER_VALUE,
        ServiceDiscoveryProtocolVersion::V4 => SERVICE_DISCOVERY_PROTOCOL_V4_HEADER_VALUE,
    }
}

fn parse_service_discovery_protocol_version_from_content_type(
    content_type: &str,
) -> Option<ServiceDiscoveryProtocolVersion> {
    match content_type {
        SERVICE_DISCOVERY_PROTOCOL_V1_HEADER_VALUE => Some(ServiceDiscoveryProtocolVersion::V1),
        SERVICE_DISCOVERY_PROTOCOL_V2_HEADER_VALUE => Some(ServiceDiscoveryProtocolVersion::V2),
        SERVICE_DISCOVERY_PROTOCOL_V3_HEADER_VALUE => Some(ServiceDiscoveryProtocolVersion::V3),
        SERVICE_DISCOVERY_PROTOCOL_V4_HEADER_VALUE => Some(ServiceDiscoveryProtocolVersion::V4),
        _ => None,
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum DiscoveryError {
    #[error("received a bad response from the SDK: {0}")]
    BadResponse(Cow<'static, str>),
    #[error(
        "received a bad response from the SDK that cannot be decoded: {0}. Discovery response: {e}",
        e = String::from_utf8_lossy(.1)
    )]
    Decode(#[source] serde_json::Error, Bytes),
    #[error(
        "got status code '404'. Make sure the provided URI has the correct path where the Restate SDK endpoint is mounted. Response headers: {0:?}. Body: {1}"
    )]
    NotFound(HeaderMap, Cow<'static, str>),
    #[error("got status code '{0}'. Response headers: {1:?}. Body: {2}")]
    BadStatusCode(StatusCode, HeaderMap, Cow<'static, str>),
    #[error(transparent)]
    Client(#[from] ServiceClientError),
    #[error("cannot read body: {0}")]
    BodyError(GenericError),
    #[error(
        "unsupported service protocol versions: [{min_version}, {max_version}]. Supported versions by this runtime are [{}, {}]. Please upgrade the SDK and try registering again.",
        i32::from(MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION),
        i32::from(MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION)
    )]
    UnsupportedServiceProtocol { min_version: i32, max_version: i32 },
    #[error(
        "the SDK reports itself as being in bidirectional protocol mode, but we are not discovering over a transport that supports it. Discovering with Lambda or HTTP < 1.1 is not supported"
    )]
    BidirectionalNotSupported,
}

impl CodedError for DiscoveryError {
    fn code(&self) -> Option<&'static codederror::Code> {
        match self {
            DiscoveryError::BadResponse(_) => Some(&META0013),
            DiscoveryError::Decode(_, _) => None,
            DiscoveryError::BadStatusCode { .. } | DiscoveryError::NotFound { .. } => {
                Some(&META0003)
            }
            // special code for possible http1.1 errors
            DiscoveryError::Client(ServiceClientError::Http(
                _,
                restate_service_client::HttpError::PossibleHTTP11Only(_),
            )) => Some(&META0014),
            DiscoveryError::Client(_) => Some(&META0003),
            DiscoveryError::UnsupportedServiceProtocol { .. } => Some(&META0012),
            DiscoveryError::BidirectionalNotSupported => Some(&META0015),
            DiscoveryError::BodyError(_) => None,
        }
    }
}

impl DiscoveryError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            DiscoveryError::BadStatusCode(status, _, _) => matches!(
                *status,
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
            | DiscoveryError::UnsupportedServiceProtocol { .. }
            | DiscoveryError::BidirectionalNotSupported => false,
            DiscoveryError::BodyError(_) => true,
            DiscoveryError::NotFound(_, _) => false,
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

impl DiscoveryClient for ServiceDiscovery {
    type Error = DiscoveryError;

    async fn discover(
        &self,
        DiscoveryRequest {
            address,
            use_http_11,
            additional_headers,
        }: DiscoveryRequest,
    ) -> Result<DiscoveryResponse, Self::Error> {
        let endpoint = match address {
            DeploymentAddress::Http(http) => {
                let is_using_https = http.uri.scheme().unwrap() == &Scheme::HTTPS;
                // Decide which HTTP version we should try
                let version = if use_http_11 {
                    Some(http::Version::HTTP_11)
                } else if is_using_https {
                    // ALPN will sort this out
                    None
                } else {
                    // By default, we use h2c on HTTP
                    Some(http::Version::HTTP_2)
                };

                Endpoint::Http(http.uri, version)
            }
            DeploymentAddress::Lambda(lambda) => {
                Endpoint::Lambda(lambda.arn, lambda.assume_role_arn.map(Into::into), None)
            }
        };

        let cloned_endpoint = endpoint.clone();
        let build_request = || {
            let mut headers = HeaderMap::from_iter([(
                ACCEPT,
                SUPPORTED_SERVICE_DISCOVERY_PROTOCOL_VERSIONS
                    .deref()
                    .clone(),
            )]);
            headers.extend(additional_headers.clone());
            let path = PathAndQuery::from_static(DISCOVER_PATH);
            Request::new(
                Parts::new(Method::GET, cloned_endpoint.clone(), path, headers),
                Empty::default(),
            )
        };

        let retry_policy = self.retry_policy.iter();
        let (mut parts, body) = Self::invoke_discovery_endpoint(
            &self.client,
            endpoint.clone(),
            build_request,
            retry_policy,
        )
        .await?;

        // Retrieve chosen service discovery protocol version.
        // No need to retry these: if the validation fails, they're sdk bugs.
        let content_type = parts.headers.remove(CONTENT_TYPE);
        let service_discovery_protocol_version =
            Self::retrieve_service_discovery_protocol_version(content_type)?;

        let x_restate_server = parts.headers.remove(X_RESTATE_SERVER);

        let response = match service_discovery_protocol_version {
            ServiceDiscoveryProtocolVersion::Unspecified => {
                unreachable!("unspecified service discovery protocol should not be chosen")
            }
            ServiceDiscoveryProtocolVersion::V1
            | ServiceDiscoveryProtocolVersion::V2
            | ServiceDiscoveryProtocolVersion::V3
            | ServiceDiscoveryProtocolVersion::V4 => {
                serde_json::from_slice(&body).map_err(|e| DiscoveryError::Decode(e, body))?
            }
        };

        let discovery_response = Self::create_discovered_metadata_from_endpoint_response(
            endpoint,
            parts.version,
            response,
            x_restate_server,
        )?;

        if discovery_response.supported_protocol_versions.end() < &4i32 {
            warn!(
                "The registered endpoint is using a service protocol version that will be removed in the future releases. \
                     Please update the SDK to the latest release and re-register the deployment. \
                     For more info, check https://docs.restate.dev/operate/versioning#deploying-new-service-versions",
            );
        }

        Ok(discovery_response)
    }
}

impl ServiceDiscovery {
    #[allow(clippy::result_large_err)]
    fn retrieve_service_discovery_protocol_version(
        content_type: Option<HeaderValue>,
    ) -> Result<ServiceDiscoveryProtocolVersion, DiscoveryError> {
        match content_type {
            // False positive with Bytes field
            #[allow(clippy::borrow_interior_mutable_const)]
            Some(ct) => {
                let content_type = ct.to_str().map_err(|e| {
                    DiscoveryError::BadResponse(
                        format!("Could not parse content type header: {e}").into(),
                    )
                })?;
                let service_discovery_protocol_version =
                    parse_service_discovery_protocol_version_from_content_type(content_type)
                        .ok_or_else(|| {
                            DiscoveryError::BadResponse(
                                format!(
                                    "Bad content type header: {ct:?}. Expected one of '{:?}'",
                                    SUPPORTED_SERVICE_DISCOVERY_PROTOCOL_VERSIONS.deref()
                                )
                                .into(),
                            )
                        })?;

                if !service_discovery_protocol_version.is_supported() {
                    return Err(DiscoveryError::BadResponse(format!("The returned service discovery protocol version '{}' is not supported by the server.", service_discovery_protocol_version.as_repr()).into()));
                }

                Ok(service_discovery_protocol_version)
            }
            None => Err(DiscoveryError::BadResponse(
                format!(
                    "No content type header was specified. Expected one of '{:?}' content type.",
                    SUPPORTED_SERVICE_DISCOVERY_PROTOCOL_VERSIONS.deref()
                )
                .into(),
            )),
        }
    }

    #[allow(clippy::result_large_err)]
    fn create_discovered_metadata_from_endpoint_response(
        endpoint: Endpoint,
        response_http_version: Version,
        endpoint_response: endpoint_manifest::Endpoint,
        x_restate_server: Option<HeaderValue>,
    ) -> Result<DiscoveryResponse, DiscoveryError> {
        let protocol_type = match endpoint_response.protocol_mode {
            Some(endpoint_manifest::ProtocolMode::BidiStream) => ProtocolType::BidiStream,
            Some(endpoint_manifest::ProtocolMode::RequestResponse) => ProtocolType::RequestResponse,
            None => {
                return Err(DiscoveryError::BadResponse("missing protocol mode".into()));
            }
        };

        match (protocol_type, &endpoint, response_http_version) {
            // all endpoints support request response
            (ProtocolType::RequestResponse, _, _) => {}
            // http2 upwards supports bidi
            (ProtocolType::BidiStream, Endpoint::Http(_, _), Version::HTTP_2 | Version::HTTP_3) => {
            }
            // http1.1 *can* support bidi depending on server implementation (and load balancers)
            // trust the user if this is what they advertise
            (ProtocolType::BidiStream, Endpoint::Http(_, _), Version::HTTP_11) => {}
            // lambda client and HTTP < 1.1 do not support bidi
            (ProtocolType::BidiStream, _, _) => {
                return Err(DiscoveryError::BidirectionalNotSupported);
            }
        }

        // Sanity checks for the service protocol version
        if endpoint_response.min_protocol_version.get() > MAX_SERVICE_PROTOCOL_VERSION_VALUE as u64
        {
            return Err(DiscoveryError::BadResponse(
                format!(
                    "min protocol version must be in [1, {MAX_SERVICE_PROTOCOL_VERSION_VALUE}]"
                )
                .into(),
            ));
        }

        if endpoint_response.max_protocol_version.get() > MAX_SERVICE_PROTOCOL_VERSION_VALUE as u64
        {
            return Err(DiscoveryError::BadResponse(
                format!(
                    "max protocol version must be in [1, {MAX_SERVICE_PROTOCOL_VERSION_VALUE}]"
                )
                .into(),
            ));
        }

        if endpoint_response.min_protocol_version > endpoint_response.max_protocol_version {
            return Err(DiscoveryError::BadResponse(
                format!("Expected min protocol version to be <= max protocol version. Received min protocol version '{}', max protocol version '{}'", endpoint_response.min_protocol_version, endpoint_response.max_protocol_version).into(),
            ));
        }

        let min_version = endpoint_response.min_protocol_version.get() as i32;
        let mut max_version = endpoint_response.max_protocol_version.get() as i32;
        let mut sdk_version = None;

        // Fix for the SDK-Typescript bad protocol version,
        //  see https://github.com/restatedev/sdk-typescript/pull/418
        if let Some(x_restate_server) = x_restate_server
            && let Ok(x_restate_server) = x_restate_server.to_str()
        {
            if x_restate_server.starts_with("restate-sdk-typescript/1.0.")
                || x_restate_server.starts_with("restate-sdk-typescript/1.1.")
                || x_restate_server.starts_with("restate-sdk-typescript/1.2.")
            {
                debug!(
                    "Applying SDK-Typescript <= 1.2.1 workaround for endpoint.maxProtocolVersion"
                );
                max_version = 1;
            }
            sdk_version = Some(x_restate_server.to_owned());
        }

        if !ServiceProtocolVersion::is_acceptable_for_discovery(min_version, max_version) {
            return Err(DiscoveryError::UnsupportedServiceProtocol {
                min_version,
                max_version,
            });
        }

        Ok(DiscoveryResponse {
            deployment_type_parameters: match endpoint {
                Endpoint::Http { .. } => DeploymentConnectionParameters::Http {
                    protocol_type,
                    http_version: response_http_version,
                },
                Endpoint::Lambda { .. } => DeploymentConnectionParameters::Lambda {
                    compression: endpoint_response.lambda_compression.map(|compression| {
                        match compression {
                            endpoint_manifest::EndpointLambdaCompression::Zstd => {
                                EndpointLambdaCompression::Zstd
                            }
                        }
                    }),
                },
            },
            services: endpoint_response.services,
            // we need to store the raw representation since the runtime might not know the latest
            // version yet.
            supported_protocol_versions: min_version..=max_version,
            sdk_version,
        })
    }

    async fn invoke_discovery_endpoint(
        client: &ServiceClient,
        address: impl Display,
        build_request: impl Fn() -> Request<Empty<Bytes>>,
        mut retry_iter: RetryIter<'_>,
    ) -> Result<(ResponseParts, Bytes), DiscoveryError> {
        loop {
            let response_fut = client.call(build_request());
            let response = async {
                let (parts, body) = response_fut
                    .await
                    .map_err(Into::<DiscoveryError>::into)?
                    .into_parts();

                if !parts.status.is_success() {
                    let body_message = body
                        .collect()
                        .await
                        .map(|b| {
                            String::from_utf8_lossy(b.to_bytes().to_vec().as_slice()).to_string()
                        })
                        .unwrap_or_else(|err| format!("Failed to read body {err}"));
                    if parts.status == StatusCode::NOT_FOUND {
                        return Err(DiscoveryError::NotFound(parts.headers, body_message.into()));
                    }
                    return Err(DiscoveryError::BadStatusCode(
                        parts.status,
                        parts.headers,
                        body_message.into(),
                    ));
                }

                Ok((
                    parts,
                    body.collect()
                        .await
                        .map_err(DiscoveryError::BodyError)?
                        .to_bytes(),
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
            if e.is_retryable()
                && let Some(next_retry_interval) = retry_iter.next()
            {
                warn!(
                    "Error when discovering deployment at address '{}'. Retrying in {} seconds: {}",
                    address,
                    next_retry_interval.as_secs(),
                    e
                );
                tokio::time::sleep(next_retry_interval).await;
                continue;
            }

            return Err(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::discovery::endpoint_manifest::ProtocolMode;
    use crate::discovery::{
        DiscoveryError, SERVICE_DISCOVERY_PROTOCOL_V1_HEADER_VALUE, ServiceDiscovery,
        parse_service_discovery_protocol_version_from_content_type,
    };
    use googletest::prelude::*;
    use http::{Uri, Version};
    use restate_service_client::Endpoint;
    use restate_types::endpoint_manifest;
    use restate_types::service_discovery::ServiceDiscoveryProtocolVersion;
    use restate_types::service_protocol::MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION;
    use std::num::NonZeroU64;

    #[test]
    fn fail_on_invalid_min_protocol_version_with_bad_response() {
        let response = endpoint_manifest::Endpoint {
            lambda_compression: None,
            min_protocol_version: NonZeroU64::MAX,
            max_protocol_version: NonZeroU64::MAX,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(
                Endpoint::Http(Uri::default(), None),
                Version::HTTP_2,
                response,
                None
            ),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_on_bidirectional_with_lambda() {
        let response = endpoint_manifest::Endpoint {
            lambda_compression: None,
            min_protocol_version: NonZeroU64::MIN,
            max_protocol_version: NonZeroU64::MIN,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(
                Endpoint::Lambda(
                    "arn:partition:lambda:region:account_id:function:name:version"
                        .parse()
                        .unwrap(),
                    None,
                    None
                ),
                Version::HTTP_11,
                response,
                None
            ),
            Err(DiscoveryError::BidirectionalNotSupported)
        ));
    }

    #[test]
    fn fail_on_invalid_max_protocol_version_with_bad_response() {
        let response = endpoint_manifest::Endpoint {
            lambda_compression: None,
            min_protocol_version: NonZeroU64::MIN,
            max_protocol_version: NonZeroU64::MAX,
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(
                Endpoint::Http(Uri::default(), None),
                Version::HTTP_2,
                response,
                None
            ),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_on_max_protocol_version_smaller_than_min_protocol_version_with_bad_response() {
        let response = endpoint_manifest::Endpoint {
            lambda_compression: None,
            min_protocol_version: NonZeroU64::new(10).unwrap(),
            max_protocol_version: NonZeroU64::new(9).unwrap(),
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert!(matches!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(
                Endpoint::Http(Uri::default(), None),
                Version::HTTP_2,
                response,
                None
            ),
            Err(DiscoveryError::BadResponse(_))
        ));
    }

    #[test]
    fn fail_with_unsupported_protocol_version() {
        let unsupported_version = MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr() + 1;
        let response = endpoint_manifest::Endpoint {
            lambda_compression: None,
            min_protocol_version: NonZeroU64::new(unsupported_version as u64).unwrap(),
            max_protocol_version: NonZeroU64::new(unsupported_version as u64).unwrap(),
            services: Vec::new(),
            protocol_mode: Some(ProtocolMode::BidiStream),
        };

        assert_that!(
            ServiceDiscovery::create_discovered_metadata_from_endpoint_response(
                Endpoint::Http(Uri::default(), None),
                Version::HTTP_2,
                response,
                None
            ),
            err(pat!(DiscoveryError::UnsupportedServiceProtocol {
                min_version: eq(unsupported_version),
                max_version: eq(unsupported_version)
            }))
        );
    }

    #[test]
    fn parse_service_discovery_protocol_version() {
        assert_eq!(
            parse_service_discovery_protocol_version_from_content_type(
                SERVICE_DISCOVERY_PROTOCOL_V1_HEADER_VALUE
            ),
            Some(ServiceDiscoveryProtocolVersion::V1)
        );

        assert_eq!(
            parse_service_discovery_protocol_version_from_content_type(
                "application/vnd.restate.endpointmanifest.v1+protobuf"
            ),
            None
        );
        assert_eq!(
            parse_service_discovery_protocol_version_from_content_type("foobar"),
            None
        );
    }
}
