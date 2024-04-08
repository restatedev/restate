// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use codederror::CodedError;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::http::response::Parts as ResponseParts;
use hyper::http::uri::PathAndQuery;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{Body, HeaderMap, StatusCode};
use restate_errors::META0003;
use restate_schema_api::deployment::ProtocolType;
use restate_service_client::{Endpoint, Parts, Request, ServiceClient, ServiceClientError};
use restate_types::retries::{RetryIter, RetryPolicy};
use std::collections::HashMap;
use std::fmt::Display;
use tracing::warn;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

const DISCOVER_PATH: &str = "/discover";

pub mod schema {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]

    use crate::discovery::schema;
    include!(concat!(env!("OUT_DIR"), "/deployment.rs"));

    impl From<ComponentType> for restate_schema_api::component::ComponentType {
        fn from(value: ComponentType) -> Self {
            match value {
                ComponentType::VirtualObject => {
                    restate_schema_api::component::ComponentType::VirtualObject
                }
                ComponentType::Service => restate_schema_api::component::ComponentType::Service,
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
    pub components: Vec<schema::Component>,
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum DiscoveryError {
    // Errors most likely related to SDK bugs
    #[error("received a bad response from the SDK: {0}")]
    #[code(unknown)]
    BadResponse(&'static str),
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
            DiscoveryError::BadResponse(_) | DiscoveryError::Decode(_, _) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComponentDiscovery {
    retry_policy: RetryPolicy,
    client: ServiceClient,
}

impl ComponentDiscovery {
    pub fn new(retry_policy: RetryPolicy, client: ServiceClient) -> Self {
        Self {
            retry_policy,
            client,
        }
    }
}

impl ComponentDiscovery {
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
            _ => {
                return Err(DiscoveryError::BadResponse("Bad content type header"));
            }
        }

        // Parse the response
        let response: schema::Deployment =
            serde_json::from_slice(&body).map_err(|e| DiscoveryError::Decode(e, body))?;

        let protocol_type = match response.protocol_mode {
            Some(schema::ProtocolMode::BidiStream) => ProtocolType::BidiStream,
            Some(schema::ProtocolMode::RequestResponse) => ProtocolType::RequestResponse,
            None => {
                return Err(DiscoveryError::BadResponse("missing protocol mode"));
            }
        };

        Ok(DiscoveredMetadata {
            protocol_type,
            components: response.components,
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
