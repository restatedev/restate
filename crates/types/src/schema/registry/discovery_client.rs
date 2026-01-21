// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::ops::RangeInclusive;

use codederror::CodedError;
use http::{HeaderName, HeaderValue};

use crate::deployment::DeploymentAddress;
use crate::endpoint_manifest;
use crate::schema::deployment::{EndpointLambdaCompression, ProtocolType};

#[derive(Debug)]
pub struct DiscoveryRequest {
    pub address: DeploymentAddress,
    pub use_http_11: bool,
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
}

/// Additional connection parameters discovered during deployment
#[derive(Debug, Clone)]
pub enum DeploymentConnectionParameters {
    Http {
        protocol_type: ProtocolType,
        http_version: http::Version,
    },
    Lambda {
        compression: Option<EndpointLambdaCompression>,
    },
}

#[derive(Debug, Clone)]
pub struct DiscoveryResponse {
    pub deployment_type_parameters: DeploymentConnectionParameters,
    // type is i32 because the generated ServiceProtocolVersion enum uses this as its representation
    // and we need to represent unknown later versions
    pub supported_protocol_versions: RangeInclusive<i32>,
    pub sdk_version: Option<String>,
    pub services: Vec<endpoint_manifest::Service>,
}

pub trait DiscoveryClient {
    type Error: CodedError + Send + Sync + 'static;

    fn discover(
        &self,
        req: DiscoveryRequest,
    ) -> impl Future<Output = Result<DiscoveryResponse, Self::Error>> + Send;
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use crate::service_protocol::{
        MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION, MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION,
    };
    use codederror::BoxedCodedError;

    impl DiscoveryResponse {
        pub fn mock(services: Vec<endpoint_manifest::Service>) -> Self {
            Self {
                deployment_type_parameters: DeploymentConnectionParameters::Http {
                    protocol_type: ProtocolType::BidiStream,
                    http_version: http::Version::HTTP_2,
                },
                supported_protocol_versions: MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr()
                    ..=MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr(),
                sdk_version: None,
                services,
            }
        }
    }

    impl DiscoveryClient for DiscoveryResponse {
        type Error = BoxedCodedError;

        fn discover(
            &self,
            _: DiscoveryRequest,
        ) -> impl Future<Output = Result<DiscoveryResponse, Self::Error>> + Send {
            std::future::ready(Ok(self.clone()))
        }
    }
}
