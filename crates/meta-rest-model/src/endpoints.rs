// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::services::ServiceRevision;

use restate_serde_util::SerdeableHeaderHashMap;

use http::Uri;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

// Export schema types to be used by other crates without exposing the fact
// that we are using proxying to restate-schema-api or restate-types
pub use restate_schema_api::endpoint::{EndpointMetadata, ProtocolType};
pub use restate_types::identifiers::{EndpointId, LambdaARN};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServiceEndpoint {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,
        protocol_type: ProtocolType,
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,
    },
    Lambda {
        arn: LambdaARN,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        assume_role_arn: Option<String>,
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,
    },
}

impl From<EndpointMetadata> for ServiceEndpoint {
    fn from(value: EndpointMetadata) -> Self {
        match value {
            EndpointMetadata::Http {
                address,
                protocol_type,
                delivery_options,
            } => Self::Http {
                uri: address,
                protocol_type,
                additional_headers: delivery_options.additional_headers.into(),
            },
            EndpointMetadata::Lambda {
                arn,
                assume_role_arn,
                delivery_options,
            } => Self::Lambda {
                arn,
                assume_role_arn: assume_role_arn.map(Into::into),
                additional_headers: delivery_options.additional_headers.into(),
            },
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterServiceEndpointRequest {
    #[serde(flatten)]
    pub endpoint_metadata: RegisterServiceEndpointMetadata,
    /// # Additional headers
    ///
    /// Additional headers added to the discover/invoke requests to the service endpoint.
    pub additional_headers: Option<SerdeableHeaderHashMap>,
    /// # Force
    ///
    /// If `true`, it will override, if existing, any endpoint using the same `uri`.
    /// Beware that this can lead in-flight invocations to an unrecoverable error state.
    ///
    /// By default, this is `true` but it might change in future to `false`.
    ///
    /// See the [versioning documentation](https://docs.restate.dev/services/upgrades-removal) for more information.
    #[serde(default = "restate_serde_util::default::bool::<true>")]
    pub force: bool,
}

#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterServiceEndpointMetadata {
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http service endpoint.
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,
    },
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda service endpoint.
        arn: String,
        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking this endpoint, to support role chaining
        assume_role_arn: Option<String>,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterServiceResponse {
    pub name: String,
    pub revision: ServiceRevision,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterServiceEndpointResponse {
    pub id: EndpointId,
    pub services: Vec<RegisterServiceResponse>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListServiceEndpointsResponse {
    pub endpoints: Vec<ServiceEndpointResponse>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceEndpointResponse {
    pub id: EndpointId,
    #[serde(flatten)]
    pub service_endpoint: ServiceEndpoint,
    /// # Services
    ///
    /// List of services exposed by this service endpoint.
    pub services: Vec<RegisterServiceResponse>,
}
