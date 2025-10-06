// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Version;
use http::{HeaderName, HeaderValue, Uri};
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::ServiceRevision;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::{EndpointLambdaCompression, ProtocolType};
use restate_types::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    #[serde_as(as = "restate_serde_util::HeaderNameSerde")]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub key: HeaderName,
    #[serde_as(as = "restate_serde_util::HeaderValueSerde")]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub value: HeaderValue,
}

// This enum could be a struct with a nested enum to avoid repeating some fields, but serde(flatten) unfortunately breaks the openapi code generation
#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterDeploymentRequest {
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "RegisterHttpDeploymentRequest",
            description = "Register HTTP deployment request"
        )
    )]
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,

        /// # Routing header
        ///
        /// Header used for routing to a specific deployment.
        /// If the load balancer between restate-server and your deployments uses a specific header to route,
        /// you should set this as the routing header, as it will be used to distinguish this deployment with other deployments with the same URL.
        routing_header: Option<Header>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        ///
        /// You typically want to include here API keys and other tokens required to send requests to deployments.
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        ///
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        use_http_11: bool,

        /// # Force
        ///
        /// If `true`, it will override, if existing, any deployment using the same `uri`.
        /// Beware that this can lead in-flight invocations to an unrecoverable error state.
        ///
        /// By default, this is `true` but it might change in future to `false`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<true>")]
        force: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "RegisterLambdaDeploymentRequest",
            description = "Register Lambda deployment request"
        )
    )]
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda deployment.
        arn: String,

        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking the addressed Lambda, to support role chaining
        assume_role_arn: Option<String>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        ///
        additional_headers: Option<SerdeableHeaderHashMap>,
        /// # Force
        ///
        /// If `true`, it will override, if existing, any deployment using the same `uri`.
        /// Beware that this can lead in-flight invocations to an unrecoverable error state.
        ///
        /// By default, this is `true` but it might change in future to `false`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<true>")]
        force: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceNameRevPair {
    pub name: String,
    pub revision: ServiceRevision,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDeploymentResponse {
    pub id: DeploymentId,
    pub services: Vec<ServiceMetadata>,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    #[serde(default)] // To make sure CLI won't complain when interacting with old runtimes
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    #[serde(default)] // To make sure CLI won't complain when interacting with old runtimes
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub sdk_version: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDeploymentsResponse {
    pub deployments: Vec<DeploymentResponse>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeploymentResponse {
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "HttpDeploymentResponse",
            description = "Deployment response for HTTP deployments"
        )
    )]
    Http {
        /// # Deployment ID
        id: DeploymentId,

        /// # Deployment URI
        ///
        /// URI used to invoke this service deployment.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,

        /// # Protocol Type
        ///
        /// Protocol type used to invoke this service deployment.
        protocol_type: ProtocolType,

        /// # HTTP Version
        ///
        /// HTTP Version used to invoke this service deployment.
        #[serde(with = "http_serde::version")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        http_version: Version,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,

        /// # Minimum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        min_protocol_version: i32,

        /// # Maximum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        max_protocol_version: i32,

        /// # SDK version
        ///
        /// SDK library and version declared during registration.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        sdk_version: Option<String>,

        /// # Services
        ///
        /// List of services exposed by this deployment.
        services: Vec<ServiceNameRevPair>,
    },
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "LambdaDeploymentResponse",
            description = "Deployment response for Lambda deployments"
        )
    )]
    Lambda {
        /// # Deployment ID
        id: DeploymentId,

        /// # Lambda ARN
        ///
        /// Lambda ARN used to invoke this service deployment.
        arn: LambdaARN,

        /// # Assume role ARN
        ///
        /// Assume role ARN used to invoke this deployment. Check https://docs.restate.dev/category/aws-lambda for more details.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assume_role_arn: Option<String>,

        /// # Compression
        ///
        /// Compression algorithm used for invoking Lambda.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compression: Option<EndpointLambdaCompression>,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        additional_headers: SerdeableHeaderHashMap,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,

        /// # Minimum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        min_protocol_version: i32,

        /// # Maximum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        max_protocol_version: i32,

        /// # SDK version
        ///
        /// SDK library and version declared during registration.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sdk_version: Option<String>,

        /// # Services
        ///
        /// List of services exposed by this deployment.
        services: Vec<ServiceNameRevPair>,
    },
}

impl DeploymentResponse {
    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http { id, .. } => *id,
            Self::Lambda { id, .. } => *id,
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DetailedDeploymentResponse {
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "HttpDetailedDeploymentResponse",
            description = "Detailed deployment response for HTTP deployments"
        )
    )]
    Http {
        /// # Deployment ID
        id: DeploymentId,

        /// # Deployment URI
        ///
        /// URI used to invoke this service deployment.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,

        /// # Protocol Type
        ///
        /// Protocol type used to invoke this service deployment.
        protocol_type: ProtocolType,

        /// # HTTP Version
        ///
        /// HTTP Version used to invoke this service deployment.
        #[serde(with = "http_serde::version")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        http_version: Version,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        additional_headers: SerdeableHeaderHashMap,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,

        /// # Minimum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        min_protocol_version: i32,

        /// # Maximum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        max_protocol_version: i32,

        /// # SDK version
        ///
        /// SDK library and version declared during registration.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sdk_version: Option<String>,

        /// # Services
        ///
        /// List of services exposed by this deployment.
        services: Vec<ServiceMetadata>,
    },
    #[cfg_attr(
        feature = "schema",
        schemars(
            title = "LambdaDetailedDeploymentResponse",
            description = "Detailed deployment response for Lambda deployments"
        )
    )]
    Lambda {
        /// # Deployment ID
        id: DeploymentId,

        /// # Lambda ARN
        ///
        /// Lambda ARN used to invoke this service deployment.
        arn: LambdaARN,

        /// # Assume role ARN
        ///
        /// Assume role ARN used to invoke this deployment. Check https://docs.restate.dev/category/aws-lambda for more details.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assume_role_arn: Option<String>,

        /// # Compression
        ///
        /// Compression algorithm used for invoking Lambda.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compression: Option<EndpointLambdaCompression>,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        additional_headers: SerdeableHeaderHashMap,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,

        /// # Minimum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        min_protocol_version: i32,

        /// # Maximum Service Protocol version
        ///
        /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
        max_protocol_version: i32,

        /// # SDK version
        ///
        /// SDK library and version declared during registration.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        sdk_version: Option<String>,

        /// # Services
        ///
        /// List of services exposed by this deployment.
        services: Vec<ServiceMetadata>,
    },
}

impl DetailedDeploymentResponse {
    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http { id, .. } => *id,
            Self::Lambda { id, .. } => *id,
        }
    }
}

// RegisterDeploymentRequest except without `force`
#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpdateDeploymentRequest {
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        ///
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        ///
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        use_http_11: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda deployment.
        arn: String,

        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking the addressed Lambda, to support role chaining
        assume_role_arn: Option<String>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        ///
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}
