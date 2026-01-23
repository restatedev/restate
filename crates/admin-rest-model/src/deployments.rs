// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{Uri, Version};
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::ServiceRevision;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::{EndpointLambdaCompression, ProtocolType};
use restate_types::schema::info::Info;
use restate_types::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;

// This enum could be a struct with a nested enum to avoid repeating some fields, but serde(flatten) unfortunately breaks the openapi code generation
#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterDeploymentRequest {
    /// Register HTTP deployment request
    #[cfg_attr(feature = "schema", schema(title = "RegisterHttpDeploymentRequest"))]
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
        uri: Uri,

        /// # Additional headers
        ///
        /// Additional headers added to every discover/invoke request to the deployment.
        ///
        /// You typically want to include here API keys and other tokens required to send requests to deployments.
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        ///
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        use_http_11: bool,

        /// # Breaking
        ///
        /// If `true`, it allows registering new service revisions with
        /// schemas incompatible with previous service revisions, such as changing service type, removing a handler, etc.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        breaking: bool,

        /// # Force
        ///
        /// If `true`, it overrides, if existing, any deployment using the same `uri`.
        /// Beware that this can lead inflight invocations to an unrecoverable error state.
        ///
        /// When set to `true`, it implies `breaking = true`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[cfg_attr(feature = "schema", schema(default = true))]
        force: Option<bool>,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        /// `force` and `breaking` will be respected.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
    /// Register Lambda deployment request
    #[cfg_attr(feature = "schema", schema(title = "RegisterLambdaDeploymentRequest"))]
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
        /// Additional headers added to every discover/invoke request to the deployment.
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        /// # Breaking
        ///
        /// If `true`, it allows registering new service revisions with
        /// schemas incompatible with previous service revisions, such as changing service type, removing a handler, etc.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        breaking: bool,

        /// # Force
        ///
        /// If `true`, it overrides, if existing, any deployment using the same `uri`.
        /// Beware that this can lead inflight invocations to an unrecoverable error state.
        ///
        /// This implies `breaking = true`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/operate/versioning) for more information.
        #[cfg_attr(feature = "schema", schema(default = true))]
        force: Option<bool>,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        /// `force` and `breaking` will be respected.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceNameRevPair {
    pub name: String,
    pub revision: ServiceRevision,
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
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

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<Info>,
}

/// List of all registered deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDeploymentsResponse {
    pub deployments: Vec<DeploymentResponse>,
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeploymentResponse {
    /// Deployment response for HTTP deployments
    #[cfg_attr(feature = "schema", schema(title = "HttpDeploymentResponse"))]
    Http {
        /// # Deployment ID
        id: DeploymentId,

        /// # Deployment URI
        ///
        /// URI used to invoke this service deployment.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
        uri: Uri,

        /// # Protocol Type
        ///
        /// Protocol type used to invoke this service deployment.
        protocol_type: ProtocolType,

        /// # HTTP Version
        ///
        /// HTTP Version used to invoke this service deployment.
        #[serde(with = "http_serde::version")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
        http_version: Version,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
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

        /// # Info
        ///
        /// List of configuration/deprecation information related to this deployment.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        info: Vec<Info>,
    },
    /// Deployment response for Lambda deployments
    #[cfg_attr(feature = "schema", schema(title = "LambdaDeploymentResponse"))]
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

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
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

        /// # Info
        ///
        /// List of configuration/deprecation information related to this deployment.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        info: Vec<Info>,
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

/// Detailed information about Restate deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DetailedDeploymentResponse {
    /// Detailed deployment response for HTTP deployments
    #[cfg_attr(feature = "schema", schema(title = "HttpDetailedDeploymentResponse"))]
    Http {
        /// # Deployment ID
        id: DeploymentId,

        /// # Deployment URI
        ///
        /// URI used to invoke this service deployment.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
        uri: Uri,

        /// # Protocol Type
        ///
        /// Protocol type used to invoke this service deployment.
        protocol_type: ProtocolType,

        /// # HTTP Version
        ///
        /// HTTP Version used to invoke this service deployment.
        #[serde(with = "http_serde::version")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
        http_version: Version,

        /// # Additional headers
        ///
        /// Additional headers used to invoke this service deployment.
        #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        additional_headers: SerdeableHeaderHashMap,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
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

        /// # Info
        ///
        /// List of configuration/deprecation information related to this deployment.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        info: Vec<Info>,
    },
    /// Detailed deployment response for Lambda deployments
    #[cfg_attr(feature = "schema", schema(title = "LambdaDetailedDeploymentResponse"))]
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

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schema(value_type = String))]
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

        /// # Info
        ///
        /// List of configuration/deprecation information related to this deployment.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        info: Vec<Info>,
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

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpdateDeploymentRequest {
    /// Update HTTP deployment request
    #[cfg_attr(feature = "schema", schema(title = "UpdateHttpDeploymentRequest"))]
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde(
            with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
            skip_serializing_if = "Option::is_none"
        )]
        #[cfg_attr(feature = "schema", schema(value_type = Option<String>, format = "uri"))]
        uri: Option<Uri>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        /// When provided, this will overwrite all the headers previously configured for this deployment.
        #[serde(skip_serializing_if = "Option::is_none")]
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        use_http_11: Option<bool>,

        /// # Overwrite
        ///
        /// If `true`, the update will overwrite the schema information, including the exposed service and handlers and service configuration, allowing **breaking changes** too. Use with caution.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        overwrite: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
    /// Update Lambda deployment request
    #[cfg_attr(feature = "schema", schema(title = "UpdateLambdaDeploymentRequest"))]
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda deployment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arn: Option<String>,

        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking the addressed Lambda, to support role chaining.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assume_role_arn: Option<String>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        /// When provided, this will overwrite all the headers previously configured for this deployment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Overwrite
        ///
        /// If `true`, the update will overwrite the schema information, including the exposed service and handlers and service configuration, allowing **breaking changes** too. Use with caution.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        overwrite: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}
