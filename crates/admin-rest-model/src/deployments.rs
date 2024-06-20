// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Uri;
use restate_schema_api::service::ServiceMetadata;
use restate_serde_util::SerdeableHeaderHashMap;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::time::SystemTime;

// Export schema types to be used by other crates without exposing the fact
// that we are using proxying to restate-schema-api or restate-types
use restate_schema_api::deployment::DeploymentType;
pub use restate_schema_api::deployment::{DeploymentMetadata, ProtocolType};
use restate_types::identifiers::ServiceRevision;
pub use restate_types::identifiers::{DeploymentId, LambdaARN};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Deployment {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,
        protocol_type: ProtocolType,
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
    },
    Lambda {
        arn: LambdaARN,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        assume_role_arn: Option<String>,
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
    },
}

impl From<DeploymentMetadata> for Deployment {
    fn from(value: DeploymentMetadata) -> Self {
        match value.ty {
            DeploymentType::Http {
                address,
                protocol_type,
            } => Self::Http {
                uri: address,
                protocol_type,
                additional_headers: value.delivery_options.additional_headers.into(),
                created_at: SystemTime::from(value.created_at).into(),
                min_protocol_version: *value.supported_protocol_versions.start(),
                max_protocol_version: *value.supported_protocol_versions.end(),
            },
            DeploymentType::Lambda {
                arn,
                assume_role_arn,
            } => Self::Lambda {
                arn,
                assume_role_arn: assume_role_arn.map(Into::into),
                additional_headers: value.delivery_options.additional_headers.into(),
                created_at: SystemTime::from(value.created_at).into(),
                min_protocol_version: *value.supported_protocol_versions.start(),
                max_protocol_version: *value.supported_protocol_versions.end(),
            },
        }
    }
}

// This enum could be a struct with a nested enum to avoid repeating some fields, but serde(flatten) unfortunately breaks the openapi code generation
#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterDeploymentRequest {
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
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceNameRevPair {
    pub name: String,
    pub revision: ServiceRevision,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDeploymentResponse {
    pub id: DeploymentId,
    pub services: Vec<ServiceMetadata>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDeploymentsResponse {
    pub deployments: Vec<DeploymentResponse>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct DeploymentResponse {
    pub id: DeploymentId,

    #[serde(flatten)]
    pub deployment: Deployment,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceNameRevPair>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct DetailedDeploymentResponse {
    pub id: DeploymentId,

    #[serde(flatten)]
    pub deployment: Deployment,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceMetadata>,
}
