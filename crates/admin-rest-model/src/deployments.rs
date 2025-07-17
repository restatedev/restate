// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Uri;
use http::Version;
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::ServiceRevision;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::DeploymentType;
use restate_types::schema::deployment::{DeploymentMetadata, ProtocolType};
use restate_types::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::time::SystemTime;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "serde_hacks::DeploymentShadow")]
#[serde(untagged)]
pub enum Deployment {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "http_serde::version")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        http_version: Version,
        #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
        #[serde(default)]
        additional_headers: SerdeableHeaderHashMap,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        sdk_version: Option<String>,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        sdk_version: Option<String>,
    },
}

impl From<DeploymentMetadata> for Deployment {
    fn from(value: DeploymentMetadata) -> Self {
        match value.ty {
            DeploymentType::Http {
                address,
                protocol_type,
                http_version,
            } => Self::Http {
                uri: address,
                protocol_type,
                http_version,
                additional_headers: value.delivery_options.additional_headers.into(),
                created_at: SystemTime::from(value.created_at).into(),
                min_protocol_version: *value.supported_protocol_versions.start(),
                max_protocol_version: *value.supported_protocol_versions.end(),
                sdk_version: value.sdk_version,
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
                sdk_version: value.sdk_version,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        assume_role_arn: Option<String>,

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
}

impl DeploymentResponse {
    pub fn new(
        id: DeploymentId,
        deployment: Deployment,
        services: Vec<ServiceNameRevPair>,
    ) -> Self {
        match deployment {
            Deployment::Http {
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
            } => Self::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            },
            Deployment::Lambda {
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
            } => Self::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            },
        }
    }

    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http { id, .. } => *id,
            Self::Lambda { id, .. } => *id,
        }
    }

    pub fn into_parts(self) -> (DeploymentId, Deployment, Vec<ServiceNameRevPair>) {
        match self {
            Self::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            } => (
                id,
                Deployment::Http {
                    uri,
                    protocol_type,
                    http_version,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
                services,
            ),
            Self::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            } => (
                id,
                Deployment::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
                services,
            ),
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
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        assume_role_arn: Option<String>,

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
        services: Vec<ServiceMetadata>,
    },
}

impl DetailedDeploymentResponse {
    pub fn new(id: DeploymentId, deployment: Deployment, services: Vec<ServiceMetadata>) -> Self {
        match deployment {
            Deployment::Http {
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
            } => Self::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            },
            Deployment::Lambda {
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
            } => Self::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            },
        }
    }

    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http { id, .. } => *id,
            Self::Lambda { id, .. } => *id,
        }
    }

    pub fn into_parts(self) -> (DeploymentId, Deployment, Vec<ServiceMetadata>) {
        match self {
            Self::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            } => (
                id,
                Deployment::Http {
                    uri,
                    protocol_type,
                    http_version,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
                services,
            ),
            Self::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                sdk_version,
                services,
            } => (
                id,
                Deployment::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
                services,
            ),
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

mod serde_hacks {
    use super::*;

    #[derive(Deserialize)]
    #[serde(untagged)]
    pub(super) enum DeploymentShadow {
        Http {
            #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
            uri: Uri,
            protocol_type: ProtocolType,
            #[serde(with = "http_serde::option::version")]
            #[serde(default)]
            // this field did not used to be provided; to provide backwards compatibility with old restate, we must consider it optional when deserialising
            http_version: Option<Version>,
            #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
            #[serde(default)]
            additional_headers: SerdeableHeaderHashMap,
            #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
            created_at: humantime::Timestamp,
            min_protocol_version: i32,
            max_protocol_version: i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            sdk_version: Option<String>,
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
            created_at: humantime::Timestamp,
            min_protocol_version: i32,
            max_protocol_version: i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(default)]
            sdk_version: Option<String>,
        },
    }

    impl From<DeploymentShadow> for Deployment {
        fn from(value: DeploymentShadow) -> Self {
            match value {
                DeploymentShadow::Http {
                    uri,
                    protocol_type,
                    http_version,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                } => Self::Http {
                    uri,
                    protocol_type,
                    http_version: http_version
                        .unwrap_or_else(|| protocol_type.default_http_version()),
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
                DeploymentShadow::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                } => Self::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    sdk_version,
                },
            }
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn can_deserialise_without_http_version() {
            let dt: super::Deployment = serde_json::from_str(
                r#"{"uri":"google.com","protocol_type":"BidiStream","created_at":"2018-02-14T00:28:07Z","min_protocol_version":1,"max_protocol_version":1}"#,
            )
                .unwrap();
            let serialised = serde_json::to_string(&dt).unwrap();
            assert_eq!(
                r#"{"uri":"google.com","protocol_type":"BidiStream","http_version":"HTTP/2.0","created_at":"2018-02-14T00:28:07Z","min_protocol_version":1,"max_protocol_version":1}"#,
                serialised
            );

            let dt: super::Deployment = serde_json::from_str(
                r#"{"uri":"google.com","protocol_type":"RequestResponse","created_at":"2018-02-14T00:28:07Z","min_protocol_version":1,"max_protocol_version":1}"#,
            )
                .unwrap();
            let serialised = serde_json::to_string(&dt).unwrap();
            assert_eq!(
                r#"{"uri":"google.com","protocol_type":"RequestResponse","http_version":"HTTP/1.1","created_at":"2018-02-14T00:28:07Z","min_protocol_version":1,"max_protocol_version":1}"#,
                serialised
            );
        }
    }
}
