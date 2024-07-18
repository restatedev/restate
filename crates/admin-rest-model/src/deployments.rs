// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::serde_util_http_0_1::{SerdeableHeaderHashMap, VersionSerde};
use http::Version;
use http::{HeaderName, HeaderValue, Uri};
use restate_types::identifiers::ServiceRevision;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::DeploymentType;
use restate_types::schema::deployment::{DeploymentMetadata, ProtocolType};
use restate_types::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use std::time::SystemTime;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "DeploymentShadow")]
#[serde(untagged)]
pub enum Deployment {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        uri: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "serde_with::As::<VersionSerde>")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        http_version: http::Version,
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

#[derive(Deserialize)]
#[serde(untagged)]
enum DeploymentShadow {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        uri: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "serde_with::As::<Option<VersionSerde>>")]
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
            } => Self::Http {
                uri,
                protocol_type,
                http_version: http_version.unwrap_or_else(|| {
                    version_http_1_to_http_01(DeploymentType::backfill_http_version(protocol_type))
                }),
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
            },
            DeploymentShadow::Lambda {
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
            } => Self::Lambda {
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
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

impl From<DeploymentMetadata> for Deployment {
    fn from(value: DeploymentMetadata) -> Self {
        match value.ty {
            DeploymentType::Http {
                address,
                protocol_type,
                http_version,
            } => Self::Http {
                // TODO these conversions should be removed once this module is ported to http 1.0
                uri: address.to_string().parse().unwrap(),
                protocol_type,
                http_version: version_http_1_to_http_01(http_version),
                additional_headers: header_map_http_1_to_http_01(
                    value.delivery_options.additional_headers,
                )
                .into(),
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
                additional_headers: header_map_http_1_to_http_01(
                    value.delivery_options.additional_headers,
                )
                .into(),
                created_at: SystemTime::from(value.created_at).into(),
                min_protocol_version: *value.supported_protocol_versions.start(),
                max_protocol_version: *value.supported_protocol_versions.end(),
            },
        }
    }
}

fn version_http_1_to_http_01(version: http_1::Version) -> Version {
    match version {
        http_1::Version::HTTP_3 => Version::HTTP_3,
        http_1::Version::HTTP_2 => Version::HTTP_2,
        http_1::Version::HTTP_11 => Version::HTTP_11,
        http_1::Version::HTTP_10 => Version::HTTP_10,
        http_1::Version::HTTP_09 => Version::HTTP_09,
        v => panic!("Unexpected http version {:?}", v),
    }
}

fn header_map_http_1_to_http_01(
    hm: HashMap<http_1::HeaderName, http_1::HeaderValue>,
) -> HashMap<HeaderName, HeaderValue> {
    hm.into_iter()
        .map(|(k, v)| {
            (
                HeaderName::try_from(k.as_str()).unwrap(),
                HeaderValue::try_from(v.as_bytes()).unwrap(),
            )
        })
        .collect()
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
