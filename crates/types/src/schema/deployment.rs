// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;

use bytestring::ByteString;
use http::Uri;
use http::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
use crate::schema::service::ServiceMetadata;
use crate::time::MillisSinceEpoch;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, derive_more::Display)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ProtocolType {
    #[display("Request/Response")]
    RequestResponse,
    #[display("Bidirectional Stream")]
    BidiStream,
}

// TODO this type is serde because it represents how data is stored in the schema registry
//  re-evaluate whether we should use another ad-hoc data structure for storage representation after schema v2 migration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeliveryOptions {
    #[serde(
        with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
    )]
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
}

impl DeliveryOptions {
    pub fn new(additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        Self { additional_headers }
    }
}

#[derive(Debug, Clone)]
pub struct Deployment {
    pub id: DeploymentId,
    pub metadata: DeploymentMetadata,
}

// TODO this type is serde because it represents how data is stored in the schema registry
//  re-evaluate whether we should use another ad-hoc data structure for storage representation after schema v2 migration.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentMetadata {
    pub ty: DeploymentType,
    pub delivery_options: DeliveryOptions,
    pub supported_protocol_versions: RangeInclusive<i32>,
    /// Declared SDK during discovery
    pub sdk_version: Option<String>,
    pub created_at: MillisSinceEpoch,
}

/// Lambda compression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum EndpointLambdaCompression {
    Zstd,
}

// TODO this type is serde because it represents how data is stored in the schema registry
//  re-evaluate whether we should use another ad-hoc data structure for storage representation after schema v2 migration.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "serde_hacks::DeploymentType")]
pub enum DeploymentType {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        address: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "serde_with::As::<restate_serde_util::VersionSerde>")]
        http_version: http::Version,
    },
    Lambda {
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compression: Option<EndpointLambdaCompression>,
    },
}

impl DeploymentType {
    // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
    // and for Lambda deployments its the ARN
    pub fn address_display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a DeploymentType);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self {
                    Wrapper(DeploymentType::Http { address, .. }) => address.fmt(f),
                    Wrapper(DeploymentType::Lambda { arn, .. }) => arn.fmt(f),
                }
            }
        }
        Wrapper(self)
    }
}

mod serde_hacks {
    use super::*;

    #[derive(serde::Deserialize)]
    pub(super) enum DeploymentType {
        Http {
            #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
            address: Uri,
            protocol_type: ProtocolType,
            #[serde(
                default,
                with = "serde_with::As::<Option<restate_serde_util::VersionSerde>>"
            )]
            // this field did not used to be stored, so we must consider it optional when deserialising
            http_version: Option<http::Version>,
        },
        Lambda {
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            compression: Option<EndpointLambdaCompression>,
        },
    }

    impl From<DeploymentType> for super::DeploymentType {
        fn from(value: DeploymentType) -> Self {
            match value {
                DeploymentType::Http {
                    address,
                    protocol_type,
                    http_version,
                } => Self::Http {
                    address,
                    protocol_type,
                    http_version: match http_version {
                        Some(v) => v,
                        None => Self::backfill_http_version(protocol_type),
                    },
                },
                DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                    compression,
                } => Self::Lambda {
                    arn,
                    assume_role_arn,
                    compression,
                },
            }
        }
    }
}

#[cfg(test)]
mod serde_tests {
    use crate::{identifiers::LambdaARN, storage::StorageCodec};
    use bytestring::ByteString;
    use http::Uri;

    use super::{DeploymentType, ProtocolType};

    #[derive(serde::Serialize, serde::Deserialize)]
    enum OldDeploymentType {
        Http {
            #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
            address: Uri,
            protocol_type: ProtocolType,
        },
        Lambda {
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
        },
    }

    crate::flexbuffers_storage_encode_decode!(OldDeploymentType);
    crate::flexbuffers_storage_encode_decode!(DeploymentType);

    #[test]
    fn can_deserialise_without_http_version() {
        let mut buf = bytes::BytesMut::default();
        StorageCodec::encode(
            &OldDeploymentType::Http {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::BidiStream,
            },
            &mut buf,
        )
        .unwrap();
        let dt: DeploymentType = StorageCodec::decode(&mut buf).unwrap();
        assert_eq!(
            DeploymentType::Http {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
            },
            dt
        );

        let mut buf = bytes::BytesMut::default();
        StorageCodec::encode(
            &OldDeploymentType::Http {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::RequestResponse,
            },
            &mut buf,
        )
        .unwrap();
        let dt: DeploymentType = StorageCodec::decode(&mut buf).unwrap();
        assert_eq!(
            DeploymentType::Http {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::RequestResponse,
                http_version: http::Version::HTTP_11,
            },
            dt
        );
    }
}

impl DeploymentType {
    pub fn backfill_http_version(protocol_type: ProtocolType) -> http::Version {
        match protocol_type {
            ProtocolType::BidiStream => http::Version::HTTP_2,
            ProtocolType::RequestResponse => http::Version::HTTP_11,
        }
    }

    pub fn protocol_type(&self) -> ProtocolType {
        match self {
            DeploymentType::Http { protocol_type, .. } => *protocol_type,
            DeploymentType::Lambda { .. } => ProtocolType::RequestResponse,
        }
    }

    pub fn normalized_address(&self) -> String {
        match self {
            DeploymentType::Http { address, .. } => {
                // We use only authority and path, as those uniquely identify the deployment.
                format!(
                    "{}{}",
                    address.authority().expect("Must have authority"),
                    address.path()
                )
            }
            DeploymentType::Lambda { arn, .. } => arn.to_string(),
        }
    }
}

impl DeploymentMetadata {
    pub fn new_http(
        address: Uri,
        protocol_type: ProtocolType,
        http_version: http::Version,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
    ) -> Self {
        Self {
            ty: DeploymentType::Http {
                address,
                protocol_type,
                http_version,
            },
            delivery_options,
            created_at: MillisSinceEpoch::now(),
            supported_protocol_versions,
            sdk_version,
        }
    }

    pub fn new_lambda(
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        compression: Option<EndpointLambdaCompression>,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
    ) -> Self {
        Self {
            ty: DeploymentType::Lambda {
                arn,
                assume_role_arn,
                compression,
            },
            delivery_options,
            created_at: MillisSinceEpoch::now(),
            supported_protocol_versions,
            sdk_version,
        }
    }

    // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
    // and for Lambda deployments its the ARN
    pub fn address_display(&self) -> impl Display + '_ {
        self.ty.address_display()
    }

    pub fn created_at(&self) -> MillisSinceEpoch {
        self.created_at
    }
}

pub trait DeploymentResolver {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment>;

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)>;

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)>;
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::*;

    use crate::service_protocol::MAX_SERVICE_PROTOCOL_VERSION_VALUE;
    use std::collections::HashMap;

    impl Deployment {
        pub fn mock() -> Deployment {
            let id = "dp_15VqmTOnXH3Vv2pl5HOG7UB"
                .parse()
                .expect("valid stable deployment id");
            let metadata = DeploymentMetadata::new_http(
                "http://localhost:9080".parse().unwrap(),
                ProtocolType::BidiStream,
                http::Version::HTTP_2,
                Default::default(),
                1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                None,
            );

            Deployment { id, metadata }
        }

        pub fn mock_with_uri(uri: &str) -> Deployment {
            let id = DeploymentId::new();
            let metadata = DeploymentMetadata::new_http(
                uri.parse().unwrap(),
                ProtocolType::BidiStream,
                http::Version::HTTP_2,
                Default::default(),
                1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                None,
            );
            Deployment { id, metadata }
        }
    }

    #[derive(Default, Clone, Debug)]
    pub struct MockDeploymentMetadataRegistry {
        pub deployments: HashMap<DeploymentId, DeploymentMetadata>,
        pub latest_deployment: HashMap<String, DeploymentId>,
    }

    impl MockDeploymentMetadataRegistry {
        pub fn mock_service(&mut self, service: &str) {
            self.mock_service_with_metadata(service, Deployment::mock());
        }

        pub fn mock_service_with_metadata(&mut self, service: &str, deployment: Deployment) {
            self.latest_deployment
                .insert(service.to_string(), deployment.id);
            self.deployments.insert(deployment.id, deployment.metadata);
        }
    }

    impl DeploymentResolver for MockDeploymentMetadataRegistry {
        fn resolve_latest_deployment_for_service(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<Deployment> {
            self.latest_deployment
                .get(service_name.as_ref())
                .and_then(|deployment_id| self.get_deployment(deployment_id))
        }

        fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
            self.deployments
                .get(deployment_id)
                .cloned()
                .map(|metadata| Deployment {
                    id: *deployment_id,
                    metadata,
                })
        }

        fn get_deployment_and_services(
            &self,
            deployment_id: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            self.deployments
                .get(deployment_id)
                .cloned()
                .map(|metadata| {
                    (
                        Deployment {
                            id: *deployment_id,
                            metadata,
                        },
                        vec![],
                    )
                })
        }

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
            self.deployments
                .iter()
                .map(|(id, metadata)| {
                    (
                        Deployment {
                            id: *id,
                            metadata: metadata.clone(),
                        },
                        vec![],
                    )
                })
                .collect()
        }
    }
}
