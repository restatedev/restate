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

use crate::config::Configuration;
use crate::deployment::{
    DeploymentAddress, Headers, HttpDeploymentAddress, LambdaDeploymentAddress,
};
use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
use crate::schema::service::ServiceMetadata;
use crate::time::MillisSinceEpoch;
use bytestring::ByteString;
use http::Uri;
use http::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeploymentMetadata {
    pub ty: DeploymentType,
    pub delivery_options: DeliveryOptions,
    pub supported_protocol_versions: RangeInclusive<i32>,
    /// Declared SDK during discovery
    pub sdk_version: Option<String>,
    pub created_at: MillisSinceEpoch,
    /// User provided metadata during registration
    pub metadata: HashMap<String, String>,
}

impl DeploymentMetadata {
    pub fn new(
        ty: DeploymentType,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            ty,
            delivery_options,
            created_at: MillisSinceEpoch::now(),
            supported_protocol_versions,
            sdk_version,
            metadata,
        }
    }

    pub fn new_http(
        address: Uri,
        protocol_type: ProtocolType,
        http_version: http::Version,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
        metadata: HashMap<String, String>,
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
            metadata,
        }
    }

    pub fn new_lambda(
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        compression: Option<EndpointLambdaCompression>,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
        metadata: HashMap<String, String>,
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
            metadata,
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

    pub fn semantic_eq_with_address_and_headers(
        &self,
        other_addess: &DeploymentAddress,
        other_additional_headers: &Headers,
    ) -> bool {
        match (&self.ty, other_addess) {
            (
                DeploymentType::Http {
                    address: this_address,
                    ..
                },
                DeploymentAddress::Http(HttpDeploymentAddress { uri: other_address }),
            ) => Self::semantic_eq_http(
                this_address,
                other_address,
                &self.delivery_options.additional_headers,
                other_additional_headers,
            ),
            (
                DeploymentType::Lambda { arn: this_arn, .. },
                DeploymentAddress::Lambda(LambdaDeploymentAddress { arn: other_arn, .. }),
            ) => Self::semantic_eq_lambda(this_arn, other_arn),
            _ => false,
        }
    }

    pub(crate) fn semantic_eq_lambda(this_arn: &LambdaARN, other_arn: &LambdaARN) -> bool {
        this_arn == other_arn
    }

    pub(crate) fn semantic_eq_http(
        this_address: &Uri,
        other_address: &Uri,
        this_additional_headers: &Headers,
        other_additional_headers: &Headers,
    ) -> bool {
        let deployment_routing_headers = &Configuration::pinned().admin.deployment_routing_headers;

        this_address.authority().expect("Must have authority")
            == other_address.authority().expect("Must have authority")
            && this_address.path() == other_address.path()
            && deployment_routing_headers.iter().all(|routing_header_key| {
                this_additional_headers.get(routing_header_key)
                    == other_additional_headers.get(routing_header_key)
            })
    }
}

/// Lambda compression
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum EndpointLambdaCompression {
    Zstd,
}

impl EndpointLambdaCompression {
    pub fn http_name(&self) -> &'static str {
        match self {
            EndpointLambdaCompression::Zstd => "zstd",
        }
    }
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
}

pub trait DeploymentResolver {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment>;

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)>;

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)>;

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)>;
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
                Default::default(),
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
                Default::default(),
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
        pub fn mock_deployment(&mut self, deployment: Deployment) {
            self.deployments.insert(deployment.id, deployment.metadata);
        }

        pub fn mock_latest_service(&mut self, service: &str, deployment_id: DeploymentId) {
            self.latest_deployment.insert(service.into(), deployment_id);
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

        fn find_deployment(
            &self,
            deployment_address: &DeploymentAddress,
            additional_headers: &Headers,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            self.deployments
                .iter()
                .find(|(_, d)| {
                    d.semantic_eq_with_address_and_headers(deployment_address, additional_headers)
                })
                .and_then(|(dp_id, _)| self.get_deployment_and_services(dp_id))
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

    impl DeploymentResolver for () {
        fn resolve_latest_deployment_for_service(&self, _: impl AsRef<str>) -> Option<Deployment> {
            None
        }

        fn find_deployment(
            &self,
            _: &DeploymentAddress,
            _: &Headers,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            None
        }

        fn get_deployment(&self, _: &DeploymentId) -> Option<Deployment> {
            None
        }

        fn get_deployment_and_services(
            &self,
            _: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            None
        }

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
            vec![]
        }
    }
}
