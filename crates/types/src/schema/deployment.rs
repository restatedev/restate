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
use restate_encoding::{BilrostAs, NetSerde};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::NetRangeInclusive;
use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
use crate::schema::Schema;
use crate::schema::service::ServiceMetadata;
use crate::time::MillisSinceEpoch;

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Enumeration, NetSerde,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ProtocolType {
    RequestResponse = 0,
    BidiStream = 1,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, BilrostAs, NetSerde)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[bilrost_as(dto::DeliveryOptions)]
pub struct DeliveryOptions {
    #[serde(
        with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "HashMap<String, String>"))]
    #[net_serde(skip)]
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
}

impl DeliveryOptions {
    pub fn new(additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        Self { additional_headers }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Deployment {
    pub id: DeploymentId,
    pub metadata: DeploymentMetadata,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message, NetSerde)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeploymentMetadata {
    #[bilrost(1)]
    pub ty: DeploymentType,
    #[bilrost(2)]
    pub delivery_options: DeliveryOptions,
    #[bilrost(3)]
    pub supported_protocol_versions: NetRangeInclusive<i32>,
    /// Declared SDK during discovery
    #[bilrost(4)]
    pub sdk_version: Option<String>,
    #[bilrost(5)]
    pub created_at: MillisSinceEpoch,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, BilrostAs, NetSerde)]
#[serde(from = "DeploymentTypeShadow")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[bilrost_as(dto::DeploymentType)]
pub enum DeploymentType {
    #[default]
    Unknown,
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        #[net_serde(skip)]
        address: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "serde_with::As::<restate_serde_util::VersionSerde>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        #[net_serde(skip)]
        http_version: http::Version,
    },
    Lambda {
        arn: LambdaARN,
        #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
        assume_role_arn: Option<ByteString>,
    },
}

#[derive(serde::Deserialize)]
enum DeploymentTypeShadow {
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
    },
}

impl From<DeploymentTypeShadow> for DeploymentType {
    fn from(value: DeploymentTypeShadow) -> Self {
        match value {
            DeploymentTypeShadow::Http {
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
            DeploymentTypeShadow::Lambda {
                arn,
                assume_role_arn,
            } => Self::Lambda {
                arn,
                assume_role_arn,
            },
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
            DeploymentType::Unknown => unreachable!("unknown deployment type"),
            DeploymentType::Http { protocol_type, .. } => *protocol_type,
            DeploymentType::Lambda { .. } => ProtocolType::RequestResponse,
        }
    }

    pub fn normalized_address(&self) -> String {
        match self {
            DeploymentType::Unknown => unreachable!("unknown deployment type"),
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
            supported_protocol_versions: supported_protocol_versions.into(),
            sdk_version,
        }
    }

    pub fn new_lambda(
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
        sdk_version: Option<String>,
    ) -> Self {
        Self {
            ty: DeploymentType::Lambda {
                arn,
                assume_role_arn,
            },
            delivery_options,
            created_at: MillisSinceEpoch::now(),
            supported_protocol_versions: supported_protocol_versions.into(),
            sdk_version,
        }
    }

    // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
    // and for Lambda deployments its the ARN
    pub fn address_display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a DeploymentType);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self {
                    Wrapper(DeploymentType::Unknown) => write!(f, "unknown"),
                    Wrapper(DeploymentType::Http { address, .. }) => address.fmt(f),
                    Wrapper(DeploymentType::Lambda { arn, .. }) => arn.fmt(f),
                }
            }
        }
        Wrapper(&self.ty)
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message, NetSerde)]
pub struct DeploymentSchemas {
    #[bilrost(1)]
    pub metadata: DeploymentMetadata,

    // We need to store ServiceMetadata here only for queries
    // We could optimize the memory impact of this by reading these info from disk
    #[bilrost(2)]
    pub services: Vec<ServiceMetadata>,
}

impl DeploymentResolver for Schema {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        let service = self.services.get(service_name.as_ref())?;
        self.deployments
            .get(&service.location.latest_deployment)
            .map(|schemas| Deployment {
                id: service.location.latest_deployment,
                metadata: schemas.metadata.clone(),
            })
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.deployments
            .get(deployment_id)
            .map(|schemas| Deployment {
                id: *deployment_id,
                metadata: schemas.metadata.clone(),
            })
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.deployments.get(deployment_id).map(|schemas| {
            (
                Deployment {
                    id: *deployment_id,
                    metadata: schemas.metadata.clone(),
                },
                schemas.services.clone(),
            )
        })
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.deployments
            .iter()
            .map(|(deployment_id, schemas)| {
                (
                    Deployment {
                        id: *deployment_id,
                        metadata: schemas.metadata.clone(),
                    },
                    schemas
                        .services
                        .iter()
                        .map(|s| (s.name.clone(), s.revision))
                        .collect(),
                )
            })
            .collect()
    }
}

mod dto {
    use std::{collections::HashMap, str::FromStr};

    use bytes::Bytes;
    use bytestring::ByteString;
    use http::{HeaderName, HeaderValue};

    use crate::identifiers::LambdaARN;

    #[derive(PartialEq, Eq, bilrost::Enumeration)]
    enum DeploymentKind {
        Unknown = 0,
        Http = 1,
        Lambda = 2,
    }

    #[derive(bilrost::Message)]
    struct HttpDeployment {
        address: String,
        protocol_type: super::ProtocolType,
        http_version: String,
    }

    #[derive(bilrost::Message)]
    struct LambdaDeployment {
        #[bilrost(1)]
        arn: LambdaARN,
        #[bilrost(2)]
        assume_role_arn: Option<ByteString>,
    }

    #[derive(bilrost::Oneof)]
    enum DeploymentTypeInner {
        Unknown,
        #[bilrost(1)]
        Http(HttpDeployment),
        #[bilrost(2)]
        Lambda(LambdaDeployment),
    }

    #[derive(bilrost::Message)]
    pub(super) struct DeploymentType {
        #[bilrost(oneof(1, 2))]
        inner: DeploymentTypeInner,
    }

    impl From<&super::DeploymentType> for DeploymentType {
        fn from(value: &super::DeploymentType) -> Self {
            let inner = match value {
                super::DeploymentType::Unknown => DeploymentTypeInner::Unknown,
                super::DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                } => DeploymentTypeInner::Lambda(LambdaDeployment {
                    arn: arn.clone(),
                    assume_role_arn: assume_role_arn.clone(),
                }),
                super::DeploymentType::Http {
                    address,
                    protocol_type,
                    http_version,
                } => DeploymentTypeInner::Http(HttpDeployment {
                    address: address.to_string(),
                    protocol_type: *protocol_type,
                    http_version: format!("{http_version:?}"),
                }),
            };

            Self { inner }
        }
    }

    impl From<DeploymentType> for super::DeploymentType {
        fn from(value: DeploymentType) -> Self {
            let DeploymentType { inner } = value;

            match inner {
                DeploymentTypeInner::Unknown => Self::Unknown,
                DeploymentTypeInner::Lambda(inner) => Self::Lambda {
                    arn: inner.arn,
                    assume_role_arn: inner.assume_role_arn,
                },
                DeploymentTypeInner::Http(inner) => Self::Http {
                    address: inner.address.parse().expect("valid uri"),
                    protocol_type: inner.protocol_type,
                    http_version: {
                        match inner.http_version.as_str() {
                            "HTTP/0.9" => http::Version::HTTP_09,
                            "HTTP/1.0" => http::Version::HTTP_10,
                            "HTTP/1.1" => http::Version::HTTP_11,
                            "HTTP/2.0" => http::Version::HTTP_2,
                            "HTTP/3.0" => http::Version::HTTP_3,
                            _ => panic!("invalid http version {}", inner.http_version),
                        }
                    },
                },
            }
        }
    }

    #[derive(Debug, Clone, Default, bilrost::Message)]
    pub struct DeliveryOptions {
        #[bilrost(1)]
        pub additional_headers: HashMap<String, Bytes>,
    }

    impl From<&super::DeliveryOptions> for DeliveryOptions {
        fn from(value: &super::DeliveryOptions) -> Self {
            let super::DeliveryOptions { additional_headers } = value;

            let mut headers = HashMap::with_capacity(additional_headers.len());
            for (header, value) in additional_headers {
                headers.insert(header.to_string(), Bytes::copy_from_slice(value.as_bytes()));
            }

            Self {
                additional_headers: headers,
            }
        }
    }

    impl From<DeliveryOptions> for super::DeliveryOptions {
        fn from(value: DeliveryOptions) -> Self {
            let DeliveryOptions { additional_headers } = value;

            let mut headers = HashMap::with_capacity(additional_headers.len());
            for (key, value) in additional_headers {
                headers.insert(
                    HeaderName::from_str(&key).expect("valid header name"),
                    HeaderValue::from_bytes(&value).expect("valid header value"),
                );
            }

            Self {
                additional_headers: headers,
            }
        }
    }
}
