// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;

use bytestring::ByteString;
use http::header::{HeaderName, HeaderValue};
use http::Uri;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
use crate::schema::service::ServiceMetadata;
use crate::schema::Schema;
use crate::time::MillisSinceEpoch;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeliveryOptions {
    #[serde(
        with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "HashMap<String, String>"))]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeploymentMetadata {
    pub ty: DeploymentType,
    pub delivery_options: DeliveryOptions,
    pub supported_protocol_versions: RangeInclusive<i32>,
    pub created_at: MillisSinceEpoch,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "DeploymentTypeShadow")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum DeploymentType {
    Http {
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        address: Uri,
        protocol_type: ProtocolType,
        #[serde(with = "serde_with::As::<restate_serde_util::VersionSerde>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
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
        }
    }

    pub fn new_lambda(
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        delivery_options: DeliveryOptions,
        supported_protocol_versions: RangeInclusive<i32>,
    ) -> Self {
        Self {
            ty: DeploymentType::Lambda {
                arn,
                assume_role_arn,
            },
            delivery_options,
            created_at: MillisSinceEpoch::now(),
            supported_protocol_versions,
        }
    }

    // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
    // and for Lambda deployments its the ARN
    pub fn address_display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a DeploymentType);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeploymentSchemas {
    pub metadata: DeploymentMetadata,

    // We need to store ServiceMetadata here only for queries
    // We could optimize the memory impact of this by reading these info from disk
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
