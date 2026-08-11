// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod http_auth;

pub use http_auth::{GoogleIdTokenAuth, HttpAuth, derive_audience};
use restate_encoding::RestateEncoding;

use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::config::Configuration;
use crate::deployment::{
    DeploymentAddress, Headers, HttpDeploymentAddress, LambdaDeploymentAddress,
};
use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
use crate::schema::info::SchemaInfo;
use crate::schema::service::ServiceMetadata;
use crate::time::MillisSinceEpoch;
use bytestring::ByteString;
use http::Uri;
use http::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Display,
    bilrost::Enumeration,
)]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
pub enum ProtocolType {
    /// Request/Response
    #[display("Request/Response")]
    RequestResponse = 0,
    /// Bidirectional Stream
    #[display("Bidirectional Stream")]
    BidiStream = 1,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Deployment {
    pub id: DeploymentId,
    pub ty: DeploymentType,
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
    pub supported_protocol_versions: RangeInclusive<i32>,
    /// Declared SDK during discovery
    pub sdk_version: Option<String>,
    pub created_at: MillisSinceEpoch,
    /// User provided metadata during registration
    pub metadata: HashMap<String, String>,
    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    pub info: Vec<SchemaInfo>,
}

impl Deployment {
    pub fn as_address(&self) -> DeploymentAddress {
        self.ty.as_address()
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
                DeploymentType::Http(HttpType {
                    address: this_address,
                    ..
                }),
                DeploymentAddress::Http(HttpDeploymentAddress {
                    uri: other_address,
                    auth: _,
                    ..
                }),
            ) => Self::semantic_eq_http(
                this_address,
                other_address,
                &self.additional_headers,
                other_additional_headers,
            ),
            (
                DeploymentType::Lambda(LambdaType { arn: this_arn, .. }),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
pub enum EndpointLambdaCompression {
    Zstd = 0,
}

impl EndpointLambdaCompression {
    pub fn http_name(&self) -> &'static str {
        match self {
            EndpointLambdaCompression::Zstd => "zstd",
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, bilrost::Message)]
pub struct HttpType {
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[bilrost(tag = 1, encoding(RestateEncoding))]
    pub address: Uri,
    #[bilrost(tag = 2)]
    pub protocol_type: ProtocolType,
    #[serde(with = "serde_with::As::<restate_serde_util::VersionSerde>")]
    #[bilrost(tag = 3, encoding(RestateEncoding))]
    pub http_version: http::Version,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(oneof(4))]
    pub auth: Option<HttpAuth>,
}

#[derive(Debug, Clone, PartialEq, Serialize, bilrost::Message)]
pub struct LambdaType {
    #[bilrost(tag = 1)]
    pub arn: LambdaARN,
    #[bilrost(tag = 2)]
    pub assume_role_arn: Option<ByteString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag = 3)]
    pub compression: Option<EndpointLambdaCompression>,
}
// TODO this type is serde because it represents how data is stored in the schema registry
//  re-evaluate whether we should use another ad-hoc data structure for storage representation after schema v2 migration.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bilrost::Oneof, bilrost::Message)]
#[serde(from = "serde_hacks::DeploymentType")]
pub enum DeploymentType {
    Unknown,
    #[bilrost(tag = 1)]
    Http(HttpType),
    #[bilrost(tag = 2)]
    Lambda(LambdaType),
}

impl DeploymentType {
    // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
    // and for Lambda deployments its the ARN
    pub fn address_display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a DeploymentType);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self {
                    Wrapper(DeploymentType::Http(HttpType { address, .. })) => address.fmt(f),
                    Wrapper(DeploymentType::Lambda(LambdaType { arn, .. })) => arn.fmt(f),
                    Wrapper(DeploymentType::Unknown) => f.write_str("unknown"),
                }
            }
        }
        Wrapper(self)
    }

    pub fn as_address(&self) -> DeploymentAddress {
        match self {
            DeploymentType::Http(HttpType { address, auth, .. }) => {
                HttpDeploymentAddress::new(address.clone())
                    .with_auth(auth.clone())
                    .into()
            }
            DeploymentType::Lambda(LambdaType {
                arn,
                assume_role_arn,
                ..
            }) => {
                LambdaDeploymentAddress::new(arn.clone(), assume_role_arn.clone().map(Into::into))
                    .into()
            }
            DeploymentType::Unknown => {
                todo!("handle unknown deployment type")
            }
        }
    }

    pub fn backfill_http_version(protocol_type: ProtocolType) -> http::Version {
        match protocol_type {
            ProtocolType::BidiStream => http::Version::HTTP_2,
            ProtocolType::RequestResponse => http::Version::HTTP_11,
        }
    }

    pub fn protocol_type(&self) -> ProtocolType {
        match self {
            DeploymentType::Http(HttpType { protocol_type, .. }) => *protocol_type,
            DeploymentType::Lambda(_) => ProtocolType::RequestResponse,
            DeploymentType::Unknown => ProtocolType::RequestResponse,
        }
    }

    pub const fn as_static_str(&self) -> &'static str {
        match self {
            DeploymentType::Http(HttpType {
                protocol_type: ProtocolType::BidiStream,
                ..
            }) => "Http/bidi-stream",
            DeploymentType::Http(HttpType {
                protocol_type: ProtocolType::RequestResponse,
                ..
            }) => "Http/request-response",
            DeploymentType::Lambda { .. } => "Lambda",
            DeploymentType::Unknown => "unknown",
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

impl<T: DeploymentResolver> DeploymentResolver for &T {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        (**self).resolve_latest_deployment_for_service(service_name)
    }

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        (**self).find_deployment(deployment_address, additional_headers)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        (**self).get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        (**self).get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        (**self).get_deployments()
    }
}

impl<T: DeploymentResolver> DeploymentResolver for Arc<T> {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        (**self).resolve_latest_deployment_for_service(service_name)
    }

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        (**self).find_deployment(deployment_address, additional_headers)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        (**self).get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        (**self).get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        (**self).get_deployments()
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
            // older records don't have this field, treat missing as None
            #[serde(default)]
            auth: Option<HttpAuth>,
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
                    auth,
                } => Self::Http(HttpType {
                    address,
                    protocol_type,
                    http_version: match http_version {
                        Some(v) => v,
                        None => Self::backfill_http_version(protocol_type),
                    },
                    auth,
                }),
                DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                    compression,
                } => Self::Lambda(LambdaType {
                    arn,
                    assume_role_arn,
                    compression,
                }),
            }
        }
    }
}

#[cfg(test)]
mod serde_tests {
    use crate::{identifiers::LambdaARN, schema::deployment::HttpType, storage::StorageCodec};
    use bytestring::ByteString;
    use http::Uri;

    use super::{DeploymentType, EndpointLambdaCompression, ProtocolType};

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

    // Records persisted with an intermediate shape (post-http_version,
    // pre-auth) must deserialise with auth defaulted to None.
    #[derive(serde::Serialize, serde::Deserialize)]
    enum PreAuthDeploymentType {
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

    crate::flexbuffers_storage_encode_decode!(PreAuthDeploymentType);

    #[test]
    fn can_deserialise_without_auth_field() {
        let mut buf = bytes::BytesMut::default();
        StorageCodec::encode(
            &PreAuthDeploymentType::Http {
                address: Uri::from_static("https://svc.example.com"),
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
            },
            &mut buf,
        )
        .unwrap();
        let dt: DeploymentType = StorageCodec::decode(&mut buf).unwrap();
        assert_eq!(
            DeploymentType::Http(HttpType {
                address: Uri::from_static("https://svc.example.com"),
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
                auth: None,
            }),
            dt
        );
    }

    #[test]
    fn auth_field_round_trips() {
        use crate::schema::deployment::{GoogleIdTokenAuth, HttpAuth};

        let original = DeploymentType::Http(HttpType {
            address: Uri::from_static("https://svc.example.com"),
            protocol_type: ProtocolType::BidiStream,
            http_version: http::Version::HTTP_2,
            auth: Some(HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
                ByteString::from_static("https://svc.example.com"),
                Some(ByteString::from_static(
                    "caller@proj.iam.gserviceaccount.com",
                )),
            ))),
        });
        let mut buf = bytes::BytesMut::default();
        StorageCodec::encode(&original, &mut buf).unwrap();
        let decoded: DeploymentType = StorageCodec::decode(&mut buf).unwrap();
        assert_eq!(original, decoded);
    }

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
            DeploymentType::Http(HttpType {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
                auth: None,
            }),
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
            DeploymentType::Http(HttpType {
                address: Uri::from_static("google.com"),
                protocol_type: ProtocolType::RequestResponse,
                http_version: http::Version::HTTP_11,
                auth: None,
            }),
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
            Deployment {
                id,
                ty: DeploymentType::Http(HttpType {
                    address: "http://localhost:9080".parse().unwrap(),
                    protocol_type: ProtocolType::BidiStream,
                    http_version: http::Version::HTTP_2,
                    auth: None,
                }),
                supported_protocol_versions: 1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                sdk_version: None,
                created_at: MillisSinceEpoch::now(),
                metadata: Default::default(),
                additional_headers: Default::default(),
                info: vec![],
            }
        }

        pub fn mock_with_uri(uri: &str) -> Deployment {
            let id = "dp_15VqmTOnXH3Vv2pl5HOG7UB"
                .parse()
                .expect("valid stable deployment id");
            Deployment {
                id,
                ty: DeploymentType::Http(HttpType {
                    address: uri.parse().unwrap(),
                    protocol_type: ProtocolType::BidiStream,
                    http_version: http::Version::HTTP_2,
                    auth: None,
                }),
                supported_protocol_versions: 1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                sdk_version: None,
                created_at: MillisSinceEpoch::now(),
                metadata: Default::default(),
                additional_headers: Default::default(),
                info: vec![],
            }
        }
    }

    #[derive(Default, Clone, Debug)]
    pub struct MockDeploymentMetadataRegistry {
        pub deployments: HashMap<DeploymentId, Deployment>,
        pub latest_deployment: HashMap<String, DeploymentId>,
    }

    impl MockDeploymentMetadataRegistry {
        pub fn mock_deployment(&mut self, deployment: Deployment) {
            self.deployments.insert(deployment.id, deployment);
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
            self.deployments.get(deployment_id).cloned()
        }

        fn get_deployment_and_services(
            &self,
            deployment_id: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            self.deployments
                .get(deployment_id)
                .cloned()
                .map(|deployment| (deployment, vec![]))
        }

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
            self.deployments
                .values()
                .map(|deployment| (deployment.clone(), vec![]))
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
