// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains all the different APIs for accessing schemas.

#[cfg(feature = "deployment")]
pub mod deployment {
    use super::service::ServiceMetadata;
    use bytes::Bytes;
    use bytestring::ByteString;
    use http::header::{HeaderName, HeaderValue};
    use http::Uri;
    use restate_types::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
    use restate_types::time::MillisSinceEpoch;
    use std::collections::HashMap;
    use std::fmt;
    use std::fmt::{Display, Formatter};

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum ProtocolType {
        RequestResponse,
        BidiStream,
    }

    #[derive(Debug, Clone, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct DeliveryOptions {
        #[cfg_attr(
            feature = "serde",
            serde(
                with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
            )
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "HashMap<String, String>"))]
        pub additional_headers: HashMap<HeaderName, HeaderValue>,
    }

    impl DeliveryOptions {
        pub fn new(additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
            Self { additional_headers }
        }
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", serde_with::serde_as)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct Deployment {
        pub id: DeploymentId,
        pub metadata: DeploymentMetadata,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", serde_with::serde_as)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct DeploymentMetadata {
        pub ty: DeploymentType,
        pub delivery_options: DeliveryOptions,
        pub created_at: MillisSinceEpoch,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", serde_with::serde_as)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum DeploymentType {
        Http {
            #[cfg_attr(
                feature = "serde",
                serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
            )]
            #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
            address: Uri,
            protocol_type: ProtocolType,
        },
        Lambda {
            arn: LambdaARN,
            #[cfg_attr(feature = "serde_schema", schemars(with = "Option<String>"))]
            assume_role_arn: Option<ByteString>,
        },
    }

    impl DeploymentType {
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
            delivery_options: DeliveryOptions,
        ) -> Self {
            Self {
                ty: DeploymentType::Http {
                    address,
                    protocol_type,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
            }
        }

        pub fn new_lambda(
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
            delivery_options: DeliveryOptions,
        ) -> Self {
            Self {
                ty: DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
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

    pub trait DeploymentMetadataResolver {
        fn resolve_latest_deployment_for_service(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<Deployment>;

        fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

        fn get_deployment_descriptor_pool(&self, deployment_id: &DeploymentId) -> Option<Bytes>;

        fn get_deployment_and_services(
            &self,
            deployment_id: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)>;

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)>;
    }

    #[cfg(feature = "mocks")]
    pub mod mocks {
        use super::*;

        use std::collections::HashMap;

        impl Deployment {
            pub fn mock() -> Deployment {
                let id = "dp_15VqmTOnXH3Vv2pl5HOG7UB"
                    .parse()
                    .expect("valid stable deployment id");
                let metadata = DeploymentMetadata::new_http(
                    "http://localhost:9080".parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                );

                Deployment { id, metadata }
            }

            pub fn mock_with_uri(uri: &str) -> Deployment {
                let id = DeploymentId::new();
                let metadata = DeploymentMetadata::new_http(
                    uri.parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                );
                Deployment { id, metadata }
            }
        }

        #[derive(Default, Clone)]
        pub struct MockDeploymentMetadataRegistry {
            pub deployments: HashMap<DeploymentId, DeploymentMetadata>,
            pub latest_deployment: HashMap<String, DeploymentId>,
        }

        impl MockDeploymentMetadataRegistry {
            pub fn mock_service(&mut self, name: &str) {
                self.mock_service_with_metadata(name, Deployment::mock());
            }

            pub fn mock_service_with_metadata(&mut self, name: &str, deployment: Deployment) {
                self.latest_deployment
                    .insert(name.to_string(), deployment.id);
                self.deployments.insert(deployment.id, deployment.metadata);
            }
        }

        impl DeploymentMetadataResolver for MockDeploymentMetadataRegistry {
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

            fn get_deployment_descriptor_pool(
                &self,
                _deployment_id: &DeploymentId,
            ) -> Option<Bytes> {
                todo!()
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
}

#[cfg(feature = "service")]
pub mod service {
    use bytes::Bytes;
    use restate_types::identifiers::{DeploymentId, ServiceRevision};

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum InstanceType {
        Keyed,
        Unkeyed,
        Singleton,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct ServiceMetadata {
        pub name: String,
        pub methods: Vec<MethodMetadata>,
        pub instance_type: InstanceType,
        /// # Deployment Id
        ///
        /// Deployment exposing the latest revision of the service.
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        pub deployment_id: DeploymentId,
        /// # Revision
        ///
        /// Latest revision of the service.
        pub revision: ServiceRevision,
        /// # Public
        ///
        /// If true, the service can be invoked through the ingress.
        /// If false, the service can be invoked only from another Restate service.
        pub public: bool,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct MethodMetadata {
        pub name: String,
        /// # Input type
        ///
        /// Fully qualified message name of the input to the method
        pub input_type: String,
        /// # Output type
        ///
        /// Fully qualified message name of the output of the method
        pub output_type: String,
        /// # Key field number
        ///
        /// If this is a keyed service, the Protobuf field number of the key within the input type,
        /// Otherwise `null`.
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
        pub key_field_number: Option<u32>,
    }

    /// This API will return services registered by the user. It won't include built-in services.
    pub trait ServiceMetadataResolver {
        fn resolve_latest_service_metadata(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceMetadata>;

        fn descriptors(&self, service_name: impl AsRef<str>) -> Option<Vec<Bytes>>;

        fn list_services(&self) -> Vec<ServiceMetadata>;

        /// Returns None if the service doesn't exists, Some(is_public) otherwise.
        fn is_service_public(&self, service_name: impl AsRef<str>) -> Option<bool>;
    }
}

#[cfg(feature = "json_conversion")]
pub mod json {
    use bytes::Bytes;

    pub trait JsonToProtobufMapper {
        fn json_to_protobuf(
            self,
            json: Bytes,
            deserialize_options: &prost_reflect::DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error>;

        fn json_value_to_protobuf(
            self,
            json: serde_json::Value,
            deserialize_options: &prost_reflect::DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error>;
    }

    pub trait ProtobufToJsonMapper {
        fn protobuf_to_json(
            self,
            protobuf: Bytes,
            serialize_options: &prost_reflect::SerializeOptions,
        ) -> Result<Bytes, anyhow::Error>;

        fn protobuf_to_json_value(
            self,
            protobuf: Bytes,
            serialize_options: &prost_reflect::SerializeOptions,
        ) -> Result<serde_json::Value, anyhow::Error>;
    }

    pub trait JsonMapperResolver {
        type JsonToProtobufMapper: JsonToProtobufMapper;
        type ProtobufToJsonMapper: ProtobufToJsonMapper;

        fn resolve_json_mapper_for_service(
            &self,
            service_name: impl AsRef<str>,
            method_name: impl AsRef<str>,
        ) -> Option<(Self::JsonToProtobufMapper, Self::ProtobufToJsonMapper)>;
    }
}

#[cfg(any(
    feature = "key_extraction",
    feature = "key_expansion",
    feature = "json_key_conversion"
))]
pub mod key {
    #[cfg(feature = "key_extraction")]
    pub mod extraction {
        use bytes::Bytes;

        #[derive(thiserror::Error, Debug)]
        pub enum Error {
            #[error("unexpected end of buffer when decoding")]
            UnexpectedEndOfBuffer,
            #[error("unexpected value when parsing the payload. It looks like the message schema and the parser directives don't match")]
            UnexpectedValue,
            #[error("error when decoding the payload to extract the message: {0}")]
            Decode(#[from] prost::DecodeError),
            #[error("cannot resolve key extractor")]
            NotFound,
        }

        /// A key extractor provides the logic to extract a key out of a request payload.
        pub trait KeyExtractor {
            /// Extract performs key extraction from a request payload, returning the key in a Restate internal format.
            ///
            /// To perform the inverse operation, check the [`KeyExpander`] trait.
            fn extract(
                &self,
                service_name: impl AsRef<str>,
                service_method: impl AsRef<str>,
                payload: Bytes,
            ) -> Result<Bytes, Error>;
        }
    }

    #[cfg(feature = "key_expansion")]
    pub mod expansion {
        use bytes::Bytes;
        use prost_reflect::DynamicMessage;

        #[derive(thiserror::Error, Debug)]
        pub enum Error {
            #[error("unexpected end of buffer when decoding")]
            UnexpectedEndOfBuffer,
            #[error("unexpected value when parsing the payload. It looks like the message schema and the parser directives don't match")]
            UnexpectedValue,
            #[error("error when decoding the payload to extract the message: {0}")]
            Decode(#[from] prost::DecodeError),
            #[error("cannot resolve key extractor")]
            NotFound,
            #[error("unexpected service instance type to expand the key. Only keys of keyed services can be expanded")]
            UnexpectedServiceInstanceType,
        }

        /// A key expander provides the inverse function of a [`KeyExtractor`].
        pub trait KeyExpander {
            /// Expand takes a Restate key and assigns it to the key field of a [`prost_reflect::DynamicMessage`] generated from the given `descriptor`.
            ///
            /// The provided [`descriptor`] MUST be the same descriptor of the request message of the given `service_name` and `service_method`.
            ///
            /// The result of this method is a message matching the provided `descriptor` with only the key field filled.
            ///
            /// This message can be mapped back and forth to JSON using `prost-reflect` `serde` feature.
            fn expand(
                &self,
                service_name: impl AsRef<str>,
                service_method: impl AsRef<str>,
                key: Bytes,
            ) -> Result<DynamicMessage, Error>;
        }
    }

    #[cfg(feature = "json_key_conversion")]
    pub mod json_conversion {
        use bytes::Bytes;
        use serde_json::Value;

        #[derive(thiserror::Error, Debug)]
        pub enum Error {
            #[error(transparent)]
            Extraction(#[from] super::KeyExtractorError),
            #[error(transparent)]
            Expansion(#[from] super::KeyExpanderError),
            #[error("cannot resolve key extractor")]
            NotFound,
            #[error("unexpected service instance type to expand the key. Only keys of keyed services can be expanded")]
            UnexpectedServiceInstanceType,
            #[error("unexpected value for a singleton service. Singleton service have no service key associated")]
            UnexpectedNonNullSingletonKey,
            #[error("bad unkeyed service key. Expected a string")]
            BadUnkeyedKey,
            #[error("error when decoding the json key: {0}")]
            DecodeJson(#[from] serde_json::Error),
        }

        pub trait RestateKeyConverter {
            fn key_to_json(
                &self,
                service_name: impl AsRef<str>,
                key: impl AsRef<[u8]>,
            ) -> Result<Value, Error>;
            fn json_to_key(
                &self,
                service_name: impl AsRef<str>,
                key: Value,
            ) -> Result<Bytes, Error>;
        }
    }

    // Re-exports
    #[cfg(feature = "key_expansion")]
    pub use expansion::{Error as KeyExpanderError, KeyExpander};
    #[cfg(feature = "key_extraction")]
    pub use extraction::{Error as KeyExtractorError, KeyExtractor};
    #[cfg(feature = "json_key_conversion")]
    pub use json_conversion::{Error as RestateKeyConverterError, RestateKeyConverter};
}

#[cfg(feature = "proto_symbol")]
pub mod proto_symbol {
    use bytes::Bytes;

    pub trait ProtoSymbolResolver {
        fn list_services(&self) -> Vec<String>;

        fn get_file_descriptors_by_symbol_name(&self, symbol: &str) -> Option<Vec<Bytes>>;

        fn get_file_descriptor(&self, file_name: &str) -> Option<Bytes>;
    }
}

#[cfg(feature = "subscription")]
pub mod subscription {
    use std::collections::HashMap;
    use std::fmt;

    #[derive(Debug, Clone, Eq, PartialEq, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum KafkaOrderingKeyFormat {
        #[default]
        ConsumerGroupTopicPartition,
        ConsumerGroupTopicPartitionKey,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Source {
        Kafka {
            cluster: String,
            topic: String,
            ordering_key_format: KafkaOrderingKeyFormat,
        },
    }

    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Source::Kafka { cluster, topic, .. } => {
                    write!(f, "kafka://{}/{}", cluster, topic)
                }
            }
        }
    }

    impl PartialEq<&str> for Source {
        fn eq(&self, other: &&str) -> bool {
            self.to_string().as_str() == *other
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum FieldRemapType {
        #[default]
        Bytes,
        String,
    }

    /// Defines how to remap the Event to the target.
    #[derive(Debug, Clone, Eq, PartialEq, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct InputEventRemap {
        /// Index and type to remap the event.key field
        pub key: Option<(u32, FieldRemapType)>,
        /// Index and type to remap the event.payload field
        pub payload: Option<(u32, FieldRemapType)>,
        /// If != 0, index to remap the event.metadata field
        pub attributes_index: Option<u32>,
    }

    /// Specialized version of [super::key::ServiceInstanceType]
    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum EventReceiverServiceInstanceType {
        Keyed {
            // If true, event.ordering_key is the key, otherwise event.key is the key
            ordering_key_is_key: bool,
        },
        Unkeyed,
        Singleton,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Sink {
        Service {
            name: String,
            method: String,
            // If none, the dev.restate.Event will be delivered as is.
            input_event_remap: Option<InputEventRemap>,
            instance_type: EventReceiverServiceInstanceType,
        },
    }

    impl fmt::Display for Sink {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Sink::Service { name, method, .. } => {
                    write!(f, "service://{}/{}", name, method)
                }
            }
        }
    }

    impl PartialEq<&str> for Sink {
        fn eq(&self, other: &&str) -> bool {
            self.to_string().as_str() == *other
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct Subscription {
        id: String,
        source: Source,
        sink: Sink,
        metadata: HashMap<String, String>,
    }

    impl Subscription {
        pub fn new(
            id: String,
            source: Source,
            sink: Sink,
            metadata: HashMap<String, String>,
        ) -> Self {
            Self {
                id,
                source,
                sink,
                metadata,
            }
        }

        pub fn id(&self) -> &str {
            &self.id
        }

        pub fn source(&self) -> &Source {
            &self.source
        }

        pub fn sink(&self) -> &Sink {
            &self.sink
        }

        pub fn metadata(&self) -> &HashMap<String, String> {
            &self.metadata
        }

        pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.metadata
        }
    }

    pub enum ListSubscriptionFilter {
        ExactMatchSink(String),
        ExactMatchSource(String),
    }

    impl ListSubscriptionFilter {
        pub fn matches(&self, sub: &Subscription) -> bool {
            match self {
                ListSubscriptionFilter::ExactMatchSink(sink) => sub.sink == sink.as_str(),
                ListSubscriptionFilter::ExactMatchSource(source) => sub.source == source.as_str(),
            }
        }
    }

    pub trait SubscriptionResolver {
        fn get_subscription(&self, id: &str) -> Option<Subscription>;

        fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription>;
    }

    pub trait SubscriptionValidator {
        type Error: Into<anyhow::Error>;

        fn validate(&self, subscription: Subscription) -> Result<Subscription, Self::Error>;
    }

    #[cfg(feature = "mocks")]
    pub mod mocks {
        use super::*;

        impl Subscription {
            pub fn mock() -> Self {
                Subscription {
                    id: "my-sub".to_string(),
                    source: Source::Kafka {
                        cluster: "my-cluster".to_string(),
                        topic: "my-topic".to_string(),
                        ordering_key_format: Default::default(),
                    },
                    sink: Sink::Service {
                        name: "MySvc".to_string(),
                        method: "MyMethod".to_string(),
                        input_event_remap: None,
                        instance_type: EventReceiverServiceInstanceType::Unkeyed,
                    },
                    metadata: Default::default(),
                }
            }
        }
    }
}
