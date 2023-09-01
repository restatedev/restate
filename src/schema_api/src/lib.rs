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

#[cfg(feature = "endpoint")]
pub mod endpoint {
    use http::header::{HeaderName, HeaderValue};
    use http::Uri;
    use restate_types::identifiers::{EndpointId, ServiceRevision};
    use std::collections::HashMap;

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
        additional_headers: HashMap<HeaderName, HeaderValue>,
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
    pub struct EndpointMetadata {
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        address: Uri,
        protocol_type: ProtocolType,
        delivery_options: DeliveryOptions,
    }

    impl EndpointMetadata {
        pub fn new(
            address: Uri,
            protocol_type: ProtocolType,
            delivery_options: DeliveryOptions,
        ) -> Self {
            Self {
                address,
                protocol_type,
                delivery_options,
            }
        }

        pub fn address(&self) -> &Uri {
            &self.address
        }

        pub fn protocol_type(&self) -> ProtocolType {
            self.protocol_type
        }

        pub fn additional_headers(&self) -> &HashMap<HeaderName, HeaderValue> {
            &self.delivery_options.additional_headers
        }

        pub fn id(&self) -> EndpointId {
            use base64::Engine;

            // For the time being we generate this from the URI
            // We use only authority and path, as those uniquely identify the endpoint.
            let authority_and_path = format!(
                "{}{}",
                self.address.authority().expect("Must have authority"),
                self.address.path()
            );
            restate_base64_util::URL_SAFE.encode(authority_and_path.as_bytes())
        }
    }

    pub trait EndpointMetadataResolver {
        fn resolve_latest_endpoint_for_service(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<EndpointMetadata>;

        fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata>;

        fn get_endpoint_and_services(
            &self,
            endpoint_id: &EndpointId,
        ) -> Option<(EndpointMetadata, Vec<(String, ServiceRevision)>)>;

        fn get_endpoints(&self) -> Vec<(EndpointMetadata, Vec<(String, ServiceRevision)>)>;
    }

    #[cfg(feature = "mocks")]
    pub mod mocks {
        use super::*;

        use std::collections::HashMap;

        impl EndpointMetadata {
            pub fn mock() -> EndpointMetadata {
                EndpointMetadata::new(
                    "http://localhost:8080".parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                )
            }

            pub fn mock_with_uri(uri: &str) -> EndpointMetadata {
                EndpointMetadata::new(
                    uri.parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                )
            }
        }

        #[derive(Default, Clone)]
        pub struct MockEndpointMetadataRegistry {
            pub endpoints: HashMap<EndpointId, EndpointMetadata>,
            pub latest_endpoint: HashMap<String, EndpointId>,
        }

        impl MockEndpointMetadataRegistry {
            pub fn mock_service(&mut self, name: &str) {
                self.mock_service_with_metadata(
                    name,
                    EndpointMetadata::new(
                        "http://localhost:8080".parse().unwrap(),
                        ProtocolType::BidiStream,
                        Default::default(),
                    ),
                );
            }

            pub fn mock_service_with_metadata(&mut self, name: &str, meta: EndpointMetadata) {
                self.latest_endpoint.insert(name.to_string(), meta.id());
                self.endpoints.insert(meta.id(), meta);
            }
        }

        impl EndpointMetadataResolver for MockEndpointMetadataRegistry {
            fn resolve_latest_endpoint_for_service(
                &self,
                service_name: impl AsRef<str>,
            ) -> Option<EndpointMetadata> {
                self.latest_endpoint
                    .get(service_name.as_ref())
                    .and_then(|endpoint_id| self.get_endpoint(endpoint_id))
            }

            fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata> {
                self.endpoints.get(endpoint_id).cloned()
            }

            fn get_endpoint_and_services(
                &self,
                endpoint_id: &EndpointId,
            ) -> Option<(EndpointMetadata, Vec<(String, ServiceRevision)>)> {
                self.endpoints
                    .get(endpoint_id)
                    .cloned()
                    .map(|e| (e, vec![]))
            }

            fn get_endpoints(&self) -> Vec<(EndpointMetadata, Vec<(String, ServiceRevision)>)> {
                self.endpoints
                    .values()
                    .map(|e| (e.clone(), vec![]))
                    .collect()
            }
        }
    }
}

#[cfg(feature = "service")]
pub mod service {
    use restate_types::identifiers::{EndpointId, ServiceRevision};

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum InstanceType {
        Keyed,
        Unkeyed,
        Singleton,
    }

    #[cfg(any(
        feature = "key_extraction",
        feature = "key_expansion",
        feature = "json_key_conversion"
    ))]
    impl From<&crate::key::ServiceInstanceType> for InstanceType {
        fn from(value: &crate::key::ServiceInstanceType) -> Self {
            match value {
                crate::key::ServiceInstanceType::Keyed { .. } => InstanceType::Keyed,
                crate::key::ServiceInstanceType::Unkeyed => InstanceType::Unkeyed,
                crate::key::ServiceInstanceType::Singleton => InstanceType::Singleton,
            }
        }
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct ServiceMetadata {
        pub name: String,
        pub methods: Vec<String>,
        pub instance_type: InstanceType,
        /// # Endpoint Id
        ///
        /// Endpoint exposing the latest revision of the service.
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        pub endpoint_id: EndpointId,
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

    /// This API will return services registered by the user. It won't include built-in services.
    pub trait ServiceMetadataResolver {
        fn resolve_latest_service_metadata(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceMetadata>;

        fn list_services(&self) -> Vec<ServiceMetadata>;

        /// Returns None if the service doesn't exists, Some(is_public) otherwise.
        fn is_service_public(&self, service_name: impl AsRef<str>) -> Option<bool>;
    }
}

#[cfg(feature = "json_conversion")]
pub mod json {
    use bytes::Bytes;

    pub trait JsonToProtobufMapper {
        fn convert_to_protobuf(
            self,
            json: Bytes,
            deserialize_options: &prost_reflect::DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error>;
    }

    pub trait ProtobufToJsonMapper {
        fn convert_to_json(
            self,
            protobuf: Bytes,
            serialize_options: &prost_reflect::SerializeOptions,
        ) -> Result<Bytes, anyhow::Error>;
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
    #[derive(Debug, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum ServiceInstanceType {
        Keyed {
            /// The `key_structure` of the key field. Every method in a keyed service MUST have the same key type,
            /// hence the key structure is the same.
            key_structure: KeyStructure,
            /// Each method request message might represent the key with a different field number. E.g.
            ///
            /// ```protobuf
            /// message SayHelloRequest {
            ///   Person person = 1 [(dev.restate.ext.field) = KEY];
            /// }
            ///
            /// message SayByeRequest {
            ///   Person person = 2 [(dev.restate.ext.field) = KEY];
            /// }
            /// ```
            service_methods_key_field_root_number: std::collections::HashMap<String, u32>,
        },
        Unkeyed,
        Singleton,
    }

    /// This structure provides the directives to the key parser to parse nested messages.
    #[derive(Debug, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum KeyStructure {
        Scalar,
        Nested(std::collections::BTreeMap<u32, KeyStructure>),
    }

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
            #[error("error when decoding the json key: {0}")]
            DecodeJson(#[from] serde_json::Error),
        }

        pub trait RestateKeyConverter {
            fn key_to_json(
                &self,
                service_name: impl AsRef<str>,
                key: Bytes,
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
    use http::Uri;
    use std::collections::HashMap;

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct Subscription {
        id: String,
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        source: Uri,
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        sink: Uri,
        metadata: HashMap<String, String>,
    }

    impl Subscription {
        pub fn new(id: String, source: Uri, sink: Uri, metadata: HashMap<String, String>) -> Self {
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

        pub fn source(&self) -> &Uri {
            &self.source
        }

        pub fn sink(&self) -> &Uri {
            &self.sink
        }

        pub fn metadata(&self) -> &HashMap<String, String> {
            &self.metadata
        }

        pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.metadata
        }
    }

    pub trait SubscriptionResolver {
        fn get_subscription(&self, id: &str) -> Option<Subscription>;
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
                    source: "kafka://my-cluster/my-topic".parse().unwrap(),
                    sink: "service://MySvc/MyMethod".parse().unwrap(),
                    metadata: Default::default(),
                }
            }
        }
    }
}
