//! This crate contains all the different APIs for accessing schemas.

#[cfg(feature = "endpoint")]
pub mod endpoint {
    use restate_types::identifiers::EndpointId;
    use restate_types::service_endpoint::EndpointMetadata;

    pub trait EndpointMetadataResolver {
        fn resolve_latest_endpoint_for_service(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<EndpointMetadata>;

        fn get_endpoint(&self, endpoint_id: &EndpointId) -> Option<EndpointMetadata>;
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
