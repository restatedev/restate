use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::HashMap;

#[cfg(feature = "key_expansion")]
mod expansion;
#[cfg(feature = "key_extraction")]
mod extraction;
#[cfg(feature = "json_key_conversion")]
mod json;

// Re-exports
pub use expansion::{Error as KeyExpanderError, KeyExpander};
pub use extraction::{Error as KeyExtractorError, KeyExtractor};
pub use json::{Error as RestateKeyConverterError, RestateKeyConverter};

// --- Common types

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
        service_methods_key_field_root_number: HashMap<String, u32>,
    },
    Unkeyed,
    Singleton,
}

/// This structure provides the directives to the key parser to parse nested messages.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum KeyStructure {
    Scalar,
    Nested(BTreeMap<u32, KeyStructure>),
}
