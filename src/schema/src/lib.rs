// TODO remove once we implement the interfaces
#![allow(unused_variables)]
#![allow(dead_code)]

use std::fmt;

#[cfg(feature = "endpoint")]
pub mod endpoint;
#[cfg(feature = "json_conversion")]
pub mod json;
#[cfg(any(
    feature = "key_extraction",
    feature = "key_expansion",
    feature = "json_key_conversion"
))]
pub mod key;
#[cfg(feature = "proto_symbol")]
pub mod proto_symbol;

// TODO perhaps add CodedError as well?
#[derive(Debug)]
pub struct NotFoundError;

impl fmt::Display for NotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not found in schema registry")
    }
}

impl std::error::Error for NotFoundError {}

/// The schema registry
#[derive(Debug)]
pub struct Schemas {}

impl Default for Schemas {
    fn default() -> Self {
        todo!()
    }
}

impl Schemas {
    fn register_new_endpoint(&self) {
        todo!()
    }
}
