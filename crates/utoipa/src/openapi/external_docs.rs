//! Implements [OpenAPI External Docs Object][external_docs] types.
//!
//! [external_docs]: https://spec.openapis.org/oas/latest.html#xml-object
use serde::{Deserialize, Serialize};

use super::extensions::Extensions;
use super::{builder, set_value};

builder! {
    ExternalDocsBuilder;

    /// Reference of external resource allowing extended documentation.
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct ExternalDocs {
        /// Target url for external documentation location.
        pub url: String,
        /// Additional description supporting markdown syntax of the external documentation.
        pub description: Option<String>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl ExternalDocs {
    /// Construct a new [`ExternalDocs`].
    ///
    /// Function takes target url argument for the external documentation location.
    pub fn new<S: AsRef<str>>(url: S) -> Self {
        Self {
            url: url.as_ref().to_string(),
            ..Default::default()
        }
    }
}

impl ExternalDocsBuilder {
    /// Add target url for external documentation location.
    pub fn url<I: Into<String>>(mut self, url: I) -> Self {
        set_value!(self url url.into())
    }

    /// Add additional description of external documentation.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}
