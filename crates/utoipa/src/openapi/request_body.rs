//! Implements [OpenAPI Request Body][request_body] types.
//!
//! [request_body]: https://spec.openapis.org/oas/latest.html#request-body-object
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::extensions::Extensions;
use super::{builder, set_value, Content, Required};

builder! {
    RequestBodyBuilder;

    /// Implements [OpenAPI Request Body][request_body].
    ///
    /// [request_body]: https://spec.openapis.org/oas/latest.html#request-body-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct RequestBody {
        /// Additional description of [`RequestBody`] supporting markdown syntax.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Map of request body contents mapped by content type e.g. `application/json`.
        pub content: BTreeMap<String, Content>,

        /// Determines whether request body is required in the request or not.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub required: Option<Required>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl RequestBody {
    /// Construct a new [`RequestBody`].
    pub fn new() -> Self {
        Default::default()
    }
}

impl RequestBodyBuilder {
    /// Add description for [`RequestBody`].
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Define [`RequestBody`] required.
    pub fn required(mut self, required: Option<Required>) -> Self {
        set_value!(self required required)
    }

    /// Add [`Content`] by content type e.g `application/json` to [`RequestBody`].
    pub fn content<S: Into<String>>(mut self, content_type: S, content: Content) -> Self {
        self.content.insert(content_type.into(), content);

        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}
