//! Implements encoding object for content.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::extensions::Extensions;
use super::{builder, path::ParameterStyle, set_value, Header};

builder! {
    EncodingBuilder;

    /// A single encoding definition applied to a single schema [`Object
    /// property`](crate::openapi::schema::Object::properties).
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    #[non_exhaustive]
    pub struct Encoding {
        /// The Content-Type for encoding a specific property. Default value depends on the property
        /// type: for string with format being binary – `application/octet-stream`; for other primitive
        /// types – `text/plain`; for object - `application/json`; for array – the default is defined
        /// based on the inner type. The value can be a specific media type (e.g. `application/json`),
        /// a wildcard media type (e.g. `image/*`), or a comma-separated list of the two types.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub content_type: Option<String>,

        /// A map allowing additional information to be provided as headers, for example
        /// Content-Disposition. Content-Type is described separately and SHALL be ignored in this
        /// section. This property SHALL be ignored if the request body media type is not a multipart.
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub headers: BTreeMap<String, Header>,

        /// Describes how a specific property value will be serialized depending on its type. See
        /// Parameter Object for details on the style property. The behavior follows the same values as
        /// query parameters, including default values. This property SHALL be ignored if the request
        /// body media type is not `application/x-www-form-urlencoded`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub style: Option<ParameterStyle>,

        /// When this is true, property values of type array or object generate separate parameters for
        /// each value of the array, or key-value-pair of the map. For other types of properties this
        /// property has no effect. When style is form, the default value is true. For all other
        /// styles, the default value is false. This property SHALL be ignored if the request body
        /// media type is not `application/x-www-form-urlencoded`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub explode: Option<bool>,

        /// Determines whether the parameter value SHOULD allow reserved characters, as defined by
        /// RFC3986 `:/?#[]@!$&'()*+,;=` to be included without percent-encoding. The default value is
        /// false. This property SHALL be ignored if the request body media type is not
        /// `application/x-www-form-urlencoded`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub allow_reserved: Option<bool>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl EncodingBuilder {
    /// Set the content type. See [`Encoding::content_type`].
    pub fn content_type<S: Into<String>>(mut self, content_type: Option<S>) -> Self {
        set_value!(self content_type content_type.map(Into::into))
    }

    /// Add a [`Header`]. See [`Encoding::headers`].
    pub fn header<S: Into<String>, H: Into<Header>>(mut self, header_name: S, header: H) -> Self {
        self.headers.insert(header_name.into(), header.into());

        self
    }

    /// Set the style [`ParameterStyle`]. See [`Encoding::style`].
    pub fn style(mut self, style: Option<ParameterStyle>) -> Self {
        set_value!(self style style)
    }

    /// Set the explode. See [`Encoding::explode`].
    pub fn explode(mut self, explode: Option<bool>) -> Self {
        set_value!(self explode explode)
    }

    /// Set the allow reserved. See [`Encoding::allow_reserved`].
    pub fn allow_reserved(mut self, allow_reserved: Option<bool>) -> Self {
        set_value!(self allow_reserved allow_reserved)
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}
