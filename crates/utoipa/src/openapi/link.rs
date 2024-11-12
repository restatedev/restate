//! Implements [Open API Link Object][link_object] for responses.
//!
//! [link_object]: https://spec.openapis.org/oas/latest.html#link-object
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::extensions::Extensions;
use super::{builder, Server};

builder! {
    LinkBuilder;

    /// Implements [Open API Link Object][link_object] for responses.
    ///
    /// The `Link` represents possible design time link for a response. It does not guarantee
    /// callers ability to invoke it but rather provides known relationship between responses and
    /// other operations.
    ///
    /// For computing links, and providing instructions to execute them,
    /// a runtime [expression][expression] is used for accessing values in an operation
    /// and using them as parameters while invoking the linked operation.
    ///
    /// [expression]: https://spec.openapis.org/oas/latest.html#runtime-expressions
    /// [link_object]: https://spec.openapis.org/oas/latest.html#link-object
    #[derive(Serialize, Deserialize, Clone, PartialEq, Default)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[non_exhaustive]
    pub struct Link {
        /// A relative or absolute URI reference to an OAS operation. This field is
        /// mutually exclusive of the _`operation_id`_ field, and **must** point to an [Operation
        /// Object][operation].
        /// Relative _`operation_ref`_ values may be used to locate an existing [Operation
        /// Object][operation] in the OpenAPI definition. See the rules for resolving [Relative
        /// References][relative_references].
        ///
        /// [relative_references]: https://spec.openapis.org/oas/latest.html#relative-references-in-uris
        /// [operation]: ../path/struct.Operation.html
        #[serde(skip_serializing_if = "String::is_empty", default)]
        pub operation_ref: String,

        /// The name of an existing, resolvable OAS operation, as defined with a unique
        /// _`operation_id`_.
        /// This field is mutually exclusive of the _`operation_ref`_ field.
        #[serde(skip_serializing_if = "String::is_empty", default)]
        pub operation_id: String,

        /// A map representing parameters to pass to an operation as specified with _`operation_id`_
        /// or identified by _`operation_ref`_. The key is parameter name to be used and value can
        /// be any value supported by JSON or an [expression][expression] e.g. `$path.id`
        ///
        /// [expression]: https://spec.openapis.org/oas/latest.html#runtime-expressions
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub parameters: BTreeMap<String, serde_json::Value>,

        /// A literal value or an [expression][expression] to be used as request body when operation is called.
        ///
        /// [expression]: https://spec.openapis.org/oas/latest.html#runtime-expressions
        #[serde(skip_serializing_if = "Option::is_none")]
        pub request_body: Option<serde_json::Value>,

        /// Description of the link. Value supports Markdown syntax.
        #[serde(skip_serializing_if = "String::is_empty", default)]
        pub description: String,

        /// A [`Server`][server] object to be used by the target operation.
        ///
        /// [server]: ../server/struct.Server.html
        #[serde(skip_serializing_if = "Option::is_none")]
        pub server: Option<Server>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl LinkBuilder {
    /// Set a relative or absolute URI reference to an OAS operation. This field is
    /// mutually exclusive of the _`operation_id`_ field, and **must** point to an [Operation
    /// Object][operation].
    ///
    /// [operation]: ../path/struct.Operation.html
    pub fn operation_ref<S: Into<String>>(mut self, operation_ref: S) -> Self {
        self.operation_ref = operation_ref.into();

        self
    }

    /// Set the name of an existing, resolvable OAS operation, as defined with a unique
    /// _`operation_id`_.
    /// This field is mutually exclusive of the _`operation_ref`_ field.
    pub fn operation_id<S: Into<String>>(mut self, operation_id: S) -> Self {
        self.operation_id = operation_id.into();

        self
    }

    /// Add parameter to be passed to [Operation][operation] upon execution.
    ///
    /// [operation]: ../path/struct.Operation.html
    pub fn parameter<N: Into<String>, V: Into<serde_json::Value>>(
        mut self,
        name: N,
        value: V,
    ) -> Self {
        self.parameters.insert(name.into(), value.into());

        self
    }

    /// Set a literal value or an [expression][expression] to be used as request body when operation is called.
    ///
    /// [expression]: https://spec.openapis.org/oas/latest.html#runtime-expressions
    pub fn request_body<B: Into<serde_json::Value>>(mut self, request_body: Option<B>) -> Self {
        self.request_body = request_body.map(|request_body| request_body.into());

        self
    }

    /// Set description of the link. Value supports Markdown syntax.
    pub fn description<S: Into<String>>(mut self, description: S) -> Self {
        self.description = description.into();

        self
    }

    /// Set a [`Server`][server] object to be used by the target operation.
    ///
    /// [server]: ../server/struct.Server.html
    pub fn server<S: Into<Server>>(mut self, server: Option<S>) -> Self {
        self.server = server.map(|server| server.into());

        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        self.extensions = extensions;

        self
    }
}
