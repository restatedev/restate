// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenAPI Path Object][paths] types.
//!
//! [paths]: https://spec.openapis.org/oas/latest.html#paths-object
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    Deprecated, ExternalDocs, Ref, RefOr, Required, Schema, Server, builder,
    extensions::Extensions,
    request_body::RequestBody,
    response::{Response, Responses},
    security::SecurityRequirement,
    set_value,
};

#[allow(missing_docs)]
#[doc(hidden)]
pub type PathsMap<K, V> = indexmap::IndexMap<K, V>;

builder! {
    PathsBuilder;

    /// Implements [OpenAPI Paths Object][paths].
    ///
    /// Holds relative paths to matching endpoints and operations. The path is appended to the url
    /// from [`Server`] object to construct a full url for endpoint.
    ///
    /// [paths]: https://spec.openapis.org/oas/latest.html#paths-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    pub struct Paths {
        /// Map of relative paths with [`PathItem`]s holding [`Operation`]s matching
        /// api endpoints.
        #[serde(flatten)]
        pub paths: PathsMap<String, PathItem>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Paths {
    /// Construct a new [`Paths`] object.
    pub fn new() -> Self {
        Default::default()
    }

    /// Return _`Option`_ of reference to [`PathItem`] by given relative path _`P`_ if one exists
    /// in [`Paths::paths`] map. Otherwise will return `None`.
    pub fn get_path_item<P: AsRef<str>>(&self, path: P) -> Option<&PathItem> {
        self.paths.get(path.as_ref())
    }

    /// Return _`Option`_ of reference to [`Operation`] from map of paths or `None` if not found.
    ///
    /// * First will try to find [`PathItem`] by given relative path _`P`_ e.g. `"/api/v1/user"`.
    /// * Then tries to find [`Operation`] from [`PathItem`]'s operations by given [`HttpMethod`].
    pub fn get_path_operation<P: AsRef<str>>(
        &self,
        path: P,
        http_method: HttpMethod,
    ) -> Option<&Operation> {
        self.paths
            .get(path.as_ref())
            .and_then(|path| match http_method {
                HttpMethod::Get => path.get.as_ref(),
                HttpMethod::Put => path.put.as_ref(),
                HttpMethod::Post => path.post.as_ref(),
                HttpMethod::Delete => path.delete.as_ref(),
                HttpMethod::Options => path.options.as_ref(),
                HttpMethod::Head => path.head.as_ref(),
                HttpMethod::Patch => path.patch.as_ref(),
                HttpMethod::Trace => path.trace.as_ref(),
            })
    }

    /// Append path operation to the list of paths.
    ///
    /// Method accepts three arguments; `path` to add operation for, `http_methods` list of
    /// allowed HTTP methods for the [`Operation`] and `operation` to be added under the _`path`_.
    ///
    /// If _`path`_ already exists, the provided [`Operation`] will be set to existing path item for
    /// given list of [`HttpMethod`]s.
    pub fn add_path_operation<P: AsRef<str>, O: Into<Operation>>(
        &mut self,
        path: P,
        http_methods: Vec<HttpMethod>,
        operation: O,
    ) {
        let path = path.as_ref();
        let operation = operation.into();
        if let Some(existing_item) = self.paths.get_mut(path) {
            for http_method in http_methods {
                match http_method {
                    HttpMethod::Get => existing_item.get = Some(operation.clone()),
                    HttpMethod::Put => existing_item.put = Some(operation.clone()),
                    HttpMethod::Post => existing_item.post = Some(operation.clone()),
                    HttpMethod::Delete => existing_item.delete = Some(operation.clone()),
                    HttpMethod::Options => existing_item.options = Some(operation.clone()),
                    HttpMethod::Head => existing_item.head = Some(operation.clone()),
                    HttpMethod::Patch => existing_item.patch = Some(operation.clone()),
                    HttpMethod::Trace => existing_item.trace = Some(operation.clone()),
                };
            }
        } else {
            self.paths.insert(
                String::from(path),
                PathItem::from_http_methods(http_methods, operation),
            );
        }
    }

    /// Merge _`other_paths`_ into `self`. On conflicting path the path item operations will be
    /// merged into existing [`PathItem`]. Otherwise path with [`PathItem`] will be appended to
    /// `self`. All [`Extensions`] will be merged from _`other_paths`_ into `self`.
    pub fn merge(&mut self, other_paths: Paths) {
        for (path, that) in other_paths.paths {
            if let Some(this) = self.paths.get_mut(&path) {
                this.merge_operations(that);
            } else {
                self.paths.insert(path, that);
            }
        }

        if let Some(other_paths_extensions) = other_paths.extensions {
            let paths_extensions = self.extensions.get_or_insert(Extensions::default());
            paths_extensions.merge(other_paths_extensions);
        }
    }
}

impl PathsBuilder {
    /// Append [`PathItem`] with path to map of paths. If path already exists it will merge [`Operation`]s of
    /// [`PathItem`] with already found path item operations.
    pub fn path<I: Into<String>>(mut self, path: I, item: PathItem) -> Self {
        let path_string = path.into();
        if let Some(existing_item) = self.paths.get_mut(&path_string) {
            existing_item.merge_operations(item);
        } else {
            self.paths.insert(path_string, item);
        }

        self
    }

    /// Add extensions to the paths section.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

builder! {
    PathItemBuilder;

    /// Implements [OpenAPI Path Item Object][path_item] what describes [`Operation`]s available on
    /// a single path.
    ///
    /// [path_item]: https://spec.openapis.org/oas/latest.html#path-item-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct PathItem {
        /// Optional summary intended to apply all operations in this [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<String>,

        /// Optional description intended to apply all operations in this [`PathItem`].
        /// Description supports markdown syntax.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Alternative [`Server`] array to serve all [`Operation`]s in this [`PathItem`] overriding
        /// the global server array.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub servers: Option<Vec<Server>>,

        /// List of [`Parameter`]s common to all [`Operation`]s in this [`PathItem`]. Parameters cannot
        /// contain duplicate parameters. They can be overridden in [`Operation`] level but cannot be
        /// removed there.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parameters: Option<Vec<RefOr<Parameter>>>,

        /// Get [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub get: Option<Operation>,

        /// Put [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub put: Option<Operation>,

        /// Post [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub post: Option<Operation>,

        /// Delete [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub delete: Option<Operation>,

        /// Options [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub options: Option<Operation>,

        /// Head [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub head: Option<Operation>,

        /// Patch [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub patch: Option<Operation>,

        /// Trace [`Operation`] for the [`PathItem`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub trace: Option<Operation>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl PathItem {
    /// Construct a new [`PathItem`] with provided [`Operation`] mapped to given [`HttpMethod`].
    pub fn new<O: Into<Operation>>(http_method: HttpMethod, operation: O) -> Self {
        let mut path_item = Self::default();
        match http_method {
            HttpMethod::Get => path_item.get = Some(operation.into()),
            HttpMethod::Put => path_item.put = Some(operation.into()),
            HttpMethod::Post => path_item.post = Some(operation.into()),
            HttpMethod::Delete => path_item.delete = Some(operation.into()),
            HttpMethod::Options => path_item.options = Some(operation.into()),
            HttpMethod::Head => path_item.head = Some(operation.into()),
            HttpMethod::Patch => path_item.patch = Some(operation.into()),
            HttpMethod::Trace => path_item.trace = Some(operation.into()),
        };

        path_item
    }

    /// Constructs a new [`PathItem`] with given [`Operation`] set for provided [`HttpMethod`]s.
    pub fn from_http_methods<I: IntoIterator<Item = HttpMethod>, O: Into<Operation>>(
        http_methods: I,
        operation: O,
    ) -> Self {
        let mut path_item = Self::default();
        let operation = operation.into();
        for method in http_methods {
            match method {
                HttpMethod::Get => path_item.get = Some(operation.clone()),
                HttpMethod::Put => path_item.put = Some(operation.clone()),
                HttpMethod::Post => path_item.post = Some(operation.clone()),
                HttpMethod::Delete => path_item.delete = Some(operation.clone()),
                HttpMethod::Options => path_item.options = Some(operation.clone()),
                HttpMethod::Head => path_item.head = Some(operation.clone()),
                HttpMethod::Patch => path_item.patch = Some(operation.clone()),
                HttpMethod::Trace => path_item.trace = Some(operation.clone()),
            };
        }

        path_item
    }

    /// Merge all defined [`Operation`]s from given [`PathItem`] to `self` if `self` does not have
    /// existing operation.
    pub fn merge_operations(&mut self, path_item: PathItem) {
        if path_item.get.is_some() && self.get.is_none() {
            self.get = path_item.get;
        }
        if path_item.put.is_some() && self.put.is_none() {
            self.put = path_item.put;
        }
        if path_item.post.is_some() && self.post.is_none() {
            self.post = path_item.post;
        }
        if path_item.delete.is_some() && self.delete.is_none() {
            self.delete = path_item.delete;
        }
        if path_item.options.is_some() && self.options.is_none() {
            self.options = path_item.options;
        }
        if path_item.head.is_some() && self.head.is_none() {
            self.head = path_item.head;
        }
        if path_item.patch.is_some() && self.patch.is_none() {
            self.patch = path_item.patch;
        }
        if path_item.trace.is_some() && self.trace.is_none() {
            self.trace = path_item.trace;
        }
    }
}

impl PathItemBuilder {
    /// Append a new [`Operation`] by [`HttpMethod`] to this [`PathItem`]. Operations can
    /// hold only one operation per [`HttpMethod`].
    pub fn operation<O: Into<Operation>>(mut self, http_method: HttpMethod, operation: O) -> Self {
        match http_method {
            HttpMethod::Get => self.get = Some(operation.into()),
            HttpMethod::Put => self.put = Some(operation.into()),
            HttpMethod::Post => self.post = Some(operation.into()),
            HttpMethod::Delete => self.delete = Some(operation.into()),
            HttpMethod::Options => self.options = Some(operation.into()),
            HttpMethod::Head => self.head = Some(operation.into()),
            HttpMethod::Patch => self.patch = Some(operation.into()),
            HttpMethod::Trace => self.trace = Some(operation.into()),
        };

        self
    }

    /// Add or change summary intended to apply all operations in this [`PathItem`].
    pub fn summary<S: Into<String>>(mut self, summary: Option<S>) -> Self {
        set_value!(self summary summary.map(|summary| summary.into()))
    }

    /// Add or change optional description intended to apply all operations in this [`PathItem`].
    /// Description supports markdown syntax.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add list of alternative [`Server`]s to serve all [`Operation`]s in this [`PathItem`] overriding
    /// the global server array.
    pub fn servers<I: IntoIterator<Item = Server>>(mut self, servers: Option<I>) -> Self {
        set_value!(self servers servers.map(|servers| servers.into_iter().collect()))
    }

    /// Append list of [`Parameter`]s common to all [`Operation`]s to this [`PathItem`].
    pub fn parameters<I: IntoIterator<Item = RefOr<Parameter>>>(
        mut self,
        parameters: Option<I>,
    ) -> Self {
        set_value!(self parameters parameters.map(|parameters| parameters.into_iter().collect()))
    }

    /// Add openapi extensions (x-something) to this [`PathItem`].
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

/// HTTP method of the operation.
///
/// List of supported HTTP methods <https://spec.openapis.org/oas/latest.html#path-item-object>
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum HttpMethod {
    /// Type mapping for HTTP _GET_ request.
    Get,
    /// Type mapping for HTTP _POST_ request.
    Post,
    /// Type mapping for HTTP _PUT_ request.
    Put,
    /// Type mapping for HTTP _DELETE_ request.
    Delete,
    /// Type mapping for HTTP _OPTIONS_ request.
    Options,
    /// Type mapping for HTTP _HEAD_ request.
    Head,
    /// Type mapping for HTTP _PATCH_ request.
    Patch,
    /// Type mapping for HTTP _TRACE_ request.
    Trace,
}

builder! {
    OperationBuilder;

    /// Implements [OpenAPI Operation Object][operation] object.
    ///
    /// [operation]: https://spec.openapis.org/oas/latest.html#operation-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Operation {
        /// List of tags used for grouping operations.
        ///
        /// When used with derive [`#[restate_utoipa::path(...)]`][derive_path] attribute macro the default
        /// value used will be resolved from handler path provided in `#[openapi(paths(...))]` with
        /// [`#[derive(OpenApi)]`][derive_openapi] macro. If path resolves to `None` value `crate` will
        /// be used by default.
        ///
        /// [derive_path]: ../../attr.path.html
        /// [derive_openapi]: ../../derive.OpenApi.html
        #[serde(skip_serializing_if = "Option::is_none")]
        pub tags: Option<Vec<String>>,

        /// Short summary what [`Operation`] does.
        ///
        /// When used with derive [`#[restate_utoipa::path(...)]`][derive_path] attribute macro the value
        /// is taken from **first line** of doc comment.
        ///
        /// [derive_path]: ../../attr.path.html
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<String>,

        /// Long explanation of [`Operation`] behaviour. Markdown syntax is supported.
        ///
        /// When used with derive [`#[restate_utoipa::path(...)]`][derive_path] attribute macro the
        /// doc comment is used as value for description.
        ///
        /// [derive_path]: ../../attr.path.html
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Unique identifier for the API [`Operation`]. Most typically this is mapped to handler function name.
        ///
        /// When used with derive [`#[restate_utoipa::path(...)]`][derive_path] attribute macro the handler function
        /// name will be used by default.
        ///
        /// [derive_path]: ../../attr.path.html
        #[serde(skip_serializing_if = "Option::is_none")]
        pub operation_id: Option<String>,

        /// Additional external documentation for this operation.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub external_docs: Option<ExternalDocs>,

        /// List of applicable parameters for this [`Operation`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub parameters: Option<Vec<RefOr<Parameter>>>,

        /// Optional request body for this [`Operation`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub request_body: Option<RequestBody>,

        /// List of possible responses returned by the [`Operation`].
        pub responses: Responses,

        // TODO
        #[allow(missing_docs)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pub callbacks: Option<String>,

        /// Define whether the operation is deprecated or not and thus should be avoided consuming.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub deprecated: Option<Deprecated>,

        /// Declaration which security mechanisms can be used for for the operation. Only one
        /// [`SecurityRequirement`] must be met.
        ///
        /// Security for the [`Operation`] can be set to optional by adding empty security with
        /// [`SecurityRequirement::default`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub security: Option<Vec<SecurityRequirement>>,

        /// Alternative [`Server`]s for this [`Operation`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub servers: Option<Vec<Server>>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Operation {
    /// Construct a new API [`Operation`].
    pub fn new() -> Self {
        Default::default()
    }
}

impl OperationBuilder {
    /// Add or change tags of the [`Operation`].
    pub fn tags<I: IntoIterator<Item = V>, V: Into<String>>(mut self, tags: Option<I>) -> Self {
        set_value!(self tags tags.map(|tags| tags.into_iter().map(Into::into).collect()))
    }

    /// Append tag to [`Operation`] tags.
    pub fn tag<S: Into<String>>(mut self, tag: S) -> Self {
        let tag_string = tag.into();
        match self.tags {
            Some(ref mut tags) => tags.push(tag_string),
            None => {
                self.tags = Some(vec![tag_string]);
            }
        }

        self
    }

    /// Add or change short summary of the [`Operation`].
    pub fn summary<S: Into<String>>(mut self, summary: Option<S>) -> Self {
        set_value!(self summary summary.map(|summary| summary.into()))
    }

    /// Add or change description of the [`Operation`].
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add or change operation id of the [`Operation`].
    pub fn operation_id<S: Into<String>>(mut self, operation_id: Option<S>) -> Self {
        set_value!(self operation_id operation_id.map(|operation_id| operation_id.into()))
    }

    /// Add or change parameters of the [`Operation`].
    pub fn parameters<I: IntoIterator<Item = P>, P: Into<RefOr<Parameter>>>(
        mut self,
        parameters: Option<I>,
    ) -> Self {
        self.parameters = parameters.map(|parameters| {
            if let Some(mut params) = self.parameters {
                params.extend(parameters.into_iter().map(|parameter| parameter.into()));
                params
            } else {
                parameters
                    .into_iter()
                    .map(|parameter| parameter.into())
                    .collect()
            }
        });

        self
    }

    /// Append parameter to [`Operation`] parameters.
    pub fn parameter<P: Into<RefOr<Parameter>>>(mut self, parameter: P) -> Self {
        match self.parameters {
            Some(ref mut parameters) => parameters.push(parameter.into()),
            None => {
                self.parameters = Some(vec![parameter.into()]);
            }
        }

        self
    }

    /// Add or change request body of the [`Operation`].
    pub fn request_body(mut self, request_body: Option<RequestBody>) -> Self {
        set_value!(self request_body request_body)
    }

    /// Add or change responses of the [`Operation`].
    pub fn responses<R: Into<Responses>>(mut self, responses: R) -> Self {
        set_value!(self responses responses.into())
    }

    /// Append status code and a [`Response`] to the [`Operation`] responses map.
    ///
    /// * `code` must be valid HTTP status code.
    /// * `response` is instances of [`Response`].
    pub fn response<S: Into<String>, R: Into<RefOr<Response>>>(
        mut self,
        code: S,
        response: R,
    ) -> Self {
        self.responses
            .responses
            .insert(code.into(), response.into());

        self
    }

    /// Add or change deprecated status of the [`Operation`].
    pub fn deprecated(mut self, deprecated: Option<Deprecated>) -> Self {
        set_value!(self deprecated deprecated)
    }

    /// Add or change list of [`SecurityRequirement`]s that are available for [`Operation`].
    pub fn securities<I: IntoIterator<Item = SecurityRequirement>>(
        mut self,
        securities: Option<I>,
    ) -> Self {
        set_value!(self security securities.map(|securities| securities.into_iter().collect()))
    }

    /// Append [`SecurityRequirement`] to [`Operation`] security requirements.
    pub fn security(mut self, security: SecurityRequirement) -> Self {
        if let Some(ref mut securities) = self.security {
            securities.push(security);
        } else {
            self.security = Some(vec![security]);
        }

        self
    }

    /// Add or change list of [`Server`]s of the [`Operation`].
    pub fn servers<I: IntoIterator<Item = Server>>(mut self, servers: Option<I>) -> Self {
        set_value!(self servers servers.map(|servers| servers.into_iter().collect()))
    }

    /// Append a new [`Server`] to the [`Operation`] servers.
    pub fn server(mut self, server: Server) -> Self {
        if let Some(ref mut servers) = self.servers {
            servers.push(server);
        } else {
            self.servers = Some(vec![server]);
        }

        self
    }

    /// Add openapi extensions (x-something) of the [`Operation`].
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

builder! {
    ParameterBuilder;

    /// Implements [OpenAPI Parameter Object][parameter] for [`Operation`].
    ///
    /// [parameter]: https://spec.openapis.org/oas/latest.html#parameter-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Parameter {
        /// Name of the parameter.
        ///
        /// * For [`ParameterIn::Path`] this must in accordance to path templating.
        /// * For [`ParameterIn::Query`] `Content-Type` or `Authorization` value will be ignored.
        pub name: String,

        /// Parameter location.
        #[serde(rename = "in")]
        pub parameter_in: ParameterIn,

        /// Markdown supported description of the parameter.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Declares whether the parameter is required or not for api.
        ///
        /// * For [`ParameterIn::Path`] this must and will be [`Required::True`].
        pub required: Required,

        /// Declares the parameter deprecated status.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub deprecated: Option<Deprecated>,
        // pub allow_empty_value: bool, this is going to be removed from further open api spec releases
        /// Schema of the parameter. Typically [`Schema::Object`] is used.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<RefOr<Schema>>,

        /// Describes how [`Parameter`] is being serialized depending on [`Parameter::schema`] (type of a content).
        /// Default value is based on [`ParameterIn`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub style: Option<ParameterStyle>,

        /// When _`true`_ it will generate separate parameter value for each parameter with _`array`_ and _`object`_ type.
        /// This is also _`true`_ by default for [`ParameterStyle::Form`].
        ///
        /// With explode _`false`_:
        /// ```text
        /// color=blue,black,brown
        /// ```
        ///
        /// With explode _`true`_:
        /// ```text
        /// color=blue&color=black&color=brown
        /// ```
        #[serde(skip_serializing_if = "Option::is_none")]
        pub explode: Option<bool>,

        /// Defines whether parameter should allow reserved characters defined by
        /// [RFC3986](https://tools.ietf.org/html/rfc3986#section-2.2) _`:/?#[]@!$&'()*+,;=`_.
        /// This is only applicable with [`ParameterIn::Query`]. Default value is _`false`_.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub allow_reserved: Option<bool>,

        /// Example of [`Parameter`]'s potential value. This examples will override example
        /// within [`Parameter::schema`] if defined.
        #[serde(skip_serializing_if = "Option::is_none")]
        example: Option<Value>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Parameter {
    /// Constructs a new required [`Parameter`] with given name.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            required: Required::True,
            ..Default::default()
        }
    }
}

impl ParameterBuilder {
    /// Add name of the [`Parameter`].
    pub fn name<I: Into<String>>(mut self, name: I) -> Self {
        set_value!(self name name.into())
    }

    /// Add in of the [`Parameter`].
    pub fn parameter_in(mut self, parameter_in: ParameterIn) -> Self {
        set_value!(self parameter_in parameter_in)
    }

    /// Add required declaration of the [`Parameter`]. If [`ParameterIn::Path`] is
    /// defined this is always [`Required::True`].
    pub fn required(mut self, required: Required) -> Self {
        self.required = required;
        // required must be true, if parameter_in is Path
        if self.parameter_in == ParameterIn::Path {
            self.required = Required::True;
        }

        self
    }

    /// Add or change description of the [`Parameter`].
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add or change [`Parameter`] deprecated declaration.
    pub fn deprecated(mut self, deprecated: Option<Deprecated>) -> Self {
        set_value!(self deprecated deprecated)
    }

    /// Add or change [`Parameter`]s schema.
    pub fn schema<I: Into<RefOr<Schema>>>(mut self, component: Option<I>) -> Self {
        set_value!(self schema component.map(|component| component.into()))
    }

    /// Add or change serialization style of [`Parameter`].
    pub fn style(mut self, style: Option<ParameterStyle>) -> Self {
        set_value!(self style style)
    }

    /// Define whether [`Parameter`]s are exploded or not.
    pub fn explode(mut self, explode: Option<bool>) -> Self {
        set_value!(self explode explode)
    }

    /// Add or change whether [`Parameter`] should allow reserved characters.
    pub fn allow_reserved(mut self, allow_reserved: Option<bool>) -> Self {
        set_value!(self allow_reserved allow_reserved)
    }

    /// Add or change example of [`Parameter`]'s potential value.
    pub fn example(mut self, example: Option<Value>) -> Self {
        set_value!(self example example)
    }

    /// Add openapi extensions (x-something) to the [`Parameter`].
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

impl From<Ref> for RefOr<Parameter> {
    fn from(r: Ref) -> Self {
        Self::Ref(r)
    }
}

/// In definition of [`Parameter`].
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum ParameterIn {
    /// Declares that parameter is used as query parameter.
    Query,
    /// Declares that parameter is used as path parameter.
    Path,
    /// Declares that parameter is used as header value.
    Header,
    /// Declares that parameter is used as cookie value.
    Cookie,
}

impl Default for ParameterIn {
    fn default() -> Self {
        Self::Path
    }
}

/// Defines how [`Parameter`] should be serialized.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[serde(rename_all = "camelCase")]
pub enum ParameterStyle {
    /// Path style parameters defined by [RFC6570](https://tools.ietf.org/html/rfc6570#section-3.2.7)
    /// e.g _`;color=blue`_.
    /// Allowed with [`ParameterIn::Path`].
    Matrix,
    /// Label style parameters defined by [RFC6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.5)
    /// e.g _`.color=blue`_.
    /// Allowed with [`ParameterIn::Path`].
    Label,
    /// Form style parameters defined by [RFC6570](https://datatracker.ietf.org/doc/html/rfc6570#section-3.2.8)
    /// e.g. _`color=blue`_. Default value for [`ParameterIn::Query`] [`ParameterIn::Cookie`].
    /// Allowed with [`ParameterIn::Query`] or [`ParameterIn::Cookie`].
    Form,
    /// Default value for [`ParameterIn::Path`] [`ParameterIn::Header`]. e.g. _`blue`_.
    /// Allowed with [`ParameterIn::Path`] or [`ParameterIn::Header`].
    Simple,
    /// Space separated array values e.g. _`blue%20black%20brown`_.
    /// Allowed with [`ParameterIn::Query`].
    SpaceDelimited,
    /// Pipe separated array values e.g. _`blue|black|brown`_.
    /// Allowed with [`ParameterIn::Query`].
    PipeDelimited,
    /// Simple way of rendering nested objects using form parameters .e.g. _`color[B]=150`_.
    /// Allowed with [`ParameterIn::Query`].
    DeepObject,
}
