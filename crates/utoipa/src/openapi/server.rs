// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenAPI Server Object][server] types to configure target servers.
//!
//! OpenAPI will implicitly add [`Server`] with `url = "/"` to [`OpenApi`][openapi] when no servers
//! are defined.
//!
//! [`Server`] can be used to alter connection url for _**path operations**_. It can be a
//! relative path e.g `/api/v1` or valid http url e.g. `http://alternative.api.com/api/v1`.
//!
//! Relative path will append to the **sever address** so the connection url for _**path operations**_
//! will become `server address + relative path`.
//!
//! Optionally it also supports parameter substitution with `{variable}` syntax.
//!
//! See [`Modify`][modify] trait for details how add servers to [`OpenApi`][openapi].
//!
//! [server]: https://spec.openapis.org/oas/latest.html#server-object
//! [openapi]: ../struct.OpenApi.html
//! [modify]: ../../trait.Modify.html
use std::{collections::BTreeMap, iter};

use serde::{Deserialize, Serialize};

use super::extensions::Extensions;
use super::{builder, set_value};

builder! {
    ServerBuilder;

    /// Represents target server object. It can be used to alter server connection for
    /// _**path operations**_.
    ///
    /// By default OpenAPI will implicitly implement [`Server`] with `url = "/"` if no servers is provided to
    /// the [`OpenApi`][openapi].
    ///
    /// [openapi]: ../struct.OpenApi.html
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Server {
        /// Target url of the [`Server`]. It can be valid http url or relative path.
        ///
        /// Url also supports variable substitution with `{variable}` syntax. The substitutions
        /// then can be configured with [`Server::variables`] map.
        pub url: String,

        /// Optional description describing the target server url. Description supports markdown syntax.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Optional map of variable name and its substitution value used in [`Server::url`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub variables: Option<BTreeMap<String, ServerVariable>>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Server {
    /// Construct a new [`Server`] with given url. Url can be valid http url or context path of the url.
    ///
    /// If url is valid http url then all path operation request's will be forwarded to the selected [`Server`].
    ///
    /// If url is path of url e.g. `/api/v1` then the url will be appended to the servers address and the
    /// operations will be forwarded to location `server address + url`.
    pub fn new<S: Into<String>>(url: S) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }
}

impl ServerBuilder {
    /// Add url to the target [`Server`].
    pub fn url<U: Into<String>>(mut self, url: U) -> Self {
        set_value!(self url url.into())
    }

    /// Add or change description of the [`Server`].
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add parameter to [`Server`] which is used to substitute values in [`Server::url`].
    ///
    /// * `name` Defines name of the parameter which is being substituted within the url. If url has
    ///   `{username}` substitution then the name should be `username`.
    /// * `parameter` Use [`ServerVariableBuilder`] to define how the parameter is being substituted
    ///   within the url.
    pub fn parameter<N: Into<String>, V: Into<ServerVariable>>(
        mut self,
        name: N,
        variable: V,
    ) -> Self {
        match self.variables {
            Some(ref mut variables) => {
                variables.insert(name.into(), variable.into());
            }
            None => {
                self.variables = Some(BTreeMap::from_iter(iter::once((
                    name.into(),
                    variable.into(),
                ))))
            }
        }

        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

builder! {
    ServerVariableBuilder;

    /// Implements [OpenAPI Server Variable][server_variable] used to substitute variables in [`Server::url`].
    ///
    /// [server_variable]: https://spec.openapis.org/oas/latest.html#server-variable-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    pub struct ServerVariable {
        /// Default value used to substitute parameter if no other value is being provided.
        #[serde(rename = "default")]
        pub default_value: String,

        /// Optional description describing the variable of substitution. Markdown syntax is supported.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Enum values can be used to limit possible options for substitution. If enum values is used
        /// the [`ServerVariable::default_value`] must contain one of the enum values.
        #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
        pub enum_values: Option<Vec<String>>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl ServerVariableBuilder {
    /// Add default value for substitution.
    pub fn default_value<S: Into<String>>(mut self, default_value: S) -> Self {
        set_value!(self default_value default_value.into())
    }

    /// Add or change description of substituted parameter.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }

    /// Add or change possible values used to substitute parameter.
    pub fn enum_values<I: IntoIterator<Item = V>, V: Into<String>>(
        mut self,
        enum_values: Option<I>,
    ) -> Self {
        set_value!(self enum_values enum_values
            .map(|enum_values| enum_values.into_iter().map(|value| value.into()).collect()))
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}
