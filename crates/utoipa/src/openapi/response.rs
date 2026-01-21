// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenApi Responses][responses].
//!
//! [responses]: https://spec.openapis.org/oas/latest.html#responses-object
use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::openapi::{Ref, RefOr};

use super::extensions::Extensions;
use super::link::Link;
use super::{Content, builder, header::Header, set_value};

builder! {
    ResponsesBuilder;

    /// Implements [OpenAPI Responses Object][responses].
    ///
    /// Responses is a map holding api operation responses identified by their status code.
    ///
    /// [responses]: https://spec.openapis.org/oas/latest.html#responses-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Responses {
        /// Map containing status code as a key with represented response as a value.
        #[serde(flatten)]
        pub responses: BTreeMap<String, RefOr<Response>>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Responses {
    /// Construct a new [`Responses`].
    pub fn new() -> Self {
        Default::default()
    }
}

impl ResponsesBuilder {
    /// Add a [`Response`].
    pub fn response<S: Into<String>, R: Into<RefOr<Response>>>(
        mut self,
        code: S,
        response: R,
    ) -> Self {
        self.responses.insert(code.into(), response.into());

        self
    }

    /// Add responses from an iterator over a pair of `(status_code, response): (String, Response)`.
    pub fn responses_from_iter<
        I: IntoIterator<Item = (C, R)>,
        C: Into<String>,
        R: Into<RefOr<Response>>,
    >(
        mut self,
        iter: I,
    ) -> Self {
        self.responses.extend(
            iter.into_iter()
                .map(|(code, response)| (code.into(), response.into())),
        );
        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

impl From<Responses> for BTreeMap<String, RefOr<Response>> {
    fn from(responses: Responses) -> Self {
        responses.responses
    }
}

impl<C, R> FromIterator<(C, R)> for Responses
where
    C: Into<String>,
    R: Into<RefOr<Response>>,
{
    fn from_iter<T: IntoIterator<Item = (C, R)>>(iter: T) -> Self {
        Self {
            responses: BTreeMap::from_iter(
                iter.into_iter()
                    .map(|(code, response)| (code.into(), response.into())),
            ),
            ..Default::default()
        }
    }
}

builder! {
    ResponseBuilder;

    /// Implements [OpenAPI Response Object][response].
    ///
    /// Response is api operation response.
    ///
    /// [response]: https://spec.openapis.org/oas/latest.html#response-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Response {
        /// Description of the response. Response support markdown syntax.
        pub description: String,

        /// Map of headers identified by their name. `Content-Type` header will be ignored.
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub headers: BTreeMap<String, Header>,

        /// Map of response [`Content`] objects identified by response body content type e.g `application/json`.
        ///
        /// [`Content`]s are stored within [`IndexMap`] to retain their insertion order. Swagger UI
        /// will create and show default example according to the first entry in `content` map.
        #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
        pub content: IndexMap<String, Content>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,

        /// A map of operations links that can be followed from the response. The key of the
        /// map is a short name for the link.
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub links: BTreeMap<String, RefOr<Link>>,
    }
}

impl Response {
    /// Construct a new [`Response`].
    ///
    /// Function takes description as argument.
    pub fn new<S: Into<String>>(description: S) -> Self {
        Self {
            description: description.into(),
            ..Default::default()
        }
    }
}

impl ResponseBuilder {
    /// Add description. Description supports markdown syntax.
    pub fn description<I: Into<String>>(mut self, description: I) -> Self {
        set_value!(self description description.into())
    }

    /// Add [`Content`] of the [`Response`] with content type e.g `application/json`.
    pub fn content<S: Into<String>>(mut self, content_type: S, content: Content) -> Self {
        self.content.insert(content_type.into(), content);

        self
    }

    /// Add response [`Header`].
    pub fn header<S: Into<String>>(mut self, name: S, header: Header) -> Self {
        self.headers.insert(name.into(), header);

        self
    }

    /// Add openapi extensions (x-something) to the [`Header`].
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }

    /// Add link that can be followed from the response.
    pub fn link<S: Into<String>, L: Into<RefOr<Link>>>(mut self, name: S, link: L) -> Self {
        self.links.insert(name.into(), link.into());

        self
    }
}

impl From<ResponseBuilder> for RefOr<Response> {
    fn from(builder: ResponseBuilder) -> Self {
        Self::T(builder.build())
    }
}

impl From<Ref> for RefOr<Response> {
    fn from(r: Ref) -> Self {
        Self::Ref(r)
    }
}
