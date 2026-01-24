// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements content object for request body and response.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use serde_json::Value;

use super::builder;
use super::example::Example;
use super::extensions::Extensions;
use super::{RefOr, Schema, encoding::Encoding, set_value};

builder! {
    ContentBuilder;


    /// Content holds request body content or response content.
    ///
    /// [`Content`] implements OpenAPI spec [Media Type Object][media_type]
    ///
    /// [media_type]: <https://spec.openapis.org/oas/latest.html#media-type-object>
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[non_exhaustive]
    pub struct Content {
        /// Schema used in response body or request body.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub schema: Option<RefOr<Schema>>,

        /// Example for request body or response body.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub example: Option<Value>,

        /// Examples of the request body or response body. [`Content::examples`] should match to
        /// media type and specified schema if present. [`Content::examples`] and
        /// [`Content::example`] are mutually exclusive. If both are defined `examples` will
        /// override value in `example`.
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub examples: BTreeMap<String, RefOr<Example>>,

        /// A map between a property name and its encoding information.
        ///
        /// The key, being the property name, MUST exist in the [`Content::schema`] as a property, with
        /// `schema` being a [`Schema::Object`] and this object containing the same property key in
        /// [`Object::properties`](crate::openapi::schema::Object::properties).
        ///
        /// The encoding object SHALL only apply to `request_body` objects when the media type is
        /// multipart or `application/x-www-form-urlencoded`.
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub encoding: BTreeMap<String, Encoding>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Content {
    /// Construct a new [`Content`] object for provided _`schema`_.
    pub fn new<I: Into<RefOr<Schema>>>(schema: Option<I>) -> Self {
        Self {
            schema: schema.map(|schema| schema.into()),
            ..Self::default()
        }
    }
}

impl ContentBuilder {
    /// Add schema.
    pub fn schema<I: Into<RefOr<Schema>>>(mut self, schema: Option<I>) -> Self {
        set_value!(self schema schema.map(|schema| schema.into()))
    }

    /// Add example of schema.
    pub fn example(mut self, example: Option<Value>) -> Self {
        set_value!(self example example)
    }

    /// Add iterator of _`(N, V)`_ where `N` is name of example and `V` is [`Example`][example] to
    /// [`Content`] of a request body or response body.
    ///
    /// [`Content::examples`] and [`Content::example`] are mutually exclusive. If both are defined
    /// `examples` will override value in `example`.
    ///
    /// [example]: ../example/Example.html
    pub fn examples_from_iter<
        E: IntoIterator<Item = (N, V)>,
        N: Into<String>,
        V: Into<RefOr<Example>>,
    >(
        mut self,
        examples: E,
    ) -> Self {
        self.examples.extend(
            examples
                .into_iter()
                .map(|(name, example)| (name.into(), example.into())),
        );

        self
    }

    /// Add an encoding.
    ///
    /// The `property_name` MUST exist in the [`Content::schema`] as a property,
    /// with `schema` being a [`Schema::Object`] and this object containing the same property
    /// key in [`Object::properties`](crate::openapi::schema::Object::properties).
    ///
    /// The encoding object SHALL only apply to `request_body` objects when the media type is
    /// multipart or `application/x-www-form-urlencoded`.
    pub fn encoding<S: Into<String>, E: Into<Encoding>>(
        mut self,
        property_name: S,
        encoding: E,
    ) -> Self {
        self.encoding.insert(property_name.into(), encoding.into());
        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}
