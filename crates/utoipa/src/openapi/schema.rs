// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenAPI Schema Object][schema] types which can be
//! used to define field properties, enum values, array or object types.
//!
//! [schema]: https://spec.openapis.org/oas/latest.html#schema-object
use std::collections::BTreeMap;

use super::RefOr;
use super::extensions::Extensions;
use super::{Response, builder, security::SecurityScheme, set_value};
use crate::openapi::path::Parameter;
use serde::{Deserialize, Serialize};

/// Create an _`empty`_ [`Schema`] that serializes to _`null`_.
///
/// Can be used in places where an item can be serialized as `null`. This is used with unit type
/// enum variants and tuple unit types.
pub fn empty() -> Schema {
    Schema::default()
}

builder! {
    ComponentsBuilder;

    /// Implements [OpenAPI Components Object][components] which holds supported
    /// reusable objects.
    ///
    /// Components can hold either reusable types themselves or references to other reusable
    /// types.
    ///
    /// [components]: https://spec.openapis.org/oas/latest.html#components-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct Components {
        /// Map of reusable [OpenAPI Schema Object][schema]s.
        ///
        /// [schema]: https://spec.openapis.org/oas/latest.html#schema-object
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub schemas: BTreeMap<String, RefOr<Schema>>,

        /// Map of reusable response name, to [OpenAPI Response Object][response]s or [OpenAPI
        /// Reference][reference]s to [OpenAPI Response Object][response]s.
        ///
        /// [response]: https://spec.openapis.org/oas/latest.html#response-object
        /// [reference]: https://spec.openapis.org/oas/latest.html#reference-object
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub responses: BTreeMap<String, RefOr<Response>>,

        /// Map of reusable [OpenAPI Parameter Object][Parameter]s.
        ///
        /// [parameter]: https://spec.openapis.org/oas/latest.html#parameter-object
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub parameters: BTreeMap<String, RefOr<Parameter>>,

        /// Map of reusable [OpenAPI Security Scheme Object][security_scheme]s.
        ///
        /// [security_scheme]: https://spec.openapis.org/oas/latest.html#security-scheme-object
        #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
        pub security_schemes: BTreeMap<String, SecurityScheme>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Components {
    /// Construct a new [`Components`].
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
    /// Add [`SecurityScheme`] to [`Components`].
    ///
    /// Accepts two arguments where first is the name of the [`SecurityScheme`]. This is later when
    /// referenced by [`SecurityRequirement`][requirement]s. Second parameter is the [`SecurityScheme`].
    ///
    /// [requirement]: ../security/struct.SecurityRequirement.html
    pub fn add_security_scheme<N: Into<String>, S: Into<SecurityScheme>>(
        &mut self,
        name: N,
        security_scheme: S,
    ) {
        self.security_schemes
            .insert(name.into(), security_scheme.into());
    }

    /// Add iterator of [`SecurityScheme`]s to [`Components`].
    ///
    /// Accepts two arguments where first is the name of the [`SecurityScheme`]. This is later when
    /// referenced by [`SecurityRequirement`][requirement]s. Second parameter is the [`SecurityScheme`].
    ///
    /// [requirement]: ../security/struct.SecurityRequirement.html
    pub fn add_security_schemes_from_iter<
        I: IntoIterator<Item = (N, S)>,
        N: Into<String>,
        S: Into<SecurityScheme>,
    >(
        &mut self,
        schemas: I,
    ) {
        self.security_schemes.extend(
            schemas
                .into_iter()
                .map(|(name, item)| (name.into(), item.into())),
        );
    }
}

impl ComponentsBuilder {
    /// Add [`Schema`] to [`Components`].
    ///
    /// Accepts two arguments where first is name of the schema and second is the schema itself.
    pub fn schema<S: Into<String>, I: Into<RefOr<Schema>>>(mut self, name: S, schema: I) -> Self {
        self.schemas.insert(name.into(), schema.into());

        self
    }

    /// Add schemas from iter
    pub fn schemas_from_iter<
        I: IntoIterator<Item = (S, C)>,
        C: Into<RefOr<Schema>>,
        S: Into<String>,
    >(
        mut self,
        schemas: I,
    ) -> Self {
        self.schemas.extend(
            schemas
                .into_iter()
                .map(|(name, schema)| (name.into(), schema.into())),
        );

        self
    }

    /// Add [`struct@Response`] to [`Components`].
    ///
    /// Method accepts tow arguments; `name` of the reusable response and `response` which is the
    /// reusable response itself.
    pub fn response<S: Into<String>, R: Into<RefOr<Response>>>(
        mut self,
        name: S,
        response: R,
    ) -> Self {
        self.responses.insert(name.into(), response.into());
        self
    }

    /// Add [`struct@Response`] to [`Components`].
    ///
    /// Method accepts tow arguments; `name` of the reusable response and `response` which is the
    /// reusable response itself.
    pub fn parameter<S: Into<String>, R: Into<RefOr<Parameter>>>(
        mut self,
        name: S,
        parameter: R,
    ) -> Self {
        self.parameters.insert(name.into(), parameter.into());
        self
    }

    /// Add multiple [`struct@Response`]s to [`Components`] from iterator.
    ///
    /// Like the [`ComponentsBuilder::schemas_from_iter`] this allows adding multiple responses by
    /// any iterator what returns tuples of (name, response) values.
    pub fn responses_from_iter<
        I: IntoIterator<Item = (S, R)>,
        S: Into<String>,
        R: Into<RefOr<Response>>,
    >(
        mut self,
        responses: I,
    ) -> Self {
        self.responses.extend(
            responses
                .into_iter()
                .map(|(name, response)| (name.into(), response.into())),
        );

        self
    }

    /// Add [`SecurityScheme`] to [`Components`].
    ///
    /// Accepts two arguments where first is the name of the [`SecurityScheme`]. This is later when
    /// referenced by [`SecurityRequirement`][requirement]s. Second parameter is the [`SecurityScheme`].
    ///
    /// [requirement]: ../security/struct.SecurityRequirement.html
    pub fn security_scheme<N: Into<String>, S: Into<SecurityScheme>>(
        mut self,
        name: N,
        security_scheme: S,
    ) -> Self {
        self.security_schemes
            .insert(name.into(), security_scheme.into());

        self
    }

    /// Add openapi extensions (x-something) of the API.
    pub fn extensions(mut self, extensions: Option<Extensions>) -> Self {
        set_value!(self extensions extensions)
    }
}

/// Is super type for [OpenAPI Schema Object][schemas]. Schema is reusable resource what can be
/// referenced from path operations and other components using [`Ref`].
///
/// [schemas]: https://spec.openapis.org/oas/latest.html#schema-object
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[serde(transparent)]
pub struct Schema(pub serde_json::Value);

impl Default for Schema {
    fn default() -> Self {
        Schema(serde_json::Value::Object(Default::default()))
    }
}

impl Schema {
    /// Create new schema. This assumes the schema is valid!
    pub fn new(schema: serde_json::Value) -> Self {
        Self(schema)
    }
}

impl From<RefOr<Schema>> for Schema {
    fn from(value: RefOr<Schema>) -> Self {
        match value {
            RefOr::Ref(_) => {
                panic!("Invalid type `RefOr::Ref` provided, cannot convert to RefOr::T<Schema>")
            }
            RefOr::T(value) => value,
        }
    }
}

builder! {
    RefBuilder;

    /// Implements [OpenAPI Reference Object][reference] that can be used to reference
    /// reusable components such as [`Schema`]s or [`Response`]s.
    ///
    /// [reference]: https://spec.openapis.org/oas/latest.html#reference-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    pub struct Ref {
        /// Reference location of the actual component.
        #[serde(rename = "$ref")]
        pub ref_location: String,

        /// A description which by default should override that of the referenced component.
        /// Description supports markdown syntax. If referenced object type does not support
        /// description this field does not have effect.
        #[serde(skip_serializing_if = "String::is_empty", default)]
        pub description: String,

        /// A short summary which by default should override that of the referenced component. If
        /// referenced component does not support summary field this does not have effect.
        #[serde(skip_serializing_if = "String::is_empty", default)]
        pub summary: String,
    }
}

impl Ref {
    /// Construct a new [`Ref`] with custom ref location. In most cases this is not necessary
    /// and [`Ref::from_schema_name`] could be used instead.
    pub fn new<I: Into<String>>(ref_location: I) -> Self {
        Self {
            ref_location: ref_location.into(),
            ..Default::default()
        }
    }

    /// Construct a new [`Ref`] from provided schema name. This will create a [`Ref`] that
    /// references the the reusable schemas.
    pub fn from_schema_name<I: Into<String>>(schema_name: I) -> Self {
        Self::new(format!("#/components/schemas/{}", schema_name.into()))
    }

    /// Construct a new [`Ref`] from provided response name. This will create a [`Ref`] that
    /// references the reusable response.
    pub fn from_response_name<I: Into<String>>(response_name: I) -> Self {
        Self::new(format!("#/components/responses/{}", response_name.into()))
    }
}

impl RefBuilder {
    /// Add or change reference location of the actual component.
    pub fn ref_location(mut self, ref_location: String) -> Self {
        set_value!(self ref_location ref_location)
    }

    /// Add or change reference location of the actual component automatically formatting the $ref
    /// to `#/components/schemas/...` format.
    pub fn ref_location_from_schema_name<S: Into<String>>(mut self, schema_name: S) -> Self {
        set_value!(self ref_location format!("#/components/schemas/{}", schema_name.into()))
    }

    // TODO: REMOVE THE unnecessary description Option wrapping.

    /// Add or change description which by default should override that of the referenced component.
    /// Description supports markdown syntax. If referenced object type does not support
    /// description this field does not have effect.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(Into::into).unwrap_or_default())
    }

    /// Add or change short summary which by default should override that of the referenced component. If
    /// referenced component does not support summary field this does not have effect.
    pub fn summary<S: Into<String>>(mut self, summary: S) -> Self {
        set_value!(self summary summary.into())
    }
}

impl From<RefBuilder> for RefOr<Schema> {
    fn from(builder: RefBuilder) -> Self {
        Self::Ref(builder.build())
    }
}

impl From<Ref> for RefOr<Schema> {
    fn from(r: Ref) -> Self {
        Self::Ref(r)
    }
}

impl<T> From<T> for RefOr<T> {
    fn from(t: T) -> Self {
        Self::T(t)
    }
}

impl Default for RefOr<Schema> {
    fn default() -> Self {
        Self::T(Schema::default())
    }
}
