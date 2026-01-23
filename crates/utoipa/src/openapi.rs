// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rust implementation of Openapi Spec V3.1.

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Expected, Visitor},
};
use std::fmt::Formatter;

use self::path::PathsMap;
pub use self::{
    content::{Content, ContentBuilder},
    external_docs::ExternalDocs,
    header::{Header, HeaderBuilder},
    info::{Contact, ContactBuilder, Info, InfoBuilder, License, LicenseBuilder},
    path::{HttpMethod, PathItem, Paths, PathsBuilder},
    response::{Response, ResponseBuilder, Responses, ResponsesBuilder},
    schema::{Components, ComponentsBuilder, Ref, Schema},
    security::SecurityRequirement,
    server::{Server, ServerBuilder, ServerVariable, ServerVariableBuilder},
    tag::Tag,
};

pub mod content;
pub mod encoding;
pub mod example;
pub mod extensions;
pub mod external_docs;
pub mod header;
pub mod info;
pub mod link;
pub mod path;
pub mod request_body;
pub mod response;
pub mod schema;
pub mod security;
pub mod server;
pub mod tag;

builder! {
    /// # Examples
    OpenApiBuilder;

    /// Root object of the OpenAPI document.
    ///
    /// You can use [`OpenApi::new`] function to construct a new [`OpenApi`] instance and then
    /// use the fields with mutable access to modify them. This is quite tedious if you are not simply
    /// just changing one thing thus you can also use the [`OpenApiBuilder::new`] to use builder to
    /// construct a new [`OpenApi`] object.
    ///
    /// See more details at <https://spec.openapis.org/oas/latest.html#openapi-object>.
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    #[serde(rename_all = "camelCase")]
    pub struct OpenApi {
        /// OpenAPI document version.
        pub openapi: OpenApiVersion,

        /// Provides metadata about the API.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#info-object>.
        pub info: Info,

        /// Optional list of servers that provides the connectivity information to target servers.
        ///
        /// This is implicitly one server with `url` set to `/`.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#server-object>.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub servers: Option<Vec<Server>>,

        /// Available paths and operations for the API.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#paths-object>.
        pub paths: Paths,

        /// Holds various reusable schemas for the OpenAPI document.
        ///
        /// Few of these elements are security schemas and object schemas.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#components-object>.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub components: Option<Components>,

        /// Declaration of global security mechanisms that can be used across the API. The individual operations
        /// can override the declarations. You can use `SecurityRequirement::default()` if you wish to make security
        /// optional by adding it to the list of securities.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#security-requirement-object>.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub security: Option<Vec<SecurityRequirement>>,

        /// Optional list of tags can be used to add additional documentation to matching tags of operations.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#tag-object>.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub tags: Option<Vec<Tag>>,

        /// Optional global additional documentation reference.
        ///
        /// See more details at <https://spec.openapis.org/oas/latest.html#external-documentation-object>.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub external_docs: Option<ExternalDocs>,

        /// Schema keyword can be used to override default _`$schema`_ dialect which is by default
        /// “<https://spec.openapis.org/oas/3.1/dialect/base>”.
        ///
        /// All the references and individual files could use their own schema dialect.
        #[serde(rename = "$schema", default, skip_serializing_if = "String::is_empty")]
        pub schema: String,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl OpenApi {
    /// Construct a new [`OpenApi`] object.
    ///
    /// Function accepts two arguments one which is [`Info`] metadata of the API; two which is [`Paths`]
    /// containing operations for the API.
    pub fn new<P: Into<Paths>>(info: Info, paths: P) -> Self {
        Self {
            info,
            paths: paths.into(),
            ..Default::default()
        }
    }

    /// Converts this [`OpenApi`] to JSON String. This method essentially calls [`serde_json::to_string`] method.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Converts this [`OpenApi`] to pretty JSON String. This method essentially calls [`serde_json::to_string_pretty`] method.
    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Merge `other` [`OpenApi`] moving `self` and returning combined [`OpenApi`].
    ///
    /// In functionality wise this is exactly same as calling [`OpenApi::merge`] but but provides
    /// leaner API for chaining method calls.
    pub fn merge_from(mut self, other: OpenApi) -> OpenApi {
        self.merge(other);
        self
    }

    /// Merge `other` [`OpenApi`] consuming it and resuming it's content.
    ///
    /// Merge function will take all `self` nonexistent _`servers`, `paths`, `schemas`, `responses`,
    /// `security_schemes`, `security_requirements` and `tags`_ from _`other`_ [`OpenApi`].
    ///
    /// This function performs a shallow comparison for `paths`, `schemas`, `responses` and
    /// `security schemes` which means that only _`name`_ and _`path`_ is used for comparison. When
    /// match occurs the whole item will be ignored from merged results. Only items not
    /// found will be appended to `self`.
    ///
    /// For _`servers`_, _`tags`_ and _`security_requirements`_ the whole item will be used for
    /// comparison. Items not found from `self` will be appended to `self`.
    ///
    /// **Note!** `info`, `openapi`, `external_docs` and `schema` will not be merged.
    pub fn merge(&mut self, mut other: OpenApi) {
        if let Some(other_servers) = &mut other.servers {
            let servers = self.servers.get_or_insert(Vec::new());
            other_servers.retain(|server| !servers.contains(server));
            servers.append(other_servers);
        }

        if !other.paths.paths.is_empty() {
            self.paths.merge(other.paths);
        };

        if let Some(other_components) = &mut other.components {
            let components = self.components.get_or_insert(Components::default());

            other_components
                .schemas
                .retain(|name, _| !components.schemas.contains_key(name));
            components.schemas.append(&mut other_components.schemas);

            other_components
                .responses
                .retain(|name, _| !components.responses.contains_key(name));
            components.responses.append(&mut other_components.responses);

            other_components
                .security_schemes
                .retain(|name, _| !components.security_schemes.contains_key(name));
            components
                .security_schemes
                .append(&mut other_components.security_schemes);
        }

        if let Some(other_security) = &mut other.security {
            let security = self.security.get_or_insert(Vec::new());
            other_security.retain(|requirement| !security.contains(requirement));
            security.append(other_security);
        }

        if let Some(other_tags) = &mut other.tags {
            let tags = self.tags.get_or_insert(Vec::new());
            other_tags.retain(|tag| !tags.contains(tag));
            tags.append(other_tags);
        }
    }

    /// Nest `other` [`OpenApi`] to this [`OpenApi`].
    ///
    /// Nesting performs custom [`OpenApi::merge`] where `other` [`OpenApi`] paths are prepended with given
    /// `path` and then appended to _`paths`_ of this [`OpenApi`] instance. Rest of the  `other`
    /// [`OpenApi`] instance is merged to this [`OpenApi`] with [`OpenApi::merge_from`] method.
    ///
    /// **If multiple** APIs are being nested with same `path` only the **last** one will be retained.
    ///
    /// Method accepts two arguments, first is the path to prepend .e.g. _`/user`_. Second argument
    /// is the [`OpenApi`] to prepend paths for.
    pub fn nest<P: Into<String>, O: Into<OpenApi>>(mut self, path: P, other: O) -> Self {
        let path: String = path.into();
        let mut other_api: OpenApi = other.into();

        let nested_paths = other_api
            .paths
            .paths
            .into_iter()
            .map(|(item_path, item)| {
                let path = format!("{path}{item_path}");
                (path, item)
            })
            .collect::<PathsMap<_, _>>();

        self.paths.paths.extend(nested_paths);

        // paths are already merged, thus we can ignore them
        other_api.paths.paths = PathsMap::new();
        self.merge_from(other_api)
    }
}

impl OpenApiBuilder {
    /// Add [`Info`] metadata of the API.
    pub fn info<I: Into<Info>>(mut self, info: I) -> Self {
        set_value!(self info info.into())
    }

    /// Add iterator of [`Server`]s to configure target servers.
    pub fn servers<I: IntoIterator<Item = Server>>(mut self, servers: Option<I>) -> Self {
        set_value!(self servers servers.map(|servers| servers.into_iter().collect()))
    }

    /// Add [`Paths`] to configure operations and endpoints of the API.
    pub fn paths<P: Into<Paths>>(mut self, paths: P) -> Self {
        set_value!(self paths paths.into())
    }

    /// Add [`Components`] to configure reusable schemas.
    pub fn components(mut self, components: Option<Components>) -> Self {
        set_value!(self components components)
    }

    /// Add iterator of [`SecurityRequirement`]s that are globally available for all operations.
    pub fn security<I: IntoIterator<Item = SecurityRequirement>>(
        mut self,
        security: Option<I>,
    ) -> Self {
        set_value!(self security security.map(|security| security.into_iter().collect()))
    }

    /// Add iterator of [`Tag`]s to add additional documentation for **operations** tags.
    pub fn tags<I: IntoIterator<Item = Tag>>(mut self, tags: Option<I>) -> Self {
        set_value!(self tags tags.map(|tags| tags.into_iter().collect()))
    }

    /// Add [`ExternalDocs`] for referring additional documentation.
    pub fn external_docs(mut self, external_docs: Option<ExternalDocs>) -> Self {
        set_value!(self external_docs external_docs)
    }

    /// Override default `$schema` dialect for the Open API doc.
    pub fn schema<S: Into<String>>(mut self, schema: S) -> Self {
        set_value!(self schema schema.into())
    }
}

/// Represents available [OpenAPI versions][version].
///
/// [version]: <https://spec.openapis.org/oas/latest.html#versions>
#[derive(Serialize, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum OpenApiVersion {
    /// Will serialize to `3.1.0` the latest released OpenAPI version.
    #[serde(rename = "3.1.0")]
    #[default]
    Version31,
}

impl<'de> Deserialize<'de> for OpenApiVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;

        impl Visitor<'_> for VersionVisitor {
            type Value = OpenApiVersion;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a version string in 3.1.x format")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_string(v.to_string())
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let version = v
                    .split('.')
                    .flat_map(|digit| digit.parse::<i8>())
                    .collect::<Vec<_>>();

                if version.len() == 3 && version.first() == Some(&3) && version.get(1) == Some(&1) {
                    Ok(OpenApiVersion::Version31)
                } else {
                    let expected: &dyn Expected = &"3.1.0";
                    Err(Error::invalid_value(
                        serde::de::Unexpected::Str(&v),
                        expected,
                    ))
                }
            }
        }

        deserializer.deserialize_string(VersionVisitor)
    }
}

/// Value used to indicate whether reusable schema, parameter or operation is deprecated.
///
/// The value will serialize to boolean.
#[derive(PartialEq, Eq, Clone, Default)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[allow(missing_docs)]
pub enum Deprecated {
    True,
    #[default]
    False,
}

impl Serialize for Deprecated {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(matches!(self, Self::True))
    }
}

impl<'de> Deserialize<'de> for Deprecated {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoolVisitor;
        impl Visitor<'_> for BoolVisitor {
            type Value = Deprecated;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a bool true or false")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v {
                    true => Ok(Deprecated::True),
                    false => Ok(Deprecated::False),
                }
            }
        }
        deserializer.deserialize_bool(BoolVisitor)
    }
}

/// Value used to indicate whether parameter or property is required.
///
/// The value will serialize to boolean.
#[derive(PartialEq, Eq, Clone, Default)]
#[allow(missing_docs)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum Required {
    True,
    #[default]
    False,
}

impl Serialize for Required {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(matches!(self, Self::True))
    }
}

impl<'de> Deserialize<'de> for Required {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoolVisitor;
        impl Visitor<'_> for BoolVisitor {
            type Value = Required;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a bool true or false")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v {
                    true => Ok(Required::True),
                    false => Ok(Required::False),
                }
            }
        }
        deserializer.deserialize_bool(BoolVisitor)
    }
}

/// A [`Ref`] or some other type `T`.
///
/// Typically used in combination with [`Components`] and is an union type between [`Ref`] and any
/// other given type such as [`Schema`] or [`Response`].
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[serde(untagged)]
pub enum RefOr<T> {
    /// Represents [`Ref`] reference to another OpenAPI object instance. e.g.
    /// `$ref: #/components/schemas/Hello`
    Ref(Ref),
    /// Represents any value that can be added to the [`struct@Components`] e.g. [`enum@Schema`]
    /// or [`struct@Response`].
    T(T),
}

macro_rules! build_fn {
    ( $vis:vis $name:ident $( $field:ident ),+ ) => {
        #[doc = concat!("Constructs a new [`", stringify!($name),"`] taking all fields values from this object.")]
        $vis fn build(self) -> $name {
            $name {
                $(
                    $field: self.$field,
                )*
            }
        }
    };
}
pub(crate) use build_fn;

macro_rules! set_value {
    ( $self:ident $field:ident $value:expr ) => {{
        $self.$field = $value;

        $self
    }};
}
pub(crate) use set_value;

macro_rules! new {
    ( $vis:vis $name:ident ) => {
        #[doc = concat!("Constructs a new [`", stringify!($name),"`].")]
        $vis fn new() -> $name {
            $name {
                ..Default::default()
            }
        }
    };
}
pub(crate) use new;

macro_rules! from {
    ( $name:ident $to:ident $( $field:ident ),+ ) => {
        impl From<$name> for $to {
            fn from(value: $name) -> Self {
                Self {
                    $( $field: value.$field, )*
                }
            }
        }

        impl From<$to> for $name {
            fn from(value: $to) -> Self {
                value.build()
            }
        }
    };
}
pub(crate) use from;

macro_rules! builder {
    ( $( #[$builder_meta:meta] )* $builder_name:ident; $(#[$meta:meta])* $vis:vis $key:ident $name:ident $( $tt:tt )* ) => {
        builder!( @type_impl $builder_name $( #[$meta] )* $vis $key $name $( $tt )* );
        builder!( @builder_impl $( #[$builder_meta] )* $builder_name $( #[$meta] )* $vis $key $name $( $tt )* );
    };

    ( @type_impl $builder_name:ident $( #[$meta:meta] )* $vis:vis $key:ident $name:ident
        { $( $( #[$field_meta:meta] )* $field_vis:vis $field:ident: $field_ty:ty, )* }
    ) => {
        $( #[$meta] )*
        $vis $key $name {
            $( $( #[$field_meta] )* $field_vis $field: $field_ty, )*
        }

        impl $name {
            #[doc = concat!("Construct a new ", stringify!($builder_name), ".")]
            #[doc = ""]
            #[doc = concat!("This is effectively same as calling [`", stringify!($builder_name), "::new`]")]
            $vis fn builder() -> $builder_name {
                $builder_name::new()
            }
        }
    };

    ( @builder_impl $( #[$builder_meta:meta] )* $builder_name:ident $( #[$meta:meta] )* $vis:vis $key:ident $name:ident
        { $( $( #[$field_meta:meta] )* $field_vis:vis $field:ident: $field_ty:ty, )* }
    ) => {
        #[doc = concat!("Builder for [`", stringify!($name),
            "`] with chainable configuration methods to create a new [`", stringify!($name) , "`].")]
        $( #[$builder_meta] )*
        #[cfg_attr(feature = "debug", derive(Debug))]
        $vis $key $builder_name {
            $( $field: $field_ty, )*
        }

        impl Default for $builder_name {
            fn default() -> Self {
                let meta_default: $name = $name::default();
                Self {
                    $( $field: meta_default.$field, )*
                }
            }
        }

        impl $builder_name {
            crate::openapi::new!($vis $builder_name);
            crate::openapi::build_fn!($vis $name $( $field ),* );
        }

        crate::openapi::from!($name $builder_name $( $field ),* );
    };
}
use crate::openapi::extensions::Extensions;
pub(crate) use builder;
