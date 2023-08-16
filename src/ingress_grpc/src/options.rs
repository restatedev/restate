// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::HyperServerIngress;
use super::*;

use prost_reflect::{DeserializeOptions, SerializeOptions};
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::key::KeyExtractor;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_types::identifiers::IngressId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// # Json options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub struct JsonOptions {
    /// # Deserialize: deny unknown fields
    ///
    /// When deserializing, return an error when encountering unknown message fields.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "JsonOptions::default_deserialize_deny_unknown_fields")
    )]
    deserialize_deny_unknown_fields: bool,
    /// # Serialize: stringify 64 bit integers
    ///
    /// When serializing, encode 64-bit integral types as strings.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.stringify_64_bit_integers)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "JsonOptions::default_serialize_stringify_64_bit_integers")
    )]
    serialize_stringify_64_bit_integers: bool,
    /// # Serialize: use enum numbers
    ///
    /// When serializing, encode enum values as their numeric value.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.use_enum_numbers)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "JsonOptions::default_serialize_use_enum_numbers")
    )]
    serialize_use_enum_numbers: bool,
    /// # Serialize: use proto field name
    ///
    /// When serializing, use the proto field name instead of the lowerCamelCase name in JSON field names.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.use_proto_field_name)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "JsonOptions::default_serialize_use_proto_field_name")
    )]
    serialize_use_proto_field_name: bool,
    /// # Serialize: skip default fields
    ///
    /// When serializing, skip fields which have their default value.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.skip_default_fields)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "JsonOptions::default_serialize_skip_default_fields")
    )]
    serialize_skip_default_fields: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            deserialize_deny_unknown_fields: JsonOptions::default_deserialize_deny_unknown_fields(),
            serialize_stringify_64_bit_integers:
                JsonOptions::default_serialize_stringify_64_bit_integers(),
            serialize_use_enum_numbers: JsonOptions::default_serialize_use_enum_numbers(),
            serialize_use_proto_field_name: JsonOptions::default_serialize_use_proto_field_name(),
            serialize_skip_default_fields: JsonOptions::default_serialize_skip_default_fields(),
        }
    }
}

impl JsonOptions {
    fn default_deserialize_deny_unknown_fields() -> bool {
        true
    }

    fn default_serialize_stringify_64_bit_integers() -> bool {
        true
    }

    fn default_serialize_use_enum_numbers() -> bool {
        false
    }

    fn default_serialize_use_proto_field_name() -> bool {
        false
    }

    fn default_serialize_skip_default_fields() -> bool {
        false
    }

    pub(crate) fn to_serialize_options(&self) -> SerializeOptions {
        SerializeOptions::new()
            .stringify_64_bit_integers(self.serialize_stringify_64_bit_integers)
            .use_enum_numbers(self.serialize_use_enum_numbers)
            .use_proto_field_name(self.serialize_use_proto_field_name)
            .skip_default_fields(self.serialize_skip_default_fields)
    }

    pub(crate) fn to_deserialize_options(&self) -> DeserializeOptions {
        DeserializeOptions::new().deny_unknown_fields(self.deserialize_deny_unknown_fields)
    }
}

/// # Ingress options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "IngressOptions"))]
#[builder(default)]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the ingress.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_bind_address")
    )]
    bind_address: SocketAddr,
    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded, the ingress will reply immediately with an appropriate status code.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_concurrency_limit")
    )]
    concurrency_limit: usize,
    /// # Json
    ///
    /// JSON/Protobuf conversion options.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    json: JsonOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: Options::default_bind_address(),
            concurrency_limit: Options::default_concurrency_limit(),
            json: Default::default(),
        }
    }
}

impl Options {
    fn default_bind_address() -> SocketAddr {
        "0.0.0.0:9090".parse().unwrap()
    }

    fn default_concurrency_limit() -> usize {
        1000
    }

    pub fn build<Schemas, JsonDecoder, JsonEncoder>(
        self,
        ingress_id: IngressId,
        schemas: Schemas,
        channel_size: usize,
    ) -> (IngressDispatcherLoop, HyperServerIngress<Schemas>)
    where
        Schemas: JsonMapperResolver<
                JsonToProtobufMapper = JsonDecoder,
                ProtobufToJsonMapper = JsonEncoder,
            > + ServiceMetadataResolver
            + ProtoSymbolResolver
            + KeyExtractor
            + Clone
            + Send
            + Sync
            + 'static,
        JsonDecoder: Send,
        JsonEncoder: Send,
    {
        let Options {
            bind_address,
            concurrency_limit,
            json,
        } = self;

        let ingress_dispatcher_loop = IngressDispatcherLoop::new(ingress_id, channel_size);

        let (hyper_ingress_server, _) = HyperServerIngress::new(
            bind_address,
            concurrency_limit,
            json,
            ingress_id,
            schemas,
            ingress_dispatcher_loop.create_command_sender(),
        );

        (ingress_dispatcher_loop, hyper_ingress_server)
    }
}
