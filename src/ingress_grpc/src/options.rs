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

use prost_reflect::{DeserializeOptions, SerializeOptions};
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::key::KeyExtractor;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// # Json options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct JsonOptions {
    /// # Deserialize: deny unknown fields
    ///
    /// When deserializing, return an error when encountering unknown message fields.
    deserialize_deny_unknown_fields: bool,

    /// # Serialize: stringify 64 bit integers
    ///
    /// When serializing, encode 64-bit integral types as strings.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.stringify_64_bit_integers)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    serialize_stringify_64_bit_integers: bool,

    /// # Serialize: use enum numbers
    ///
    /// When serializing, encode enum values as their numeric value.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.use_enum_numbers)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    serialize_use_enum_numbers: bool,

    /// # Serialize: use proto field name
    ///
    /// When serializing, use the proto field name instead of the lowerCamelCase name in JSON field names.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.use_proto_field_name)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    serialize_use_proto_field_name: bool,

    /// # Serialize: skip default fields
    ///
    /// When serializing, skip fields which have their default value.
    ///
    /// For more details, check the [prost-reflect](https://docs.rs/prost-reflect/0.11.4/prost_reflect/struct.SerializeOptions.html#method.skip_default_fields)
    /// and [Protobuf encoding](https://protobuf.dev/programming-guides/proto3/#json-options) documentation
    serialize_skip_default_fields: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            deserialize_deny_unknown_fields: true,
            serialize_stringify_64_bit_integers: true,
            serialize_use_enum_numbers: false,
            serialize_use_proto_field_name: false,
            serialize_skip_default_fields: false,
        }
    }
}

impl JsonOptions {
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
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the ingress.
    bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded, the ingress will reply immediately with an appropriate status code.
    concurrency_limit: usize,

    /// # Json
    ///
    /// JSON/Protobuf conversion options.
    json: JsonOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            concurrency_limit: 1000,
            json: Default::default(),
        }
    }
}

impl Options {
    pub fn build<Schemas, JsonDecoder, JsonEncoder>(
        self,
        request_tx: restate_ingress_dispatcher::IngressRequestSender,
        schemas: Schemas,
    ) -> HyperServerIngress<Schemas>
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

        let (hyper_ingress_server, _) =
            HyperServerIngress::new(bind_address, concurrency_limit, json, schemas, request_tx);

        hyper_ingress_server
    }
}
