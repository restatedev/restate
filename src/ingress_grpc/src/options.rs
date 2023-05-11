use super::reflection::ServerReflection;
use super::HyperServerIngress;
use super::*;

use prost_reflect::{DeserializeOptions, SerializeOptions};
use restate_common::types::IngressId;
use restate_service_metadata::MethodDescriptorRegistry;
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
        true
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
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "IngressOptions"))]
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

    pub fn build<DescriptorRegistry, InvocationFactory, ReflectionService>(
        self,
        ingress_id: IngressId,
        descriptor_registry: DescriptorRegistry,
        invocation_factory: InvocationFactory,
        reflection_service: ReflectionService,
    ) -> (
        IngressDispatcherLoop,
        HyperServerIngress<DescriptorRegistry, InvocationFactory, ReflectionService>,
    )
    where
        DescriptorRegistry: MethodDescriptorRegistry + Clone + Send + 'static,
        InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
        ReflectionService: ServerReflection,
    {
        let Options {
            bind_address,
            concurrency_limit,
            json,
        } = self;

        let ingress_dispatcher_loop = IngressDispatcherLoop::new(ingress_id);

        let (hyper_ingress_server, _) = HyperServerIngress::new(
            bind_address,
            concurrency_limit,
            json,
            ingress_id,
            descriptor_registry,
            invocation_factory,
            reflection_service,
            ingress_dispatcher_loop.create_command_sender(),
        );

        (ingress_dispatcher_loop, hyper_ingress_server)
    }
}
