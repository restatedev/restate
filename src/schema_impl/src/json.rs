// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions};
use restate_schema_api::json::{JsonMapperResolver, JsonToProtobufMapper, ProtobufToJsonMapper};

pub struct JsonToProtobufConverter(MessageDescriptor);
pub struct ProtobufToJsonConverter(MessageDescriptor);

impl JsonMapperResolver for Schemas {
    type JsonToProtobufMapper = JsonToProtobufConverter;
    type ProtobufToJsonMapper = ProtobufToJsonConverter;

    fn resolve_json_mapper_for_service(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
    ) -> Option<(Self::JsonToProtobufMapper, Self::ProtobufToJsonMapper)> {
        self.use_service_schema(service_name, |service_schemas| {
            let method_desc = service_schemas
                .methods
                .get(method_name.as_ref())?
                .descriptor();
            Some((
                JsonToProtobufConverter(method_desc.input()),
                ProtobufToJsonConverter(method_desc.output()),
            ))
        })
        .flatten()
    }
}

mod json_impl {
    use super::*;
    use anyhow::Error;
    use serde_json::Value;

    impl JsonToProtobufMapper for JsonToProtobufConverter {
        fn json_to_protobuf(
            self,
            json: Bytes,
            deserialize_options: &DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error> {
            let mut deser = serde_json::Deserializer::from_reader(json.reader());
            let dynamic_message =
                DynamicMessage::deserialize_with_options(self.0, &mut deser, deserialize_options)?;
            deser.end()?;
            Ok(Bytes::from(dynamic_message.encode_to_vec()))
        }

        fn json_value_to_protobuf(
            self,
            json: Value,
            deserialize_options: &DeserializeOptions,
        ) -> Result<Bytes, Error> {
            let dynamic_message =
                DynamicMessage::deserialize_with_options(self.0, json, deserialize_options)?;
            Ok(Bytes::from(dynamic_message.encode_to_vec()))
        }
    }

    impl ProtobufToJsonMapper for ProtobufToJsonConverter {
        fn protobuf_to_json(
            self,
            protobuf: Bytes,
            serialize_options: &SerializeOptions,
        ) -> Result<Bytes, anyhow::Error> {
            let msg = DynamicMessage::decode(self.0, protobuf)?;
            let mut ser = serde_json::Serializer::new(BytesMut::new().writer());
            msg.serialize_with_options(&mut ser, serialize_options)?;
            Ok(ser.into_inner().into_inner().freeze())
        }

        fn protobuf_to_json_value(
            self,
            protobuf: Bytes,
            serialize_options: &SerializeOptions,
        ) -> Result<Value, Error> {
            let msg = DynamicMessage::decode(self.0, protobuf)?;
            Ok(msg.serialize_with_options(serde_json::value::Serializer, serialize_options)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schemas_impl::{
        MethodSchemas, ServiceInstanceType, ServiceLocation, ServiceSchemas,
    };
    use prost_reflect::{MethodDescriptor, ServiceDescriptor};
    use serde::Serialize;
    use serde_json::json;

    fn greeter_service_descriptor() -> ServiceDescriptor {
        restate_pb::mocks::DESCRIPTOR_POOL
            .services()
            .find(|svc| svc.full_name() == "greeter.Greeter")
            .unwrap()
    }

    fn greeter_greet_method_descriptor() -> MethodDescriptor {
        greeter_service_descriptor()
            .methods()
            .find(|m| m.name() == "Greet")
            .unwrap()
    }

    fn schemas_mock() -> Schemas {
        let schemas = Schemas::default();
        schemas.add_mock_service(
            "greeter.Greeter",
            ServiceSchemas {
                revision: 1,
                methods: [(
                    "Greet".to_string(),
                    MethodSchemas::new(greeter_greet_method_descriptor(), Default::default()),
                )]
                .into_iter()
                .collect(),
                instance_type: ServiceInstanceType::Unkeyed,
                location: ServiceLocation::ServiceEndpoint {
                    latest_endpoint: "".to_string(),
                    public: true,
                },
            },
        );
        schemas
    }

    #[test]
    fn decode_greet_json() {
        let schemas = schemas_mock();
        let json_payload = json!({"person": "Francesco"});

        let (decoder, _) = schemas
            .resolve_json_mapper_for_service("greeter.Greeter", "Greet")
            .unwrap();
        let protobuf = decoder
            .json_to_protobuf(
                json_payload.to_string().into(),
                &DeserializeOptions::default(),
            )
            .unwrap();

        assert_eq!(
            restate_pb::mocks::greeter::GreetingRequest::decode(protobuf).unwrap(),
            restate_pb::mocks::greeter::GreetingRequest {
                person: "Francesco".to_string()
            }
        );
    }

    #[test]
    fn decode_invoke_json() {
        let schemas = schemas_mock();

        let json_payload = json!({
            "service": "greeter.Greeter",
            "method": "Greet",
            "argument": {
                "person": "Francesco"
            }
        });

        let (decoder, _) = schemas
            .resolve_json_mapper_for_service("dev.restate.Ingress", "Invoke")
            .unwrap();
        let protobuf = decoder
            .json_to_protobuf(
                json_payload.to_string().into(),
                &DeserializeOptions::default(),
            )
            .unwrap();

        let mut dynamic_message_greeting_request =
            DynamicMessage::new(greeter_greet_method_descriptor().input());
        dynamic_message_greeting_request
            .transcode_from(&restate_pb::mocks::greeter::GreetingRequest {
                person: "Francesco".to_string(),
            })
            .unwrap();
        let json_greeting_request = dynamic_message_greeting_request
            .serialize(serde_json::value::Serializer)
            .unwrap();
        let struct_greeting_request: prost_reflect::prost_types::Struct =
            DynamicMessage::deserialize(
                prost_reflect::DescriptorPool::global()
                    .get_message_by_name("google.protobuf.Struct")
                    .unwrap(),
                json_greeting_request,
            )
            .unwrap()
            .transcode_to()
            .unwrap();

        assert_eq!(
            restate_pb::restate::InvokeRequest::decode(protobuf).unwrap(),
            restate_pb::restate::InvokeRequest {
                service: "greeter.Greeter".to_string(),
                method: "Greet".to_string(),
                argument: Some(restate_pb::restate::invoke_request::Argument::Json(
                    struct_greeting_request
                )),
            }
        );
    }

    #[test]
    fn encode_greet_json() {
        let schemas = schemas_mock();

        let pb_response = Bytes::from(
            restate_pb::mocks::greeter::GreetingResponse {
                greeting: "Hello Francesco".to_string(),
            }
            .encode_to_vec(),
        );

        let (_, encoder) = schemas
            .resolve_json_mapper_for_service("greeter.Greeter", "Greet")
            .unwrap();

        let json_encoded = encoder
            .protobuf_to_json(pb_response, &SerializeOptions::default())
            .unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&json_encoded).unwrap();

        assert_eq!(
            json_body.get("greeting").unwrap().as_str().unwrap(),
            "Hello Francesco"
        );
    }

    #[test]
    fn encode_empty_message() {
        let schemas = schemas_mock();

        let pb_response = Bytes::default();

        let (_, encoder) = schemas
            .resolve_json_mapper_for_service("greeter.Greeter", "Greet")
            .unwrap();

        let json_encoded = encoder
            .protobuf_to_json(pb_response, &SerializeOptions::default())
            .unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&json_encoded).unwrap();

        assert!(json_body.as_object().unwrap().is_empty())
    }

    #[test]
    fn encode_empty_message_with_defaults_encoding() {
        let schemas = schemas_mock();

        let pb_response = Bytes::default();

        let (_, encoder) = schemas
            .resolve_json_mapper_for_service("greeter.Greeter", "Greet")
            .unwrap();

        let json_encoded = encoder
            .protobuf_to_json(
                pb_response,
                &SerializeOptions::new().skip_default_fields(false),
            )
            .unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&json_encoded).unwrap();

        assert_eq!(json_body.get("greeting").unwrap().as_str().unwrap(), "");
    }
}
