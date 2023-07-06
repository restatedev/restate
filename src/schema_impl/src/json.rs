use super::Schemas;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions};
use restate_schema_api::json::{JsonMapperResolver, JsonToProtobufMapper, ProtobufToJsonMapper};

pub struct JsonToProtobufConverter(json_impl::JsonToProtobufConverterInner);
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
            let method_desc = service_schemas.methods.get(method_name.as_ref())?;

            let input_msg = method_desc.input();
            let json_to_protobuf_mapper = if input_msg.full_name() == "dev.restate.InvokeRequest" {
                JsonToProtobufConverter(
                    json_impl::JsonToProtobufConverterInner::DevRestateInvokeMessage(
                        input_msg,
                        Clone::clone(self),
                    ),
                )
            } else {
                JsonToProtobufConverter(json_impl::JsonToProtobufConverterInner::Other(input_msg))
            };

            Some((
                json_to_protobuf_mapper,
                ProtobufToJsonConverter(method_desc.output()),
            ))
        })
        .flatten()
    }
}

mod json_impl {
    use super::*;

    use anyhow::anyhow;
    use prost_reflect::Value;
    use serde::de::IntoDeserializer;
    use serde::Deserialize;

    impl JsonToProtobufMapper for JsonToProtobufConverter {
        fn convert_to_protobuf(
            self,
            json: Bytes,
            deserialize_options: &DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error> {
            self.0.convert_to_protobuf(json, deserialize_options)
        }
    }

    impl ProtobufToJsonMapper for ProtobufToJsonConverter {
        fn convert_to_json(
            self,
            protobuf: Bytes,
            serialize_options: &SerializeOptions,
        ) -> Result<Bytes, anyhow::Error> {
            let msg = DynamicMessage::decode(self.0, protobuf)?;
            let mut ser = serde_json::Serializer::new(BytesMut::new().writer());
            msg.serialize_with_options(&mut ser, serialize_options)?;
            Ok(ser.into_inner().into_inner().freeze())
        }
    }

    pub(super) enum JsonToProtobufConverterInner {
        Other(MessageDescriptor),
        DevRestateInvokeMessage(MessageDescriptor, Schemas),
    }

    impl JsonToProtobufMapper for JsonToProtobufConverterInner {
        fn convert_to_protobuf(
            self,
            json: Bytes,
            deserialize_options: &DeserializeOptions,
        ) -> Result<Bytes, anyhow::Error> {
            let dynamic_msg = match self {
                JsonToProtobufConverterInner::Other(msg_desc) => {
                    let mut deser = serde_json::Deserializer::from_reader(json.reader());
                    let dynamic_message = DynamicMessage::deserialize_with_options(
                        msg_desc,
                        &mut deser,
                        deserialize_options,
                    )?;
                    deser.end()?;
                    Ok(dynamic_message)
                }
                JsonToProtobufConverterInner::DevRestateInvokeMessage(
                    invoke_request_msg_desc,
                    schemas,
                ) => read_json_invoke_request(
                    invoke_request_msg_desc,
                    schemas,
                    deserialize_options,
                    json,
                ),
            }?;

            Ok(Bytes::from(dynamic_msg.encode_to_vec()))
        }
    }

    fn read_json_invoke_request(
        invoke_request_msg_desc: MessageDescriptor,
        schemas: Schemas,
        deserialize_options: &DeserializeOptions,
        payload_buf: impl Buf + Sized,
    ) -> Result<DynamicMessage, anyhow::Error> {
        #[derive(Deserialize)]
        struct InvokeRequestAdapter {
            service: String,
            method: String,
            argument: serde_json::Value,
        }

        let adapter: InvokeRequestAdapter = serde_json::from_reader(payload_buf.reader())?;

        // Load schemas
        let descriptor = schemas
            .use_service_schema(&adapter.service, |s| {
                s.methods.get(&adapter.method).cloned()
            })
            .flatten()
            .ok_or_else(|| {
                // TODO Not great error propagation, we should probably return an adhoc error,
                //  or at least something that can be used to downcast_ref
                anyhow!("{}/{} not found", adapter.service, adapter.method)
            })?;

        let argument_dynamic_message = DynamicMessage::deserialize_with_options(
            descriptor.input(),
            adapter.argument.into_deserializer(),
            deserialize_options,
        )?;

        let mut invoke_req_msg = DynamicMessage::new(invoke_request_msg_desc);
        invoke_req_msg.set_field_by_name("service", Value::String(adapter.service));
        invoke_req_msg.set_field_by_name("method", Value::String(adapter.method));
        // TODO can skip this serialization by implementing prost::Message on InvokeRequestAdapter
        invoke_req_msg.set_field_by_name(
            "argument",
            Value::Bytes(argument_dynamic_message.encode_to_vec().into()),
        );

        Ok(invoke_req_msg)
    }
}
