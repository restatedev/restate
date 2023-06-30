use super::Schemas;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions};
use restate_schema_api::json::{JsonMapperResolver, JsonToProtobufMapper, ProtobufToJsonMapper};

// TODO we should move here the "special handling logic" of dev.restate.InvokeRequest, encapsulated in this JsonProtobufConverter

pub struct JsonProtobufConverter(MessageDescriptor);

impl JsonToProtobufMapper for JsonProtobufConverter {
    fn convert_to_protobuf(
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
}

impl ProtobufToJsonMapper for JsonProtobufConverter {
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

impl JsonMapperResolver for Schemas {
    type JsonToProtobufMapper = JsonProtobufConverter;
    type ProtobufToJsonMapper = JsonProtobufConverter;

    fn resolve_json_mapper_for_service(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
    ) -> Option<(Self::JsonToProtobufMapper, Self::ProtobufToJsonMapper)> {
        self.use_service_schema(service_name, |service_schemas| {
            let method_desc = service_schemas.methods.get(method_name.as_ref())?;
            Some((
                JsonProtobufConverter(method_desc.input()),
                JsonProtobufConverter(method_desc.output()),
            ))
        })
        .flatten()
    }
}
