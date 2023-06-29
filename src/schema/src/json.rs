use super::Schemas;

use bytes::Bytes;

pub trait JsonToProtobufMapper {
    fn convert_to_protobuf(self, json: Bytes) -> Result<Bytes, anyhow::Error>;
}

pub trait ProtobufToJsonMapper {
    fn convert_to_json(self, protobuf: Bytes) -> Result<Bytes, anyhow::Error>;
}

pub trait JsonMapperResolver {
    type JsonToProtobufMapper: JsonToProtobufMapper;
    type ProtobufToJsonMapper: ProtobufToJsonMapper;

    fn resolve_json_mapper_for_service(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
    ) -> Option<(Self::JsonToProtobufMapper, Self::ProtobufToJsonMapper)>;
}

// TODO to remove, just here to show the interface and make them compilable
pub struct SampleConverter;

impl JsonToProtobufMapper for SampleConverter {
    fn convert_to_protobuf(self, json: Bytes) -> Result<Bytes, anyhow::Error> {
        todo!()
    }
}

impl ProtobufToJsonMapper for SampleConverter {
    fn convert_to_json(self, protobuf: Bytes) -> Result<Bytes, anyhow::Error> {
        todo!()
    }
}

impl JsonMapperResolver for Schemas {
    type JsonToProtobufMapper = SampleConverter;
    type ProtobufToJsonMapper = SampleConverter;

    fn resolve_json_mapper_for_service(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
    ) -> Option<(Self::JsonToProtobufMapper, Self::ProtobufToJsonMapper)> {
        todo!()
    }
}
