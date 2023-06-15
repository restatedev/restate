use super::Schemas;

use bytes::Bytes;
use restate_common::utils::GenericError;

pub trait JsonToProtobufMapper {
    // TODO replace GenericError with anyhow::Error,
    //  or another error type which we can downcast to ErrorNotFound in lib.rs
    //  See https://github.com/restatedev/restate/issues/471
    fn convert_to_protobuf(self, json: Bytes) -> Result<Bytes, GenericError>;
}

pub trait ProtobufToJsonMapper {
    fn convert_to_json(self, protobuf: Bytes) -> Result<Bytes, GenericError>;
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
    fn convert_to_protobuf(self, json: Bytes) -> Result<Bytes, GenericError> {
        todo!()
    }
}

impl ProtobufToJsonMapper for SampleConverter {
    fn convert_to_json(self, protobuf: Bytes) -> Result<Bytes, GenericError> {
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
