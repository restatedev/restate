use std::ascii;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use bytes::Buf;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, PrimitiveArray, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::datatypes::{
    DataType, Field, TimestampMillisecondType, UInt32Type, UInt64Type,
};
use datafusion::arrow::temporal_conversions;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, Value};
use serde::Serialize;

// Extends datafusion::arrow::datatypes::Field to have more data types with custom encodings
pub(crate) struct ExtendedField {
    name: String,
    data_type: ExtendedDataType,
    nullable: bool,
    value: bool,
}

impl ExtendedField {
    pub(crate) fn new_for_key_column(name: &str, kind: Kind) -> Self {
        let (data_type, nullable) = match name {
            "invocation_id" => (ExtendedDataType::Uuid, false),
            "state_key" => (ExtendedDataType::Hex, false),
            "service_key" => (ExtendedDataType::ServiceKey, true),
            "timestamp" => (ExtendedDataType::Timestamp, false),
            _ => (
                ExtendedDataType::Passthrough(match kind {
                    Kind::Uint32 => DataType::UInt32,
                    Kind::Uint64 => DataType::UInt64,
                    Kind::Bytes => DataType::Binary,
                    Kind::String => DataType::Utf8,
                    typ => panic!("unimplemented datatype {typ:?} in field {name}"),
                }),
                false,
            ),
        };
        ExtendedField {
            name: name.to_string(),
            data_type,
            nullable,
            value: false,
        }
    }

    pub(crate) fn new_value_for_table(table_name: &str) -> Self {
        let (name, data_type) = match table_name {
            "deduplication" => ("sequence_number", ExtendedDataType::UInt64),
            "partition_state_machine" => ("state_value", ExtendedDataType::UInt64),
            // these fields can be reflected into json
            "inbox" | "journal" | "outbox" | "status" | "timers" => (
                "json_value",
                ExtendedDataType::ProtoMessage(table_name.to_string()),
            ),
            "state" => ("state_value", ExtendedDataType::EscapedAscii),
            // unchanged, for anything unimplemented
            _ => ("value", ExtendedDataType::Passthrough(DataType::Binary)),
        };

        ExtendedField {
            name: name.to_string(),
            data_type,
            nullable: false,
            value: true,
        }
    }

    pub(crate) fn wire_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.wire_data_type(),
            self.nullable,
        )
        .with_metadata(self.metadata())
    }

    pub(crate) fn encoded_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.encoded_data_type(),
            self.nullable,
        )
        .with_metadata(self.metadata())
    }

    pub(crate) fn is_value(&self) -> bool {
        self.value
    }

    fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = self.data_type.metadata();
        metadata.insert("value".to_string(), self.value.to_string());
        metadata
    }

    // decode knows how to translate a scalar that the user might provide eg 'deadbeef' into the correct
    // arrow scalar, according to the ExtendedDataType
    pub(crate) fn decode(&self, literal: ScalarValue) -> Result<ScalarValue, DataFusionError> {
        match (&self.data_type, literal) {
            (ExtendedDataType::Uuid, ScalarValue::Utf8(Some(literal))) => {
                Ok(ScalarValue::Binary(Some(
                    uuid::Uuid::parse_str(literal.as_str())
                        .expect("invocation_id was not a valid uuid")
                        .into_bytes()
                        .as_slice()
                        .into(),
                )))
            }
            (ExtendedDataType::ServiceKey, ScalarValue::Utf8(None)) => Ok(ScalarValue::Binary(Some(vec![]))),
            (ExtendedDataType::ServiceKey, ScalarValue::Utf8(Some(literal))) => {
                match hex::decode(literal) {
                    Ok(bytes) => Ok(ScalarValue::Binary(bytes.into())),
                    Err(err) => Err(DataFusionError::Plan(format!("invalid service_key, should be hex: {err}")))
                }
            }
            (ExtendedDataType::Hex, ScalarValue::Utf8(Some(literal))) => {
                match hex::decode(literal) {
                    Ok(bytes) => Ok(ScalarValue::Binary(bytes.into())),
                    Err(err) => Err(DataFusionError::Plan(format!("cannot compare with {}, should be hex: {err}", self.name)))
                }
            }
            (ExtendedDataType::Timestamp, ScalarValue::TimestampMillisecond(Some(millis), Some(tz))) => {
                Ok(ScalarValue::UInt64(Some(
                    temporal_conversions::as_datetime_with_timezone::<TimestampMillisecondType>(
                        millis,
                        arrow_array::timezone::Tz::from_str(tz.deref())?,
                    )
                        .unwrap()
                        .timestamp_millis() as u64,
                )))
            }
            (ExtendedDataType::Passthrough(data_type), literal) if literal.get_datatype().equals_datatype(data_type) => Ok(literal),
            (ExtendedDataType::ProtoMessage(_) | ExtendedDataType::EscapedAscii | ExtendedDataType::UInt64, _) => {
                Err(DataFusionError::Internal(format!(
                    "it should not be necessary to decode {:?} for comparison to field {} as it is only used in value types",
                    self.data_type,
                    self.name,
                )))
            }
            (edt, literal) => {
                Err(DataFusionError::Plan(format!("don't know how to compare a literal of type {:?} with column {} (type {:?})", literal.get_datatype(), self.name, edt)))
            }
        }
    }

    // encode knows how to turn an arrow array created from the grpc response (eg containing binary)
    // into the end-user encoding (eg a hex string)
    pub(crate) fn encode(&self, descriptor_pool: DescriptorPool, array: ArrayRef) -> ArrayRef {
        match &self.data_type {
            ExtendedDataType::Passthrough(_) => array,
            ExtendedDataType::Hex => Arc::new(StringArray::from_iter(
                array
                    .as_binary::<i32>()
                    .iter()
                    .map(|bytes| Some(hex::encode(bytes?))),
            )) as ArrayRef,
            // TODO
            ExtendedDataType::ServiceKey => Arc::new(StringArray::from_iter(
                array
                    .as_binary::<i32>()
                    .iter()
                    .map(|bytes| Some(hex::encode(bytes?))),
            )) as ArrayRef,
            ExtendedDataType::Uuid => Arc::new(StringArray::from_iter(
                array.as_binary::<i32>().iter().map(|bytes| {
                    Some(
                        uuid::Uuid::from_slice(bytes?)
                            .unwrap_or_else(|err| {
                                panic!("uuid is invalid in field {}: {err}", self.name)
                            })
                            .to_string(),
                    )
                }),
            )) as ArrayRef,
            ExtendedDataType::Timestamp => Arc::new(
                TimestampMillisecondArray::from_iter(
                    array
                        .as_primitive::<UInt64Type>()
                        .iter()
                        .map(|timestamp| Some(timestamp? as i64)),
                )
                .with_timezone("+00:00"),
            ) as ArrayRef,
            ExtendedDataType::ProtoMessage(table_name) => {
                let desc = descriptor_pool
                    .get_message_by_name(match table_name.as_str() {
                        "inbox" => "dev.restate.storage.domain.v1.InboxEntry",
                        "journal" => "dev.restate.storage.domain.v1.JournalEntry",
                        "outbox" => "dev.restate.storage.domain.v1.OutboxMessage",
                        "status" => "dev.restate.storage.domain.v1.InvocationStatus",
                        "timers" => "dev.restate.storage.domain.v1.Timer",
                        other => panic!(
                            "don't know how to decode value proto for table {other} in field {}",
                            self.name
                        ),
                    })
                    .expect("descriptor_pool must contain the table value protos");
                Arc::new(StringArray::from_iter(array.as_binary::<i32>().iter().map(|bytes| {
                    let dynamic = DynamicMessage::decode(desc.clone(), bytes?)
                        .unwrap_or_else(|err| panic!("failed to parse {} bytes as protobuf message; perhaps you need to update your cli? {err}", self.name));

                    let mut serializer = serde_json::Serializer::new(vec![]);
                    dynamic.serialize(&mut serializer).expect("serialization failed");
                    Some(String::from_utf8(serializer.into_inner()).expect("json serializer did not return utf8 bytes"))
                })))
            }
            ExtendedDataType::EscapedAscii => Arc::new(StringArray::from_iter(
                array.as_binary::<i32>().iter().map(|bytes| {
                    Some(
                        String::from_utf8(
                            bytes?
                                .iter()
                                .flat_map(|byte| ascii::escape_default(*byte))
                                .collect(),
                        )
                        .expect("ascii escape_default must return utf8"),
                    )
                }),
            )) as ArrayRef,
            ExtendedDataType::UInt64 => Arc::new(PrimitiveArray::<UInt64Type>::from_iter(
                array
                    .as_binary::<i32>()
                    .iter()
                    .map(|bytes| Some(bytes?.get_u64())),
            )) as ArrayRef,
        }
    }

    // proto_to_wire knows how to produce arrow arrays of the *wire* type from protobuf values
    pub(crate) fn proto_to_wire<'a>(&self, keys: impl Iterator<Item = Cow<'a, Value>>) -> ArrayRef {
        match self.data_type.wire_data_type() {
            DataType::UInt64 => Arc::new(PrimitiveArray::<UInt64Type>::from_iter_values(keys.map(
                |field| {
                    field
                        .as_u64()
                        .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                },
            ))) as ArrayRef,
            DataType::UInt32 => Arc::new(PrimitiveArray::<UInt32Type>::from_iter_values(keys.map(
                |field| {
                    field
                        .as_u32()
                        .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                },
            ))) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from_iter_values(keys.map(|field| {
                field
                    .as_str()
                    .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                    .to_owned()
            }))) as ArrayRef,
            DataType::Binary => Arc::new(BinaryArray::from_iter_values(keys.map(|field| {
                field
                    .as_bytes()
                    .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                    .to_owned()
            }))) as ArrayRef,
            other => panic!("unsupported field type {other}"),
        }
    }
}

impl From<Field> for ExtendedField {
    fn from(field: Field) -> Self {
        let data_type = ExtendedDataType::from_field(&field);

        let value = bool::from_str(
            field
                .metadata()
                .get("value")
                .expect("fields must have value metadata"),
        )
        .expect("value metadata must be a bool");

        ExtendedField {
            name: field.name().clone(),
            data_type,
            nullable: field.is_nullable(),
            value,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ExtendedDataType {
    Passthrough(DataType),
    Hex,
    ServiceKey,
    Uuid,
    Timestamp,
    ProtoMessage(String),
    EscapedAscii,
    UInt64,
}

impl ExtendedDataType {
    fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        let edt = match self {
            ExtendedDataType::Passthrough(_) => "Passthrough",
            ExtendedDataType::Hex => "Hex",
            ExtendedDataType::ServiceKey => "ServiceKey",
            ExtendedDataType::Uuid => "Uuid",
            ExtendedDataType::Timestamp => "Timestamp",
            ExtendedDataType::ProtoMessage(table_name) => {
                metadata.insert("proto_message_table".to_string(), table_name.clone());
                "ProtoMessage"
            }
            ExtendedDataType::EscapedAscii => "EscapedAscii",
            ExtendedDataType::UInt64 => "UInt64",
        };
        metadata.insert("extended_data_type".to_string(), edt.to_string());
        metadata
    }

    fn from_field(field: &Field) -> Self {
        match field
            .metadata()
            .get("extended_data_type")
            .expect("fields must have extended_data_type metadata")
            .as_str()
        {
            "Passthrough" => ExtendedDataType::Passthrough(field.data_type().clone()),
            "Hex" => ExtendedDataType::Hex,
            "ServiceKey" => ExtendedDataType::ServiceKey,
            "Uuid" => ExtendedDataType::Uuid,
            "Timestamp" => ExtendedDataType::Timestamp,
            "ProtoMessage" => ExtendedDataType::ProtoMessage(
                field
                    .metadata()
                    .get("proto_message_table")
                    .expect("ProtoMessage fields must have proto_message_table metadata")
                    .to_string(),
            ),
            "EscapedAscii" => ExtendedDataType::EscapedAscii,
            "UInt64" => ExtendedDataType::UInt64,
            other => panic!("unexpected extended_data_type {other}"),
        }
    }

    pub(crate) fn encoded_data_type(&self) -> DataType {
        match self {
            ExtendedDataType::Passthrough(data_type) => data_type.clone(),
            ExtendedDataType::Hex
            | ExtendedDataType::Uuid
            | ExtendedDataType::ServiceKey
            | ExtendedDataType::ProtoMessage(_)
            | ExtendedDataType::EscapedAscii => DataType::Utf8,
            ExtendedDataType::Timestamp => DataType::Timestamp(Millisecond, Some("+00:00".into())),
            ExtendedDataType::UInt64 => DataType::UInt64,
        }
    }

    pub(crate) fn wire_data_type(&self) -> DataType {
        match self {
            ExtendedDataType::Passthrough(data_type) => data_type.clone(),
            ExtendedDataType::Hex
            | ExtendedDataType::Uuid
            | ExtendedDataType::ServiceKey
            | ExtendedDataType::ProtoMessage(_)
            | ExtendedDataType::EscapedAscii
            | ExtendedDataType::UInt64 => DataType::Binary,
            ExtendedDataType::Timestamp => DataType::UInt64,
        }
    }
}
