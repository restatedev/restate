use std::collections::HashMap;
use std::slice::Iter;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use datafusion::arrow::array::{ArrayRef, BinaryArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, UInt64Type};
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde::Serialize;

pub(crate) fn is_value_field(field: &Field) -> bool {
    match field.metadata().get("value") {
        Some(flag) => flag.as_str() == "true",
        None => false,
    }
}

pub(crate) fn value_field(table_name: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("value".to_string(), "true".to_string());
    match table_name {
        "deduplication" => {
            Field::new("sequence_number", DataType::UInt64, false).with_metadata(metadata)
        }
        "partition_state_machine" => {
            Field::new("state_value", DataType::UInt64, false).with_metadata(metadata)
        }
        // these fields can be reflected into json
        "inbox" | "journal" | "outbox" | "status" | "timers" => {
            metadata.insert(
                "proto_message".to_string(),
                match table_name {
                    "inbox" => "dev.restate.storage.domain.v1.InboxEntry",
                    "journal" => "dev.restate.storage.domain.v1.JournalEntry",
                    "outbox" => "dev.restate.storage.domain.v1.OutboxMessage",
                    "status" => "dev.restate.storage.domain.v1.InvocationStatus",
                    "timers" => "dev.restate.storage.domain.v1.Timer",
                    _ => unreachable!(),
                }
                .to_string(),
            );
            Field::new("json_value", DataType::Utf8, false).with_metadata(metadata)
        }
        // unchanged, eg for state and anything unimplemented
        _ => Field::new("value", DataType::Binary, false).with_metadata(metadata),
    }
}

pub(crate) fn value_to_typed(
    descriptor_pool: DescriptorPool,
    field: &Field,
    values: Iter<Bytes>,
) -> ArrayRef {
    match field.data_type() {
        DataType::Binary => Arc::new(BinaryArray::from_iter_values(values)),
        DataType::Utf8 => {
            if let Some(message) = field.metadata().get("proto_message") {
                let desc = descriptor_pool
                    .get_message_by_name(message)
                    .unwrap_or_else(|| panic!("must have message {message} in descriptor pool"));
                Arc::new(StringArray::from_iter_values(values.map(|value| {
                    let dynamic = DynamicMessage::decode(desc.clone(), value.clone())
                        .unwrap_or_else(|err| panic!("failed to parse value bytes from field {field} with error {err}. perhaps you need to update your cli?"));

                    let mut serializer = serde_json::Serializer::new(vec![]);
                    dynamic.serialize(&mut serializer).expect("serialization failed");
                    String::from_utf8(serializer.into_inner()).expect("json serializer did not return utf8 bytes")
                })))
            } else {
                panic!(
                    "received utf8 value field {} without knowing how to transcode into json",
                    field
                )
            }
        }
        DataType::UInt64 => Arc::new(PrimitiveArray::<UInt64Type>::from_iter_values(
            values.map(|value| value.clone().get_u64()),
        )),
        _ => panic!(
            "received a value field {} with a type we don't know how to parse",
            field
        ),
    }
}
