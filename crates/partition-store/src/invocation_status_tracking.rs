// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    ffi::{CStr, CString},
    time::Duration,
};

use rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use tokio::time::Instant;
use tracing::{debug, warn};

use restate_storage_api::protobuf_types::v1::invocation_status_v2::Status;
use restate_types::storage::StorageCodecKind;

use crate::keys::KeyKind;

/// User-collected table property key for the maximum `modification_time` (millis since epoch)
/// of any `InvocationStatus` row in the SST file.
pub const INVOCATION_STATUS_MAX_MODIFICATION_TIME_PROPERTY: &CStr =
    c"invocation_status_max_modification_time";

/// User-collected table property key for the maximum `creation_time` (millis since epoch)
/// of any `InvocationStatus` row in the SST file.
pub const INVOCATION_STATUS_MAX_CREATION_TIME_PROPERTY: &CStr =
    c"invocation_status_max_creation_time";

/// User-collected table property key indicating whether every `InvocationStatus` row in the
/// SST file has `Completed` status. Value is `"true"` or `"false"`.
pub const INVOCATION_STATUS_ALL_COMPLETED_PROPERTY: &CStr = c"invocation_status_all_completed";

/// Protobuf field number for `status` in `InvocationStatusV2`.
const STATUS_FIELD_TAG: u32 = 1;
/// Protobuf field number for `creation_time` in `InvocationStatusV2`.
const CREATION_TIME_FIELD_TAG: u32 = 5;
/// Protobuf field number for `modification_time` in `InvocationStatusV2`.
const MODIFICATION_TIME_FIELD_TAG: u32 = 6;

/// Collects per-SST metadata for `InvocationStatus` rows: maximum `creation_time`,
/// maximum `modification_time`, and whether all rows are `Completed`.
///
/// Fields are extracted from the raw protobuf values, so they reflect the true
/// values regardless of when the SST was created (flush or compaction).
///
/// If any row fails to parse, the collector is poisoned and no properties are
/// emitted for this SST, since the tracked metadata would be unreliable.
#[derive(Default)]
pub(crate) struct InvocationStatusCollector {
    max_modification_time: Option<u64>,
    max_creation_time: Option<u64>,
    all_completed: Option<bool>,
    /// Set to true if any InvocationStatus value failed to parse. When poisoned,
    /// no properties are emitted in `finish()`.
    poisoned: bool,
    properties: Vec<(CString, CString)>,
    elapsed: Duration,
}

impl TablePropertiesCollector for InvocationStatusCollector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> Result<(), CollectorError> {
        if self.poisoned
            || !matches!(entry_type, EntryType::EntryPut)
            || !key.starts_with(KeyKind::InvocationStatus.as_bytes())
        {
            return Ok(());
        }

        let start = Instant::now();

        match extract_fields(value) {
            Some(fields) => {
                self.max_creation_time = self.max_creation_time.max(Some(fields.creation_time));
                self.max_modification_time = self
                    .max_modification_time
                    .max(Some(fields.modification_time));
                *self.all_completed.get_or_insert(true) &= fields.completed;
            }
            None => {
                self.poisoned = true;
            }
        }

        self.elapsed += start.elapsed();

        Ok(())
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        if !self.poisoned {
            debug!(
                max_creation_time = ?self.max_creation_time,
                max_modification_time = ?self.max_modification_time,
                all_completed = ?self.all_completed,
                elapsed = ?self.elapsed,
                "InvocationStatusCollector added properties to SST"
            );

            if let Some(creation_time) = self.max_creation_time {
                self.properties.push((
                    CString::from(INVOCATION_STATUS_MAX_CREATION_TIME_PROPERTY),
                    CString::new(creation_time.to_string()).unwrap(),
                ));
            }
            if let Some(mod_time) = self.max_modification_time {
                self.properties.push((
                    CString::from(INVOCATION_STATUS_MAX_MODIFICATION_TIME_PROPERTY),
                    CString::new(mod_time.to_string()).unwrap(),
                ));
            }
            if let Some(all_completed) = self.all_completed {
                self.properties.push((
                    CString::from(INVOCATION_STATUS_ALL_COMPLETED_PROPERTY),
                    CString::new(all_completed.to_string()).unwrap(),
                ));
            }
        }
        Ok(self.properties.iter())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        self.properties.iter()
    }

    fn name(&self) -> &CStr {
        c"InvocationStatusCollector"
    }
}

pub(crate) struct InvocationStatusCollectorFactory;

impl TablePropertiesCollectorFactory for InvocationStatusCollectorFactory {
    type Collector = InvocationStatusCollector;

    fn create(
        &mut self,
        _context: rocksdb::table_properties::TablePropertiesCollectorContext,
    ) -> InvocationStatusCollector {
        InvocationStatusCollector::default()
    }

    fn name(&self) -> &CStr {
        c"InvocationStatusCollectorFactory"
    }
}

struct ExtractedFields {
    completed: bool,
    creation_time: u64,
    modification_time: u64,
}

/// Extract `status` (field 1), `creation_time` (field 5), and `modification_time` (field 6)
/// from a raw `InvocationStatus` value.
///
/// The value must have a 1-byte [`StorageCodecKind::Protobuf`] prefix followed by protobuf
/// data. Returns `None` and logs a warning if the value cannot be parsed.
fn extract_fields(value: &[u8]) -> Option<ExtractedFields> {
    if value.is_empty() {
        warn!("Cannot extract InvocationStatus fields: empty value");
        return None;
    }

    // Validate the storage codec prefix
    let codec = match StorageCodecKind::try_from(value[0]) {
        Ok(codec) => codec,
        Err(_) => {
            warn!(
                codec_byte = value[0],
                "Cannot extract InvocationStatus fields: unknown storage codec kind"
            );
            return None;
        }
    };
    if codec != StorageCodecKind::Protobuf {
        warn!(
            %codec,
            "Cannot extract InvocationStatus fields: unexpected storage codec kind"
        );
        return None;
    }

    let mut buf: &[u8] = &value[1..];
    let mut status = None;
    let mut creation_time = None;
    let mut modification_time = None;

    let ctx = prost::encoding::DecodeContext::default();
    while !buf.is_empty() {
        let (tag, wire_type) = match prost::encoding::decode_key(&mut buf) {
            Ok(v) => v,
            Err(err) => {
                warn!(%err, "Cannot extract InvocationStatus fields: failed to decode protobuf key");
                return None;
            }
        };
        if tag == STATUS_FIELD_TAG
            || tag == CREATION_TIME_FIELD_TAG
            || tag == MODIFICATION_TIME_FIELD_TAG
        {
            match prost::encoding::decode_varint(&mut buf) {
                Ok(v) => match tag {
                    STATUS_FIELD_TAG => status = Some(v as i32),
                    CREATION_TIME_FIELD_TAG => creation_time = Some(v),
                    MODIFICATION_TIME_FIELD_TAG => modification_time = Some(v),
                    _ => unreachable!(),
                },
                Err(err) => {
                    warn!(%err, tag, "Cannot extract InvocationStatus fields: failed to decode varint");
                    return None;
                }
            }
        } else if let Err(err) = prost::encoding::skip_field(wire_type, tag, &mut buf, ctx.clone())
        {
            warn!(%err, tag, "Cannot extract InvocationStatus fields: failed to skip protobuf field");
            return None;
        }

        if let Some(status) = status
            && let Some(creation_time) = creation_time
            && let Some(modification_time) = modification_time
        {
            return Some(ExtractedFields {
                completed: status == Status::Completed as i32,
                creation_time,
                modification_time,
            });
        }
    }

    warn!(
        ?status,
        ?creation_time,
        ?modification_time,
        "Cannot extract InvocationStatus fields: not all fields are present"
    );
    None
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use rocksdb::table_properties::{EntryType, TablePropertiesCollector};

    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus, PreFlightInvocationMetadata, ScheduledInvocation,
        StatusTimestamps,
    };
    use restate_storage_api::protobuf_types::{
        PartitionStoreProtobufValue, ProtobufStorageWrapper,
    };
    use restate_types::identifiers::{InvocationUuid, PartitionKey};
    use restate_types::storage::StorageCodec;

    use crate::invocation_status_table::InvocationStatusKey;
    use crate::keys::TableKey;

    use super::*;

    fn encode_invocation_status(creation_time_millis: u64, mod_time_millis: u64) -> BytesMut {
        let timestamps = StatusTimestamps::new(
            creation_time_millis.into(),
            mod_time_millis.into(),
            None,
            None,
            None,
            None,
        );
        let mut metadata = PreFlightInvocationMetadata::mock();
        metadata.timestamps = timestamps;
        let status = InvocationStatus::Scheduled(ScheduledInvocation { metadata });

        let proto: <InvocationStatus as PartitionStoreProtobufValue>::ProtobufType = status.into();
        let mut buf = BytesMut::new();
        StorageCodec::encode(&ProtobufStorageWrapper(proto), &mut buf).unwrap();
        buf
    }

    fn encode_completed_invocation_status(
        creation_time_millis: u64,
        mod_time_millis: u64,
    ) -> BytesMut {
        let mut completed = CompletedInvocation::mock_neo();
        completed.timestamps = StatusTimestamps::new(
            creation_time_millis.into(),
            mod_time_millis.into(),
            None,
            None,
            None,
            None,
        );
        let status = InvocationStatus::Completed(completed);

        let proto: <InvocationStatus as PartitionStoreProtobufValue>::ProtobufType = status.into();
        let mut buf = BytesMut::new();
        StorageCodec::encode(&ProtobufStorageWrapper(proto), &mut buf).unwrap();
        buf
    }

    fn inv_status_key_bytes() -> BytesMut {
        let key = InvocationStatusKey {
            partition_key: 42 as PartitionKey,
            invocation_uuid: InvocationUuid::mock_random(),
        };
        let mut buf = BytesMut::new();
        key.serialize_to(&mut buf);
        buf
    }

    #[test]
    fn extract_and_collect_fields() {
        let creation_a: u64 = 1700000000000;
        let mod_a: u64 = 1700000010000;
        let creation_b: u64 = 1700000050000;
        let mod_b: u64 = 1700000099000;

        // Verify extraction from encoded protobuf (Scheduled status)
        let value_a = encode_invocation_status(creation_a, mod_a);
        let fields_a = extract_fields(&value_a).unwrap();
        assert!(!fields_a.completed);
        assert_eq!(fields_a.creation_time, creation_a);
        assert_eq!(fields_a.modification_time, mod_a);

        let value_b = encode_invocation_status(creation_b, mod_b);
        let fields_b = extract_fields(&value_b).unwrap();
        assert!(!fields_b.completed);
        assert_eq!(fields_b.creation_time, creation_b);
        assert_eq!(fields_b.modification_time, mod_b);

        // Verify collector tracks the max across multiple entries
        let mut collector = InvocationStatusCollector::default();
        let key_buf = inv_status_key_bytes();

        collector
            .add_user_key(&key_buf, &value_a, EntryType::EntryPut, 0, 0)
            .unwrap();
        collector
            .add_user_key(&key_buf, &value_b, EntryType::EntryPut, 0, 0)
            .unwrap();
        assert_eq!(collector.max_creation_time, Some(creation_b));
        assert_eq!(collector.max_modification_time, Some(mod_b));
        // Both entries are Scheduled, so all_completed is false
        assert_eq!(collector.all_completed, Some(false));

        // Non-invocation-status keys are ignored
        collector
            .add_user_key(b"deother_key", b"some_value", EntryType::EntryPut, 0, 0)
            .unwrap();
        assert_eq!(collector.max_creation_time, Some(creation_b));
        assert_eq!(collector.max_modification_time, Some(mod_b));

        // Deletes are ignored
        collector
            .add_user_key(&key_buf, b"", EntryType::EntryDelete, 0, 0)
            .unwrap();
        assert_eq!(collector.max_creation_time, Some(creation_b));
        assert_eq!(collector.max_modification_time, Some(mod_b));

        // Properties are emitted (creation_time, modification_time, all_completed)
        let props: Vec<_> = collector.finish().unwrap().into_iter().collect();
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn poisoned_collector_emits_no_properties() {
        let mut collector = InvocationStatusCollector::default();
        let key_buf = inv_status_key_bytes();

        // Add a valid entry first
        let value = encode_invocation_status(1700000000000, 1700000010000);
        collector
            .add_user_key(&key_buf, &value, EntryType::EntryPut, 0, 0)
            .unwrap();
        assert!(!collector.poisoned);

        // Add a corrupt value with a valid inv status key - this poisons the collector
        let mut corrupt_value = vec![0x01u8]; // valid Protobuf codec prefix
        corrupt_value.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // garbage protobuf
        collector
            .add_user_key(&key_buf, &corrupt_value, EntryType::EntryPut, 0, 0)
            .unwrap();
        assert!(collector.poisoned);

        // No properties emitted despite having valid data from the first entry
        let props: Vec<_> = collector.finish().unwrap().into_iter().collect();
        assert!(props.is_empty());
    }

    #[test]
    fn all_completed_when_only_completed_rows() {
        let key_buf = inv_status_key_bytes();
        let mut collector = InvocationStatusCollector::default();

        let value_a = encode_completed_invocation_status(1700000000000, 1700000010000);
        let fields_a = extract_fields(&value_a).unwrap();
        assert!(fields_a.completed);

        collector
            .add_user_key(&key_buf, &value_a, EntryType::EntryPut, 0, 0)
            .unwrap();
        let value_b = encode_completed_invocation_status(1700000050000, 1700000099000);
        collector
            .add_user_key(&key_buf, &value_b, EntryType::EntryPut, 0, 0)
            .unwrap();

        assert_eq!(collector.all_completed, Some(true));

        let props: Vec<_> = collector.finish().unwrap().into_iter().collect();
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn not_all_completed_with_mixed_statuses() {
        let key_buf = inv_status_key_bytes();
        let mut collector = InvocationStatusCollector::default();

        let completed = encode_completed_invocation_status(1700000000000, 1700000010000);
        collector
            .add_user_key(&key_buf, &completed, EntryType::EntryPut, 0, 0)
            .unwrap();
        assert_eq!(collector.all_completed, Some(true));

        let scheduled = encode_invocation_status(1700000050000, 1700000099000);
        collector
            .add_user_key(&key_buf, &scheduled, EntryType::EntryPut, 0, 0)
            .unwrap();
        assert_eq!(collector.all_completed, Some(false));
    }

    #[test]
    fn extract_returns_none_for_invalid_data() {
        assert!(extract_fields(b"").is_none());
        // Invalid codec prefix (0 is not a valid StorageCodecKind)
        assert!(extract_fields(b"\x00").is_none());
        // Wrong codec kind (FlexbuffersSerde = 2)
        assert!(extract_fields(b"\x02").is_none());
    }
}
