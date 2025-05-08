use std::ffi::{CStr, CString};
use std::sync::Arc;

use ahash::HashMap;
use dashmap::DashMap;
use rocksdb::event_listener::{EventListener, FlushJobInfo};
use rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use tracing::warn;

use restate_storage_api::StorageError;
use restate_types::{identifiers::PartitionId, logs::Lsn};

use crate::PARTITION_CF_PREFIX;
use crate::fsm_table::{PartitionStateMachineKey, SequenceNumber, fsm_variable};
use crate::keys::{KeyKind, TableKey};
use crate::protobuf_types::PartitionStoreProtobufValue;

pub(crate) struct LatestAppliedLsnCollector {
    applied_lsns: HashMap<PartitionId, Lsn>,
    properties: Vec<(CString, CString)>,
}

impl LatestAppliedLsnCollector {
    fn new() -> Self {
        LatestAppliedLsnCollector {
            applied_lsns: Default::default(),
            properties: vec![],
        }
    }
}

impl TablePropertiesCollector for LatestAppliedLsnCollector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: rocksdb::table_properties::EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> Result<(), CollectorError> {
        if !matches!(entry_type, EntryType::EntryPut) {
            return Ok(());
        }

        match extract_partition_applied_lsn(key, value) {
            Ok(None) => Ok(()),
            Ok(Some((partition_id, lsn))) => {
                self.applied_lsns
                    .entry(partition_id)
                    .and_modify(|existing| {
                        if lsn > *existing {
                            *existing = lsn;
                        }
                    })
                    .or_insert(lsn);
                Ok(())
            }
            Err(err) => {
                warn!("Failed to decode partition LSN from raw key-value: {}", err);
                Err(CollectorError::new())
            }
        }
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        for (partition_id, lsn) in &self.applied_lsns {
            self.properties.push((
                CString::new(applied_lsn_property_name(*partition_id)).unwrap(),
                CString::new(lsn.as_u64().to_string()).unwrap(),
            ));
        }

        Ok(self.properties.iter())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        self.properties.iter()
    }

    fn name(&self) -> &CStr {
        c"LatestAppliedLsnCollector"
    }
}

#[derive(Default)]
pub(crate) struct LatestAppliedLsnCollectorFactory {}

impl TablePropertiesCollectorFactory for LatestAppliedLsnCollectorFactory {
    type Collector = LatestAppliedLsnCollector;

    fn create(
        &mut self,
        _context: rocksdb::table_properties::TablePropertiesCollectorContext,
    ) -> LatestAppliedLsnCollector {
        LatestAppliedLsnCollector::new()
    }

    fn name(&self) -> &CStr {
        c"LatestAppliedLsnCollectorFactory"
    }
}

pub type PersistedLsnLookup = DashMap<PartitionId, Lsn>;

/// Event listener tracking persisted LSNs across
///
/// This listener works in conjunction with the [`AppliedLsnCollector`] to track the high watermark
/// applied LSNs per partition, once tables are flushed to disk. The event listener will only track
/// partitions for which there are already entries in the [`persisted_lsns`] map.
#[derive(Default)]
pub(crate) struct PersistedLsnEventListener {
    pub(crate) persisted_lsns: Arc<PersistedLsnLookup>,
}

impl PersistedLsnEventListener {
    pub fn create() -> Self {
        PersistedLsnEventListener {
            persisted_lsns: Default::default(),
        }
    }
}

impl EventListener for PersistedLsnEventListener {
    fn on_flush_completed(&self, flush_job_info: FlushJobInfo) {
        if let Some(id_str) = flush_job_info.cf_name.strip_prefix(PARTITION_CF_PREFIX) {
            let Ok(id) = id_str.parse::<u16>() else {
                warn!(
                    "Failed to parse partition id from cf_name: {}",
                    flush_job_info.cf_name
                );
                return;
            };

            let partition_id = PartitionId::from(id);
            let key = applied_lsn_property_name(partition_id);
            if let Some(applied_lsn) = flush_job_info.get_user_collected_property(&key) {
                match applied_lsn.to_str().expect("valid string").parse::<u64>() {
                    Ok(lsn) => {
                        // Note: we only modify entries, never insert, from the event listener. If
                        // we don't find an existing entry for the partition, that means that the
                        // PartitionStoreManager has already closed or dropped it.
                        self.persisted_lsns
                            .entry(partition_id)
                            .and_modify(|persisted_lsn| *persisted_lsn = lsn.into());
                    }
                    Err(err) => warn!(
                        key,
                        "Failed to parse applied LSN from table property: {}", err
                    ),
                };
            }
        }
    }
}

/// Custom table property name for Restate applied LSN
#[inline]
fn applied_lsn_property_name(partition_id: PartitionId) -> String {
    format!("restate.partition_{}.applied_lsn", partition_id)
}

/// Given a raw key-value pair, extract the partition id and its applied LSN, if the key is an Applied LSN FSM variable
#[inline]
fn extract_partition_applied_lsn(
    mut key: &[u8],
    value: &[u8],
) -> Result<Option<(PartitionId, Lsn)>, StorageError> {
    if !key.starts_with(KeyKind::Fsm.as_bytes()) {
        return Ok(None);
    }

    let fsm_key = PartitionStateMachineKey::deserialize_from(&mut key)?;
    if fsm_key.state_id == Some(fsm_variable::APPLIED_LSN) {
        if let Some(padded_partition_id) = fsm_key.partition_id {
            let partition_id = PartitionId::from(padded_partition_id);
            let applied_lsn = decode_lsn(value)?;
            return Ok(Some((partition_id, applied_lsn)));
        }
    }

    Ok(None)
}

#[inline]
fn decode_lsn(mut value: &[u8]) -> Result<Lsn, StorageError> {
    SequenceNumber::decode(&mut value).map(|sn| Lsn::from(u64::from(sn)))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use restate_storage_api::StorageError;
    use restate_types::storage::StorageCodec;

    use super::*;
    use crate::protobuf_types::ProtobufStorageWrapper;

    #[test]
    fn test_extract_partition_applied_lsn() -> googletest::Result<()> {
        let partition_id = PartitionId::from(42);
        let lsn = Lsn::from(12345);

        let applied_lsn_key = PartitionStateMachineKey {
            partition_id: Some(partition_id.into()),
            state_id: Some(fsm_variable::APPLIED_LSN),
        };

        let mut key_buf = BytesMut::new();
        applied_lsn_key.serialize_to(&mut key_buf);
        let key = key_buf.as_ref();

        let mut value_buf = BytesMut::new();
        let storage_wrapper: ProtobufStorageWrapper<
            <SequenceNumber as PartitionStoreProtobufValue>::ProtobufType,
        > = ProtobufStorageWrapper(SequenceNumber::from(lsn.as_u64()).into());
        StorageCodec::encode(&storage_wrapper, &mut value_buf)
            .map_err(|e| StorageError::Generic(e.into()))
            .unwrap();
        let value = value_buf.as_ref();

        let result = extract_partition_applied_lsn(key, value)?;
        assert_eq!(result, Some((partition_id, lsn)));

        Ok(())
    }

    #[test]
    fn test_extract_partition_applied_lsn_invalid_key() -> googletest::Result<()> {
        let key = b"invalid_key";
        let value = b"some_value";

        let result = extract_partition_applied_lsn(key, value)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_extract_partition_applied_lsn_incorrect_key() -> googletest::Result<()> {
        let partition_id = PartitionId::from(42);
        let lsn = Lsn::from(12345);

        let other_fsm_key = PartitionStateMachineKey {
            partition_id: Some(partition_id.into()),
            state_id: Some(fsm_variable::INBOX_SEQ_NUMBER),
        };

        let mut key_buf = BytesMut::new();
        other_fsm_key.serialize_to(&mut key_buf);
        let key = key_buf.as_ref();

        let mut value_buf = BytesMut::new();
        let storage_wrapper: ProtobufStorageWrapper<
            <SequenceNumber as PartitionStoreProtobufValue>::ProtobufType,
        > = ProtobufStorageWrapper(SequenceNumber::from(lsn.as_u64()).into());
        StorageCodec::encode(&storage_wrapper, &mut value_buf)
            .map_err(|e| StorageError::Generic(e.into()))
            .unwrap();
        let value = value_buf.as_ref();

        let result = extract_partition_applied_lsn(key, value)?;
        assert!(result.is_none());

        Ok(())
    }
}
