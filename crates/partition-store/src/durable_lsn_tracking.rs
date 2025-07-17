use std::ffi::{CStr, CString};
use std::sync::Arc;

use ahash::HashMap;
use rocksdb::event_listener::{EventListener, FlushJobInfo};
use rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use tracing::warn;

use restate_storage_api::StorageError;
use restate_storage_api::fsm_table::SequenceNumber;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::{identifiers::PartitionId, logs::Lsn};

use crate::SharedState;
use crate::fsm_table::{PartitionStateMachineKey, fsm_variable};
use crate::keys::{KeyKind, TableKey};

const APPLIED_LSNS_PROPERTY_PREFIX: &str = "p:";

#[derive(Default)]
pub(crate) struct AppliedLsnCollector {
    applied_lsns: HashMap<PartitionId, Lsn>,
    properties: Vec<(CString, CString)>,
}

impl TablePropertiesCollector for AppliedLsnCollector {
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
                Err(CollectorError::default())
            }
        }
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        for (partition_id, lsn) in &self.applied_lsns {
            self.properties.push((
                CString::new(format!("{APPLIED_LSNS_PROPERTY_PREFIX}{partition_id}")).unwrap(),
                CString::new(lsn.to_string()).unwrap(),
            ));
        }

        Ok(self.properties.iter())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        self.properties.iter()
    }

    fn name(&self) -> &CStr {
        c"AppliedLsnCollector"
    }
}

pub(crate) struct AppliedLsnCollectorFactory;

impl TablePropertiesCollectorFactory for AppliedLsnCollectorFactory {
    type Collector = AppliedLsnCollector;

    fn create(
        &mut self,
        _context: rocksdb::table_properties::TablePropertiesCollectorContext,
    ) -> AppliedLsnCollector {
        AppliedLsnCollector::default()
    }

    fn name(&self) -> &CStr {
        c"AppliedLsnCollectorFactory"
    }
}

/// Event listener tracking durable LSNs across
///
/// This listener works in conjunction with the [`AppliedLsnCollector`] to track the high watermark
/// applied LSNs per partition, once tables are flushed to disk. The event listener will only track
/// partitions for which there are already entries in the [`shared_state`].
#[derive(Default)]
pub(crate) struct DurableLsnEventListener {
    shared_state: Arc<SharedState>,
}

impl DurableLsnEventListener {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }
}

impl EventListener for DurableLsnEventListener {
    fn on_flush_completed(&self, flush_job_info: FlushJobInfo) {
        for key in flush_job_info.get_user_collected_property_keys(APPLIED_LSNS_PROPERTY_PREFIX) {
            let partition_id = key[APPLIED_LSNS_PROPERTY_PREFIX.len()..]
                .to_string_lossy()
                .parse::<u16>()
                .map(PartitionId::from);

            let lsn = flush_job_info
                .get_user_collected_property(key)
                .map(|v| v.to_string_lossy().parse::<u64>().map(Lsn::from))
                .transpose();

            if let (Ok(ref partition_id), Ok(Some(ref lsn))) = (partition_id, lsn) {
                self.shared_state.note_durable_lsn(*partition_id, *lsn);
            } else {
                warn!(
                    cf_name = flush_job_info.cf_name,
                    ?key,
                    "Failed to decode partition applied LSN from flush event",
                );
            }
        }
    }
}

/// Given a raw key-value pair, extract the partition id and its applied LSN, if the key is an Applied LSN FSM variable
fn extract_partition_applied_lsn(
    mut key: &[u8],
    mut value: &[u8],
) -> Result<Option<(PartitionId, Lsn)>, StorageError> {
    if !key.starts_with(KeyKind::Fsm.as_bytes()) {
        return Ok(None);
    }

    let fsm_key = PartitionStateMachineKey::deserialize_from(&mut key)?;
    if fsm_key.state_id == Some(fsm_variable::APPLIED_LSN) {
        if let Some(padded_partition_id) = fsm_key.partition_id {
            let partition_id = PartitionId::from(padded_partition_id);
            let applied_lsn = SequenceNumber::decode(&mut value).map(u64::from)?.into();
            return Ok(Some((partition_id, applied_lsn)));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use restate_storage_api::StorageError;
    use restate_types::storage::StorageCodec;

    use super::*;
    use restate_storage_api::protobuf_types::ProtobufStorageWrapper;

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
