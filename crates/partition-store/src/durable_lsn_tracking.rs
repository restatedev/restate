// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::sync::Arc;

use ahash::HashMap;
use rocksdb::event_listener::{EventListener, FlushJobInfo};
use rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use tokio::sync::watch;
use tracing::warn;

use restate_platform::sync::RwLock;
use restate_storage_api::StorageError;
use restate_storage_api::fsm_table::SequenceNumber;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::logs::SequenceNumber as _;
use restate_types::{identifiers::PartitionId, logs::Lsn};

use crate::fsm_table::{PartitionStateMachineKey, fsm_variable};
#[cfg(test)]
use crate::keys::EncodeTableKey;
use crate::keys::{DecodeTableKey, KeyKind};

const APPLIED_LSNS_PROPERTY_PREFIX: &CStr = c"p:";

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
        let prefix = APPLIED_LSNS_PROPERTY_PREFIX.to_str().unwrap();
        for (partition_id, lsn) in &self.applied_lsns {
            self.properties.push((
                CString::new(format!("{prefix}{partition_id}")).unwrap(),
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
        &self,
        _context: rocksdb::table_properties::TablePropertiesCollectorContext,
    ) -> Option<AppliedLsnCollector> {
        Some(AppliedLsnCollector::default())
    }

    fn name(&self) -> &CStr {
        c"AppliedLsnCollectorFactory"
    }
}

/// Notification state for one physical database open, shared by its partition
/// handles and flush listener. A reopened database must get a fresh registry.
///
/// A native callback can become its last owner. This must never own partition
/// cells, column-family handles, or databases: releasing it on a flush thread
/// must not initiate database shutdown and wait for that same flush.
#[derive(Default)]
pub(crate) struct DurableLsnSenders {
    senders: RwLock<HashMap<PartitionId, Registration>>,
    #[cfg(test)]
    before_note: restate_platform::sync::Mutex<Option<NotificationPause>>,
}

struct Registration {
    cf_id: u32,
    sender: watch::Sender<Lsn>,
}

impl DurableLsnSenders {
    pub fn register(&self, partition_id: PartitionId, cf_id: u32, sender: watch::Sender<Lsn>) {
        let mut senders = self.senders.write();
        if let Some(previous) = senders.insert(partition_id, Registration { cf_id, sender }) {
            previous.sender.send_replace(Lsn::INVALID);
        }
    }

    pub fn unregister(&self, partition_id: PartitionId, cf_id: u32) {
        let mut senders = self.senders.write();
        if let std::collections::hash_map::Entry::Occupied(entry) = senders.entry(partition_id)
            && entry.get().cf_id == cf_id
        {
            entry.remove().sender.send_replace(Lsn::INVALID);
        }
    }

    fn note_durable_lsn(&self, partition_id: PartitionId, cf_id: u32, lsn: Lsn) {
        #[cfg(test)]
        let completed = {
            let pause = self.before_note.lock().take();
            pause.map(|pause| {
                let _ = pause.reached.send(());
                let _ = pause.resume.recv();
                pause.completed
            })
        };
        // Keep the read lock through notification so unregister cannot reset an
        // old watch to INVALID and then have an in-flight callback update it again.
        let senders = self.senders.read();
        if let Some(registration) = senders.get(&partition_id)
            && registration.cf_id == cf_id
        {
            registration.sender.send_replace(lsn);
        }
        #[cfg(test)]
        if let Some(completed) = completed {
            let _ = completed.send(());
        }
    }

    /// Pause a native callback before it selects a registration. Dropping the
    /// resume sender releases it, including if the test fails.
    #[cfg(test)]
    pub fn pause_next_notification(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        *self.before_note.lock() = Some(NotificationPause {
            reached: reached_tx,
            resume: resume_rx,
            completed: completed_tx,
        });
        (reached_rx, resume_tx, completed_rx)
    }
}

#[cfg(test)]
struct NotificationPause {
    reached: tokio::sync::oneshot::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
    completed: tokio::sync::oneshot::Sender<()>,
}

/// Event listener tracking durable LSNs across partitions.
///
/// This listener works in conjunction with the [`AppliedLsnCollector`] to track the high watermark
/// applied LSNs per partition, once tables are flushed to disk. The event listener will only track
/// partitions with a registered sender in [`DurableLsnSenders`].
pub(crate) struct DurableLsnEventListener {
    notifications: Arc<DurableLsnSenders>,
}

impl DurableLsnEventListener {
    pub fn new(notifications: Arc<DurableLsnSenders>) -> Self {
        Self { notifications }
    }
}

impl EventListener for DurableLsnEventListener {
    fn on_flush_completed(&self, flush_job_info: &FlushJobInfo) {
        for key in flush_job_info.get_user_collected_property_keys(APPLIED_LSNS_PROPERTY_PREFIX) {
            let partition_id = key[APPLIED_LSNS_PROPERTY_PREFIX.to_bytes().len()..]
                .to_string_lossy()
                .parse::<u16>()
                .map(PartitionId::from);

            let lsn = flush_job_info
                .get_user_collected_property(key)
                .map(|v| v.to_string_lossy().parse::<u64>().map(Lsn::from))
                .transpose();

            if let (Ok(ref partition_id), Ok(Some(ref lsn))) = (partition_id, lsn) {
                self.notifications
                    .note_durable_lsn(*partition_id, flush_job_info.cf_id(), *lsn);
            } else {
                let cf_name = flush_job_info.cf_name();
                let cf_name = cf_name
                    .as_ref()
                    .map(|name| std::string::String::from_utf8_lossy(name))
                    .unwrap_or(Cow::Borrowed("unknown"));
                warn!(%cf_name, ?key, "Failed to decode partition applied LSN from flush event");
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
    if fsm_key.state_id == fsm_variable::APPLIED_LSN {
        let partition_id = PartitionId::from(fsm_key.partition_id);
        let applied_lsn = SequenceNumber::decode(&mut value).map(u64::from)?.into();
        return Ok(Some((partition_id, applied_lsn)));
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
    fn extract_partition_applied_lsn() -> googletest::Result<()> {
        let partition_id = PartitionId::from(42);
        let lsn = Lsn::from(12345);

        let applied_lsn_key = PartitionStateMachineKey {
            partition_id: partition_id.into(),
            state_id: fsm_variable::APPLIED_LSN,
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

        let result = super::extract_partition_applied_lsn(key, value)?;
        assert_eq!(result, Some((partition_id, lsn)));

        Ok(())
    }

    #[test]
    fn extract_partition_applied_lsn_invalid_key() -> googletest::Result<()> {
        let key = b"invalid_key";
        let value = b"some_value";

        let result = super::extract_partition_applied_lsn(key, value)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn extract_partition_applied_lsn_incorrect_key() -> googletest::Result<()> {
        let partition_id = PartitionId::from(42);
        let lsn = Lsn::from(12345);

        let other_fsm_key = PartitionStateMachineKey {
            partition_id: partition_id.into(),
            state_id: fsm_variable::INBOX_SEQ_NUMBER,
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

        let result = super::extract_partition_applied_lsn(key, value)?;
        assert!(result.is_none());

        Ok(())
    }
}
