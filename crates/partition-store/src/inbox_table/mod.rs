// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;

use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::inbox_table::{
    InboxEntry, InboxTable, ReadOnlyInboxTable, ScanInboxTable, SequenceNumberInboxEntry,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::message::MessageIndex;

use crate::TableKind::Inbox;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, TableScanIterationDecision,
};

define_table_key!(
    Inbox,
    KeyKind::Inbox,
    InboxKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString,
        sequence_number: u64
    )
);

fn peek_inbox<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<Option<SequenceNumberInboxEntry>> {
    let key = InboxKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage.get_first_blocking(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), key),
        |kv| match kv {
            Some((k, v)) => {
                let entry = decode_inbox_key_value(k, v)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        },
    )
}

fn inbox<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send> {
    let key = InboxKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    Ok(stream::iter(storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), key),
        |k, v| {
            let inbox_entry = decode_inbox_key_value(k, v);
            TableScanIterationDecision::Emit(inbox_entry)
        },
    )?))
}

impl ReadOnlyInboxTable for PartitionStore {
    async fn peek_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        self.assert_partition_key(service_id)?;
        peek_inbox(self, service_id)
    }

    fn inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send> {
        self.assert_partition_key(service_id)?;
        inbox(self, service_id)
    }
}

impl ScanInboxTable for PartitionStore {
    fn scan_inboxes(
        &self,
        range: std::ops::RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send> {
        self.run_iterator(
            "df-inbox",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<InboxKey>(range),
            |(mut key, mut value)| {
                let key = InboxKey::deserialize_from(&mut key)?;
                let sequence_number = *key.sequence_number_ok_or()?;
                let inbox_entry = InboxEntry::decode(&mut value)?;

                Ok(SequenceNumberInboxEntry::new(sequence_number, inbox_entry))
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyInboxTable for PartitionStoreTransaction<'_> {
    async fn peek_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        self.assert_partition_key(service_id)?;
        peek_inbox(self, service_id)
    }

    fn inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send> {
        self.assert_partition_key(service_id)?;
        inbox(self, service_id)
    }
}

impl InboxTable for PartitionStoreTransaction<'_> {
    async fn put_inbox_entry(
        &mut self,
        inbox_sequence_number: MessageIndex,
        inbox_entry: &InboxEntry,
    ) -> Result<()> {
        let service_id = inbox_entry.service_id();
        self.assert_partition_key(service_id)?;

        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone())
            .sequence_number(inbox_sequence_number);

        self.put_kv(key, inbox_entry)
    }

    async fn delete_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        sequence_number: u64,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_inbox_entry(self, service_id, sequence_number)
    }

    async fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        self.assert_partition_key(service_id)?;
        let _x = RocksDbPerfGuard::new("pop-inbox");
        let result = peek_inbox(self, service_id);

        if let Ok(Some(ref inbox_entry)) = result {
            delete_inbox_entry(self, service_id, inbox_entry.inbox_sequence_number)?;
        }

        result
    }
}

fn delete_inbox_entry(
    txn: &mut PartitionStoreTransaction,
    service_id: &ServiceId,
    sequence_number: u64,
) -> Result<()> {
    let key = InboxKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .sequence_number(sequence_number);

    txn.delete_key(&key)
}

fn decode_inbox_key_value(k: &[u8], mut v: &[u8]) -> Result<SequenceNumberInboxEntry> {
    let key = InboxKey::deserialize_from(&mut Cursor::new(k))?;
    let sequence_number = *key.sequence_number_ok_or()?;

    let inbox_entry = InboxEntry::decode(&mut v)?;

    Ok(SequenceNumberInboxEntry::new(sequence_number, inbox_entry))
}

#[cfg(test)]
mod tests {
    use crate::inbox_table::InboxKey;
    use crate::keys::TableKey;
    use bytes::{Bytes, BytesMut};
    use restate_types::identifiers::{ServiceId, WithPartitionKey};

    fn message_key(service_id: &ServiceId, sequence_number: u64) -> Bytes {
        let key = InboxKey {
            partition_key: Some(service_id.partition_key()),
            service_name: Some(service_id.service_name.clone()),
            service_key: Some(service_id.key.clone()),
            sequence_number: Some(sequence_number),
        };
        let mut buf = BytesMut::new();
        key.serialize_to(&mut buf);
        buf.freeze()
    }

    fn inbox_key(service_id: &ServiceId) -> Bytes {
        let key = InboxKey {
            partition_key: Some(service_id.partition_key()),
            service_name: Some(service_id.service_name.clone()),
            service_key: Some(service_id.key.clone()),
            sequence_number: None,
        };

        key.serialize().freeze()
    }

    #[test]
    fn inbox_key_covers_all_messages_of_a_service() {
        let prefix_key = inbox_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"));

        let low_key = message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), 0);
        assert!(low_key.starts_with(&prefix_key));

        let high_key = message_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            u64::MAX,
        );
        assert!(high_key.starts_with(&prefix_key));
    }

    #[test]
    fn message_keys_sort_lex() {
        //
        // across services
        //
        assert!(
            message_key(&ServiceId::with_partition_key(1337, "svc-1", ""), 0)
                < message_key(&ServiceId::with_partition_key(1337, "svc-2", ""), 0)
        );
        //
        // same service across keys
        //
        assert!(
            message_key(&ServiceId::with_partition_key(1337, "svc-1", "a"), 0)
                < message_key(&ServiceId::with_partition_key(1337, "svc-1", "b"), 0)
        );
        //
        // within the same service and key
        //
        let mut previous_key =
            message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), 0);
        for i in 1..300 {
            let current_key =
                message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), i);
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
