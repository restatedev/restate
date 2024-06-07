// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{define_table_key, KeyKind, TableKey};
use crate::TableKind::Inbox;
use crate::{PartitionStore, RocksDBTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::inbox_table::{InboxEntry, InboxTable, SequenceNumberInboxEntry};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::storage::StorageCodec;
use std::io::Cursor;
use std::ops::RangeInclusive;

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

impl<'a> InboxTable for RocksDBTransaction<'a> {
    async fn put_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        SequenceNumberInboxEntry {
            inbox_sequence_number,
            inbox_entry,
        }: SequenceNumberInboxEntry,
    ) {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone())
            .sequence_number(inbox_sequence_number);

        self.put_kv(key, inbox_entry);
    }

    async fn delete_inbox_entry(&mut self, service_id: &ServiceId, sequence_number: u64) {
        delete_inbox_entry(self, service_id, sequence_number);
    }

    async fn peek_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        peek_inbox(self, service_id)
    }

    async fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        let _x = RocksDbPerfGuard::new("pop-inbox");
        let result = peek_inbox(self, service_id);

        if let Ok(Some(ref inbox_entry)) = result {
            delete_inbox_entry(self, service_id, inbox_entry.inbox_sequence_number)
        }

        result
    }

    fn inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        stream::iter(self.for_each_key_value_in_place(
            TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), key),
            |k, v| {
                let inbox_entry = decode_inbox_key_value(k, v);
                TableScanIterationDecision::Emit(inbox_entry)
            },
        ))
    }
}

fn peek_inbox(
    txn: &mut RocksDBTransaction,
    service_id: &ServiceId,
) -> Result<Option<SequenceNumberInboxEntry>> {
    let key = InboxKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    txn.get_first_blocking(
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

fn delete_inbox_entry(txn: &mut RocksDBTransaction, service_id: &ServiceId, sequence_number: u64) {
    let key = InboxKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .sequence_number(sequence_number);

    txn.delete_key(&key);
}

fn decode_inbox_key_value(k: &[u8], mut v: &[u8]) -> Result<SequenceNumberInboxEntry> {
    let key = InboxKey::deserialize_from(&mut Cursor::new(k))?;
    let sequence_number = *key.sequence_number_ok_or()?;

    let inbox_entry = StorageCodec::decode::<InboxEntry, _>(&mut v)
        .map_err(|error| StorageError::Generic(error.into()))?;

    Ok(SequenceNumberInboxEntry::new(sequence_number, inbox_entry))
}

impl PartitionStore {
    pub fn all_inboxes(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<SequenceNumberInboxEntry>> + Send {
        stream::iter(self.for_each_key_value_in_place(
            TableScan::FullScanPartitionKeyRange::<InboxKey>(range),
            |k, v| {
                let inbox_entry = decode_inbox_key_value(k, v);
                TableScanIterationDecision::Emit(inbox_entry)
            },
        ))
    }
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
