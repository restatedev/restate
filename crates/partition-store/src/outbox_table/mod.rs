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
use std::ops::RangeInclusive;

use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable, ReadOnlyOutboxTable};
use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;

use crate::keys::{define_table_key, KeyKind, TableKey};
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::TableKind::Outbox;
use crate::{
    PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan,
};

define_table_key!(
    Outbox,
    KeyKind::Outbox,
    OutboxKey(partition_id: PaddedPartitionId, message_index: u64)
);

impl PartitionStoreProtobufValue for OutboxMessage {
    type ProtobufType = crate::protobuf_types::v1::OutboxMessage;
}

fn add_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    message_index: u64,
    outbox_message: &OutboxMessage,
) {
    let key = OutboxKey::default()
        .partition_id(partition_id.into())
        .message_index(message_index);

    storage.put_kv(key, outbox_message);
}

fn get_outbox_head_seq_number<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<Option<u64>> {
    let _x = RocksDbPerfGuard::new("get-head-outbox");
    let start = OutboxKey::default().partition_id(partition_id.into());

    let end = OutboxKey::default()
        .partition_id(partition_id.into())
        .message_index(u64::MAX);

    storage.get_first_blocking(
        TableScan::KeyRangeInclusiveInSinglePartition(partition_id, start, end),
        |kv| {
            if let Some((k, v)) = kv {
                let (seq_no, _) = decode_key_value(k, v)?;
                Ok(Some(seq_no))
            } else {
                Ok(None)
            }
        },
    )
}

fn get_next_outbox_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    next_sequence_number: u64,
) -> Result<Option<(u64, OutboxMessage)>> {
    let _x = RocksDbPerfGuard::new("get-next-outbox");
    let start = OutboxKey::default()
        .partition_id(partition_id.into())
        .message_index(next_sequence_number);

    let end = OutboxKey::default()
        .partition_id(partition_id.into())
        .message_index(u64::MAX);

    storage.get_first_blocking(
        TableScan::KeyRangeInclusiveInSinglePartition(partition_id, start, end),
        |kv| {
            if let Some((k, v)) = kv {
                let t = decode_key_value(k, v)?;
                Ok(Some(t))
            } else {
                Ok(None)
            }
        },
    )
}

fn get_outbox_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    sequence_number: u64,
) -> Result<Option<OutboxMessage>> {
    let _x = RocksDbPerfGuard::new("get-outbox");
    let outbox_key = OutboxKey::default()
        .partition_id(partition_id.into())
        .message_index(sequence_number);

    storage.get_value(outbox_key)
}

fn truncate_outbox<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    range: RangeInclusive<u64>,
) {
    let _x = RocksDbPerfGuard::new("truncate-outbox");
    let mut key = OutboxKey::default().partition_id(partition_id.into());
    for seq in range {
        key.message_index = Some(seq);
        storage.delete_key(&key);
    }
}

impl ReadOnlyOutboxTable for PartitionStore {
    async fn get_outbox_head_seq_number(&mut self) -> Result<Option<u64>> {
        get_outbox_head_seq_number(self, self.partition_id())
    }
}

impl OutboxTable for PartitionStore {
    async fn put_outbox_message(&mut self, message_index: u64, outbox_message: &OutboxMessage) {
        add_message(self, self.partition_id(), message_index, outbox_message)
    }

    async fn get_next_outbox_message(
        &mut self,
        next_sequence_number: u64,
    ) -> Result<Option<(u64, OutboxMessage)>> {
        get_next_outbox_message(self, self.partition_id(), next_sequence_number)
    }

    async fn get_outbox_message(&mut self, sequence_number: u64) -> Result<Option<OutboxMessage>> {
        get_outbox_message(self, self.partition_id(), sequence_number)
    }

    async fn truncate_outbox(&mut self, range: RangeInclusive<u64>) {
        truncate_outbox(self, self.partition_id(), range)
    }
}

impl<'a> ReadOnlyOutboxTable for PartitionStoreTransaction<'a> {
    async fn get_outbox_head_seq_number(&mut self) -> Result<Option<u64>> {
        get_outbox_head_seq_number(self, self.partition_id())
    }
}

impl<'a> OutboxTable for PartitionStoreTransaction<'a> {
    async fn put_outbox_message(&mut self, message_index: u64, outbox_message: &OutboxMessage) {
        add_message(self, self.partition_id(), message_index, outbox_message)
    }

    async fn get_next_outbox_message(
        &mut self,
        next_sequence_number: u64,
    ) -> Result<Option<(u64, OutboxMessage)>> {
        get_next_outbox_message(self, self.partition_id(), next_sequence_number)
    }

    async fn get_outbox_message(&mut self, sequence_number: u64) -> Result<Option<OutboxMessage>> {
        get_outbox_message(self, self.partition_id(), sequence_number)
    }

    async fn truncate_outbox(&mut self, range: RangeInclusive<u64>) {
        truncate_outbox(self, self.partition_id(), range)
    }
}

fn decode_key_value(k: &[u8], v: &[u8]) -> crate::Result<(u64, OutboxMessage)> {
    // decode key
    let key = OutboxKey::deserialize_from(&mut Cursor::new(k))?;
    let sequence_number = *key.message_index_ok_or()?;

    // decode value
    let outbox_message = decode_value(v)?;

    Ok((sequence_number, outbox_message))
}

fn decode_value(mut v: &[u8]) -> crate::Result<OutboxMessage> {
    // decode value
    let outbox_message = OutboxMessage::decode(&mut v)?;

    Ok(outbox_message)
}
