// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Outbox;
use crate::{RocksDBStorage, RocksDBTransaction, StorageAccess, TableScan};

use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::PartitionId;
use restate_types::storage::StorageCodec;
use std::io::Cursor;
use std::ops::Range;

define_table_key!(
    Outbox,
    OutboxKey(partition_id: PartitionId, message_index: u64)
);

fn add_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    message_index: u64,
    outbox_message: OutboxMessage,
) {
    let key = OutboxKey::default()
        .partition_id(partition_id)
        .message_index(message_index);

    storage.put_kv(key, outbox_message);
}

fn get_next_outbox_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    next_sequence_number: u64,
) -> Result<Option<(u64, OutboxMessage)>> {
    let start = OutboxKey::default()
        .partition_id(partition_id)
        .message_index(next_sequence_number);

    let end = OutboxKey::default()
        .partition_id(partition_id)
        .message_index(u64::MAX);

    storage.get_first_blocking(TableScan::KeyRangeInclusive(start, end), |kv| {
        if let Some((k, v)) = kv {
            let t = decode_key_value(k, v)?;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    })
}

fn get_outbox_message<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    sequence_number: u64,
) -> Result<Option<OutboxMessage>> {
    let outbox_key = OutboxKey::default()
        .partition_id(partition_id)
        .message_index(sequence_number);

    storage.get_value(outbox_key)
}

fn truncate_outbox<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    seq_to_truncate: Range<u64>,
) {
    let mut key = OutboxKey::default().partition_id(partition_id);
    let k = &mut key;

    for seq in seq_to_truncate {
        k.message_index = Some(seq);
        storage.delete_key(k);
    }
}

impl OutboxTable for RocksDBStorage {
    async fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) {
        add_message(self, partition_id, message_index, outbox_message)
    }

    async fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> Result<Option<(u64, OutboxMessage)>> {
        get_next_outbox_message(self, partition_id, next_sequence_number)
    }

    async fn get_outbox_message(
        &mut self,
        partition_id: PartitionId,
        sequence_number: u64,
    ) -> Result<Option<OutboxMessage>> {
        get_outbox_message(self, partition_id, sequence_number)
    }

    async fn truncate_outbox(&mut self, partition_id: PartitionId, seq_to_truncate: Range<u64>) {
        truncate_outbox(self, partition_id, seq_to_truncate)
    }
}

impl<'a> OutboxTable for RocksDBTransaction<'a> {
    async fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) {
        add_message(self, partition_id, message_index, outbox_message)
    }

    async fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> Result<Option<(u64, OutboxMessage)>> {
        get_next_outbox_message(self, partition_id, next_sequence_number)
    }

    async fn get_outbox_message(
        &mut self,
        partition_id: PartitionId,
        sequence_number: u64,
    ) -> Result<Option<OutboxMessage>> {
        get_outbox_message(self, partition_id, sequence_number)
    }

    async fn truncate_outbox(&mut self, partition_id: PartitionId, seq_to_truncate: Range<u64>) {
        truncate_outbox(self, partition_id, seq_to_truncate)
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

fn decode_value(v: &[u8]) -> crate::Result<OutboxMessage> {
    // decode value
    let outbox_message = StorageCodec::decode::<OutboxMessage>(v)
        .map_err(|error| StorageError::Conversion(error.into()))?;

    Ok(outbox_message)
}
