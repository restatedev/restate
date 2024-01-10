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
use crate::TableKind::PartitionStateMachine;
use crate::{
    RocksDBStorage, RocksDBTransaction, StorageAccess, TableScan, TableScanIterationDecision,
};
use bytes::Bytes;
use futures::Stream;
use futures_util::stream;
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use std::future;
use std::future::Future;
use std::io::Cursor;

define_table_key!(
    PartitionStateMachine,
    PartitionStateMachineKey(partition_id: PartitionId, state_id: u64)
);

fn get<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<Option<Bytes>> {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id)
        .state_id(state_id);
    storage.get_blocking(key, |_k, v| Ok(v.map(Bytes::copy_from_slice)))
}

fn put<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
    state_value: impl AsRef<[u8]>,
) {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id)
        .state_id(state_id);
    storage.put_kv(key, state_value.as_ref());
}

fn clear<S: StorageAccess>(storage: &mut S, partition_id: PartitionId, state_id: u64) {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id)
        .state_id(state_id);
    storage.delete_key(&key);
}

fn get_all_states<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Vec<Result<(u64, Bytes)>> {
    storage.for_each_key_value_in_place(
        TableScan::Partition::<PartitionStateMachineKey>(partition_id),
        move |k, v| {
            let res = decode_key_value(k, v);
            TableScanIterationDecision::Emit(res)
        },
    )
}

impl ReadOnlyFsmTable for RocksDBStorage {
    async fn get(&mut self, partition_id: PartitionId, state_id: u64) -> Result<Option<Bytes>> {
        get(self, partition_id, state_id)
    }

    fn get_all_states(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + Send {
        stream::iter(get_all_states(self, partition_id))
    }
}

impl<'a> ReadOnlyFsmTable for RocksDBTransaction<'a> {
    async fn get(&mut self, partition_id: PartitionId, state_id: u64) -> Result<Option<Bytes>> {
        get(self, partition_id, state_id)
    }

    fn get_all_states(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + Send {
        stream::iter(get_all_states(self, partition_id))
    }
}

impl<'a> FsmTable for RocksDBTransaction<'a> {
    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send {
        put(self, partition_id, state_id, state_value);
        future::ready(())
    }

    async fn clear(&mut self, partition_id: PartitionId, state_id: u64) {
        clear(self, partition_id, state_id)
    }
}

fn decode_key_value(k: &[u8], v: &[u8]) -> crate::Result<(u64, Bytes)> {
    let key = PartitionStateMachineKey::deserialize_from(&mut Cursor::new(k))?;
    let state_id = *key.state_id_ok_or()?;
    let value = Bytes::copy_from_slice(v);
    Ok((state_id, value))
}
