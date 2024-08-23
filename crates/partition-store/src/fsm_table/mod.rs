// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{define_table_key, KeyKind};
use crate::TableKind::PartitionStateMachine;
use crate::{PaddedPartitionId, PartitionStore, RocksDBTransaction, StorageAccess};
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use restate_types::storage::{StorageDecode, StorageEncode};
use std::future;
use std::future::Future;

define_table_key!(
    PartitionStateMachine,
    KeyKind::Fsm,
    PartitionStateMachineKey(partition_id: PaddedPartitionId, state_id: u64)
);

fn get<T: StorageDecode, S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<Option<T>> {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.get_value(key)
}

fn put<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
    state_value: impl StorageEncode,
) {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.put_kv(key, state_value);
}

fn clear<S: StorageAccess>(storage: &mut S, partition_id: PartitionId, state_id: u64) {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.delete_key(&key);
}

impl ReadOnlyFsmTable for PartitionStore {
    async fn get<T>(&mut self, partition_id: PartitionId, state_id: u64) -> Result<Option<T>>
    where
        T: StorageDecode,
    {
        get(self, partition_id, state_id)
    }
}

impl<'a> ReadOnlyFsmTable for RocksDBTransaction<'a> {
    async fn get<T>(&mut self, partition_id: PartitionId, state_id: u64) -> Result<Option<T>>
    where
        T: StorageDecode,
    {
        get(self, partition_id, state_id)
    }
}

impl<'a> FsmTable for RocksDBTransaction<'a> {
    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl StorageEncode,
    ) -> impl Future<Output = ()> + Send {
        put(self, partition_id, state_id, state_value);
        future::ready(())
    }

    async fn clear(&mut self, partition_id: PartitionId, state_id: u64) {
        clear(self, partition_id, state_id)
    }
}
