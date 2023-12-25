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
use crate::{RocksDBTransaction, TableScan, TableScanIterationDecision};
use bytes::Bytes;
use futures::Stream;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use std::future::Future;
use std::io::Cursor;

define_table_key!(
    PartitionStateMachine,
    PartitionStateMachineKey(partition_id: PartitionId, state_id: u64)
);

impl<'a> FsmTable for RocksDBTransaction<'a> {
    async fn get(&mut self, partition_id: PartitionId, state_id: u64) -> Result<Option<Bytes>> {
        let key = PartitionStateMachineKey::default()
            .partition_id(partition_id)
            .state_id(state_id);
        self.get_blocking(key, |_k, v| Ok(v.map(Bytes::copy_from_slice)))
            .await
    }

    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send {
        let key = PartitionStateMachineKey::default()
            .partition_id(partition_id)
            .state_id(state_id);
        self.put_kv(key, state_value.as_ref());
        futures::future::ready(())
    }

    async fn clear(&mut self, partition_id: PartitionId, state_id: u64) {
        let key = PartitionStateMachineKey::default()
            .partition_id(partition_id)
            .state_id(state_id);
        self.delete_key(&key);
    }

    fn get_all_states(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + Send {
        self.for_each_key_value_in_place(
            TableScan::Partition::<PartitionStateMachineKey>(partition_id),
            move |k, v| {
                let res = decode_key_value(k, v);
                TableScanIterationDecision::Emit(res)
            },
        )
    }
}

fn decode_key_value(k: &[u8], v: &[u8]) -> crate::Result<(u64, Bytes)> {
    let key = PartitionStateMachineKey::deserialize_from(&mut Cursor::new(k))?;
    let state_id = *key.state_id_ok_or()?;
    let value = Bytes::copy_from_slice(v);
    Ok((state_id, value))
}
