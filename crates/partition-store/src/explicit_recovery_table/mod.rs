// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The explicit recovery table: the invocation state and the signals of invocations on the
//! explicit recovery protocol family.
//!
//! Every key starts with (partition key, invocation uuid), so one prefix holds everything of an
//! invocation and one prefix scan drops it. A `kind` byte follows: 0 for invocation state, 1 for
//! signals. The key layout and the `b"eR"` key kind never change.

use bytes::Bytes;
use bytestring::ByteString;

use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::explicit_recovery_table::{
    ReadExplicitRecoveryTable, StoredSignal, WriteExplicitRecoveryTable,
};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::journal_v2::SignalResult;

use crate::TableKind::ExplicitRecovery;
use crate::keys::{DecodeTableKey, KeyKind, define_table_key};
use crate::{
    PartitionStore, PartitionStoreSnapshot, PartitionStoreTransaction, StorageAccess, StorageRead,
    TableScan, TableScanIterationDecision,
};

const STATE_KIND: u8 = 0;
const SIGNAL_KIND: u8 = 1;

define_table_key!(
    ExplicitRecovery,
    KeyKind::ExplicitRecovery,
    ErStateKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        kind: u8,
        state_key: Bytes,
    )
);

define_table_key!(
    ExplicitRecovery,
    KeyKind::ExplicitRecovery,
    ErSignalKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        kind: u8,
        signal_name: ByteString,
        // `u64::MAX - index`, so the first key under a name's prefix is its highest index.
        reversed_index: u64,
    )
);

fn state_key(invocation_id: &InvocationId, key: &Bytes) -> ErStateKey {
    ErStateKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        kind: STATE_KIND,
        state_key: key.clone(),
    }
}

fn signal_key(invocation_id: &InvocationId, name: &str, index: u32) -> ErSignalKey {
    ErSignalKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        kind: SIGNAL_KIND,
        signal_name: ByteString::from(name),
        reversed_index: u64::MAX - u64::from(index),
    }
}

fn get_invocation_state<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
    key: &Bytes,
) -> Result<Option<Bytes>> {
    let _x = RocksDbReadPerfGuard::new("get-er-invocation-state");
    storage.get_kv_raw(state_key(invocation_id, key), |_, v| {
        Ok(v.map(Bytes::copy_from_slice))
    })
}

fn get_all_invocation_state<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Vec<(Bytes, Bytes)>> {
    let _x = RocksDbReadPerfGuard::new("get-all-er-invocation-state");
    let prefix = ErStateKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .kind(STATE_KIND);
    storage
        .for_each_key_value_in_place(TableScan::Prefix(prefix), |mut k, v| {
            TableScanIterationDecision::Emit(
                ErStateKey::deserialize_from(&mut k)
                    .map(|key| (key.state_key, Bytes::copy_from_slice(v))),
            )
        })?
        .into_iter()
        .collect()
}

fn get_signal<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
    name: &str,
    index: u32,
) -> Result<Option<SignalResult>> {
    let _x = RocksDbReadPerfGuard::new("get-er-signal");
    Ok(storage
        .get_value_proto::<_, StoredSignal>(signal_key(invocation_id, name, index))?
        .map(|s| s.0))
}

fn next_signal_index<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
    name: &str,
) -> Result<u32> {
    let _x = RocksDbReadPerfGuard::new("next-er-signal-index");
    let prefix = ErSignalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .kind(SIGNAL_KIND)
        .signal_name(ByteString::from(name));
    storage.get_first_blocking(TableScan::Prefix(prefix), |item| match item {
        None => Ok(0),
        Some((mut k, _)) => {
            let key = ErSignalKey::deserialize_from(&mut k)?;
            let highest = u64::MAX - key.reversed_index;
            Ok(u32::try_from(highest + 1).expect("signal index to fit in u32"))
        }
    })
}

/// Deletes every key under the prefix: of one kind, or of the whole invocation.
fn delete_prefix<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    kind: Option<u8>,
) -> Result<()> {
    let _x = RocksDbReadPerfGuard::new("delete-er-prefix");
    let prefix = ErStateKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    // No range deletes in a transaction: delete key by key.
    let keys = match kind {
        Some(kind) => storage
            .for_each_key_value_in_place(TableScan::Prefix(prefix.kind(kind)), |k, _| {
                TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k)))
            })?,
        None => storage.for_each_key_value_in_place(TableScan::Prefix(prefix), |k, _| {
            TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k)))
        })?,
    };
    for key in keys {
        storage.delete_cf(ExplicitRecovery, key?)?;
    }
    Ok(())
}

macro_rules! impl_read {
    ($ty:ty) => {
        impl ReadExplicitRecoveryTable for $ty {
            async fn get_invocation_state(
                &mut self,
                invocation_id: &InvocationId,
                key: &Bytes,
            ) -> Result<Option<Bytes>> {
                self.assert_partition_key(invocation_id)?;
                get_invocation_state(self, invocation_id, key)
            }

            async fn get_all_invocation_state(
                &mut self,
                invocation_id: &InvocationId,
            ) -> Result<Vec<(Bytes, Bytes)>> {
                self.assert_partition_key(invocation_id)?;
                get_all_invocation_state(self, invocation_id)
            }

            async fn get_signal(
                &mut self,
                invocation_id: &InvocationId,
                name: &str,
                index: u32,
            ) -> Result<Option<SignalResult>> {
                self.assert_partition_key(invocation_id)?;
                get_signal(self, invocation_id, name, index)
            }

            async fn next_signal_index(
                &mut self,
                invocation_id: &InvocationId,
                name: &str,
            ) -> Result<u32> {
                self.assert_partition_key(invocation_id)?;
                next_signal_index(self, invocation_id, name)
            }
        }
    };
}

impl_read!(PartitionStore);
impl_read!(PartitionStoreTransaction<'_>);
impl_read!(PartitionStoreSnapshot<'_>);

impl WriteExplicitRecoveryTable for PartitionStoreTransaction<'_> {
    fn put_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
        key: &Bytes,
        value: &Bytes,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        self.put_kv_raw(state_key(invocation_id, key), value)
    }

    fn delete_invocation_state(&mut self, invocation_id: &InvocationId, key: &Bytes) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        self.delete_key(&state_key(invocation_id, key))
    }

    fn delete_all_invocation_state(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_prefix(self, invocation_id, Some(STATE_KIND))
    }

    fn put_signal(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
        index: u32,
        result: &SignalResult,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        self.put_kv_proto_owned(
            signal_key(invocation_id, name, index),
            StoredSignal(result.clone()),
        )
    }

    fn delete_all_explicit_recovery(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_prefix(self, invocation_id, None)
    }
}
