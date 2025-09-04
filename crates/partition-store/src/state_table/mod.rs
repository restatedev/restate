// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::state_table::{ReadOnlyStateTable, ScanStateTable, StateTable};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};

use crate::TableKind::State;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, break_on_err};
use crate::{TableScan, TableScanIterationDecision};

define_table_key!(
    State,
    KeyKind::State,
    StateKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString,
        state_key: Bytes
    ),
    service_key_prefix = [partition_key, service_name, service_key],
);

#[inline]
fn write_state_entry_key(service_id: &ServiceId, state_key: impl AsRef<[u8]>) -> StateKey {
    StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .state_key(state_key.as_ref().to_vec().into())
}

fn user_state_key_from_slice(key: &[u8]) -> Result<Bytes> {
    // note that this is to let a single allocation to be done which will later be decomposed into
    // a few ByteStrings that indexes that allocation. This is to avoid smaller allocations on
    // every portion of the key.
    let mut key = Bytes::copy_from_slice(key);
    let key = StateKey::deserialize_from(&mut key)?;
    let key = key
        .state_key
        .ok_or_else(|| StorageError::DataIntegrityError)?;

    Ok(key)
}

fn put_user_state<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
    state_value: impl AsRef<[u8]>,
) -> Result<()> {
    let key = write_state_entry_key(service_id, state_key);
    storage.put_kv_raw(key, state_value.as_ref())
}

fn delete_user_state<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
) -> Result<()> {
    let key = write_state_entry_key(service_id, state_key);
    storage.delete_key(&key)
}

fn delete_all_user_state<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) -> Result<()> {
    let prefix_key = StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage.delete_prefix(State, prefix_key.service_key_prefix())
}

fn get_user_state<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
) -> Result<Option<Bytes>> {
    let _x = RocksDbPerfGuard::new("get-user-state");
    let key = write_state_entry_key(service_id, state_key);
    storage.get_kv_raw(key, move |_k, v| Ok(v.map(Bytes::copy_from_slice)))
}

fn get_all_user_states_for_service<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<Vec<Result<(Bytes, Bytes)>>> {
    let _x = RocksDbPerfGuard::new("get-all-user-state");
    let key = StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), key),
        |k, v| TableScanIterationDecision::Emit(decode_user_state_key_value(k, v)),
    )
}

impl ReadOnlyStateTable for PartitionStore {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, service_id, state_key)
    }

    fn get_all_user_states_for_service(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self, service_id,
        )?))
    }
}

impl ScanStateTable for PartitionStore {
    fn for_each_user_state<
        F: FnMut((ServiceId, Bytes, &[u8])) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-user-state",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<StateKey>(range),
            move |(mut key, value)| {
                let row_key = break_on_err(StateKey::deserialize_from(&mut key))?;
                let (partition_key, service_name, service_key, state_key) =
                    break_on_err(row_key.into_inner_ok_or())?;

                let service_id = ServiceId::from_parts(partition_key, service_name, service_key);

                f((service_id, state_key, value)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyStateTable for PartitionStoreTransaction<'_> {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, service_id, state_key)
    }

    fn get_all_user_states_for_service(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self, service_id,
        )?))
    }
}

impl StateTable for PartitionStoreTransaction<'_> {
    async fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_user_state(self, service_id, state_key, state_value)
    }

    async fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_user_state(self, service_id, state_key)
    }

    async fn delete_all_user_state(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_user_state(self, service_id)
    }
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);
    Ok((user_key, user_value))
}

#[cfg(test)]
mod tests {
    use crate::keys::TableKey;
    use crate::state_table::{user_state_key_from_slice, write_state_entry_key};
    use bytes::{Bytes, BytesMut};
    use restate_types::identifiers::ServiceId;

    static EMPTY: Bytes = Bytes::from_static(b"");

    fn state_entry_key(service_id: &ServiceId, state_key: &Bytes) -> BytesMut {
        write_state_entry_key(service_id, state_key).serialize()
    }

    #[test]
    fn keys_sort_services() {
        assert!(
            state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", ""), &EMPTY)
                < state_entry_key(&ServiceId::with_partition_key(1337, "svc-2", ""), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_but_different_keys() {
        assert!(
            state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "a"), &EMPTY)
                < state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "b"), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_and_keys_but_different_states() {
        let a = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"a"),
        );
        let b = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"b"),
        );
        assert!(a < b);
    }

    #[test]
    fn user_state_key_can_be_extracted() {
        let a = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"seen_count"),
        );

        assert_eq!(
            user_state_key_from_slice(&a).unwrap(),
            Bytes::from_static(b"seen_count")
        );
    }
}
