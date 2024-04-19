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
use crate::owned_iter::OwnedIterator;
use crate::TableKind::State;
use crate::{RocksDBStorage, RocksDBTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use std::future;
use std::future::Future;
use std::ops::RangeInclusive;

define_table_key!(
    State,
    KeyKind::State,
    StateKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        state_key: Bytes
    )
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
) {
    let key = write_state_entry_key(service_id, state_key);
    storage.put_kv_raw(key, state_value.as_ref());
}

fn delete_user_state<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
) {
    let key = write_state_entry_key(service_id, state_key);
    storage.delete_key(&key);
}

fn delete_all_user_state<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) -> Result<()> {
    let prefix_key = StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    let keys = storage.for_each_key_value_in_place(TableScan::KeyPrefix(prefix_key), |k, _| {
        TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k)))
    });

    for k in keys {
        storage.delete_cf(State, &k?);
    }

    Ok(())
}

fn get_user_state<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
) -> Result<Option<Bytes>> {
    let key = write_state_entry_key(service_id, state_key);
    storage.get_kv_raw(key, move |_k, v| Ok(v.map(Bytes::copy_from_slice)))
}

fn get_all_user_states<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Vec<Result<(Bytes, Bytes)>> {
    let key = StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage.for_each_key_value_in_place(TableScan::KeyPrefix(key), |k, v| {
        TableScanIterationDecision::Emit(decode_user_state_key_value(k, v))
    })
}

impl ReadOnlyStateTable for RocksDBStorage {
    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send {
        future::ready(get_user_state(self, service_id, state_key))
    }

    fn get_all_user_states(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = Result<(Bytes, Bytes)>> + Send {
        stream::iter(get_all_user_states(self, service_id))
    }
}

impl<'a> ReadOnlyStateTable for RocksDBTransaction<'a> {
    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send {
        future::ready(get_user_state(self, service_id, state_key))
    }

    fn get_all_user_states(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = Result<(Bytes, Bytes)>> + Send {
        stream::iter(get_all_user_states(self, service_id))
    }
}

impl<'a> StateTable for RocksDBTransaction<'a> {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send {
        put_user_state(self, service_id, state_key, state_value);
        future::ready(())
    }

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send {
        delete_user_state(self, service_id, state_key);
        future::ready(())
    }

    fn delete_all_user_state(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<()>> + Send {
        future::ready(delete_all_user_state(self, service_id))
    }
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);
    Ok((user_key, user_value))
}

#[derive(Clone, Debug)]
pub struct OwnedStateRow {
    pub partition_key: PartitionKey,
    pub service: ByteString,
    pub service_key: Bytes,
    pub state_key: Bytes,
    pub state_value: Bytes,
}

impl RocksDBStorage {
    pub fn all_states(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedStateRow> + '_ {
        let iter = self.iterator_from(TableScan::PartitionKeyRange::<StateKey>(range));
        OwnedIterator::new(iter).map(|(mut key, value)| {
            let row_key = StateKey::deserialize_from(&mut key).unwrap();
            OwnedStateRow {
                partition_key: row_key.partition_key.unwrap(),
                service: row_key.service_name.unwrap(),
                service_key: row_key.service_key.unwrap(),
                state_key: row_key.state_key.unwrap(),
                state_value: value,
            }
        })
    }
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
