// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::state_table::{ReadStateTable, ScanStateTable, WriteStateTable};
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};

use crate::TableKind::State;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan,
    TableScanIterationDecision, break_on_err,
};

define_table_key!(
    State,
    KeyKind::State,
    StateKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString,
        state_key: Bytes
    )
);

#[inline]
fn write_state_entry_key(service_id: &ServiceId, state_key: impl AsRef<[u8]>) -> StateKey {
    StateKey {
        partition_key: service_id.partition_key(),
        service_name: service_id.service_name.clone(),
        service_key: service_id.key.clone(),
        state_key: state_key.as_ref().to_vec().into(),
    }
}

#[inline]
fn user_state_key_from_slice(mut key: &[u8]) -> Result<Bytes> {
    Ok(StateKey::deserialize_from(&mut key)?.state_key)
}

/// Lazy iterator over state entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
pub struct StateEntryIter<'a, DB: DBAccess> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
}

impl<'a, DB: DBAccess> StateEntryIter<'a, DB> {
    fn new(iter: DBRawIteratorWithThreadMode<'a, DB>) -> Self {
        Self { iter }
    }

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        match self.iter.item() {
            Some((k, v)) => Some(Ok((k, v))),
            None => self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into()))),
        }
    }

    /// Advances the iterator to the next entry.
    pub fn advance(&mut self) {
        self.iter.next();
    }
}

impl<DB: DBAccess> Iterator for StateEntryIter<'_, DB> {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = match self.peek_item()? {
            Ok(item) => item,
            Err(e) => return Some(Err(e)),
        };
        let result = decode_user_state_key_value(k, v);
        self.advance();
        Some(result)
    }
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
    let prefix_key = StateKey::builder()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    let keys = storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix_key),
        |k, _| TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k))),
    )?;

    for k in keys {
        let key = k?;
        storage.delete_cf(State, &key)?;
    }

    Ok(())
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

fn get_all_user_states_for_service<'a, S: StorageAccess>(
    storage: &'a S,
    service_id: &ServiceId,
) -> Result<StateEntryIter<'a, S::DBAccess<'a>>> {
    let _x = RocksDbPerfGuard::new("get-all-user-state-iter-setup");
    let key = StateKey::builder()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
        service_id.partition_key(),
        key,
    ))?;

    Ok(StateEntryIter::new(iter))
}

impl ReadStateTable for PartitionStore {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, service_id, state_key)
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self, service_id,
        )?))
    }

    fn get_all_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>>
        + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        let iter = get_all_user_states_for_service(self, service_id)?;
        Ok(budgeted_state_stream(iter, budget))
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
                let (partition_key, service_name, service_key, state_key) = row_key.split();

                let service_id = ServiceId::from_parts(partition_key, service_name, service_key);

                f((service_id, state_key, value)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadStateTable for PartitionStoreTransaction<'_> {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, service_id, state_key)
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self, service_id,
        )?))
    }

    fn get_all_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>>
        + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        let iter = get_all_user_states_for_service(self, service_id)?;
        Ok(budgeted_state_stream(iter, budget))
    }
}

impl WriteStateTable for PartitionStoreTransaction<'_> {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_user_state(self, service_id, state_key, state_value)
    }

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_user_state(self, service_id, state_key)
    }

    fn delete_all_user_state(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_user_state(self, service_id)
    }
}

/// Wraps a [`StateEntryIter`] into an async [`Stream`] that acquires a memory
/// lease from `budget` **before** decoding each entry.
///
/// Each entry is produced via a unified reserve-read-adjust loop: peek the raw
/// byte size, attempt synchronous [`LocalMemoryPool::try_reserve`], and decode
/// from the same iterator slices on success (fast path — no `.await`). When
/// `try_reserve` fails the borrow is dropped and the deficit is awaited via
/// [`LocalMemoryPool::reserve`], then the loop re-peeks the (unchanged)
/// iterator position.
fn budgeted_state_stream<'a, DB: DBAccess + Send>(
    iter: StateEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> impl Stream<Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>>
+ Send
+ 'a {
    futures::stream::unfold((iter, budget), |(mut iter, budget)| async move {
        let mut lease = budget.empty_lease();
        loop {
            let deficit = {
                let (k, v) = match iter.peek_item() {
                    Some(Ok(item)) => item,
                    Some(Err(e)) => return Some((Err(e.into()), (iter, budget))),
                    None => return None,
                };

                let raw_size = k.len() + v.len();
                if raw_size <= lease.size() {
                    lease.shrink(lease.size() - raw_size);
                    match decode_user_state_key_value(k, v) {
                        Ok((key, value)) => {
                            iter.advance();
                            return Some((Ok((key, value, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                let deficit = raw_size - lease.size();
                if let Some(extra) = budget.try_reserve(deficit) {
                    lease.merge(extra);
                    match decode_user_state_key_value(k, v) {
                        Ok((key, value)) => {
                            iter.advance();
                            return Some((Ok((key, value, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                deficit
            };

            let extra = match budget.reserve(deficit).await {
                Ok(l) => l,
                Err(e) => return Some((Err(e.into()), (iter, budget))),
            };
            lease.merge(extra);
        }
    })
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);
    Ok((user_key, user_value))
}

#[cfg(test)]
mod tests {
    use crate::keys::TableKeyPrefix;
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
