// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! State storage with legacy, non-mem-comparable keys.
//!
//! Both [`StateKey`] and [`ScopedStateKey`] are **not ordered across all logical
//! fields**: their service names, service keys, and state keys are length-prefixed;
//! scoped keys also use the legacy optional-scope codec. The partition-key prefix
//! supports range scans, but iteration does not imply lexicographic state-key order.

mod binding;
mod scan;

pub use binding::{ScopedStateBinding, StateBinding};
pub use scan::{ScopedStateTable, StateTable};

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;

use restate_memory::{
    ClaimedMemoryPool, LentMemoryPool, LocalMemoryLease, LocalMemoryPool, PinnableMemoryStream,
    ReserveMemory, WithPinHandle,
};
use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::state_table::{ReadStateTable, ScanStateTable, WriteStateTable};
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::{Scope, ServiceName};
use restate_util_string::ReString;

use crate::TableKind::State;
use crate::budgeted_reads::budgeted_get;
use crate::features::StorageFeatures;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, OptionalScope, define_table_key};
use crate::scan::PhysicalScan;
use crate::{
    PartitionStore, PartitionStoreSnapshot, PartitionStoreTransaction, StorageAccess,
    StorageIterator, StorageRead, TableScan, TableScanIterationDecision, break_on_err,
};

define_table_key!(
    State,
    KeyKind::State,
    StateKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString,
        state_key: Bytes,
    )
);

define_table_key!(
    State,
    KeyKind::ScopedState,
    ScopedStateKey(
        partition_key: PartitionKey,
        scope: Option<Scope> => OptionalScope,
        service_name: ServiceName,
        service_key: ReString,
        // unfortunately, journal v1 allowed to pass in Bytes as a state key :-(
        // Can switch to a StringLike type once we remove support for journal v1.
        state_key: Bytes,
    )
);

#[inline]
fn write_state_entry_key(service_id: &ServiceId, state_key: &Bytes) -> StateKey {
    StateKey {
        partition_key: service_id.partition_key(),
        service_name: service_id.service_name.clone(),
        service_key: service_id.key.clone(),
        state_key: state_key.clone(),
    }
}

#[inline]
fn user_state_key_from_slice(mut key: &[u8]) -> Result<Bytes> {
    // Determine key kind from the first 2 bytes to dispatch to the right deserializer
    if key.len() >= 2
        && KeyKind::from_bytes(key[..2].try_into().unwrap()) == Some(KeyKind::ScopedState)
    {
        return Ok(ScopedStateKey::deserialize_from(&mut key)?.state_key);
    }
    Ok(StateKey::deserialize_from(&mut key)?.state_key)
}

/// Lazy iterator over state entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
pub(crate) struct StateEntryIter<I> {
    iter: I,
}

impl<I: StorageIterator> StateEntryIter<I> {
    fn new(iter: I) -> Self {
        Self { iter }
    }

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        self.iter.item().transpose()
    }

    /// Advances the iterator to the next entry.
    pub fn advance(&mut self) {
        self.iter.next();
    }
}

impl<I: StorageIterator> Iterator for StateEntryIter<I> {
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

/// Returns `true` if the call should use the scoped state table exclusively
#[inline]
fn use_scoped_state(storage_features: StorageFeatures, service_id: &ServiceId) -> bool {
    storage_features.is_migrated_to_scoped_state_table || service_id.scope.is_some()
}

fn put_user_state<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    state_key: &Bytes,
    state_value: impl AsRef<[u8]>,
) -> Result<()> {
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key)
            .state_key(state_key)
            .into_complete()
            .expect("key to be complete");

        storage.put_kv_raw(key, state_value.as_ref())
    } else {
        let key = write_state_entry_key(service_id, state_key);
        storage.put_kv_raw(key, state_value.as_ref())
    }
}

fn delete_user_state<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    state_key: &Bytes,
) -> Result<()> {
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key)
            .state_key(state_key)
            .into_complete()
            .expect("key to be complete");

        storage.delete_key(&key)
    } else {
        let key = write_state_entry_key(service_id, state_key);
        storage.delete_key(&key)
    }
}

fn delete_all_user_state<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
) -> Result<()> {
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let prefix_key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key);

        // Right now the WBWI does not support range deletions :-(
        // That's why we need to iterate over the individual state entries.
        let keys = storage.for_each_key_value_in_place(TableScan::Prefix(prefix_key), |k, _| {
            TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k)))
        })?;

        for k in keys {
            let key = k?;
            storage.delete_cf(State, &key)?;
        }
    } else {
        let prefix_key = StateKey::builder()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        let keys = storage.for_each_key_value_in_place(TableScan::Prefix(prefix_key), |k, _| {
            TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k)))
        })?;

        for k in keys {
            let key = k?;
            storage.delete_cf(State, &key)?;
        }
    }

    Ok(())
}

fn get_user_state<S: StorageRead>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    state_key: &Bytes,
) -> Result<Option<Bytes>> {
    let _x = RocksDbReadPerfGuard::new("get-user-state");
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key)
            .state_key(state_key)
            .into_complete()
            .expect("key to be complete");

        storage.get_kv_raw(key, move |_k, v| Ok(v.map(Bytes::copy_from_slice)))
    } else {
        let key = write_state_entry_key(service_id, state_key);
        storage.get_kv_raw(key, move |_k, v| Ok(v.map(Bytes::copy_from_slice)))
    }
}

fn get_all_user_states_for_service_physical_scan(
    storage_features: StorageFeatures,
    service_id: &ServiceId,
) -> PhysicalScan<Bytes> {
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key);

        TableScan::Prefix(key).into()
    } else {
        let key = StateKey::builder()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        TableScan::Prefix(key).into()
    }
}

fn get_all_user_states_for_service<'a, S: StorageRead>(
    storage: &'a S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
) -> Result<StateEntryIter<S::Iterator<'a>>> {
    let _x = RocksDbReadPerfGuard::new("get-all-user-state-iter-setup");

    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key);

        let iter = storage.iterator_from(TableScan::Prefix(key))?;
        Ok(StateEntryIter::new(iter))
    } else {
        let key = StateKey::builder()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        let iter = storage.iterator_from(TableScan::Prefix(key))?;
        Ok(StateEntryIter::new(iter))
    }
}

impl ReadStateTable for PartitionStore {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: &Bytes,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, self.storage_features(), service_id, state_key)
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self,
            self.storage_features(),
            service_id,
        )?))
    }
}

impl ScanStateTable for PartitionStore {
    fn for_each_user_state<
        F: FnMut((ServiceId, Bytes, &[u8])) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: KeyRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        // Share callback between two sequential scans via Arc<Mutex<>>
        // (needed because iterator_for_each requires 'static closures).
        // No contention: scans are awaited sequentially.
        let f = Arc::new(parking_lot::Mutex::new(f));

        // Only scan the legacy unscoped table while we may still hold data there.
        // After migration the range was deleted, so the scoped scan covers everything.
        let unscoped = if self.storage_features().is_migrated_to_scoped_state_table {
            None
        } else {
            let f_unscoped = Arc::clone(&f);
            Some(
                self.iterator_for_each(
                    "df-user-state",
                    Priority::Low,
                    TableScan::ScanPartitionKeyRange::<StateKey>(range),
                    move |(mut key, value)| {
                        let row_key = break_on_err(StateKey::deserialize_from(&mut key))?;
                        let (partition_key, service_name, service_key, state_key) = row_key.split();
                        let service_id =
                            ServiceId::from_parts(partition_key, service_name, service_key);
                        f_unscoped.lock()((service_id, state_key, value)).map_break(Ok)
                    },
                )
                .map_err(|_| StorageError::OperationalError)?,
            )
        };

        let f_scoped = f;
        let scoped = self
            .iterator_for_each(
                "df-user-state-scoped",
                Priority::Low,
                TableScan::ScanPartitionKeyRange::<ScopedStateKey>(range),
                move |(mut key, value)| {
                    let row_key = break_on_err(ScopedStateKey::deserialize_from(&mut key))?;
                    let (_partition_key, scope, service_name, service_key, state_key) =
                        row_key.split();
                    let service_id = ServiceId::new(
                        scope,
                        ByteString::from(service_name.as_str()),
                        ByteString::from(service_key.as_str()),
                    );
                    f_scoped.lock()((service_id, state_key, value)).map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(async move {
            if let Some(unscoped) = unscoped {
                unscoped.await?;
            }
            scoped.await?;
            Ok(())
        })
    }
}

impl ReadStateTable for PartitionStoreTransaction<'_> {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: &Bytes,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, self.storage_features(), service_id, state_key)
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self,
            self.storage_features(),
            service_id,
        )?))
    }
}

impl ReadStateTable for PartitionStoreSnapshot<'_> {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: &Bytes,
    ) -> Result<Option<Bytes>> {
        self.assert_partition_key(service_id)?;
        get_user_state(self, self.storage_features(), service_id, state_key)
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        self.assert_partition_key(service_id)?;
        Ok(stream::iter(get_all_user_states_for_service(
            self,
            self.storage_features(),
            service_id,
        )?))
    }
}

impl PartitionStoreSnapshot<'_> {
    /// Budget-gated state stream.
    ///
    /// Each state entry's raw byte size is peeked **before** deserialization.
    /// A [`LocalMemoryLease`] is acquired from `budget` for that size, and only
    /// then is the entry decoded.
    ///
    /// `budget` may be lent to a storage worker until the stream is done or
    /// dropped: the stream keeps it borrowed, and claiming it again afterwards
    /// waits for it to come back.
    pub fn get_all_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: ClaimedMemoryPool<'a>,
    ) -> Result<
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        let scan =
            get_all_user_states_for_service_physical_scan(self.storage_features(), service_id);
        self.budgeted_scan(
            "get-all-user-states-budgeted",
            scan,
            budget,
            |key, value| key.len() + value.len(),
            |key, value, lease| {
                TableScanIterationDecision::Emit(
                    decode_user_state_key_value(key, value).map(|(key, value)| (key, value, lease)),
                )
            },
        )
    }
}

impl WriteStateTable for PartitionStoreTransaction<'_> {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: &Bytes,
        state_value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_user_state(
            self,
            self.storage_features(),
            service_id,
            state_key,
            state_value,
        )
    }

    fn delete_user_state(&mut self, service_id: &ServiceId, state_key: &Bytes) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_user_state(self, self.storage_features(), service_id, state_key)
    }

    fn delete_all_user_state(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_user_state(self, self.storage_features(), service_id)
    }
}

impl PartitionStore {
    /// Budget-gated point read of a single state key.
    ///
    /// The [`LocalMemoryLease`], for the size of the key and the value, is
    /// acquired from `budget` **before** the value is copied out. Waits for
    /// reclaimable memory when the reservation is feasible, and returns a
    /// budget error otherwise.
    pub async fn get_user_state_budgeted(
        &mut self,
        service_id: &ServiceId,
        state_key: &ByteString,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(Bytes, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(service_id)?;
        let features = self.storage_features();
        get_user_state_budgeted(self, features, service_id, state_key, budget).await
    }
}

/// Budget-gated point read of a state key: reserves the size of the key and
/// the value before copying the value out.
async fn get_user_state_budgeted<S: StorageRead, B: ReserveMemory>(
    storage: &S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    state_key: &ByteString,
    budget: &mut B,
) -> std::result::Result<Option<(Bytes, LocalMemoryLease)>, BudgetedReadError> {
    let key = encode_user_state_key(storage_features, service_id, state_key.as_bytes());
    budgeted_get(
        storage,
        "get-user-states-budgeted-point-read",
        user_state_key_kind(storage_features, service_id),
        &key,
        budget,
        |value| state_key.len() + value.len(),
        |value, lease| Ok((Bytes::copy_from_slice(value), lease)),
    )
    .await
}

impl PartitionStoreSnapshot<'_> {
    /// Budget-gated point reads of `keys`, in order. Keys without a value are
    /// omitted. Each key is read when the stream is polled for it, and its
    /// [`LocalMemoryLease`] is acquired **before** its value is copied out.
    ///
    /// Waits for reclaimable memory when a reservation is feasible, and yields
    /// a budget error otherwise. The stream ends after an error.
    ///
    /// `budget` is lent to the stream until it's dropped. The stream owns its
    /// service ID and keys.
    pub fn get_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        keys: &[ByteString],
        budget: ClaimedMemoryPool<'a>,
    ) -> Result<
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        // Own the keys, so that the stream doesn't borrow the caller's.
        let keys = keys.to_vec();
        let (budget, pins) = budget.lend();
        let reads = UserStateReads {
            snapshot: self,
            service_id: service_id.clone(),
            keys: keys.into_iter(),
            budget,
        };
        let rows = stream::try_unfold(reads, |mut reads| async move {
            Ok(reads.next().await?.map(|row| (row, reads)))
        });
        Ok(WithPinHandle::new(rows, pins))
    }
}

/// The point reads behind [`PartitionStoreSnapshot::get_user_states_budgeted`].
struct UserStateReads<'a, 'b> {
    snapshot: &'a PartitionStoreSnapshot<'b>,
    service_id: ServiceId,
    keys: std::vec::IntoIter<ByteString>,
    budget: LentMemoryPool,
}

impl UserStateReads<'_, '_> {
    /// Reads the next key that has a value.
    async fn next(
        &mut self,
    ) -> std::result::Result<Option<(Bytes, Bytes, LocalMemoryLease)>, BudgetedReadError> {
        let storage_features = self.snapshot.storage_features();
        for state_key in self.keys.by_ref() {
            let row = get_user_state_budgeted(
                self.snapshot,
                storage_features,
                &self.service_id,
                &state_key,
                &mut self.budget,
            )
            .await?;
            if let Some((value, lease)) = row {
                return Ok(Some((state_key.into_bytes(), value, lease)));
            }
        }
        Ok(None)
    }
}

/// The kind of the keys that [`encode_user_state_key`] builds.
fn user_state_key_kind(storage_features: StorageFeatures, service_id: &ServiceId) -> KeyKind {
    if use_scoped_state(storage_features, service_id) {
        KeyKind::ScopedState
    } else {
        KeyKind::State
    }
}

/// Serializes the full storage key for `state_key` exactly as it is stored, so
/// it can be used as a point-lookup key against the state table.
fn encode_user_state_key(
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    state_key: &Bytes,
) -> Bytes {
    let mut buf = BytesMut::new();
    if use_scoped_state(storage_features, service_id) {
        //todo(tillrohrmann) remove once ServiceId carries the right types
        let service_name = ServiceName::new(service_id.service_name.as_ref());
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let key = ScopedStateKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key)
            .state_key(state_key)
            .into_complete()
            .expect("key to be complete");

        buf.reserve(key.serialized_length());
        key.serialize_to(&mut buf);
    } else {
        let key = write_state_entry_key(service_id, state_key);
        buf.reserve(key.serialized_length());
        key.serialize_to(&mut buf);
    }
    buf.freeze()
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);
    Ok((user_key, user_value))
}

#[cfg(test)]
mod tests {
    use crate::keys::EncodeTableKeyPrefix;
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
