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
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{
    AvailabilityNotified, LocalMemoryLease, LocalMemoryPool, PinnableMemoryStream,
};
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
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
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
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
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

/// A budget-gated state stream that acquires a [`LocalMemoryLease`] from
/// `budget` **before** decoding each entry.
///
/// Implements [`PinnableMemoryStream`] so that callers that accumulate
/// (merge) per-entry leases (e.g. `collect_eager_state`) can signal
/// pinned (non-reclaimable) memory. The `pinned` counter is a plain
/// `usize` — no atomics or shared state needed.
///
/// The poll loop is a unified reserve-read-adjust cycle: peek the raw byte
/// size, attempt synchronous [`LocalMemoryPool::try_reserve`], and decode
/// on success (fast path — no waking). When `try_reserve` fails the deficit
/// is stashed and the stream waits for an [`AvailabilityNotified`] signal,
/// then retries.
#[pin_project::pin_project]
struct BudgetedStateStream<'a, DB: DBAccess> {
    iter: StateEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
    /// Memory the caller has accumulated (merged) that won't be released
    /// until the caller's operation completes.
    pinned: usize,
    /// Per-entry lease being built for the current item.
    lease: LocalMemoryLease,
    /// Non-zero when try_reserve failed and we're waiting for budget.
    pending_deficit: usize,
    /// Notification future while waiting for budget availability.
    #[pin]
    notified: Option<AvailabilityNotified>,
}

/// Result of a synchronous attempt to produce the next state entry.
enum TryProduce {
    /// Entry decoded and lease split off. Ready to yield.
    Ready(std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>),
    /// Iterator exhausted — stream is done.
    Exhausted,
    /// `try_reserve` failed with this deficit; caller should await budget.
    NeedsBudget(usize),
}

impl<DB: DBAccess + Send> BudgetedStateStream<'_, DB> {
    /// Try to peek, reserve, decode, and split a lease for the next entry.
    ///
    /// On success the entry's lease is split from `lease`, which retains any
    /// leftover capacity for the next iteration (avoiding a round-trip to the
    /// global pool).
    fn try_produce_next(
        iter: &mut StateEntryIter<'_, DB>,
        budget: &mut LocalMemoryPool,
        lease: &mut LocalMemoryLease,
    ) -> TryProduce {
        let (k, v) = match iter.peek_item() {
            Some(Ok(item)) => item,
            Some(Err(e)) => return TryProduce::Ready(Err(e.into())),
            None => return TryProduce::Exhausted,
        };

        let raw_size = k.len() + v.len();

        // Fast path 1: existing lease already covers the entry.
        if raw_size <= lease.size() {
            // Decode copies data out of the iterator, releasing the borrow.
            let result = decode_user_state_key_value(k, v);
            iter.advance();
            return TryProduce::Ready(
                result
                    .map(|(key, value)| (key, value, lease.split(raw_size)))
                    .map_err(BudgetedReadError::from),
            );
        }

        // Fast path 2: synchronous try_reserve for the deficit.
        let deficit = raw_size - lease.size();
        if let Some(extra) = budget.try_reserve(deficit) {
            lease.merge(extra);
            let result = decode_user_state_key_value(k, v);
            iter.advance();
            return TryProduce::Ready(
                result
                    .map(|(key, value)| (key, value, lease.split(raw_size)))
                    .map_err(BudgetedReadError::from),
            );
        }

        TryProduce::NeedsBudget(deficit)
    }
}

impl<DB: DBAccess + Send> Stream for BudgetedStateStream<'_, DB> {
    type Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            // If we're waiting for a notification, poll it first.
            if let Some(notified) = this.notified.as_mut().as_pin_mut() {
                ready!(notified.poll(cx));
                this.notified.set(None);
                // Notification fired — retry the reserve below.
            }

            // If we have a pending deficit from a previous failed try_reserve,
            // attempt to resolve it.
            if *this.pending_deficit > 0 {
                let deficit = *this.pending_deficit;
                if let Some(extra) = this.budget.try_reserve(deficit) {
                    this.lease.merge(extra);
                    *this.pending_deficit = 0;
                    // Fall through to try_produce_next below.
                } else if this.budget.is_out_of_memory(deficit, *this.pinned) {
                    *this.pending_deficit = 0;
                    return Poll::Ready(Some(Err(restate_memory::OutOfMemory {
                        needed: deficit.saturating_add(*this.pinned),
                    }
                    .into())));
                } else {
                    // Transient failure — register for notification and wait.
                    this.notified.set(Some(this.budget.availability_notified()));
                    // Poll the newly created notification future to register waker.
                    let _ = this.notified.as_mut().as_pin_mut().unwrap().poll(cx);
                    return Poll::Pending;
                }
            }

            // Try the synchronous fast path.
            match Self::try_produce_next(this.iter, this.budget, this.lease) {
                TryProduce::Exhausted => return Poll::Ready(None),
                TryProduce::Ready(result) => return Poll::Ready(Some(result)),
                TryProduce::NeedsBudget(deficit) => {
                    *this.pending_deficit = deficit;
                    // Loop back to the deficit-resolution logic above.
                }
            }
        }
    }
}

impl<DB: DBAccess + Send> PinnableMemoryStream for BudgetedStateStream<'_, DB> {
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        *self.project().pinned += amount;
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        let pinned = self.project().pinned;
        *pinned = pinned.saturating_sub(amount);
    }
}

fn budgeted_state_stream<'a, DB: DBAccess + Send>(
    iter: StateEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> BudgetedStateStream<'a, DB> {
    let lease = budget.empty_lease();
    BudgetedStateStream {
        iter,
        budget,
        pinned: 0,
        lease,
        pending_deficit: 0,
        notified: None,
    }
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
