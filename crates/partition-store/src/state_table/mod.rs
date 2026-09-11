// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{
    AvailabilityNotified, IgnorePinnableMemoryStream, LocalMemoryLease, LocalMemoryPool,
    PinnableMemoryStream,
};
use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::state_table::{ReadStateTable, ScanStateTable, WriteStateTable};
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::{Scope, ServiceName};
use restate_util_string::ReString;

use crate::TableKind::State;
use crate::features::StorageFeatures;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
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
        state_key: Bytes,
    )
);

define_table_key!(
    State,
    KeyKind::ScopedState,
    ScopedStateKey(
        partition_key: PartitionKey,
        scope: Option<Scope>,
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

fn get_user_state<S: StorageAccess>(
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

fn get_all_user_states_for_service<'a, S: StorageAccess>(
    storage: &'a S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
) -> Result<StateEntryIter<'a, S::DBAccess<'a>>> {
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
        let iter = get_all_user_states_for_service(self, self.storage_features(), service_id)?;
        Ok(BudgetedStateStream::new(iter, budget))
    }

    fn get_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        keys: Vec<Bytes>,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        let preloaded =
            preload_user_states(self, self.storage_features(), service_id, keys, budget)?;
        // The entries are already leased, so the stream never waits on the budget:
        // pin/unpin tracking is unnecessary.
        Ok(IgnorePinnableMemoryStream::new(stream::iter(
            preloaded.into_iter().map(Ok::<_, BudgetedReadError>),
        )))
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
        let iter = get_all_user_states_for_service(self, self.storage_features(), service_id)?;
        Ok(BudgetedStateStream::new(iter, budget))
    }

    fn get_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        keys: Vec<Bytes>,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(service_id)?;
        let preloaded =
            preload_user_states(self, self.storage_features(), service_id, keys, budget)?;
        // The entries are already leased, so the stream never waits on the budget:
        // pin/unpin tracking is unnecessary.
        Ok(IgnorePinnableMemoryStream::new(stream::iter(
            preloaded.into_iter().map(Ok::<_, BudgetedReadError>),
        )))
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

/// Source of raw state entries for [`BudgetedStateStream`]. Lets the same
/// budget-reserve/wait/pin loop drive both a full-service scan and a set of
/// point reads.
trait BudgetedStateItems {
    /// Byte size (raw key + value) of the next entry without consuming it, or
    /// `None` when exhausted. Must be idempotent for the same entry: the loop
    /// may re-peek across a budget wait.
    fn peek_size(&mut self) -> Option<Result<usize>>;

    /// Materialize the peeked entry as owned `(key, value)` and advance. Only
    /// called after a `peek_size` that returned `Some(Ok(_))`.
    fn take_next(&mut self) -> Result<(Bytes, Bytes)>;
}

impl<DB: DBAccess> BudgetedStateItems for StateEntryIter<'_, DB> {
    fn peek_size(&mut self) -> Option<Result<usize>> {
        match self.peek_item()? {
            Ok((k, v)) => Some(Ok(k.len() + v.len())),
            Err(e) => Some(Err(e)),
        }
    }

    fn take_next(&mut self) -> Result<(Bytes, Bytes)> {
        let (k, v) = self.peek_item().expect("peeked before take")?;
        let result = decode_user_state_key_value(k, v);
        self.advance();
        result
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
struct BudgetedStateStream<'a, I> {
    items: I,
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

impl<'a, I> BudgetedStateStream<'a, I> {
    fn new(items: I, budget: &'a mut LocalMemoryPool) -> Self {
        let lease = budget.empty_lease();
        Self {
            items,
            budget,
            pinned: 0,
            lease,
            pending_deficit: 0,
            notified: None,
        }
    }
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

impl<I: BudgetedStateItems> BudgetedStateStream<'_, I> {
    /// Try to peek, reserve, materialize, and split a lease for the next entry.
    ///
    /// On success the entry's lease is split from `lease`, which retains any
    /// leftover capacity for the next iteration (avoiding a round-trip to the
    /// global pool).
    fn try_produce_next(
        items: &mut I,
        budget: &mut LocalMemoryPool,
        lease: &mut LocalMemoryLease,
    ) -> TryProduce {
        let raw_size = match items.peek_size() {
            Some(Ok(size)) => size,
            Some(Err(e)) => return TryProduce::Ready(Err(e.into())),
            None => return TryProduce::Exhausted,
        };

        // Fast path 1: existing lease already covers the entry.
        if raw_size <= lease.size() {
            return TryProduce::Ready(
                items
                    .take_next()
                    .map(|(key, value)| (key, value, lease.split(raw_size)))
                    .map_err(BudgetedReadError::from),
            );
        }

        // Fast path 2: synchronous try_reserve for the deficit.
        let deficit = raw_size - lease.size();
        if let Some(extra) = budget.try_reserve(deficit) {
            lease.merge(extra);
            return TryProduce::Ready(
                items
                    .take_next()
                    .map(|(key, value)| (key, value, lease.split(raw_size)))
                    .map_err(BudgetedReadError::from),
            );
        }

        TryProduce::NeedsBudget(deficit)
    }
}

impl<I: BudgetedStateItems> Stream for BudgetedStateStream<'_, I> {
    type Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            // If we have a pending deficit from a previous failed try_reserve,
            // attempt to resolve it.
            if *this.pending_deficit > 0 {
                let deficit = *this.pending_deficit;

                // Ensure a notification future exists BEFORE checking
                // availability. `notify_waiters()` only wakes futures that
                // have already been polled (registered as waiters), so we
                // must create + poll the future before `try_reserve` to
                // avoid losing concurrent return-memory notifications.
                if this.notified.as_mut().as_pin_mut().is_none() {
                    this.notified.set(Some(this.budget.availability_notified()));
                }

                if let Some(extra) = this.budget.try_reserve(deficit) {
                    this.lease.merge(extra);
                    *this.pending_deficit = 0;
                    this.notified.set(None);
                    // Fall through to try_produce_next below.
                } else if let Err(oom) = this
                    .budget
                    .check_out_of_memory(deficit, *this.pinned + this.lease.size())
                {
                    *this.pending_deficit = 0;
                    this.notified.set(None);
                    return Poll::Ready(Some(Err(oom.into())));
                } else {
                    // Transient failure — poll the notification to register
                    // the waker or consume a pending notification.
                    match this.notified.as_mut().as_pin_mut().unwrap().poll(cx) {
                        Poll::Ready(()) => {
                            // Notification consumed — clear and retry.
                            this.notified.set(None);
                            continue;
                        }
                        Poll::Pending => {
                            // Waker registered — wait for memory.
                            return Poll::Pending;
                        }
                    }
                }
            }

            // Try the synchronous fast path.
            match Self::try_produce_next(this.items, this.budget, this.lease) {
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

impl<I: BudgetedStateItems> PinnableMemoryStream for BudgetedStateStream<'_, I> {
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        let this = self.project();
        *this.pinned = this.pinned.saturating_add(amount);
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        let pinned = self.project().pinned;
        *pinned = pinned.saturating_sub(amount);
    }
}

/// Point-reads the requested `keys` for `service_id`, preloading each present
/// entry into an owned `(state_key, value)` pair together with a
/// [`LocalMemoryLease`] covering its size. Absent keys are skipped.
///
/// Used to build the always-eager whitelist for lazy state: it issues one point
/// lookup per key instead of opening a full service-prefix scan.
///
/// Budget: the lease for an entry is reserved from `budget` **before** the value
/// is copied out of the store, so preloaded memory is fully accounted. Because
/// this runs in a synchronous context it cannot wait for budget to free up: if a
/// reservation fails, preloading stops and the remaining keys are simply served
/// lazily on demand (this is a lazy default, so that path always exists).
fn preload_user_states<S: StorageAccess>(
    storage: &S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    keys: Vec<Bytes>,
    budget: &mut LocalMemoryPool,
) -> Result<Vec<(Bytes, Bytes, LocalMemoryLease)>> {
    let _x = RocksDbReadPerfGuard::new("get-user-states-budgeted");
    let mut preloaded = Vec::with_capacity(keys.len());
    for state_key in keys {
        let encoded = encode_user_state_key(storage_features, service_id, &state_key);
        let Some(value) = storage.get(State, &encoded)? else {
            continue;
        };
        let value = value.as_ref();
        // Reserve before materializing the owned copy. On failure, stop: the rest
        // is served lazily rather than allocated off-budget.
        let Some(lease) = budget.try_reserve(state_key.len() + value.len()) else {
            break;
        };
        preloaded.push((state_key, Bytes::copy_from_slice(value), lease));
    }
    Ok(preloaded)
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
