// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-invocation memory budget with inbound/outbound directional isolation.
//!
//! Each active invocation gets an [`InvocationMemory`] with two [`LocalMemoryPool`]s:
//! one for the outbound path (RocksDB → invoker → hyper → deployment) and one for the
//! inbound path (deployment → hyper → decoder → invoker → Bifrost).
//!
//! # Memory model
//!
//! Each [`LocalMemoryPool`] maintains a *local capacity* backed by a [`MemoryLease`]
//! from the global invoker pool. Memory is reserved from local capacity first; when
//! local capacity is insufficient, additional memory is reserved from the global pool
//! and merged into the local lease. When outstanding [`LocalMemoryLease`]s are dropped,
//! in-flight decreases and available capacity is restored. Excess local capacity above
//! `min_reserved` can be released back to the global pool via
//! [`release_excess`](LocalMemoryPool::release_excess).
//!
//! # Concurrency
//!
//! A `LocalMemoryPool` is designed for **single-task access**. Both inbound and
//! outbound budgets are owned and operated by the protocol runner task alone (via a
//! `tokio::select!` loop). The [`LocalMemoryLease`] type is `Send` so it can be stored in
//! types that cross `.await` points, but all reserve/release operations happen within
//! the same task.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use crate::{MemoryLease, MemoryPool};

/// Returned by [`LocalMemoryPool::reserve`] when the requested memory cannot
/// be satisfied even in the best case (all in-flight reclaimed + entire global
/// pool available).
#[derive(Debug)]
pub struct OutOfMemory {
    /// The number of bytes that were requested but could not be allocated.
    pub needed: usize,
}

/// Per-invocation budget grouping inbound and outbound directional budgets.
pub struct InvocationMemory {
    pub inbound: LocalMemoryPool,
    pub outbound: LocalMemoryPool,
}

impl fmt::Debug for InvocationMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvocationMemory").finish_non_exhaustive()
    }
}

impl InvocationMemory {
    /// Creates a new invocation budget with per-direction seeds and upper bounds.
    ///
    /// Each seed's size defines the `min_reserved` floor for that direction's budget
    /// (i.e. [`release_excess`](LocalMemoryPool::release_excess) will never shrink below it).
    ///
    /// Both seeds must originate from the same [`MemoryPool`].
    pub fn new(
        inbound_seed: MemoryLease,
        inbound_upper_bound: usize,
        outbound_seed: MemoryLease,
        outbound_upper_bound: usize,
    ) -> Self {
        let pool = inbound_seed.budget().clone();
        let inbound_min = inbound_seed.size().as_usize();
        let outbound_min = outbound_seed.size().as_usize();

        Self {
            inbound: LocalMemoryPool::new(
                pool.clone(),
                inbound_seed,
                inbound_min,
                inbound_upper_bound,
            ),
            outbound: LocalMemoryPool::new(pool, outbound_seed, outbound_min, outbound_upper_bound),
        }
    }

    /// Returns surplus local capacity of inbound and outbound (above `min_reserved`) to the global pool.
    ///
    /// Will not shrink below `min_reserved` or below current `in_flight` of each budget.
    pub fn release_excess(&mut self) {
        self.inbound.release_excess();
        self.outbound.release_excess();
    }
}

/// A directional memory budget backed by a local [`MemoryLease`] from the global pool.
///
/// **Local capacity** = the size of the internal `MemoryLease`.
/// **Available** = local capacity − in-flight.
///
/// When [`try_reserve`](Self::try_reserve) is called:
/// 1. If the request fits within available local capacity, it succeeds immediately.
/// 2. If not, the budget grows its local capacity from the global pool (up to
///    `upper_bound`) by the deficit, then re-checks.
/// 3. If the global pool cannot provide enough, the reservation fails.
///
/// When a [`LocalMemoryLease`] is dropped, in-flight decreases and available increases.
///
/// # Drop behavior
///
/// When a `LocalMemoryPool` is dropped, it atomically snapshots the in-flight
/// count (and sets the dead bit) via a single [`swap`](AtomicUsize::swap). The
/// internal `MemoryLease` is then shrunk by the in-flight amount so that only
/// `(capacity - in_flight)` bytes are returned to the global pool. The withheld
/// bytes are returned individually by orphaned [`LocalMemoryLease`] drops, which detect
/// the dead bit and call [`MemoryPool::return_memory`] directly.
pub struct LocalMemoryPool {
    shared: Arc<SharedState>,
    /// Local capacity held from the global pool.
    local_capacity: MemoryLease,
    /// Floor: [`release_excess`](Self::release_excess) never shrinks below this.
    min_reserved: usize,
    /// Ceiling: max local capacity size.
    upper_bound: usize,
}

/// Shared state between the budget and its outstanding leases.
///
/// `Arc`-wrapped so that [`LocalMemoryLease`] drops can decrement `in_flight` and notify
/// waiters.
///
/// # Dead-bit protocol
///
/// The high bit of `in_flight` serves as a "budget dead" flag. When
/// [`LocalMemoryPool::drop`] runs, it atomically sets the dead bit via
/// [`swap`](AtomicUsize::swap), obtaining the exact in-flight count at that instant.
/// Subsequent [`LocalMemoryLease`] drops detect the dead bit and return their bytes
/// directly to the global pool instead of just decrementing in-flight.
///
/// This single-atomic approach avoids the TOCTOU race that would exist with
/// separate `alive` and `in_flight` atomics.
struct SharedState {
    pool: MemoryPool,
    /// Bytes currently held by outstanding leases, with [`DEAD_BIT`] set once the
    /// owning [`LocalMemoryPool`] has been dropped.
    in_flight: AtomicUsize,
    /// Wakes waiters when in-flight decreases.
    notify: Arc<Notify>,
}

/// Sentinel bit in `in_flight` indicating the budget is dead.
const DEAD_BIT: usize = 1 << (usize::BITS - 1);

impl SharedState {
    /// Returns `amount` bytes of in-flight memory.
    ///
    /// Uses a CAS loop on `in_flight` to atomically determine whether the budget
    /// is alive or dead:
    /// - **Alive** (no dead bit): decrements in-flight and wakes waiters.
    /// - **Dead** (dead bit set): returns bytes directly to the global pool without
    ///   touching in-flight (the budget already snapshot it via `swap`).
    fn return_memory(&self, amount: usize) {
        let result = self
            .in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                // Only subtract if the budget is alive (dead bit not set).
                // If dead, return None to signal the CAS should fail.
                (current & DEAD_BIT == 0).then_some(current - amount)
            });

        match result {
            Ok(_) => {
                // Pool is alive — we decremented in-flight. Wake reserve waiters.
                self.notify.notify_waiters();
            }
            Err(_) => {
                // Pool is dead — return directly to the global pool.
                self.pool.return_memory(amount);
            }
        }
    }
}

impl LocalMemoryPool {
    /// Creates a new directional budget.
    ///
    /// - `pool`: the global memory pool.
    /// - `local_capacity`: initial memory lease from the global pool.
    /// - `min_reserved`: floor that [`release_excess`](Self::release_excess) will not
    ///   shrink below.
    /// - `upper_bound`: ceiling on local capacity.
    pub fn new(
        pool: MemoryPool,
        local_capacity: MemoryLease,
        min_reserved: usize,
        upper_bound: usize,
    ) -> Self {
        Self {
            shared: Arc::new(SharedState {
                pool,
                in_flight: AtomicUsize::new(0),
                notify: Arc::new(Notify::new()),
            }),
            local_capacity,
            min_reserved,
            upper_bound,
        }
    }

    /// Tries to reserve `size` bytes without blocking.
    ///
    /// Checks available local capacity first. If insufficient, grows the local
    /// capacity from the global pool by the exact deficit (capped at `upper_bound`).
    /// Returns `None` if the memory cannot be reserved.
    pub fn try_reserve(&mut self, size: usize) -> Option<LocalMemoryLease> {
        if size == 0 {
            return Some(LocalMemoryLease {
                shared: Arc::clone(&self.shared),
                size: 0,
            });
        }

        let available = self.available();

        if available < size {
            // Need additional memory from the global pool.
            let deficit = size - available;
            let capacity = self.local_capacity.size().as_usize();
            if capacity.saturating_add(deficit) > self.upper_bound {
                return None;
            }
            if !self.local_capacity.try_grow(deficit) {
                return None;
            }
        }

        self.shared.in_flight.fetch_add(size, Ordering::Relaxed);
        Some(LocalMemoryLease {
            shared: Arc::clone(&self.shared),
            size,
        })
    }

    /// Reserves `size` bytes, waiting if necessary for in-flight memory to be
    /// reclaimed and/or global pool capacity to become available.
    ///
    /// Returns [`Err(OutOfMemory)`] if the request is infeasible — meaning
    /// even after reclaiming all in-flight memory and tapping the entire global
    /// pool, the request cannot be satisfied. This is checked on every
    /// iteration after a notification wakes us, so transient exhaustion is
    /// retried while permanent exhaustion is detected promptly.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel-safe: dropping the future before completion has no
    /// effect on the budget (no memory is reserved until the method returns).
    pub async fn reserve(&mut self, size: usize) -> Result<LocalMemoryLease, OutOfMemory> {
        loop {
            // Create the notification future *before* checking availability so
            // that a concurrent release (local in-flight drop or global pool
            // return) between our check and the `.await` is guaranteed to
            // wake us.
            let notified = self.availability_notified();
            tokio::pin!(notified);

            if let Some(lease) = self.try_reserve(size) {
                return Ok(lease);
            }
            if self.is_out_of_memory(size) {
                return Err(OutOfMemory { needed: size });
            }

            // try_reserve failed but the request is feasible. Wait for either
            // a local in-flight lease to be dropped or global pool availability
            // to change, then retry.
            notified.await;
        }
    }

    /// Non-blocking reserve for use in [`poll_next`](futures::Stream::poll_next)
    /// implementations.
    ///
    /// Returns:
    /// - `Poll::Ready(Ok(lease))` if the reservation succeeded immediately.
    /// - `Poll::Ready(Err(OutOfMemory))` if the request is permanently infeasible.
    /// - `Poll::Pending` if the reservation failed but may succeed later (e.g.
    ///   after in-flight leases are dropped or global capacity is returned).
    ///
    /// **Waker registration:** This method does *not* register a waker. The
    /// caller is responsible for arranging a retry, for example via a
    /// `tokio::time::Sleep` that re-polls after a short delay.
    pub fn poll_reserve(&mut self, size: usize) -> Poll<Result<LocalMemoryLease, OutOfMemory>> {
        if let Some(lease) = self.try_reserve(size) {
            return Poll::Ready(Ok(lease));
        }
        if self.is_out_of_memory(size) {
            return Poll::Ready(Err(OutOfMemory { needed: size }));
        }
        Poll::Pending
    }

    /// Returns surplus local capacity (above `min_reserved`) to the global pool.
    ///
    /// Will not shrink below `min_reserved` or below current `in_flight`.
    pub fn release_excess(&mut self) {
        let capacity = self.local_capacity.size().as_usize();
        let floor = self.min_reserved.max(self.in_flight());
        if capacity > floor {
            self.local_capacity.shrink(capacity - floor);
        }
    }

    /// Returns `true` if the requested memory fundamentally cannot be satisfied
    /// even after all in-flight memory is reclaimed and the global pool is tapped.
    ///
    /// The theoretical maximum is `min(upper_bound, local_capacity + global_available)`:
    /// - `upper_bound` is the hard ceiling per direction.
    /// - `local_capacity + global_available` is the most we could ever hold locally
    ///   (current capacity plus everything the global pool can give us), which accounts
    ///   for in-flight reclaims since in-flight bytes are already part of local capacity.
    pub fn is_out_of_memory(&self, needed: usize) -> bool {
        if needed > self.upper_bound {
            return true;
        }
        let capacity = self.local_capacity.size().as_usize();
        let global_available = self.shared.pool.available();
        needed > capacity.saturating_add(global_available)
    }

    /// Returns available memory (local capacity − in-flight).
    pub fn available(&self) -> usize {
        let capacity = self.local_capacity.size().as_usize();
        capacity.saturating_sub(self.in_flight())
    }

    /// Returns total local capacity held from the global pool.
    pub fn local_capacity(&self) -> usize {
        self.local_capacity.size().as_usize()
    }

    /// Returns memory currently held by outstanding leases.
    pub fn in_flight(&self) -> usize {
        self.shared.in_flight.load(Ordering::Relaxed) & !DEAD_BIT
    }

    /// Returns the minimum reserved capacity for this budget.
    ///
    /// [`release_excess`](Self::release_excess) never shrinks below this value.
    pub fn min_reserved(&self) -> usize {
        self.min_reserved
    }

    /// Creates a zero-sized lease from this budget.
    ///
    /// The returned lease shares the same [`Arc<SharedState>`] as leases produced
    /// by [`try_reserve`](Self::try_reserve) / [`reserve`](Self::reserve), so it
    /// can be [`merged`](LocalMemoryLease::merge) with them. Dropping an empty lease
    /// is a no-op.
    pub fn empty_lease(&self) -> LocalMemoryLease {
        LocalMemoryLease {
            shared: Arc::clone(&self.shared),
            size: 0,
        }
    }

    /// Returns a future that resolves when budget availability may have changed.
    ///
    /// The returned [`AvailabilityNotified`] combines local in-flight reclaim
    /// notifications with global pool availability changes. It resolves when
    /// either source fires.
    ///
    /// This future should be created *before* checking availability (e.g.
    /// before calling [`try_reserve`](Self::try_reserve)) to avoid missing
    /// concurrent notifications.
    pub fn availability_notified(&self) -> AvailabilityNotified {
        AvailabilityNotified {
            local: Arc::clone(&self.shared.notify).notified_owned(),
            global: self.shared.pool.availability_notified_owned(),
        }
    }
}

impl Drop for LocalMemoryPool {
    fn drop(&mut self) {
        // Atomically read the exact in-flight count AND set the dead bit in one
        // operation. After this, any LocalMemoryLease::drop will see the dead bit and
        // return its bytes directly to the global pool.
        let in_flight = self.shared.in_flight.swap(DEAD_BIT, Ordering::Relaxed);
        debug_assert!(in_flight & DEAD_BIT == 0, "LocalMemoryPool dropped twice");

        if in_flight > 0 {
            // Split off the in-flight portion and forget it so those bytes are NOT
            // returned to the global pool now. The withheld bytes will be returned
            // individually by orphaned LocalMemoryLease drops (which detect the dead bit).
            // Note: we cannot use `shrink()` here because that returns bytes to the
            // pool immediately — we need to withhold them.
            let withheld = self.local_capacity.split(in_flight);
            std::mem::forget(withheld);
        }

        // self.local_capacity drops here, returning (capacity - in_flight) to the pool.
    }
}

/// RAII memory lease from a [`LocalMemoryPool`].
///
/// When dropped, decrements in-flight and wakes anyone waiting in
/// [`LocalMemoryPool::reserve`].
#[must_use]
pub struct LocalMemoryLease {
    shared: Arc<SharedState>,
    size: usize,
}

impl LocalMemoryLease {
    /// Returns the size of this lease in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Merges `other` into `self`, combining their sizes.
    ///
    /// Both leases must originate from the same [`LocalMemoryPool`] (debug-asserted).
    /// After the merge, `other` is consumed without decrementing in-flight — the
    /// combined in-flight is now tracked by `self` alone.
    pub fn merge(&mut self, other: LocalMemoryLease) {
        debug_assert!(
            Arc::ptr_eq(&self.shared, &other.shared),
            "cannot merge leases from different budgets"
        );
        self.size += other.size;
        // Prevent `other`'s Drop from decrementing in-flight.
        std::mem::forget(other);
    }

    /// Splits off `amount` bytes into a new lease.
    ///
    /// The new lease shares the same [`SharedState`] as `self`. No memory is
    /// returned to the budget — the total in-flight stays the same, just split
    /// across two leases. Panics if `amount > self.size`.
    pub fn split(&mut self, amount: usize) -> LocalMemoryLease {
        assert!(amount <= self.size, "cannot split more than lease size");
        self.size -= amount;
        LocalMemoryLease {
            shared: Arc::clone(&self.shared),
            size: amount,
        }
    }

    /// Shrinks by `amount` bytes (clamped to current size), returning them to
    /// the budget immediately.
    pub fn shrink(&mut self, amount: usize) {
        let shrink_by = amount.min(self.size);
        if shrink_by > 0 {
            self.size -= shrink_by;
            self.shared.return_memory(shrink_by);
        }
    }
}

impl Drop for LocalMemoryLease {
    fn drop(&mut self) {
        if self.size > 0 {
            self.shared.return_memory(self.size);
        }
    }
}

impl std::fmt::Debug for LocalMemoryLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalMemoryLease")
            .field("size", &self.size)
            .finish()
    }
}

pin_project_lite::pin_project! {
    /// Future that resolves when budget availability may have changed.
    ///
    /// Combines local in-flight reclaim notifications with global pool
    /// availability changes. Resolves when either source fires.
    ///
    /// Created by [`LocalMemoryPool::availability_notified`].
    pub struct AvailabilityNotified {
        #[pin]
        local: OwnedNotified,
        #[pin]
        global: Option<OwnedNotified>,
    }
}

impl Future for AvailabilityNotified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        if this.local.poll(cx).is_ready() {
            return Poll::Ready(());
        }
        if let Some(global) = this.global.as_pin_mut()
            && Future::poll(global, cx).is_ready()
        {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

// Ensure Send + Sync
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LocalMemoryPool>();
    assert_send_sync::<InvocationMemory>();
    assert_send_sync::<LocalMemoryLease>();
    assert_send_sync::<AvailabilityNotified>();
};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MemoryPool, NonZeroByteCount};

    use super::*;

    fn pool(capacity: usize) -> MemoryPool {
        MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(capacity).unwrap()))
    }

    #[test]
    fn basic_reserve_and_release() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let lease1 = budget.try_reserve(100).unwrap();
        assert_eq!(lease1.size(), 100);
        assert_eq!(budget.in_flight(), 100);
        assert_eq!(budget.local_capacity(), 100);
        assert_eq!(budget.available(), 0);

        drop(lease1);
        assert_eq!(budget.in_flight(), 0);
        assert_eq!(budget.local_capacity(), 100);
        assert_eq!(budget.available(), 100);

        let lease2 = budget.try_reserve(50).unwrap();
        assert_eq!(lease2.size(), 50);
        assert_eq!(budget.in_flight(), 50);
        assert_eq!(budget.available(), 50);
        assert_eq!(budget.local_capacity(), 100);

        drop(lease2);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn partial_local_partial_global() {
        let global = pool(1000);
        let seed = global.try_reserve(60).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);
        assert_eq!(budget.local_capacity(), 60);

        let lease = budget.try_reserve(100).unwrap();
        assert_eq!(lease.size(), 100);
        assert_eq!(budget.local_capacity(), 100);
        assert_eq!(budget.in_flight(), 100);
        assert_eq!(budget.available(), 0);
        assert_eq!(global.used().as_usize(), 100);

        drop(lease);
        assert_eq!(budget.available(), 100);

        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn upper_bound_enforced() {
        let global = pool(10000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 200);

        let lease1 = budget.try_reserve(150).unwrap();
        assert!(budget.try_reserve(100).is_none());
        let lease2 = budget.try_reserve(50).unwrap();
        assert_eq!(lease2.size(), 50);

        drop(lease1);
        drop(lease2);
    }

    #[test]
    fn seeded_local_capacity() {
        let global = pool(1000);
        let seed = global.try_reserve(64).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 64, 500);

        assert_eq!(budget.local_capacity(), 64);
        assert_eq!(budget.available(), 64);
        assert_eq!(global.used().as_usize(), 64);

        let lease = budget.try_reserve(32).unwrap();
        assert_eq!(budget.available(), 32);
        assert_eq!(budget.in_flight(), 32);
        assert_eq!(budget.local_capacity(), 64);

        drop(lease);
        assert_eq!(budget.available(), 64);

        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn release_excess_returns_to_global() {
        let global = pool(1000);
        let seed = global.try_reserve(64).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 64, 500);

        let lease = budget.try_reserve(200).unwrap();
        drop(lease);
        assert_eq!(budget.local_capacity(), 200);
        assert_eq!(budget.available(), 200);

        budget.release_excess();
        assert_eq!(budget.local_capacity(), 64);
        assert_eq!(global.used().as_usize(), 64);

        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn release_excess_respects_in_flight() {
        let global = pool(1000);
        let seed = global.try_reserve(64).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        let lease = budget.try_reserve(200).unwrap();
        assert_eq!(budget.local_capacity(), 200);

        budget.release_excess();
        assert_eq!(budget.local_capacity(), 200);

        drop(lease);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn global_pool_exhaustion() {
        let global = pool(100);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let lease1 = budget.try_reserve(80).unwrap();
        assert!(budget.try_reserve(30).is_none());
        let lease2 = budget.try_reserve(20).unwrap();

        drop(lease1);
        drop(lease2);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn is_out_of_memory_checks_theoretical_max() {
        let global = pool(100);
        let budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        assert!(!budget.is_out_of_memory(100));
        assert!(budget.is_out_of_memory(101));
    }

    #[test]
    fn is_out_of_memory_respects_upper_bound() {
        let global = pool(10000);
        let budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 200);

        assert!(!budget.is_out_of_memory(200));
        assert!(budget.is_out_of_memory(201));
    }

    #[test]
    fn invocation_budget_creation() {
        let global = pool(10000);
        let inbound_seed = global.try_reserve(64).unwrap();
        let outbound_seed = global.try_reserve(32).unwrap();
        let ib = InvocationMemory::new(inbound_seed, 1024, outbound_seed, 512);

        assert_eq!(ib.inbound.local_capacity(), 64);
        assert_eq!(ib.inbound.available(), 64);
        assert_eq!(ib.inbound.min_reserved(), 64);
        assert_eq!(ib.outbound.local_capacity(), 32);
        assert_eq!(ib.outbound.available(), 32);
        assert_eq!(ib.outbound.min_reserved(), 32);
        assert_eq!(global.used().as_usize(), 96);

        drop(ib);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[tokio::test]
    async fn reserve_waits_for_reclaim() {
        let global = pool(100);
        let seed = global.try_reserve(100).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        let lease1 = budget.try_reserve(100).unwrap();
        assert!(budget.try_reserve(50).is_none());

        tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(lease1);
        });

        let lease2 = budget.reserve(50).await.unwrap();
        assert_eq!(lease2.size(), 50);
        assert_eq!(budget.in_flight(), 50);
        assert_eq!(budget.available(), 50);

        drop(lease2);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[tokio::test]
    async fn reserve_waits_for_global_pool() {
        let global = pool(100);
        let external = global.try_reserve(40).unwrap();

        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let lease1 = budget.try_reserve(60).unwrap();
        assert_eq!(global.available(), 0);
        assert!(budget.try_reserve(50).is_none());

        tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(lease1);
        });

        let lease2 = budget.reserve(50).await.unwrap();
        assert_eq!(lease2.size(), 50);

        drop(lease2);
        drop(external);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[tokio::test]
    async fn reserve_waits_for_global_pool_release() {
        let global = pool(100);
        let seed = global.try_reserve(50).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        let external = global.try_reserve(50).unwrap();
        assert_eq!(global.available(), 0);

        let lease = budget.reserve(50).await.unwrap();
        assert_eq!(lease.size(), 50);

        assert!(budget.try_reserve(30).is_none());

        tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(lease);
        });

        let lease2 = budget.reserve(30).await.unwrap();
        assert_eq!(lease2.size(), 30);

        drop(lease2);
        drop(external);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[tokio::test]
    async fn reserve_yields_when_exceeds_upper_bound() {
        let global = pool(10000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 200);

        assert!(budget.reserve(201).await.is_err());

        let lease = budget.reserve(200).await.unwrap();
        assert_eq!(lease.size(), 200);
        drop(lease);
    }

    #[tokio::test]
    async fn reserve_yields_when_global_pool_insufficient() {
        let global = pool(100);
        let external = global.try_reserve(90).unwrap();

        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        assert!(budget.reserve(200).await.is_err());

        let lease = budget.reserve(10).await.unwrap();
        assert_eq!(lease.size(), 10);

        drop(lease);
        drop(external);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    /// Verifies that `reserve()` wakes promptly when an *external* consumer
    /// returns memory to the global pool (not via local in-flight reclaim).
    /// Before the global notification mechanism, this path relied on a 500ms
    /// periodic timeout — this test would fail or be very slow without it.
    #[tokio::test]
    async fn reserve_wakes_on_global_pool_return() {
        let global = pool(200);
        let seed = global.try_reserve(100).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        // Exhaust local capacity with an in-flight lease.
        let local_lease = budget.try_reserve(100).unwrap();
        // Exhaust the remaining global pool with an external reservation.
        let external = global.try_reserve(100).unwrap();
        assert_eq!(global.available(), 0);

        // Budget has local_capacity=100 (all in-flight) + global_available=0.
        // is_out_of_memory(50): 50 <= 100 + 0 → feasible (in-flight will reclaim).
        // try_reserve(50): available=0 → fails. Growth needs 50 from global → fails.
        assert!(budget.try_reserve(50).is_none());
        assert!(!budget.is_out_of_memory(50));

        // Drop the external lease on another task — this returns memory to
        // the global pool (not to our local SharedState::notify). This lets
        // the budget grow from the global pool.
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(external);
        });

        // reserve() should wake up promptly via the global pool notification
        // and grow local capacity from the freed global pool memory.
        let lease = tokio::time::timeout(std::time::Duration::from_millis(100), budget.reserve(50))
            .await
            .expect("should resolve promptly via global notification, not timeout")
            .unwrap();
        assert_eq!(lease.size(), 50);

        drop(local_lease);
        drop(lease);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn zero_size_reserve() {
        let global = pool(100);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let lease = budget.try_reserve(0).unwrap();
        assert_eq!(lease.size(), 0);
        assert_eq!(budget.in_flight(), 0);
        drop(lease);
    }

    #[test]
    fn unlimited_global_pool() {
        let global = MemoryPool::unlimited();
        let seed = global.try_reserve(64).unwrap();
        let mut budget = LocalMemoryPool::new(global, seed, 64, 1024);

        assert_eq!(budget.local_capacity(), 64);

        let lease = budget.try_reserve(500).unwrap();
        assert_eq!(lease.size(), 500);
        assert_eq!(budget.in_flight(), 500);
        assert_eq!(budget.local_capacity(), 500);

        drop(lease);
        drop(budget);
    }

    #[test]
    fn orphaned_lease_returns_to_global_pool() {
        let global = pool(1000);
        let seed = global.try_reserve(100).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        let lease = budget.try_reserve(60).unwrap();
        assert_eq!(global.used().as_usize(), 100);

        drop(budget);
        assert_eq!(global.used().as_usize(), 60);

        drop(lease);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn multiple_orphaned_leases() {
        let global = pool(1000);
        let seed = global.try_reserve(200).unwrap();
        let mut budget = LocalMemoryPool::new(global.clone(), seed, 0, 500);

        let l1 = budget.try_reserve(80).unwrap();
        let l2 = budget.try_reserve(50).unwrap();
        assert_eq!(budget.in_flight(), 130);
        assert_eq!(global.used().as_usize(), 200);

        drop(budget);
        assert_eq!(global.used().as_usize(), 130);

        drop(l1);
        assert_eq!(global.used().as_usize(), 50);

        drop(l2);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn multiple_reserves_grow_incrementally() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let l1 = budget.try_reserve(100).unwrap();
        assert_eq!(budget.local_capacity(), 100);

        let l2 = budget.try_reserve(50).unwrap();
        assert_eq!(budget.local_capacity(), 150);

        drop(l1);
        assert_eq!(budget.available(), 100);

        let l3 = budget.try_reserve(80).unwrap();
        assert_eq!(budget.local_capacity(), 150);

        drop(l2);
        drop(l3);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn empty_lease_and_merge() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let empty = budget.empty_lease();
        assert_eq!(empty.size(), 0);
        assert_eq!(budget.in_flight(), 0);

        // Dropping an empty lease is a no-op.
        drop(empty);
        assert_eq!(budget.in_flight(), 0);

        // Merge empty into a real lease.
        let mut real = budget.try_reserve(100).unwrap();
        let empty = budget.empty_lease();
        real.merge(empty);
        assert_eq!(real.size(), 100);
        assert_eq!(budget.in_flight(), 100);

        // Merge a real lease into an empty lease.
        let mut empty = budget.empty_lease();
        let real2 = budget.try_reserve(50).unwrap();
        empty.merge(real2);
        assert_eq!(empty.size(), 50);
        assert_eq!(budget.in_flight(), 150);

        drop(real);
        drop(empty);
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    fn merge_leases_combines_sizes() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut l1 = budget.try_reserve(100).unwrap();
        let l2 = budget.try_reserve(50).unwrap();
        assert_eq!(budget.in_flight(), 150);

        l1.merge(l2);
        assert_eq!(l1.size(), 150);
        // in_flight unchanged — no double-counting
        assert_eq!(budget.in_flight(), 150);

        // Dropping the merged lease returns all memory
        drop(l1);
        assert_eq!(budget.in_flight(), 0);
        assert_eq!(budget.available(), 150);
    }

    #[test]
    fn split_and_drop_independently() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        assert_eq!(budget.in_flight(), 100);

        // Split off 40 bytes — total in-flight stays 100.
        let split = lease.split(40);
        assert_eq!(lease.size(), 60);
        assert_eq!(split.size(), 40);
        assert_eq!(budget.in_flight(), 100);

        // Dropping the split portion returns its share.
        drop(split);
        assert_eq!(budget.in_flight(), 60);

        // Dropping the remainder returns the rest.
        drop(lease);
        assert_eq!(budget.in_flight(), 0);
        assert_eq!(budget.available(), 100);
    }

    #[test]
    fn split_to_zero() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        let zero = lease.split(0);
        assert_eq!(lease.size(), 100);
        assert_eq!(zero.size(), 0);
        // Dropping a zero-size split is a no-op.
        drop(zero);
        assert_eq!(budget.in_flight(), 100);
        drop(lease);
    }

    #[test]
    fn split_entire_lease() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        let all = lease.split(100);
        assert_eq!(lease.size(), 0);
        assert_eq!(all.size(), 100);
        assert_eq!(budget.in_flight(), 100);

        // Original is now zero-sized — dropping it is a no-op.
        drop(lease);
        assert_eq!(budget.in_flight(), 100);

        drop(all);
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    #[should_panic(expected = "cannot split more than lease size")]
    fn split_panics_on_overflow() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(50).unwrap();
        let _ = lease.split(51);
    }

    #[test]
    fn shrink_returns_to_budget() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        assert_eq!(budget.in_flight(), 100);

        // Shrink by 30 — returns 30 to the budget immediately.
        lease.shrink(30);
        assert_eq!(lease.size(), 70);
        assert_eq!(budget.in_flight(), 70);
        assert_eq!(budget.available(), 30);

        drop(lease);
        assert_eq!(budget.in_flight(), 0);
        assert_eq!(budget.available(), 100);
    }

    #[test]
    fn shrink_clamped_to_size() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(50).unwrap();
        // Shrink by more than the lease size — clamped to 50.
        lease.shrink(200);
        assert_eq!(lease.size(), 0);
        assert_eq!(budget.in_flight(), 0);

        // Dropping a zero-size lease is a no-op.
        drop(lease);
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    fn shrink_zero_is_noop() {
        let global = pool(1000);
        let mut budget = LocalMemoryPool::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        lease.shrink(0);
        assert_eq!(lease.size(), 100);
        assert_eq!(budget.in_flight(), 100);
        drop(lease);
    }
}
