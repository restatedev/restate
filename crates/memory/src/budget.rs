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
//! Each active invocation gets an [`InvocationBudget`] with two [`DirectionalBudget`]s:
//! one for the outbound path (RocksDB → invoker → hyper → deployment) and one for the
//! inbound path (deployment → hyper → decoder → invoker → Bifrost).
//!
//! # Memory model
//!
//! Each [`DirectionalBudget`] maintains a *local capacity* backed by a [`MemoryLease`]
//! from the global invoker pool. Memory is reserved from local capacity first; when
//! local capacity is insufficient, additional memory is reserved from the global pool
//! and merged into the local lease. When outstanding [`BudgetLease`]s are dropped,
//! in-flight decreases and available capacity is restored. Excess local capacity above
//! `min_reserved` can be released back to the global pool via
//! [`release_excess`](DirectionalBudget::release_excess).
//!
//! # Concurrency
//!
//! A `DirectionalBudget` is designed for **single-task access**. Both inbound and
//! outbound budgets are owned and operated by the protocol runner task alone (via a
//! `tokio::select!` loop). The [`BudgetLease`] type is `Send` so it can be stored in
//! types that cross `.await` points, but all reserve/release operations happen within
//! the same task.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use crate::{MemoryLease, MemoryPool};

/// Returned by [`DirectionalBudget::reserve`] when the requested memory cannot
/// be satisfied even in the best case (all in-flight reclaimed + entire global
/// pool available).
#[derive(Debug)]
pub struct BudgetExhausted {
    /// The number of bytes that were requested but could not be allocated.
    pub needed: usize,
}

/// Per-invocation budget grouping inbound and outbound directional budgets.
pub struct InvocationBudget {
    pub inbound: DirectionalBudget,
    pub outbound: DirectionalBudget,
}

impl fmt::Debug for InvocationBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvocationBudget").finish_non_exhaustive()
    }
}

impl InvocationBudget {
    /// Creates a new invocation budget.
    ///
    /// - `seed`: initial memory lease from the global pool, used as the inbound budget's
    ///   starting local capacity. Must be at least `inbound_min_reserved` bytes.
    /// - `inbound_min_reserved`: minimum local capacity for the inbound budget that
    ///   [`release_excess`](DirectionalBudget::release_excess) will never shrink below.
    /// - `upper_bound`: maximum local capacity per direction.
    pub fn new(seed: MemoryLease, inbound_min_reserved: usize, upper_bound: usize) -> Self {
        debug_assert!(
            seed.size().as_usize() >= inbound_min_reserved,
            "seed ({}) must be >= inbound_min_reserved ({inbound_min_reserved})",
            seed.size(),
        );

        let pool = seed.budget().clone();
        let outbound_seed = pool.empty_lease();

        Self {
            inbound: DirectionalBudget::new(pool.clone(), seed, inbound_min_reserved, upper_bound),
            outbound: DirectionalBudget::new(pool, outbound_seed, 0, upper_bound),
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
/// When a [`BudgetLease`] is dropped, in-flight decreases and available increases.
///
/// # Drop behavior
///
/// When a `DirectionalBudget` is dropped, it atomically snapshots the in-flight
/// count (and sets the dead bit) via a single [`swap`](AtomicUsize::swap). The
/// internal `MemoryLease` is then shrunk by the in-flight amount so that only
/// `(capacity - in_flight)` bytes are returned to the global pool. The withheld
/// bytes are returned individually by orphaned [`BudgetLease`] drops, which detect
/// the dead bit and call [`MemoryPool::return_memory`] directly.
pub struct DirectionalBudget {
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
/// `Arc`-wrapped so that [`BudgetLease`] drops can decrement `in_flight` and notify
/// waiters.
///
/// # Dead-bit protocol
///
/// The high bit of `in_flight` serves as a "budget dead" flag. When
/// [`DirectionalBudget::drop`] runs, it atomically sets the dead bit via
/// [`swap`](AtomicUsize::swap), obtaining the exact in-flight count at that instant.
/// Subsequent [`BudgetLease`] drops detect the dead bit and return their bytes
/// directly to the global pool instead of just decrementing in-flight.
///
/// This single-atomic approach avoids the TOCTOU race that would exist with
/// separate `alive` and `in_flight` atomics.
struct SharedState {
    pool: MemoryPool,
    /// Bytes currently held by outstanding leases, with [`DEAD_BIT`] set once the
    /// owning [`DirectionalBudget`] has been dropped.
    in_flight: AtomicUsize,
    /// Wakes waiters when in-flight decreases.
    notify: Notify,
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
                // Budget is alive — we decremented in-flight. Wake reserve waiters.
                self.notify.notify_waiters();
            }
            Err(_) => {
                // Budget is dead — return directly to the global pool.
                self.pool.return_memory(amount);
            }
        }
    }
}

impl DirectionalBudget {
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
                notify: Notify::new(),
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
    pub fn try_reserve(&mut self, size: usize) -> Option<BudgetLease> {
        if size == 0 {
            return Some(BudgetLease {
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
        Some(BudgetLease {
            shared: Arc::clone(&self.shared),
            size,
        })
    }

    /// Reserves `size` bytes, waiting if necessary for in-flight memory to be
    /// reclaimed and/or global pool capacity to become available.
    ///
    /// Returns [`Err(BudgetExhausted)`] if the request is infeasible — meaning
    /// even after reclaiming all in-flight memory and tapping the entire global
    /// pool, the request cannot be satisfied. This check is performed on every
    /// iteration, so the caller does not need to call a separate
    /// `budget_exhausted` check.
    ///
    /// On each iteration the method:
    /// 1. Attempts a non-blocking [`try_reserve`](Self::try_reserve).
    /// 2. If that fails, checks feasibility and returns `Err(BudgetExhausted)`
    ///    if the request is impossible.
    /// 3. Otherwise, waits for an in-flight lease to be dropped (which increases
    ///    available capacity) or a periodic timeout, then retries.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel-safe: dropping the future before completion has no
    /// effect on the budget (no memory is reserved until the method returns).
    pub async fn reserve(&mut self, size: usize) -> Result<BudgetLease, BudgetExhausted> {
        loop {
            if let Some(lease) = self.try_reserve(size) {
                return Ok(lease);
            }
            if self.budget_exhausted(size) {
                return Err(BudgetExhausted { needed: size });
            }

            // try_reserve failed but the request is feasible. Wait for either a
            // local in-flight lease to be dropped (which frees available capacity)
            // or a periodic timeout to re-check feasibility.
            //
            // The timeout is needed because the global pool's available capacity
            // can decrease (another invocation reserving memory) without any
            // notification to us. On timeout we loop back, re-check budget_exhausted
            // with fresh data, and either yield or wait again.
            //
            // TODO: This timeout could be eliminated if MemoryPool notified
            // waiters on reservations (not just returns). That would let us
            // react immediately when global pool capacity changes in either
            // direction.
            let shared = Arc::clone(&self.shared);
            let local_reclaim = shared.notify.notified();
            tokio::select! {
                () = local_reclaim => {}
                () = tokio::time::sleep(Self::FEASIBILITY_RECHECK_INTERVAL) => {}
            }
        }
    }

    /// How often to re-check feasibility while waiting in [`reserve`].
    const FEASIBILITY_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

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
    pub fn budget_exhausted(&self, needed: usize) -> bool {
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
    /// can be [`merged`](BudgetLease::merge) with them. Dropping an empty lease
    /// is a no-op.
    pub fn empty_lease(&self) -> BudgetLease {
        BudgetLease {
            shared: Arc::clone(&self.shared),
            size: 0,
        }
    }
}

impl Drop for DirectionalBudget {
    fn drop(&mut self) {
        // Atomically read the exact in-flight count AND set the dead bit in one
        // operation. After this, any BudgetLease::drop will see the dead bit and
        // return its bytes directly to the global pool.
        let in_flight = self.shared.in_flight.swap(DEAD_BIT, Ordering::Relaxed);
        debug_assert!(in_flight & DEAD_BIT == 0, "DirectionalBudget dropped twice");

        if in_flight > 0 {
            // Split off the in-flight portion and forget it so those bytes are NOT
            // returned to the global pool now. The withheld bytes will be returned
            // individually by orphaned BudgetLease drops (which detect the dead bit).
            // Note: we cannot use `shrink()` here because that returns bytes to the
            // pool immediately — we need to withhold them.
            let withheld = self.local_capacity.split(in_flight);
            std::mem::forget(withheld);
        }

        // self.local_capacity drops here, returning (capacity - in_flight) to the pool.
    }
}

/// RAII memory lease from a [`DirectionalBudget`].
///
/// When dropped, decrements in-flight and wakes anyone waiting in
/// [`DirectionalBudget::reserve`].
#[must_use]
pub struct BudgetLease {
    shared: Arc<SharedState>,
    size: usize,
}

impl BudgetLease {
    /// Returns the size of this lease in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Merges `other` into `self`, combining their sizes.
    ///
    /// Both leases must originate from the same [`DirectionalBudget`] (debug-asserted).
    /// After the merge, `other` is consumed without decrementing in-flight — the
    /// combined in-flight is now tracked by `self` alone.
    pub fn merge(&mut self, other: BudgetLease) {
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
    pub fn split(&mut self, amount: usize) -> BudgetLease {
        assert!(amount <= self.size, "cannot split more than lease size");
        self.size -= amount;
        BudgetLease {
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

impl Drop for BudgetLease {
    fn drop(&mut self) {
        if self.size > 0 {
            self.shared.return_memory(self.size);
        }
    }
}

impl std::fmt::Debug for BudgetLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetLease")
            .field("size", &self.size)
            .finish()
    }
}

// Ensure Send + Sync
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DirectionalBudget>();
    assert_send_sync::<InvocationBudget>();
    assert_send_sync::<BudgetLease>();
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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);
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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 200);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 64, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 64, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        let lease1 = budget.try_reserve(80).unwrap();
        assert!(budget.try_reserve(30).is_none());
        let lease2 = budget.try_reserve(20).unwrap();

        drop(lease1);
        drop(lease2);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn budget_exhausted_checks_theoretical_max() {
        let global = pool(100);
        let budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        assert!(!budget.budget_exhausted(100));
        assert!(budget.budget_exhausted(101));
    }

    #[test]
    fn budget_exhausted_respects_upper_bound() {
        let global = pool(10000);
        let budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 200);

        assert!(!budget.budget_exhausted(200));
        assert!(budget.budget_exhausted(201));
    }

    #[test]
    fn invocation_budget_creation() {
        let global = pool(10000);
        let seed = global.try_reserve(64).unwrap();
        let ib = InvocationBudget::new(seed, 64, 1024);

        assert_eq!(ib.inbound.local_capacity(), 64);
        assert_eq!(ib.inbound.available(), 64);
        assert_eq!(ib.outbound.local_capacity(), 0);
        assert_eq!(ib.outbound.available(), 0);
        assert_eq!(global.used().as_usize(), 64);

        drop(ib);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[tokio::test]
    async fn reserve_waits_for_reclaim() {
        let global = pool(100);
        let seed = global.try_reserve(100).unwrap();
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);

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

        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 200);

        assert!(budget.reserve(201).await.is_err());

        let lease = budget.reserve(200).await.unwrap();
        assert_eq!(lease.size(), 200);
        drop(lease);
    }

    #[tokio::test]
    async fn reserve_yields_when_global_pool_insufficient() {
        let global = pool(100);
        let external = global.try_reserve(90).unwrap();

        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        assert!(budget.reserve(200).await.is_err());

        let lease = budget.reserve(10).await.unwrap();
        assert_eq!(lease.size(), 10);

        drop(lease);
        drop(external);
        drop(budget);
        assert_eq!(global.used().as_usize(), 0);
    }

    #[test]
    fn zero_size_reserve() {
        let global = pool(100);
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        let lease = budget.try_reserve(0).unwrap();
        assert_eq!(lease.size(), 0);
        assert_eq!(budget.in_flight(), 0);
        drop(lease);
    }

    #[test]
    fn unlimited_global_pool() {
        let global = MemoryPool::unlimited();
        let seed = global.try_reserve(64).unwrap();
        let mut budget = DirectionalBudget::new(global, seed, 64, 1024);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), seed, 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(50).unwrap();
        let _ = lease.split(51);
    }

    #[test]
    fn shrink_returns_to_budget() {
        let global = pool(1000);
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

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
        let mut budget = DirectionalBudget::new(global.clone(), global.empty_lease(), 0, 500);

        let mut lease = budget.try_reserve(100).unwrap();
        lease.shrink(0);
        assert_eq!(lease.size(), 100);
        assert_eq!(budget.in_flight(), 100);
        drop(lease);
    }
}
