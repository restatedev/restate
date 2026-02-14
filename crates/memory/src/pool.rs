// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Memory budget for tracking and bounding memory usage across async tasks.
//!
//! # Features
//!
//! - **Live capacity updates** via [`MemoryBudget::set_capacity`]
//! - **Minimum capacity guarantee**: Capacity is clamped to a configured minimum,
//!   ensuring that at least one lease of that size can always proceed
//! - **Zero-overhead unlimited mode**: Unlimited budgets skip all tracking

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use restate_serde_util::{ByteCount, NonZeroByteCount};

/// A memory budget that tracks and limits memory usage.
///
/// Cheaply cloneable (uses `Arc` internally). Capacity can be updated at
/// runtime via [`set_capacity`](Self::set_capacity).
#[derive(Debug, Clone)]
pub struct MemoryPool {
    inner: Option<Arc<BoundedBudgetInner>>,
}

#[derive(Debug)]
struct BoundedBudgetInner {
    capacity: AtomicUsize,
    used: AtomicUsize,
    notify: Notify,
}

impl MemoryPool {
    /// Creates an unlimited budget (zero overhead, no tracking).
    #[inline]
    pub const fn unlimited() -> Self {
        Self { inner: None }
    }

    /// Creates a bounded budget with the given capacity in bytes.
    pub fn with_capacity(capacity: NonZeroByteCount) -> Self {
        Self {
            inner: Some(Arc::new(BoundedBudgetInner {
                capacity: AtomicUsize::new(capacity.as_usize()),
                used: AtomicUsize::new(0),
                notify: Notify::new(),
            })),
        }
    }

    #[inline]
    pub fn is_unlimited(&self) -> bool {
        self.inner.is_none()
    }

    #[inline]
    pub fn capacity(&self) -> ByteCount {
        match &self.inner {
            Some(inner) => ByteCount::from(inner.capacity.load(Ordering::Relaxed)),
            None => ByteCount::ZERO,
        }
    }

    /// Updates capacity at runtime (clamped to min_capacity). Wakes waiters to re-check.
    #[inline]
    pub fn set_capacity(&self, capacity: impl Into<NonZeroByteCount>) {
        if let Some(inner) = &self.inner {
            inner
                .capacity
                .store(capacity.into().as_usize(), Ordering::Relaxed);
            inner.notify.notify_waiters();
        }
    }

    #[inline]
    pub fn used(&self) -> ByteCount {
        match &self.inner {
            Some(inner) => ByteCount::from(inner.used.load(Ordering::Relaxed)),
            None => ByteCount::ZERO,
        }
    }

    /// Tries to reserve `size` bytes without waiting.
    ///
    /// Returns `None` if insufficient capacity.
    #[inline]
    pub fn try_reserve(&self, size: usize) -> Option<MemoryLease> {
        if size == 0 {
            return Some(MemoryLease {
                budget: self.clone(),
                size,
            });
        }

        match self.inner {
            Some(ref inner) => {
                inner
                    .used
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                        let capacity = inner.capacity.load(Ordering::Relaxed);
                        let new_used = used.checked_add(size)?;
                        (new_used <= capacity).then_some(new_used)
                    })
                    .ok()?;
                Some(MemoryLease {
                    budget: self.clone(),
                    size,
                })
            }
            None => Some(MemoryLease {
                budget: self.clone(),
                size,
            }),
        }
    }

    /// Reserves `size` bytes, waiting if necessary.
    pub async fn reserve(&self, size: usize) -> MemoryLease {
        if size == 0 {
            return MemoryLease {
                budget: self.clone(),
                size,
            };
        }

        match &self.inner {
            Some(inner) => {
                // Use enable() before try_reserve to prevent lost wakeups
                let mut future = std::pin::pin!(inner.notify.notified());

                loop {
                    future.as_mut().enable();
                    if let Some(lease) = self.try_reserve(size) {
                        return lease;
                    }
                    future.as_mut().await;
                    future.set(inner.notify.notified());
                }
            }
            None => MemoryLease {
                budget: self.clone(),
                size,
            },
        }
    }

    #[inline]
    pub fn empty_lease(&self) -> MemoryLease {
        MemoryLease {
            budget: self.clone(),
            size: 0,
        }
    }

    #[inline]
    fn return_memory(&self, amount: usize) {
        if amount == 0 {
            return;
        }
        if let Some(inner) = &self.inner {
            inner.used.fetch_sub(amount, Ordering::Relaxed);
            inner.notify.notify_waiters();
        }
    }

    #[inline]
    fn try_acquire(&self, amount: usize) -> bool {
        match &self.inner {
            Some(inner) => inner
                .used
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                    let capacity = inner.capacity.load(Ordering::Relaxed);
                    let new_used = used.checked_add(amount)?;
                    (new_used <= capacity).then_some(new_used)
                })
                .is_ok(),
            None => true,
        }
    }
}

/// A lease of memory from a [`MemoryBudget`].
///
/// Memory is returned to the budget on drop. Leases can be split, merged,
/// grown, and shrunk.
#[must_use]
#[clippy::has_significant_drop]
pub struct MemoryLease {
    budget: MemoryPool,
    size: usize,
}

impl std::fmt::Debug for MemoryLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLease")
            .field("size", &self.size)
            .finish()
    }
}

impl MemoryLease {
    /// Creates an unlinked lease that doesn't track memory.
    ///
    /// This is backed by an unlimited budget, so it has zero overhead and
    /// doesn't apply any memory pressure. Use this for:
    /// - Tests that don't need memory tracking
    /// - Transition code during migration to memory-bounded channels
    /// - Messages where memory tracking is not applicable
    #[inline]
    pub fn unlinked() -> Self {
        Self {
            budget: MemoryPool::unlimited(),
            size: 0,
        }
    }

    #[inline]
    pub fn size(&self) -> ByteCount {
        self.size.into()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn budget(&self) -> &MemoryPool {
        &self.budget
    }

    /// Shrinks by `amount` bytes (clamped to current size).
    #[inline]
    pub fn shrink(&mut self, amount: usize) {
        let shrink_by = amount.min(self.size);
        if shrink_by > 0 {
            self.size -= shrink_by;
            self.budget.return_memory(shrink_by);
        }
    }

    /// Releases all memory, returning bytes released.
    #[inline]
    pub fn release(&mut self) -> usize {
        let size = self.size;
        if size > 0 {
            self.size = 0;
            self.budget.return_memory(size);
        }
        size
    }

    /// Tries to grow by `additional` bytes. Returns false if insufficient capacity.
    #[inline]
    pub fn try_grow(&mut self, additional: usize) -> bool {
        if additional == 0 {
            return true;
        }
        if self.budget.try_acquire(additional) {
            self.size += additional;
            true
        } else {
            false
        }
    }

    /// Grows by `additional` bytes. Panics if insufficient capacity.
    #[inline]
    pub fn grow(&mut self, additional: usize) {
        assert!(
            additional == 0 || self.budget.try_acquire(additional),
            "insufficient capacity to grow lease"
        );
        self.size += additional;
    }

    /// Splits off `amount` bytes into a new lease. Panics if `amount > size`.
    #[inline]
    pub fn split(&mut self, amount: usize) -> MemoryLease {
        assert!(amount <= self.size, "cannot split more than lease size");
        self.size -= amount;
        MemoryLease {
            budget: self.budget.clone(),
            size: amount,
        }
    }

    /// Merges `other` into self. Debug-asserts same budget.
    #[inline]
    pub fn merge(&mut self, mut other: MemoryLease) {
        if self.budget.is_unlimited() {
            self.size = other.size;
            other.size = 0;
            std::mem::swap(&mut self.budget, &mut other.budget);
        } else if other.budget.is_unlimited() {
            // ignore it.
        } else {
            debug_assert!(
                std::ptr::eq(
                    self.budget
                        .inner
                        .as_ref()
                        .map(Arc::as_ptr)
                        .unwrap_or(std::ptr::null()),
                    other
                        .budget
                        .inner
                        .as_ref()
                        .map(Arc::as_ptr)
                        .unwrap_or(std::ptr::null()),
                ),
                "cannot merge leases from different budgets"
            );
            self.size = self.size.saturating_add(other.size);
            // Necessary to ensure it doesn't return its memory to the budget on drop.
            other.size = 0;
        }
    }

    /// Takes all bytes, leaving self empty.
    #[inline]
    pub fn take(&mut self) -> MemoryLease {
        let size = self.size;
        self.size = 0;
        MemoryLease {
            budget: self.budget.clone(),
            size,
        }
    }

    #[inline]
    pub fn new_empty(&self) -> MemoryLease {
        MemoryLease {
            budget: self.budget.clone(),
            size: 0,
        }
    }
}

impl Drop for MemoryLease {
    #[inline]
    fn drop(&mut self) {
        if self.size > 0 {
            self.budget.return_memory(self.size);
        }
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MemoryPool>();
    assert_send_sync::<MemoryLease>();
};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn budget(capacity: usize) -> MemoryPool {
        MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(capacity).unwrap()))
    }

    fn bytes(n: usize) -> ByteCount {
        ByteCount::from(n)
    }

    fn nz_bytes(n: usize) -> NonZeroByteCount {
        NonZeroByteCount::new(NonZeroUsize::new(n).unwrap())
    }

    #[test]
    fn unlimited_budget() {
        let budget = MemoryPool::unlimited();
        assert!(budget.is_unlimited());
        assert_eq!(budget.capacity(), ByteCount::ZERO);
        assert_eq!(budget.used(), ByteCount::ZERO);

        let r1 = budget.try_reserve(1000).unwrap();
        assert_eq!(r1.size(), bytes(1000));
        assert_eq!(budget.used(), ByteCount::ZERO); // unlimited budgets don't track
    }

    #[test]
    fn bounded_budget_reserve_and_release() {
        let budget = budget(100);
        assert!(!budget.is_unlimited());
        assert_eq!(budget.capacity(), bytes(100));

        let r1 = budget.try_reserve(30).unwrap();
        let r2 = budget.try_reserve(50).unwrap();
        assert_eq!(budget.used(), bytes(80));

        // Can't exceed capacity
        assert!(budget.try_reserve(30).is_none());

        drop(r1);
        assert_eq!(budget.used(), bytes(50));

        drop(r2);
        assert_eq!(budget.used(), bytes(0));
    }

    #[test]
    fn lease_split_merge_take() {
        let budget = budget(100);

        let mut r1 = budget.try_reserve(60).unwrap();

        // Split
        let r2 = r1.split(20);
        assert_eq!(r1.size(), bytes(40));
        assert_eq!(r2.size(), bytes(20));
        assert_eq!(budget.used(), bytes(60));

        // Merge
        let mut r3 = budget.try_reserve(10).unwrap();
        r3.merge(r2);
        assert_eq!(r3.size(), bytes(30));
        assert_eq!(budget.used(), bytes(70));

        // Take
        let r4 = r1.take();
        assert_eq!(r1.size(), bytes(0));
        assert_eq!(r4.size(), bytes(40));
        assert_eq!(budget.used(), bytes(70));

        drop(r3);
        drop(r4);
        assert_eq!(budget.used(), bytes(0));
    }

    #[test]
    fn lease_grow_shrink_release() {
        let budget = budget(100);
        let mut r1 = budget.try_reserve(30).unwrap();

        // Grow
        assert!(r1.try_grow(20));
        assert_eq!(r1.size(), bytes(50));
        assert!(!r1.try_grow(60)); // exceeds capacity
        assert_eq!(r1.size(), bytes(50));

        // Shrink
        r1.shrink(30);
        assert_eq!(r1.size(), bytes(20));
        r1.shrink(100); // clamps to size
        assert_eq!(r1.size(), bytes(0));
        assert_eq!(budget.used(), bytes(0));

        // Release
        assert!(r1.try_grow(50));
        assert_eq!(r1.release(), 50);
        assert_eq!(r1.size(), bytes(0));
        assert_eq!(budget.used(), bytes(0));
    }

    #[test]
    fn empty_lease_and_new_empty() {
        let budget = budget(100);

        let mut r1 = budget.empty_lease();
        assert!(r1.is_empty());
        assert!(r1.try_grow(50));
        assert_eq!(budget.used(), bytes(50));

        let mut r2 = r1.new_empty();
        assert!(r2.try_grow(20));
        assert_eq!(budget.used(), bytes(70));
    }

    #[test]
    #[should_panic(expected = "cannot split")]
    fn split_too_much_panics() {
        let budget = budget(100);
        let mut r1 = budget.try_reserve(50).unwrap();
        let _ = r1.split(60);
    }

    #[test]
    fn budget_clone_shares_state() {
        let budget1 = budget(100);
        let budget2 = budget1.clone();

        let _r = budget1.try_reserve(60).unwrap();
        assert_eq!(budget2.used(), bytes(60));
    }

    #[test]
    fn set_capacity() {
        let budget = budget(100);
        let r1 = budget.try_reserve(60).unwrap();

        budget.set_capacity(nz_bytes(200));
        // capacity=200, used=60, so 140 free
        let r2 = budget.try_reserve(100).unwrap();

        // Decrease below usage - existing leases valid, new ones blocked
        budget.set_capacity(nz_bytes(50));
        assert!(budget.try_reserve(1).is_none());
        assert_eq!(budget.used(), bytes(160));

        drop(r1);
        drop(r2);
        assert_eq!(budget.used(), bytes(0));
        assert_eq!(budget.capacity(), bytes(50));
    }

    #[tokio::test]
    async fn async_reserve_waits() {
        let budget = budget(100);
        let r1 = budget.reserve(100).await;

        let budget_clone = budget.clone();
        let handle = tokio::spawn(async move { budget_clone.reserve(50).await });

        tokio::task::yield_now().await;
        drop(r1);

        let r2 = handle.await.unwrap();
        assert_eq!(r2.size(), bytes(50));
    }

    #[test]
    fn cannot_reserve_beyond_capacity_even_when_empty() {
        let budget = budget(100);
        // Over-capacity is rejected even when budget is empty
        assert!(budget.try_reserve(150).is_none());
        assert_eq!(budget.used(), bytes(0));
    }

    #[tokio::test]
    async fn set_capacity_wakes_waiters() {
        let budget = budget(100);
        let r1 = budget.try_reserve(100).unwrap();

        let budget_clone = budget.clone();
        let handle = tokio::spawn(async move { budget_clone.reserve(50).await });

        tokio::task::yield_now().await;
        budget.set_capacity(nz_bytes(150));

        let r2 = handle.await.unwrap();
        assert_eq!(r2.size(), bytes(50));
        assert_eq!(budget.used(), bytes(150));

        drop(r1);
        drop(r2);
    }

    #[tokio::test]
    async fn multiple_waiters() {
        let budget = budget(100);
        let r1 = budget.try_reserve(100).unwrap();

        let mut handles = Vec::new();
        for size in [30, 20, 10] {
            let b = budget.clone();
            handles.push(tokio::spawn(async move { b.reserve(size).await }));
        }

        // Give spawned tasks time to register as waiters
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(r1);

        let mut leases = Vec::new();
        for h in handles {
            leases.push(h.await.unwrap());
        }

        let total: usize = leases.iter().map(|r| r.size().as_usize()).sum();
        assert_eq!(total, 60);
        assert_eq!(budget.used(), bytes(60));
    }

    #[tokio::test]
    async fn stress_concurrent_acquire_release() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let budget = budget(100);
        let count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let budget = budget.clone();
                let count = count.clone();
                tokio::spawn(async move {
                    for _ in 0..25 {
                        let r = budget.reserve(20).await;
                        count.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                        drop(r);
                    }
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(count.load(Ordering::Relaxed), 100);
        assert_eq!(budget.used(), bytes(0));
    }
}
