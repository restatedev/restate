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

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;
use tracing::{info, trace};
use triomphe::Arc;

use restate_serde_util::{ByteCount, NonZeroByteCount};

/// A memory budget that tracks and limits memory usage.
///
/// Cheaply cloneable (uses `Arc` internally). Capacity can be updated at
/// runtime via [`set_capacity`](Self::set_capacity).
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    name: &'static str,
    inner: Option<Arc<BoundedBudgetInner>>,
}

#[derive(Debug)]
struct BoundedBudgetInner {
    min_capacity: NonZeroUsize,
    capacity: AtomicUsize,
    used: AtomicUsize,
    notify: Notify,
}

impl MemoryBudget {
    /// Creates an unlimited budget (zero overhead, no tracking).
    #[inline]
    pub const fn unlimited(name: &'static str) -> Self {
        Self { name, inner: None }
    }

    /// Creates a bounded budget from a [`NonZeroByteCount`] (for config deserialization).
    ///
    /// Capacity is clamped to at least `min_capacity`.
    pub fn bounded(
        name: &'static str,
        capacity: NonZeroByteCount,
        min_capacity: NonZeroUsize,
    ) -> Self {
        Self::with_capacity(
            name,
            NonZeroUsize::new(capacity.as_usize()).unwrap(),
            min_capacity,
        )
    }

    /// Creates a bounded budget with the given capacity in bytes.
    ///
    /// Capacity is clamped to at least `min_capacity`.
    pub fn with_capacity(
        name: &'static str,
        capacity: NonZeroUsize,
        min_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            name,
            inner: Some(Arc::new(BoundedBudgetInner {
                capacity: AtomicUsize::new(capacity.get().max(min_capacity.get())),
                min_capacity,
                used: AtomicUsize::new(0),
                notify: Notify::new(),
            })),
        }
    }

    /// Creates a memory budget with the given capacity in bytes.
    ///
    /// Creates an unlimited budget if `capacity` is zero. Otherwise, capacity is
    /// clamped to at least `min_capacity`.
    pub fn new(name: &'static str, capacity: usize, min_capacity: NonZeroUsize) -> Self {
        NonZeroUsize::new(capacity).map_or_else(
            || Self::unlimited(name),
            |cap| Self::with_capacity(name, cap, min_capacity),
        )
    }

    /// Returns the name of this budget.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[inline]
    pub fn is_unlimited(&self) -> bool {
        self.inner.is_none()
    }

    #[inline]
    pub fn capacity(&self) -> Option<usize> {
        self.inner
            .as_ref()
            .map(|inner| inner.capacity.load(Ordering::Relaxed))
    }

    /// Returns the minimum capacity for bounded budgets, `None` for unlimited.
    #[inline]
    pub fn min_capacity(&self) -> Option<NonZeroUsize> {
        self.inner.as_ref().map(|inner| inner.min_capacity)
    }

    /// Updates capacity at runtime (clamped to min_capacity). Wakes waiters to re-check.
    #[inline]
    pub fn set_capacity(&self, capacity: NonZeroUsize) {
        if let Some(inner) = &self.inner {
            let clamped = capacity.get().max(inner.min_capacity.get());
            inner.capacity.store(clamped, Ordering::Relaxed);
            inner.notify.notify_waiters();
        }
    }

    #[inline]
    pub fn available(&self) -> usize {
        match &self.inner {
            Some(inner) => {
                let capacity = inner.capacity.load(Ordering::Relaxed);
                let used = inner.used.load(Ordering::Relaxed);
                capacity.saturating_sub(used)
            }
            None => usize::MAX,
        }
    }

    #[inline]
    pub fn reserved(&self) -> usize {
        match &self.inner {
            Some(inner) => inner.used.load(Ordering::Relaxed),
            None => 0,
        }
    }

    /// Tries to reserve `size` bytes without waiting.
    ///
    /// Returns `None` if insufficient capacity.
    #[inline]
    pub fn try_reserve(&self, size: usize) -> Option<MemoryLease> {
        match &self.inner {
            Some(inner) => {
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
        match &self.inner {
            Some(inner) => {
                // Use enable() before try_reserve to prevent lost wakeups
                let mut future = std::pin::pin!(inner.notify.notified());

                loop {
                    future.as_mut().enable();
                    if let Some(lease) = self.try_reserve(size) {
                        trace!(
                            "Memory budget is available, reserving {}",
                            ByteCount::from(size)
                        );
                        return lease;
                    }
                    info!(
                        "Waiting for memory budget to become available, requesting {}",
                        ByteCount::from(size)
                    );
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

impl Default for MemoryBudget {
    fn default() -> Self {
        Self::unlimited("")
    }
}

/// A lease of memory from a [`MemoryBudget`].
///
/// Memory is returned to the budget on drop. Leases can be split, merged,
/// grown, and shrunk.
#[must_use]
pub struct MemoryLease {
    budget: MemoryBudget,
    size: usize,
}

impl Default for MemoryLease {
    /// Creates an unlinked (zero-size) lease from an unlimited budget.
    ///
    /// This is useful for tests or transition code where memory tracking is not needed.
    fn default() -> Self {
        Self::unlinked()
    }
}

impl std::fmt::Debug for MemoryLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLease")
            .field("size", &self.size)
            .field("budget_capacity", &self.budget.capacity())
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
            budget: MemoryBudget::unlimited(""),
            size: 0,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn budget(&self) -> &MemoryBudget {
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
    assert_send_sync::<MemoryBudget>();
    assert_send_sync::<MemoryLease>();
};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    const MIN: NonZeroUsize = NonZeroUsize::new(1).unwrap();

    fn budget(capacity: usize) -> MemoryBudget {
        MemoryBudget::with_capacity("test", NonZeroUsize::new(capacity).unwrap(), MIN)
    }

    #[test]
    fn unlimited_budget() {
        let budget = MemoryBudget::unlimited("unlimited-test");
        assert!(budget.is_unlimited());
        assert_eq!(budget.name(), "unlimited-test");
        assert_eq!(budget.capacity(), None);
        assert_eq!(budget.available(), usize::MAX);
        assert_eq!(budget.reserved(), 0);

        let r1 = budget.try_reserve(1000).unwrap();
        assert_eq!(r1.size(), 1000);
        assert_eq!(budget.reserved(), 0); // unlimited budgets don't track
    }

    #[test]
    fn bounded_budget_reserve_and_release() {
        let budget = budget(100);
        assert!(!budget.is_unlimited());
        assert_eq!(budget.name(), "test");
        assert_eq!(budget.capacity(), Some(100));

        let r1 = budget.try_reserve(30).unwrap();
        let r2 = budget.try_reserve(50).unwrap();
        assert_eq!(budget.reserved(), 80);
        assert_eq!(budget.available(), 20);

        // Can't exceed capacity
        assert!(budget.try_reserve(30).is_none());

        drop(r1);
        assert_eq!(budget.reserved(), 50);

        drop(r2);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn lease_split_merge_take() {
        let budget = budget(100);

        let mut r1 = budget.try_reserve(60).unwrap();

        // Split
        let r2 = r1.split(20);
        assert_eq!(r1.size(), 40);
        assert_eq!(r2.size(), 20);
        assert_eq!(budget.reserved(), 60);

        // Merge
        let mut r3 = budget.try_reserve(10).unwrap();
        r3.merge(r2);
        assert_eq!(r3.size(), 30);
        assert_eq!(budget.reserved(), 70);

        // Take
        let r4 = r1.take();
        assert_eq!(r1.size(), 0);
        assert_eq!(r4.size(), 40);
        assert_eq!(budget.reserved(), 70);

        drop(r3);
        drop(r4);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn lease_grow_shrink_release() {
        let budget = budget(100);
        let mut r1 = budget.try_reserve(30).unwrap();

        // Grow
        assert!(r1.try_grow(20));
        assert_eq!(r1.size(), 50);
        assert!(!r1.try_grow(60)); // exceeds capacity
        assert_eq!(r1.size(), 50);

        // Shrink
        r1.shrink(30);
        assert_eq!(r1.size(), 20);
        r1.shrink(100); // clamps to size
        assert_eq!(r1.size(), 0);
        assert_eq!(budget.reserved(), 0);

        // Release
        assert!(r1.try_grow(50));
        assert_eq!(r1.release(), 50);
        assert_eq!(r1.size(), 0);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn empty_lease_and_new_empty() {
        let budget = budget(100);

        let mut r1 = budget.empty_lease();
        assert!(r1.is_empty());
        assert!(r1.try_grow(50));
        assert_eq!(budget.reserved(), 50);

        let mut r2 = r1.new_empty();
        assert!(r2.try_grow(20));
        assert_eq!(budget.reserved(), 70);
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
        assert_eq!(budget2.reserved(), 60);
    }

    #[test]
    fn set_capacity() {
        let budget = budget(100);
        let r1 = budget.try_reserve(60).unwrap();

        budget.set_capacity(NonZeroUsize::new(200).unwrap());
        assert_eq!(budget.available(), 140);

        let r2 = budget.try_reserve(100).unwrap();

        // Decrease below usage - existing leases valid, new ones blocked
        budget.set_capacity(NonZeroUsize::new(50).unwrap());
        assert!(budget.try_reserve(1).is_none());
        assert_eq!(budget.reserved(), 160);

        drop(r1);
        drop(r2);
        assert_eq!(budget.available(), 50);
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
        assert_eq!(r2.size(), 50);
    }

    #[test]
    fn min_capacity_clamps_capacity_upward() {
        let min = NonZeroUsize::new(200).unwrap();
        let budget = MemoryBudget::with_capacity("test-min", NonZeroUsize::new(50).unwrap(), min);

        // Capacity was clamped to min_capacity
        assert_eq!(budget.capacity(), Some(200));
        assert_eq!(budget.min_capacity(), Some(min));

        // A lease up to the clamped capacity succeeds
        let r1 = budget.try_reserve(200).unwrap();
        assert_eq!(r1.size(), 200);
        assert_eq!(budget.reserved(), 200);

        drop(r1);
        assert_eq!(budget.available(), 200);
    }

    #[test]
    fn set_capacity_clamped_to_min() {
        let min = NonZeroUsize::new(100).unwrap();
        let budget =
            MemoryBudget::with_capacity("test-clamp", NonZeroUsize::new(200).unwrap(), min);
        assert_eq!(budget.capacity(), Some(200));

        // Trying to set below min_capacity gets clamped
        budget.set_capacity(NonZeroUsize::new(50).unwrap());
        assert_eq!(budget.capacity(), Some(100));

        // Setting above min_capacity works normally
        budget.set_capacity(NonZeroUsize::new(300).unwrap());
        assert_eq!(budget.capacity(), Some(300));
    }

    #[test]
    fn cannot_reserve_beyond_capacity_even_when_empty() {
        let budget = budget(100);
        // Over-capacity is rejected even when budget is empty
        assert!(budget.try_reserve(150).is_none());
        assert_eq!(budget.reserved(), 0);
    }

    #[tokio::test]
    async fn set_capacity_wakes_waiters() {
        let budget = budget(100);
        let r1 = budget.try_reserve(100).unwrap();

        let budget_clone = budget.clone();
        let handle = tokio::spawn(async move { budget_clone.reserve(50).await });

        tokio::task::yield_now().await;
        budget.set_capacity(NonZeroUsize::new(150).unwrap());

        let r2 = handle.await.unwrap();
        assert_eq!(r2.size(), 50);
        assert_eq!(budget.reserved(), 150);

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

        let total: usize = leases.iter().map(|r| r.size()).sum();
        assert_eq!(total, 60);
        assert_eq!(budget.reserved(), 60);
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
        assert_eq!(budget.reserved(), 0);
    }
}
