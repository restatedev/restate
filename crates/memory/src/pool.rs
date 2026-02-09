// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Memory pool for tracking and bounding memory usage across async tasks.
//!
//! # Features
//!
//! - **Live capacity updates** via [`MemoryPool::set_capacity`]
//! - **Over-capacity reservations**: A single reservation can exceed capacity
//!   when the pool is empty, allowing large requests to proceed one at a time
//! - **Zero-overhead unlimited mode**: Unlimited pools skip all tracking

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;
use tracing::{info, trace};
use triomphe::Arc;

use restate_serde_util::{ByteCount, NonZeroByteCount};

/// A memory pool that tracks and limits memory usage.
///
/// Cheaply cloneable (uses `Arc` internally). Capacity can be updated at
/// runtime via [`set_capacity`](Self::set_capacity).
#[derive(Debug, Clone)]
pub struct MemoryPool {
    inner: Option<Arc<BoundedPoolInner>>,
}

#[derive(Debug)]
struct BoundedPoolInner {
    capacity: AtomicUsize,
    used: AtomicUsize,
    notify: Notify,
}

impl MemoryPool {
    /// Creates an unlimited pool (zero overhead, no tracking).
    #[inline]
    pub const fn unlimited() -> Self {
        Self { inner: None }
    }

    /// Creates a bounded pool from a [`NonZeroByteCount`] (for config deserialization).
    pub fn bounded(capacity: NonZeroByteCount) -> Self {
        Self::with_capacity(NonZeroUsize::new(capacity.as_usize()).unwrap())
    }

    /// Creates a bounded pool with the given capacity in bytes.
    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Some(Arc::new(BoundedPoolInner {
                capacity: AtomicUsize::new(capacity.get()),
                used: AtomicUsize::new(0),
                notify: Notify::new(),
            })),
        }
    }

    /// Creates a memory pool with the given capacity in bytes.
    ///
    /// Creates an unlimited pool if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        NonZeroUsize::new(capacity).map_or_else(Self::unlimited, Self::with_capacity)
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

    /// Updates capacity at runtime. Wakes waiters to re-check.
    #[inline]
    pub fn set_capacity(&self, capacity: NonZeroUsize) {
        if let Some(inner) = &self.inner {
            inner.capacity.store(capacity.get(), Ordering::Relaxed);
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
    /// Returns `None` if insufficient capacity. A reservation may exceed
    /// capacity if the pool is currently empty.
    #[inline]
    pub fn try_reserve(&self, size: usize) -> Option<MemoryReservation> {
        match &self.inner {
            Some(inner) => {
                inner
                    .used
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                        let capacity = inner.capacity.load(Ordering::Relaxed);
                        let new_used = used.checked_add(size)?;
                        (new_used <= capacity || used == 0).then_some(new_used)
                    })
                    .ok()?;
                Some(MemoryReservation {
                    pool: self.clone(),
                    size,
                })
            }
            None => Some(MemoryReservation {
                pool: self.clone(),
                size,
            }),
        }
    }

    /// Reserves `size` bytes, waiting if necessary.
    ///
    /// A reservation may exceed capacity if the pool is empty (waits for
    /// complete drain first).
    pub async fn reserve(&self, size: usize) -> MemoryReservation {
        match &self.inner {
            Some(inner) => {
                // Use enable() before try_reserve to prevent lost wakeups
                let mut future = std::pin::pin!(inner.notify.notified());

                loop {
                    future.as_mut().enable();
                    if let Some(reservation) = self.try_reserve(size) {
                        trace!(
                            "Memory pool is available, reserving {}",
                            ByteCount::from(size)
                        );
                        return reservation;
                    }
                    info!(
                        "Waiting for memory pool to become available, requesting {}",
                        ByteCount::from(size)
                    );
                    future.as_mut().await;
                    future.set(inner.notify.notified());
                }
            }
            None => MemoryReservation {
                pool: self.clone(),
                size,
            },
        }
    }

    #[inline]
    pub fn empty_reservation(&self) -> MemoryReservation {
        MemoryReservation {
            pool: self.clone(),
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

impl Default for MemoryPool {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// A reservation of memory from a [`MemoryPool`].
///
/// Memory is returned to the pool on drop. Reservations can be split, merged,
/// grown, and shrunk.
#[must_use]
pub struct MemoryReservation {
    pool: MemoryPool,
    size: usize,
}

impl Default for MemoryReservation {
    /// Creates an unlinked (zero-size) reservation from an unlimited pool.
    ///
    /// This is useful for tests or transition code where memory tracking is not needed.
    fn default() -> Self {
        Self::unlinked()
    }
}

impl std::fmt::Debug for MemoryReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("size", &self.size)
            .field("pool_capacity", &self.pool.capacity())
            .finish()
    }
}

impl MemoryReservation {
    /// Creates an unlinked reservation that doesn't track memory.
    ///
    /// This is backed by an unlimited pool, so it has zero overhead and
    /// doesn't apply any memory pressure. Use this for:
    /// - Tests that don't need memory tracking
    /// - Transition code during migration to memory-bounded channels
    /// - Messages where memory tracking is not applicable
    #[inline]
    pub fn unlinked() -> Self {
        Self {
            pool: MemoryPool::unlimited(),
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
    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    /// Shrinks by `amount` bytes (clamped to current size).
    #[inline]
    pub fn shrink(&mut self, amount: usize) {
        let shrink_by = amount.min(self.size);
        if shrink_by > 0 {
            self.size -= shrink_by;
            self.pool.return_memory(shrink_by);
        }
    }

    /// Releases all memory, returning bytes released.
    #[inline]
    pub fn release(&mut self) -> usize {
        let size = self.size;
        if size > 0 {
            self.size = 0;
            self.pool.return_memory(size);
        }
        size
    }

    /// Tries to grow by `additional` bytes. Returns false if insufficient capacity.
    #[inline]
    pub fn try_grow(&mut self, additional: usize) -> bool {
        if additional == 0 {
            return true;
        }
        if self.pool.try_acquire(additional) {
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
            additional == 0 || self.pool.try_acquire(additional),
            "insufficient capacity to grow reservation"
        );
        self.size += additional;
    }

    /// Splits off `amount` bytes into a new reservation. Panics if `amount > size`.
    #[inline]
    pub fn split(&mut self, amount: usize) -> MemoryReservation {
        assert!(
            amount <= self.size,
            "cannot split more than reservation size"
        );
        self.size -= amount;
        MemoryReservation {
            pool: self.pool.clone(),
            size: amount,
        }
    }

    /// Merges `other` into self. Debug-asserts same pool.
    #[inline]
    pub fn merge(&mut self, mut other: MemoryReservation) {
        if self.pool.is_unlimited() {
            self.size = other.size;
            other.size = 0;
            std::mem::swap(&mut self.pool, &mut other.pool);
        } else if other.pool.is_unlimited() {
            // ignore it.
        } else {
            debug_assert!(
                std::ptr::eq(
                    self.pool
                        .inner
                        .as_ref()
                        .map(Arc::as_ptr)
                        .unwrap_or(std::ptr::null()),
                    other
                        .pool
                        .inner
                        .as_ref()
                        .map(Arc::as_ptr)
                        .unwrap_or(std::ptr::null()),
                ),
                "cannot merge reservations from different pools"
            );
            self.size = self.size.saturating_add(other.size);
            // Necessary to ensure it doesn't return its memory to the pool on drop.
            other.size = 0;
        }
    }

    /// Takes all bytes, leaving self empty.
    #[inline]
    pub fn take(&mut self) -> MemoryReservation {
        let size = self.size;
        self.size = 0;
        MemoryReservation {
            pool: self.pool.clone(),
            size,
        }
    }

    #[inline]
    pub fn new_empty(&self) -> MemoryReservation {
        MemoryReservation {
            pool: self.pool.clone(),
            size: 0,
        }
    }
}

impl Drop for MemoryReservation {
    #[inline]
    fn drop(&mut self) {
        if self.size > 0 {
            self.pool.return_memory(self.size);
        }
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MemoryPool>();
    assert_send_sync::<MemoryReservation>();
};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn pool(capacity: usize) -> MemoryPool {
        MemoryPool::with_capacity(NonZeroUsize::new(capacity).unwrap())
    }

    #[test]
    fn unlimited_pool() {
        let pool = MemoryPool::unlimited();
        assert!(pool.is_unlimited());
        assert_eq!(pool.capacity(), None);
        assert_eq!(pool.available(), usize::MAX);
        assert_eq!(pool.reserved(), 0);

        let r1 = pool.try_reserve(1000).unwrap();
        assert_eq!(r1.size(), 1000);
        assert_eq!(pool.reserved(), 0); // unlimited pools don't track
    }

    #[test]
    fn bounded_pool_reserve_and_release() {
        let pool = pool(100);
        assert!(!pool.is_unlimited());
        assert_eq!(pool.capacity(), Some(100));

        let r1 = pool.try_reserve(30).unwrap();
        let r2 = pool.try_reserve(50).unwrap();
        assert_eq!(pool.reserved(), 80);
        assert_eq!(pool.available(), 20);

        // Can't exceed capacity
        assert!(pool.try_reserve(30).is_none());

        drop(r1);
        assert_eq!(pool.reserved(), 50);

        drop(r2);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn reservation_split_merge_take() {
        let pool = pool(100);

        let mut r1 = pool.try_reserve(60).unwrap();

        // Split
        let r2 = r1.split(20);
        assert_eq!(r1.size(), 40);
        assert_eq!(r2.size(), 20);
        assert_eq!(pool.reserved(), 60);

        // Merge
        let mut r3 = pool.try_reserve(10).unwrap();
        r3.merge(r2);
        assert_eq!(r3.size(), 30);
        assert_eq!(pool.reserved(), 70);

        // Take
        let r4 = r1.take();
        assert_eq!(r1.size(), 0);
        assert_eq!(r4.size(), 40);
        assert_eq!(pool.reserved(), 70);

        drop(r3);
        drop(r4);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn reservation_grow_shrink_release() {
        let pool = pool(100);
        let mut r1 = pool.try_reserve(30).unwrap();

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
        assert_eq!(pool.reserved(), 0);

        // Release
        assert!(r1.try_grow(50));
        assert_eq!(r1.release(), 50);
        assert_eq!(r1.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn empty_reservation_and_new_empty() {
        let pool = pool(100);

        let mut r1 = pool.empty_reservation();
        assert!(r1.is_empty());
        assert!(r1.try_grow(50));
        assert_eq!(pool.reserved(), 50);

        let mut r2 = r1.new_empty();
        assert!(r2.try_grow(20));
        assert_eq!(pool.reserved(), 70);
    }

    #[test]
    #[should_panic(expected = "cannot split")]
    fn split_too_much_panics() {
        let pool = pool(100);
        let mut r1 = pool.try_reserve(50).unwrap();
        let _ = r1.split(60);
    }

    #[test]
    fn pool_clone_shares_state() {
        let pool1 = pool(100);
        let pool2 = pool1.clone();

        let _r = pool1.try_reserve(60).unwrap();
        assert_eq!(pool2.reserved(), 60);
    }

    #[test]
    fn set_capacity() {
        let pool = pool(100);
        let r1 = pool.try_reserve(60).unwrap();

        pool.set_capacity(NonZeroUsize::new(200).unwrap());
        assert_eq!(pool.available(), 140);

        let r2 = pool.try_reserve(100).unwrap();

        // Decrease below usage - existing reservations valid, new ones blocked
        pool.set_capacity(NonZeroUsize::new(50).unwrap());
        assert!(pool.try_reserve(1).is_none());
        assert_eq!(pool.reserved(), 160);

        drop(r1);
        drop(r2);
        assert_eq!(pool.available(), 50);
    }

    #[tokio::test]
    async fn async_reserve_waits() {
        let pool = pool(100);
        let r1 = pool.reserve(100).await;

        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move { pool_clone.reserve(50).await });

        tokio::task::yield_now().await;
        drop(r1);

        let r2 = handle.await.unwrap();
        assert_eq!(r2.size(), 50);
    }

    #[tokio::test]
    async fn over_capacity_when_empty() {
        let pool = pool(100);

        // Over-capacity succeeds when empty
        let r1 = pool.reserve(150).await;
        assert_eq!(r1.size(), 150);
        assert_eq!(pool.reserved(), 150);
        assert!(pool.try_reserve(1).is_none());

        drop(r1);
        assert_eq!(pool.available(), 100);
    }

    #[tokio::test]
    async fn over_capacity_waits_for_drain() {
        let pool = pool(100);
        let r1 = pool.try_reserve(60).unwrap();
        let r2 = pool.try_reserve(30).unwrap();

        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move { pool_clone.reserve(150).await });

        tokio::task::yield_now().await;
        drop(r1);
        tokio::task::yield_now().await;
        assert_eq!(pool.reserved(), 30); // still waiting

        drop(r2);
        let r3 = handle.await.unwrap();
        assert_eq!(r3.size(), 150);
    }

    #[tokio::test]
    async fn set_capacity_wakes_waiters() {
        let pool = pool(100);
        let r1 = pool.try_reserve(100).unwrap();

        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move { pool_clone.reserve(50).await });

        tokio::task::yield_now().await;
        pool.set_capacity(NonZeroUsize::new(150).unwrap());

        let r2 = handle.await.unwrap();
        assert_eq!(r2.size(), 50);
        assert_eq!(pool.reserved(), 150);

        drop(r1);
        drop(r2);
    }

    #[tokio::test]
    async fn multiple_waiters() {
        let pool = pool(100);
        let r1 = pool.try_reserve(100).unwrap();

        let mut handles = Vec::new();
        for size in [30, 20, 10] {
            let p = pool.clone();
            handles.push(tokio::spawn(async move { p.reserve(size).await }));
        }

        // Give spawned tasks time to register as waiters
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(r1);

        let mut reservations = Vec::new();
        for h in handles {
            reservations.push(h.await.unwrap());
        }

        let total: usize = reservations.iter().map(|r| r.size()).sum();
        assert_eq!(total, 60);
        assert_eq!(pool.reserved(), 60);
    }

    #[tokio::test]
    async fn stress_concurrent_acquire_release() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let pool = pool(100);
        let count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let pool = pool.clone();
                let count = count.clone();
                tokio::spawn(async move {
                    for _ in 0..25 {
                        let r = pool.reserve(20).await;
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
        assert_eq!(pool.reserved(), 0);
    }
}
