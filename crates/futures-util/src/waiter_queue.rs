// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A queue of waiters that are drained when a monotonically advancing
//! threshold is reached.
//!
//! [`WaiterQueue<K, V>`] stores `(key, value)` pairs where `K` is an ordered
//! threshold (e.g. an offset, a [`Token`], a sequence number) and `V` is a
//! payload to deliver when the threshold is met. Calling
//! [`drain_up_to(threshold, f)`](WaiterQueue::drain_up_to) invokes `f` on
//! every entry whose key `<= threshold`.
//!
//! # Ordering invariant
//!
//! The queue maintains entries in non-decreasing key order at all times.
//! [`push`](WaiterQueue::push) appends to the back in the common case
//! (monotonically non-decreasing keys), making it O(1) amortised.
//!
//! **Out-of-order pushes are expected to be rare** (e.g. repair stores,
//! retries). When detected, `push` falls back to a binary-search + insert to
//! restore sorted order — O(N) in the worst case, but negligible when
//! amortised over many in-order pushes.
//!
//! Because the queue is always sorted, [`drain_up_to`](WaiterQueue::drain_up_to)
//! is always a simple front-pop — **O(drained)** with no remainder scan.
//!
//! # Design decisions
//!
//! Several strategies were benchmarked (see `benches/waiter_queue.rs`):
//!
//! - **Always sorted-insert** uses binary search on every push, which is
//!   3-10x slower overall because the O(N) shift cost dominates even when
//!   entries are already in order.
//!
//! - **Unsorted push + scan-on-drain** keeps push O(1) but pays an
//!   O(remainder) scan on every drain, even when the queue is perfectly
//!   sorted.
//!
//! - **Adaptive sorted-on-demand** (chosen) tracks the maximum key pushed so
//!   far. In-order pushes are a simple `push_back`. Out-of-order pushes use
//!   binary-search insert to maintain sorted order. Drain is always a
//!   front-pop with no scan.
//!
//! [`Token`]: crate::monotonic_token::Token

use std::collections::VecDeque;

/// A queue of in-flight waiters keyed by a monotonically advancing threshold.
///
/// `K` is the target key (must be [`Ord`] + [`Clone`]). `V` is the waiter
/// payload.
///
/// Entries are maintained in non-decreasing key order. In-order pushes are
/// O(1) amortised; out-of-order pushes (expected to be rare) use
/// binary-search insert. Drain is always a simple front-pop.
pub struct WaiterQueue<K, V> {
    inner: VecDeque<Entry<K, V>>,
    /// `None` when the queue is empty. Tracks the maximum key pushed so far
    /// to distinguish in-order pushes (fast `push_back`) from out-of-order
    /// pushes (binary-search `insert`).
    max_key: Option<K>,
}

struct Entry<K, V> {
    target: K,
    value: V,
}

impl<K, V> Default for WaiterQueue<K, V> {
    fn default() -> Self {
        Self {
            inner: VecDeque::new(),
            max_key: None,
        }
    }
}

impl<K: Ord + Clone, V> WaiterQueue<K, V> {
    /// Pushes a waiter that will be drained when the threshold reaches
    /// `target`.
    ///
    /// When `target >= max_key` (the common case), the entry is appended to
    /// the back in O(1) amortised. When `target < max_key` (out-of-order),
    /// a binary-search insert maintains sorted order in O(N). Out-of-order
    /// pushes are expected to be rare.
    pub fn push(&mut self, target: K, value: V) {
        let in_order = self.max_key.as_ref().is_none_or(|max| target >= *max);

        if in_order {
            self.max_key = Some(target.clone());
            self.inner.push_back(Entry { target, value });
        } else {
            let pos = self.inner.partition_point(|e| e.target <= target);
            self.inner.insert(pos, Entry { target, value });
        }
    }

    /// Drains all entries whose `target <= threshold` (inclusive), invoking `f`
    /// on each drained value.
    ///
    /// Because entries are always in non-decreasing order, this is a simple
    /// front-pop — O(drained).
    pub fn drain_up_to(&mut self, threshold: K, mut f: impl FnMut(V)) {
        while let Some(entry) = self.inner.pop_front_if(|e| e.target <= threshold) {
            f(entry.value);
        }
        if self.inner.is_empty() {
            self.max_key = None;
        }
    }

    /// Drains **all** entries unconditionally, invoking `f` on each.
    pub fn drain_all(&mut self, mut f: impl FnMut(V)) {
        for entry in self.inner.drain(..) {
            f(entry.value);
        }
        self.max_key = None;
    }

    /// Returns `true` if the queue contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of entries in the queue.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monotonic_token::TokenOwner;

    struct Test;

    #[test]
    fn drain_ordered() {
        let mut q = WaiterQueue::default();
        q.push(5u64, "a");
        q.push(10, "b");
        q.push(15, "c");

        let mut drained = Vec::new();
        q.drain_up_to(10, |v| drained.push(v));
        assert_eq!(drained, vec!["a", "b"]);
        assert_eq!(q.len(), 1);

        q.drain_up_to(20, |v| drained.push(v));
        assert_eq!(drained, vec!["a", "b", "c"]);
        assert!(q.is_empty());
    }

    #[test]
    fn drain_inclusive() {
        let mut q = WaiterQueue::default();
        q.push(5u64, "x");
        q.push(6, "y");

        let mut drained = Vec::new();
        q.drain_up_to(5, |v| drained.push(v));
        assert_eq!(drained, vec!["x"]);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn drain_out_of_order() {
        let mut q = WaiterQueue::default();
        q.push(5u64, "a");
        q.push(10, "b");
        // Out-of-order: inserted before "a" and "b" in sorted position.
        q.push(3, "c");
        q.push(15, "d");

        let mut drained = Vec::new();
        q.drain_up_to(10, |v| drained.push(v));
        // Sorted insert places "c" before "a", so drain order is c, a, b.
        assert_eq!(drained, vec!["c", "a", "b"]);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn drain_all_entries() {
        let mut q = WaiterQueue::default();
        q.push(1u64, 10);
        q.push(2, 20);
        q.push(3, 30);

        let mut sum = 0;
        q.drain_all(|v| sum += v);
        assert_eq!(sum, 60);
        assert!(q.is_empty());
    }

    #[test]
    fn empty_drain_is_noop() {
        let mut q: WaiterQueue<u64, &str> = WaiterQueue::default();
        let mut count = 0;
        q.drain_up_to(100, |_| count += 1);
        assert_eq!(count, 0);
        q.drain_all(|_| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn token_keyed_drain() {
        let owner = TokenOwner::<Test>::new();
        let generator = owner.new_tokens();

        let t1 = generator.next();
        let t2 = generator.next();
        let t3 = generator.next();

        let mut q = WaiterQueue::default();
        q.push(t1, "first");
        q.push(t2, "second");
        q.push(t3, "third");

        let mut drained = Vec::new();
        q.drain_up_to(t2, |v| drained.push(v));
        assert_eq!(drained, vec!["first", "second"]);
        assert_eq!(q.len(), 1);

        q.drain_up_to(t3, |v| drained.push(v));
        assert_eq!(drained, vec!["first", "second", "third"]);
        assert!(q.is_empty());
    }

    #[test]
    fn monotonic_pushes_and_partial_drain() {
        let mut q = WaiterQueue::default();
        for i in 0u64..100 {
            q.push(i, i);
        }

        // Drain half.
        let mut count = 0;
        q.drain_up_to(49, |_| count += 1);
        assert_eq!(count, 50);
        assert_eq!(q.len(), 50);

        // Continue pushing in order — should not regress.
        q.push(100, 100);
        q.push(101, 101);
        assert_eq!(q.len(), 52);
    }

    #[test]
    fn out_of_order_push_maintains_sort() {
        let mut q = WaiterQueue::default();
        q.push(10u64, "a");
        q.push(20, "b");
        q.push(5, "c"); // out-of-order — sorted insert
        q.push(30, "d");

        // Drain up to 20 — "c"(5), "a"(10), "b"(20) should all come out.
        let mut drained = Vec::new();
        q.drain_up_to(20, |v| drained.push(v));
        assert_eq!(drained, vec!["c", "a", "b"]);
        assert_eq!(q.len(), 1);

        // New monotonic pushes continue normally.
        q.push(40, "e");
        q.push(50, "f");
        assert_eq!(q.len(), 3);

        q.drain_up_to(50, |v| drained.push(v));
        assert_eq!(drained, vec!["c", "a", "b", "d", "e", "f"]);
        assert!(q.is_empty());
    }

    #[test]
    fn drain_all_clears_max_key() {
        let mut q = WaiterQueue::default();
        q.push(10u64, 1);
        q.push(5, 2); // out-of-order

        let mut sum = 0;
        q.drain_all(|v| sum += v);
        assert_eq!(sum, 3);
        assert!(q.max_key.is_none());

        // After drain_all, a push with any key should work without
        // false out-of-order detection.
        q.push(1, 100);
        assert_eq!(q.max_key, Some(1));
    }

    #[test]
    fn max_key_tracks_correctly() {
        let mut q = WaiterQueue::default();
        q.push(10u64, "a");
        assert_eq!(q.max_key, Some(10));

        q.push(20, "b");
        assert_eq!(q.max_key, Some(20));

        // Out-of-order push: max_key should not decrease.
        q.push(5, "c");
        assert_eq!(q.max_key, Some(20));

        // Drain everything.
        q.drain_up_to(20, |_| {});
        assert!(q.is_empty());
        assert!(q.max_key.is_none());
    }
}
