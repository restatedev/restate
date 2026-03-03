// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing four WaiterQueue push/drain strategies:
//!
//! 1. **Naive unsorted:** `push` always appends. `drain_up_to` pops from the
//!    front then uses a `remove(i)` loop for out-of-order stragglers —
//!    O(k * remainder).
//!
//! 2. **Compact unsorted:** Same push, but the drain slow path uses a
//!    single-pass swap-and-compact — O(remainder) regardless of match count.
//!
//! 3. **Adaptive sorted-on-demand (production):** Tracks `max_key`. In-order
//!    pushes use `push_back`; out-of-order pushes use binary-search insert to
//!    maintain sorted order. Drain is always a simple front-pop — no scan.
//!    This is what [`WaiterQueue`] uses in production.
//!
//! 4. **Always sorted-insert:** Every `push` uses binary search + insert.
//!    Drain is a simple front-pop.
//!
//! We benchmark realistic scenarios:
//! - **All ordered:** Entries arrive in order (common case).
//! - **Few out-of-order:** ~5% of entries are displaced (repair stores).
//! - **Many out-of-order:** ~30% of entries are displaced (stress case).
//! - **Push-only:** Measuring push throughput in isolation.

use std::collections::VecDeque;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use restate_futures_util::waiter_queue::WaiterQueue;

// ---------------------------------------------------------------------------
// Naive unsorted variant (old slow path with remove(i) — O(k * remainder))
// ---------------------------------------------------------------------------

struct NaiveUnsortedQueue<K, V> {
    inner: VecDeque<(K, V)>,
}

impl<K, V> NaiveUnsortedQueue<K, V> {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn push(&mut self, target: K, value: V) {
        self.inner.push_back((target, value));
    }
}

impl<K: Ord, V> NaiveUnsortedQueue<K, V> {
    fn drain_up_to(&mut self, threshold: K, mut f: impl FnMut(V)) {
        while let Some(entry) = self.inner.pop_front_if(|e| e.0 <= threshold) {
            f(entry.1);
        }
        let mut i = 0;
        while i < self.inner.len() {
            if self.inner[i].0 <= threshold {
                f(self.inner.remove(i).unwrap().1);
            } else {
                i += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Compact unsorted variant (production — O(remainder) swap-and-compact)
// ---------------------------------------------------------------------------

struct CompactQueue<K, V> {
    inner: VecDeque<(K, V)>,
}

impl<K, V> CompactQueue<K, V> {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn push(&mut self, target: K, value: V) {
        self.inner.push_back((target, value));
    }
}

impl<K: Ord, V> CompactQueue<K, V> {
    fn drain_up_to(&mut self, threshold: K, mut f: impl FnMut(V)) {
        // Fast path: pop from front while resolved.
        while let Some(entry) = self.inner.pop_front_if(|e| e.0 <= threshold) {
            f(entry.1);
        }
        // Slow path: single-pass swap-compact.
        let mut write = 0;
        for read in 0..self.inner.len() {
            if self.inner[read].0 <= threshold {
                self.inner.swap(write, read);
                write += 1;
            }
        }
        for _ in 0..write {
            f(self.inner.pop_front().unwrap().1);
        }
    }
}

// ---------------------------------------------------------------------------
// Sorted-insert variant (candidate)
// ---------------------------------------------------------------------------

struct SortedQueue<K, V> {
    inner: VecDeque<(K, V)>,
}

impl<K, V> SortedQueue<K, V> {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }
}

impl<K: Ord, V> SortedQueue<K, V> {
    fn push(&mut self, target: K, value: V) {
        // Binary search on VecDeque for the insertion point.
        let pos = self.inner.partition_point(|e| e.0 <= target);
        self.inner.insert(pos, (target, value));
    }

    fn drain_up_to(&mut self, threshold: K, mut f: impl FnMut(V)) {
        // Everything is sorted, so just pop from front.
        while let Some(entry) = self.inner.pop_front_if(|e| e.0 <= threshold) {
            f(entry.1);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generates a sequence of keys. `out_of_order_pct` controls how many entries
/// are displaced backward by a random amount.
fn generate_keys(n: usize, out_of_order_pct: f64) -> Vec<u64> {
    // Deterministic "random" via simple LCG for reproducibility.
    let mut rng_state: u64 = 0xDEAD_BEEF;
    let mut next_rng = || -> u64 {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        rng_state >> 33
    };

    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let base = (i as u64 + 1) * 10; // spread out so there's room to insert before
        // Decide if this entry is out-of-order.
        let is_ooo = (next_rng() % 1000) < (out_of_order_pct * 1000.0) as u64;
        if is_ooo && base > 20 {
            // Push it backward by some amount (simulates a repair store).
            let displacement = (next_rng() % (base / 2)).max(1);
            keys.push(base - displacement);
        } else {
            keys.push(base);
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_push_and_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("waiter_queue/push_and_drain");

    for &(label, ooo_pct) in &[
        ("all_ordered", 0.0),
        ("5pct_ooo", 0.05),
        ("30pct_ooo", 0.30),
    ] {
        for &n in &[50, 200, 1000] {
            let keys = generate_keys(n, ooo_pct);
            // Drain threshold: drain roughly half the entries each round.
            let threshold = keys.iter().copied().max().unwrap() / 2;

            group.bench_with_input(
                BenchmarkId::new(format!("naive/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = NaiveUnsortedQueue::new();
                        for &k in keys {
                            q.push(k, k);
                        }
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("compact/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = CompactQueue::new();
                        for &k in keys {
                            q.push(k, k);
                        }
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("adaptive/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = WaiterQueue::default();
                        for &k in keys {
                            q.push(k, k);
                        }
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("sorted/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = SortedQueue::new();
                        for &k in keys {
                            q.push(k, k);
                        }
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_push_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("waiter_queue/push_only");

    for &(label, ooo_pct) in &[("all_ordered", 0.0), ("5pct_ooo", 0.05)] {
        for &n in &[50, 200, 1000] {
            let keys = generate_keys(n, ooo_pct);

            // Naive and compact have identical push — benchmark one as "unsorted".
            group.bench_with_input(
                BenchmarkId::new(format!("unsorted/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = CompactQueue::new();
                        for &k in keys {
                            q.push(k, k);
                        }
                        black_box(&q);
                    });
                },
            );

            // Adaptive has the same push_back but also tracks max_key/sorted.
            group.bench_with_input(
                BenchmarkId::new(format!("adaptive/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = WaiterQueue::default();
                        for &k in keys {
                            q.push(k, k);
                        }
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("sorted/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = SortedQueue::new();
                        for &k in keys {
                            q.push(k, k);
                        }
                        black_box(&q);
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_drain_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("waiter_queue/drain_only");

    for &(label, ooo_pct) in &[
        ("all_ordered", 0.0),
        ("5pct_ooo", 0.05),
        ("30pct_ooo", 0.30),
    ] {
        for &n in &[50, 200, 1000] {
            let keys = generate_keys(n, ooo_pct);
            let threshold = keys.iter().copied().max().unwrap() / 2;

            // Pre-build unsorted queue (same data for naive and compact).
            let mut unsorted_template = CompactQueue::new();
            for &k in &keys {
                unsorted_template.push(k, k);
            }

            // Pre-build sorted queue.
            let mut sorted_template = SortedQueue::new();
            for &k in &keys {
                sorted_template.push(k, k);
            }

            group.bench_with_input(
                BenchmarkId::new(format!("naive/{label}"), n),
                &unsorted_template.inner,
                |b, template| {
                    b.iter(|| {
                        let mut q = NaiveUnsortedQueue {
                            inner: template.clone(),
                        };
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("compact/{label}"), n),
                &unsorted_template.inner,
                |b, template| {
                    b.iter(|| {
                        let mut q = CompactQueue {
                            inner: template.clone(),
                        };
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            // Adaptive: build via push (which sets sorted flag correctly).
            group.bench_with_input(
                BenchmarkId::new(format!("adaptive/{label}"), n),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        let mut q = WaiterQueue::default();
                        for &k in keys {
                            q.push(k, k);
                        }
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("sorted/{label}"), n),
                &sorted_template.inner,
                |b, template| {
                    b.iter(|| {
                        let mut q = SortedQueue {
                            inner: template.clone(),
                        };
                        q.drain_up_to(threshold, |v| {
                            black_box(v);
                        });
                        black_box(&q);
                    });
                },
            );
        }
    }

    group.finish();
}

/// Simulates the real steady-state: interleaved pushes and drains,
/// as the log-server worker would do (batch of pushes, then drain).
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("waiter_queue/mixed_workload");

    for &(label, ooo_pct) in &[("all_ordered", 0.0), ("5pct_ooo", 0.05)] {
        let keys = generate_keys(500, ooo_pct);
        // Simulate: push 50 entries, drain, push 50, drain, ...
        let batch_size = 50;

        group.bench_function(BenchmarkId::new(format!("naive/{label}"), 500), |b| {
            b.iter(|| {
                let mut q = NaiveUnsortedQueue::new();
                for chunk in keys.chunks(batch_size) {
                    for &k in chunk {
                        q.push(k, k);
                    }
                    let threshold = *chunk.iter().max().unwrap();
                    q.drain_up_to(threshold, |v| {
                        black_box(v);
                    });
                }
                black_box(&q);
            });
        });

        group.bench_function(BenchmarkId::new(format!("compact/{label}"), 500), |b| {
            b.iter(|| {
                let mut q = CompactQueue::new();
                for chunk in keys.chunks(batch_size) {
                    for &k in chunk {
                        q.push(k, k);
                    }
                    let threshold = *chunk.iter().max().unwrap();
                    q.drain_up_to(threshold, |v| {
                        black_box(v);
                    });
                }
                black_box(&q);
            });
        });

        group.bench_function(BenchmarkId::new(format!("adaptive/{label}"), 500), |b| {
            b.iter(|| {
                let mut q = WaiterQueue::default();
                for chunk in keys.chunks(batch_size) {
                    for &k in chunk {
                        q.push(k, k);
                    }
                    let threshold = *chunk.iter().max().unwrap();
                    q.drain_up_to(threshold, |v| {
                        black_box(v);
                    });
                }
                black_box(&q);
            });
        });

        group.bench_function(BenchmarkId::new(format!("sorted/{label}"), 500), |b| {
            b.iter(|| {
                let mut q = SortedQueue::new();
                for chunk in keys.chunks(batch_size) {
                    for &k in chunk {
                        q.push(k, k);
                    }
                    let threshold = *chunk.iter().max().unwrap();
                    q.drain_up_to(threshold, |v| {
                        black_box(v);
                    });
                }
                black_box(&q);
            });
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_push_and_drain, bench_push_only, bench_drain_only, bench_mixed_workload
);
criterion_main!(benches);
