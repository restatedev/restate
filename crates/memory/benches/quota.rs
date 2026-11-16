// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Costs of lending from a [`Quota`] against reserving from its parent [`MemoryPool`]: a lease
//! and its drop on one thread, then leases dropped on several threads at once, as owners on
//! other tasks return them.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::task::Waker;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::task::AtomicWaker;

use restate_memory::{Amount, MemoryPool, NonZeroByteCount, Quota, QuotaPolicy};

/// One small message's worth.
const ONE: Amount = Amount {
    bytes: 64,
    slots: 1,
};

fn parent() -> MemoryPool {
    MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(1 << 40).unwrap()))
}

/// A quota holding plenty, waking `driver` as leases drop.
fn quota(driver: Arc<AtomicWaker>) -> Quota {
    let limit = 1 << 32;
    let mut quota = Quota::new(parent(), limit, QuotaPolicy::default(), driver);
    assert!(quota.grow(Amount {
        bytes: limit,
        slots: 1 << 30,
    }));
    quota
}

fn single(c: &mut Criterion) {
    let mut group = c.benchmark_group("quota");
    let mut idle = quota(Arc::default());
    group.bench_function("lease-drop", |b| {
        b.iter(|| drop(black_box(idle.try_lease(ONE))))
    });
    // The driver registers before each, as one polling its link does, so each drop wakes it.
    let driver = Arc::new(AtomicWaker::new());
    let mut woken = quota(Arc::clone(&driver));
    group.bench_function("lease-drop-woken", |b| {
        b.iter(|| {
            driver.register(Waker::noop());
            drop(black_box(woken.try_lease(ONE)))
        })
    });
    // What each reply would cost without a local quota.
    let pool = parent();
    group.bench_function("parent-reserve-drop", |b| {
        b.iter(|| drop(black_box(pool.try_reserve(64))))
    });
    group.finish();
}

/// The time for `threads` threads, each with leases from `take(thread)`, to drop exactly `iters`
/// leases in all, starting together:
///
/// ```text
///   workers:  spawn ── ready ── spin on `go` ─────────── drop their leases ── end ┐
///   main:              ready ── start ── set `go` ── join (not timed)            │
///             timed: from `start` to the last worker's `end` ────────────────────┘
/// ```
///
/// The interval includes the workers seeing `go`, but neither spawning nor joining them. It
/// panics if any worker began dropping before the start.
fn drop_on<L: Send>(threads: usize, iters: u64, mut take: impl FnMut(usize) -> L) -> Duration {
    let iters = iters as usize;
    let batches: Vec<Vec<L>> = (0..threads)
        .map(|thread| {
            let count = iters / threads + usize::from(thread < iters % threads);
            (0..count).map(|_| take(thread)).collect()
        })
        .collect();
    let ready = Barrier::new(threads + 1);
    let go = AtomicBool::new(false);
    let (start, spans) = std::thread::scope(|scope| {
        let workers: Vec<_> = batches
            .into_iter()
            .map(|batch| {
                let (ready, go) = (&ready, &go);
                scope.spawn(move || {
                    ready.wait();
                    while !go.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    let began = Instant::now();
                    drop(batch);
                    (began, Instant::now())
                })
            })
            .collect();
        ready.wait();
        let start = Instant::now();
        go.store(true, Ordering::Release);
        let spans: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        (start, spans)
    });
    assert!(
        spans.iter().all(|(began, _)| *began >= start),
        "a drop before the start"
    );
    let end = spans.iter().map(|(_, end)| *end).max().unwrap_or(start);
    end - start
}

/// Leases dropped on several threads at once, the time per drop: each thread's leases from a
/// quota of its own, as links on different cores; all from one quota, as one link's owners on
/// many tasks; all from one parent, as every link reserving from the node pool directly.
fn contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("contended-drop");
    for threads in [1, 2, 4, 8] {
        group.bench_function(BenchmarkId::new("own-quota", threads), |b| {
            b.iter_custom(|iters| {
                // A driver per link, as links have.
                let mut quotas: Vec<_> = (0..threads).map(|_| quota(Arc::default())).collect();
                drop_on(threads, iters, |thread| {
                    quotas[thread].try_lease(ONE).unwrap()
                })
            })
        });
        group.bench_function(BenchmarkId::new("shared-quota", threads), |b| {
            b.iter_custom(|iters| {
                let mut quota = quota(Arc::new(AtomicWaker::new()));
                drop_on(threads, iters, |_| quota.try_lease(ONE).unwrap())
            })
        });
        group.bench_function(BenchmarkId::new("shared-parent", threads), |b| {
            b.iter_custom(|iters| {
                let pool = parent();
                drop_on(threads, iters, |_| pool.try_reserve(64).unwrap())
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = single, contended
}
criterion_main!(benches);
