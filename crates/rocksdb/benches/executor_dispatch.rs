// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Measures how fast `SinglePoolExecutor` runs batches of small blocking tasks
//! submitted from several threads at once. Tasks are tiny, so the numbers mostly
//! reflect dispatch, queueing, and waking workers.
//!
//! `BENCH_WORKERS` (default 8) sets the pool size, and `BENCH_SUBMITTERS` (default
//! `1,4,8`) the numbers of submitting threads. On hybrid CPUs, confine the run to
//! one core type, e.g. `taskset -c 0-7`.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::Thread;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use restate_core::io::IoExecutor;
use restate_rocksdb::SinglePoolExecutor;

const TASKS_PER_BATCH: usize = 8192;

// Signals the measuring thread once the last task of a batch finishes.
struct Batch {
    remaining: AtomicUsize,
    waiter: Thread,
}

fn spin(cost: Duration) {
    let start = Instant::now();
    while start.elapsed() < cost {
        std::hint::spin_loop();
    }
}

fn run_batch(executor: &SinglePoolExecutor, submitters: usize, cost: Duration) {
    let batch = Arc::new(Batch {
        remaining: AtomicUsize::new(TASKS_PER_BATCH),
        waiter: std::thread::current(),
    });
    std::thread::scope(|scope| {
        for submitter in 0..submitters {
            let batch = &batch;
            let tasks = TASKS_PER_BATCH / submitters
                + usize::from(submitter < TASKS_PER_BATCH % submitters);
            scope.spawn(move || {
                for _ in 0..tasks {
                    let batch = Arc::clone(batch);
                    executor
                        .dispatch_fn("bench", move || {
                            spin(black_box(cost));
                            if batch.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                                batch.waiter.unpark();
                            }
                        })
                        .unwrap();
                }
            });
        }
    });
    while batch.remaining.load(Ordering::Acquire) != 0 {
        std::thread::park();
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: &str) -> Vec<T> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|v| v.trim().parse().ok().expect("valid benchmark setting"))
        .collect()
}

fn dispatch(c: &mut Criterion) {
    let workers = env_or::<NonZeroUsize>("BENCH_WORKERS", "8")[0];
    let submitter_counts = env_or::<usize>("BENCH_SUBMITTERS", "1,4,8");
    let executor = SinglePoolExecutor::new("bench", workers);
    let mut group = c.benchmark_group(format!("executor_dispatch/workers={workers}"));
    group.throughput(Throughput::Elements(TASKS_PER_BATCH as u64));
    for cost in [Duration::ZERO, Duration::from_micros(2)] {
        for &submitters in &submitter_counts {
            let id = BenchmarkId::new(
                format!("cost={}ns", cost.as_nanos()),
                format!("submitters={submitters}"),
            );
            group.bench_with_input(id, &(), |b, _| {
                b.iter(|| run_batch(&executor, submitters, cost))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, dispatch);
criterion_main!(benches);
