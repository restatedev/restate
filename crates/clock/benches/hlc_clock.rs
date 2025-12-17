// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use restate_clock::{AtomicStorage, ClockUpkeep, HlcClock, LocalStorage, WallClock};

pub fn hlc_clock_single_threaded(c: &mut Criterion) {
    // Start the ClockUpkeep thread before benchmarking
    let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep thread");

    let mut group = c.benchmark_group("hlc-single-threaded");

    group
        .measurement_time(Duration::from_secs(5))
        .sample_size(1000)
        .throughput(Throughput::Elements(1))
        .bench_function(BenchmarkId::new("LocalStorage", "next"), |b| {
            let clock =
                HlcClock::new(None, WallClock, LocalStorage::default()).expect("clock creation");
            b.iter(|| std::hint::black_box(clock.next()));
        })
        .bench_function(BenchmarkId::new("AtomicStorage", "next"), |b| {
            let clock = HlcClock::new(None, WallClock, Arc::new(AtomicStorage::default()))
                .expect("clock creation");
            b.iter(|| std::hint::black_box(clock.next()));
        });

    group.finish();
}

pub fn hlc_clock_multi_threaded(c: &mut Criterion) {
    // Start the ClockUpkeep thread before benchmarking
    let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep thread");

    let num_threads = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    let mut group = c.benchmark_group("hlc-multi-threaded");

    // Total operations = num_threads * iters
    group
        .measurement_time(Duration::from_secs(5))
        .sample_size(1000)
        .throughput(Throughput::Elements(num_threads as u64))
        .bench_function(BenchmarkId::new("AtomicStorage", "next"), |b| {
            let clock = HlcClock::new(None, WallClock, Arc::new(AtomicStorage::default()))
                .expect("clock creation");
            b.iter_custom(|iters| {
                let mut children = Vec::with_capacity(num_threads);
                let start = std::time::Instant::now();
                for _ in 0..num_threads {
                    let clock = clock.clone();
                    children.push(std::thread::spawn(move || {
                        for _ in 0..iters {
                            std::hint::black_box(clock.next());
                        }
                    }));
                }
                for child in children {
                    child.join().unwrap()
                }
                start.elapsed()
            });
        });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = hlc_clock_single_threaded, hlc_clock_multi_threaded
);

criterion_main!(benches);
