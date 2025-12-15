// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, SystemTime};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use restate_clock::Clock;
use restate_clock::time::MillisSinceEpoch;
use restate_clock::{ClockUpkeep, WallClock};

pub fn wall_clock_single_threaded(c: &mut Criterion) {
    // Start the ClockUpkeep thread before benchmarking
    let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep thread");

    let mut group = c.benchmark_group("single-threaded");

    group
        .measurement_time(Duration::from_secs(5))
        .sample_size(1000)
        .bench_function(BenchmarkId::new("cached", "WallClock.now"), |b| {
            b.iter(|| std::hint::black_box(WallClock.now()));
        })
        .bench_function(BenchmarkId::new("uncached", "SystemTime::now"), |b| {
            b.iter(|| std::hint::black_box(MillisSinceEpoch::from(SystemTime::now())));
        });

    group.finish();
}

pub fn wall_clock_multi_threaded(c: &mut Criterion) {
    // Start the ClockUpkeep thread before benchmarking
    let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep thread");

    let num_threads = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    let mut group = c.benchmark_group("multi-threaded");

    group
        .measurement_time(Duration::from_secs(5))
        .sample_size(1000)
        .bench_function(BenchmarkId::new("cached", "WallClock.now"), |b| {
            b.iter_custom(|iters| {
                let mut children = Vec::with_capacity(num_threads);
                let start = std::time::Instant::now();
                for _i in 0..num_threads {
                    children.push(std::thread::spawn(move || {
                        for _i in 0..iters {
                            std::hint::black_box(WallClock.now());
                        }
                    }));
                }
                for child in children {
                    child.join().unwrap()
                }
                start.elapsed()
            });
        })
        .bench_function(BenchmarkId::new("uncached", "SystemTime::now"), |b| {
            b.iter_custom(|iters| {
                let mut children = Vec::with_capacity(num_threads);
                let start = std::time::Instant::now();
                for _i in 0..num_threads {
                    children.push(std::thread::spawn(move || {
                        for _i in 0..iters {
                            std::hint::black_box(MillisSinceEpoch::from(SystemTime::now()));
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
    targets = wall_clock_single_threaded, wall_clock_multi_threaded
);

criterion_main!(benches);
