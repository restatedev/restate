// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A multi-threaded stress test to see if we can overflow the HLC logical counter.
//!
//! This variant uses WallClock with AtomicStorage for multi-threaded access.
//!
//! The logical counter is 22 bits (max value 4,194,303). This test spawns threads
//! on all available cores, each calling `next()` in a tight loop on a shared HLC
//! clock to see if we can overflow the counter within a single millisecond.
//!
//! Run with: cargo run --example brrr_multi -p restate-clock --features hlc --release

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use restate_clock::{AtomicStorage, ClockUpkeep, HlcClock, WallClock};

/// Maximum value for 22-bit logical counter
const LC_MAX: u64 = (1 << 22) - 1;

/// Number of calls each thread will make
const CALLS_PER_THREAD: u64 = 1_000_000;

/// Per-thread statistics
struct ThreadStats {
    calls: u64,
    max_logical: u64,
    elapsed_ms: f64,
}

fn main() {
    let num_cpus = thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    println!("=== HLC Logical Counter Overflow Test (Multi-threaded) ===\n");
    println!("Physical clock:         WallClock (SystemTime based)");
    println!("Storage:                AtomicStorage");
    println!("Logical counter bits:   22");
    println!("Max logical counter:    {LC_MAX} (0x{LC_MAX:x})");
    println!("Number of threads:      {num_cpus}");
    println!("Calls per thread:       {CALLS_PER_THREAD}");
    println!();
    let _upkeep = ClockUpkeep::start().expect("failed to start upkeep");

    // Create a shared atomic HLC clock
    let hlc = HlcClock::new(
        NonZeroUsize::new(5000), // 5 second max drift
        WallClock,
        Arc::new(AtomicStorage::default()),
    )
    .expect("failed to create HLC clock");

    let start_ts = hlc.next();
    println!("Starting timestamp:     {start_ts:?}");
    println!("\nSpawning {num_cpus} threads, each making {CALLS_PER_THREAD} calls...\n");

    let start = Instant::now();

    // Spawn worker threads - each runs independently with no shared coordination state
    let handles: Vec<_> = (0..num_cpus)
        .map(|_| {
            let hlc = hlc.clone();
            thread::spawn(move || {
                let mut stats = ThreadStats {
                    calls: 0,
                    max_logical: 0,
                    elapsed_ms: 0.0,
                };

                let thread_start = Instant::now();

                // Fixed iteration count - no shared atomic for coordination
                for _ in 0..CALLS_PER_THREAD {
                    let ts = hlc.next();
                    stats.calls += 1;
                    let logical = ts.as_u64() & LC_MAX;

                    // Track max locally - no shared atomic
                    if logical > stats.max_logical {
                        stats.max_logical = logical;
                    }
                }

                stats.elapsed_ms = thread_start.elapsed().as_secs_f64() * 1000.0;
                stats
            })
        })
        .collect();

    // Collect results from all threads
    let mut total_calls: u64 = 0;
    let mut max_logical_seen: u64 = 0;
    let mut max_thread_time_ms: f64 = 0.0;

    println!("Thread results:");
    for (i, handle) in handles.into_iter().enumerate() {
        let stats = handle.join().expect("thread panicked");
        println!(
            "  Thread {i:2}: {:>10} calls in {:>8.2}ms, max logical: {:>7}",
            stats.calls, stats.elapsed_ms, stats.max_logical
        );
        total_calls += stats.calls;
        max_logical_seen = max_logical_seen.max(stats.max_logical);
        max_thread_time_ms = max_thread_time_ms.max(stats.elapsed_ms);
    }

    let elapsed = start.elapsed();
    let final_ts = hlc.snapshot();

    println!("\n=== Results ===");
    println!(
        "Wall clock time:        {:.2}ms",
        elapsed.as_secs_f64() * 1000.0
    );
    println!("Total next() calls:     {total_calls}");
    println!(
        "Max logical seen:       {max_logical_seen} ({:.2}% of max)",
        (max_logical_seen as f64 / LC_MAX as f64) * 100.0
    );
    println!("Final timestamp:        {final_ts:?}");

    // Use wall clock time for throughput calculation
    let elapsed_ms = elapsed.as_millis() as u64;
    if elapsed_ms > 0 {
        let calls_per_ms = total_calls / elapsed_ms;
        let pct_of_max = (calls_per_ms as f64 / LC_MAX as f64) * 100.0;
        println!("Throughput:             {calls_per_ms} calls/ms ({pct_of_max:.2}% of max)");
        println!(
            "Per-thread throughput:  {} calls/ms",
            calls_per_ms / num_cpus as u64
        );

        if calls_per_ms > LC_MAX {
            println!("\n!!! DANGER: Throughput exceeds LC_MAX !!!");
            println!("This system CAN overflow the logical counter!");
        } else if max_logical_seen > LC_MAX - 1000 {
            println!("\n!! WARNING: Got within 1000 of overflow!");
            println!("This system is at RISK of logical counter overflow under load.");
        } else {
            let headroom = LC_MAX - calls_per_ms;
            let safety_factor = LC_MAX as f64 / calls_per_ms as f64;
            println!("Headroom:               {headroom} ({safety_factor:.1}x safety factor)");
            println!("\nThis system appears SAFE from logical counter overflow.");
        }
    }
}
