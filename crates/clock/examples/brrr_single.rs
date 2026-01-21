// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A single-threaded stress test to see if we can overflow the HLC logical counter.
//!
//! The logical counter is 22 bits (max value 4,194,303). This test calls `next()`
//! in a tight loop to measure the maximum throughput without any contention.
//!
//! Run with: cargo run --example brrr_single -p restate-clock --features hlc --release

use std::num::NonZeroUsize;
use std::time::Instant;

use restate_clock::{ClockUpkeep, HlcClock, LocalStorage, WallClock};

/// Maximum value for 22-bit logical counter
const LC_MAX: u64 = (1 << 22) - 1;

/// Number of calls to make
const TOTAL_CALLS: u64 = 10_000_000;

fn main() {
    println!("=== HLC Logical Counter Overflow Test (Single-threaded) ===\n");
    println!("Physical clock:         SystemClock");
    println!("Storage:                LocalStorage (non-atomic)");
    println!("Logical counter bits:   22");
    println!("Max logical counter:    {LC_MAX} (0x{LC_MAX:x})");
    println!("Total calls:            {TOTAL_CALLS}");
    println!();

    let _upkeep = ClockUpkeep::start().expect("failed to start upkeep");
    // Create a local HLC clock - no atomic overhead
    let hlc = HlcClock::new(
        NonZeroUsize::new(5000), // 5 second max drift
        WallClock,
        LocalStorage::default(),
    )
    .expect("failed to create HLC clock");

    let start_ts = hlc.next();
    println!("Starting timestamp:     {start_ts:?}");
    println!("\nRunning {TOTAL_CALLS} calls in tight loop...\n");

    let mut calls: u64 = 0;
    let mut max_logical: u64 = 0;

    let start = Instant::now();

    for _ in 0..TOTAL_CALLS {
        let ts = hlc.next();
        calls += 1;
        let logical = ts.as_u64() & LC_MAX;

        if logical > max_logical {
            max_logical = logical;
        }
    }

    let elapsed = start.elapsed();
    let final_ts = hlc.snapshot();

    println!("=== Results ===");
    println!(
        "Wall clock time:        {:.2}ms",
        elapsed.as_secs_f64() * 1000.0
    );
    println!("Total next() calls:     {calls}");
    println!(
        "Max logical seen:       {max_logical} ({:.2}% of max)",
        (max_logical as f64 / LC_MAX as f64) * 100.0
    );
    println!("Final timestamp:        {final_ts:?}");

    let elapsed_ms = elapsed.as_millis() as u64;
    if elapsed_ms > 0 {
        let calls_per_ms = calls / elapsed_ms;
        let pct_of_max = (calls_per_ms as f64 / LC_MAX as f64) * 100.0;
        println!("Throughput:             {calls_per_ms} calls/ms ({pct_of_max:.2}% of max)");

        if calls_per_ms > LC_MAX {
            println!("\n!!! DANGER: Throughput exceeds LC_MAX !!!");
            println!("This system CAN overflow the logical counter!");
        } else if max_logical > LC_MAX - 1000 {
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
