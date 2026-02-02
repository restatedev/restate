// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile-time generated string interning for ID types.
//!
//! This module provides zero-allocation string lookups for common ID types.
//! IDs 0-1024 use compile-time generated strings, while IDs > 1024 fall back
//! to a lazily-populated cache.

use std::sync::{Mutex, OnceLock};

include!(concat!(env!("OUT_DIR"), "/short_id_strings.rs"));

/// Maximum ID with a pre-computed string (inclusive).
const MAX_PRECOMPUTED_ID: u64 = 1024;

/// Overflow storage for IDs > 1024 (extremely rare in practice).
static OVERFLOW_ID_STRINGS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();
static OVERFLOW_NODE_ID_STRINGS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();

/// Returns a static string representation of the given numeric ID.
/// Optimized for IDs 0-1024 (compile-time generated, single array lookup).
#[inline]
pub(crate) fn id_to_str(id: u64) -> &'static str {
    if id <= MAX_PRECOMPUTED_ID {
        // SAFETY: bounds check above, MAX_PRECOMPUTED_ID < METRIC_ID_STRINGS.len()
        return SHORT_ID_STRINGS[id as usize];
    }

    // Cold path: IDs > 1024 (should be extremely rare)
    overflow_id_lookup(id)
}

#[cold]
#[inline(never)]
fn overflow_id_lookup(id: u64) -> &'static str {
    let overflow = OVERFLOW_ID_STRINGS.get_or_init(|| Mutex::new(Vec::new()));
    let idx = (id - MAX_PRECOMPUTED_ID - 1) as usize;

    let mut guard = overflow.lock().unwrap();

    // Extend if necessary
    if idx >= guard.len() {
        guard.resize(idx + 1, "");
    }

    if guard[idx].is_empty() {
        // Leak the string - this only happens once per ID
        guard[idx] = id.to_string().leak();
    }

    guard[idx]
}

/// Returns a static N-prefixed string for the given node ID.
/// Optimized for IDs 0-1024 (compile-time generated, single array lookup).
#[inline]
pub(crate) fn node_id_to_str(id: u32) -> &'static str {
    if (id as u64) <= MAX_PRECOMPUTED_ID {
        // SAFETY: bounds check above
        return NODE_ID_STRINGS[id as usize];
    }

    // Cold path: IDs > 1024 (should be extremely rare)
    overflow_node_id_lookup(id)
}

#[cold]
#[inline(never)]
fn overflow_node_id_lookup(id: u32) -> &'static str {
    let overflow = OVERFLOW_NODE_ID_STRINGS.get_or_init(|| Mutex::new(Vec::new()));
    let idx = (id as u64 - MAX_PRECOMPUTED_ID - 1) as usize;

    let mut guard = overflow.lock().unwrap();

    if idx >= guard.len() {
        guard.resize(idx + 1, "");
    }

    if guard[idx].is_empty() {
        guard[idx] = format!("N{id}").leak();
    }

    guard[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_to_str() {
        // Fast path
        assert_eq!(id_to_str(0), "0");
        assert_eq!(id_to_str(1), "1");
        assert_eq!(id_to_str(1024), "1024");

        // Overflow path
        assert_eq!(id_to_str(1025), "1025");
        assert_eq!(id_to_str(2000), "2000");
    }

    #[test]
    fn test_node_id_to_str() {
        // Fast path
        assert_eq!(node_id_to_str(0), "N0");
        assert_eq!(node_id_to_str(1), "N1");
        assert_eq!(node_id_to_str(1024), "N1024");

        // Overflow path
        assert_eq!(node_id_to_str(1025), "N1025");
        assert_eq!(node_id_to_str(2000), "N2000");
    }

    /// Stress test: Verify no contention issues when multiple threads
    /// simultaneously access overflow IDs.
    ///
    /// This test spawns multiple threads that all try to access the same
    /// overflow IDs concurrently, which exercises the mutex-protected
    /// lazy initialization path.
    #[test]
    fn test_overflow_contention() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;
        use std::time::{Duration, Instant};

        const NUM_THREADS: usize = 16;
        const ITERATIONS_PER_THREAD: usize = 10_000;
        // Use IDs in the overflow range (> 1024)
        const OVERFLOW_ID_START: u64 = 5000;
        const NUM_OVERFLOW_IDS: u64 = 100;

        let success_count = Arc::new(AtomicUsize::new(0));
        let start_barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));

        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|thread_id| {
                let success_count = Arc::clone(&success_count);
                let barrier = Arc::clone(&start_barrier);

                thread::spawn(move || {
                    // Wait for all threads to be ready
                    barrier.wait();

                    for i in 0..ITERATIONS_PER_THREAD {
                        // Each thread accesses different overflow IDs to stress test
                        // the Vec growth and lazy initialization
                        let id = OVERFLOW_ID_START
                            + ((thread_id as u64 * 7 + i as u64) % NUM_OVERFLOW_IDS);
                        let s = id_to_str(id);

                        // Verify correctness
                        assert_eq!(s, id.to_string());
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        let start = Instant::now();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let elapsed = start.elapsed();
        let total_ops = success_count.load(Ordering::Relaxed);

        assert_eq!(
            total_ops,
            NUM_THREADS * ITERATIONS_PER_THREAD,
            "All operations should complete successfully"
        );

        // Sanity check: should complete reasonably fast (< 5 seconds)
        // This would fail if there's severe lock contention
        assert!(
            elapsed < Duration::from_secs(5),
            "Contention test took too long: {:?} (expected < 5s). Possible lock contention issue.",
            elapsed
        );

        // Print throughput for informational purposes
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
        eprintln!(
            "Overflow contention test: {} ops in {:?} ({:.0} ops/sec)",
            total_ops, elapsed, ops_per_sec
        );
    }

    /// Stress test for node_id overflow path
    #[test]
    fn test_node_id_overflow_contention() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        const NUM_THREADS: usize = 8;
        const ITERATIONS_PER_THREAD: usize = 5_000;
        const OVERFLOW_ID_START: u32 = 3000;
        const NUM_OVERFLOW_IDS: u32 = 50;

        let success_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));

        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|thread_id| {
                let success_count = Arc::clone(&success_count);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..ITERATIONS_PER_THREAD {
                        let id = OVERFLOW_ID_START
                            + ((thread_id as u32 * 3 + i as u32) % NUM_OVERFLOW_IDS);
                        let s = node_id_to_str(id);
                        assert_eq!(s, format!("N{id}"));
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(
            success_count.load(Ordering::Relaxed),
            NUM_THREADS * ITERATIONS_PER_THREAD
        );
    }

    /// Stress test: Mixed fast-path and overflow access
    ///
    /// This simulates realistic usage where most accesses hit the fast path
    /// but occasionally overflow IDs are accessed.
    #[test]
    fn test_mixed_fast_and_overflow_contention() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        const NUM_THREADS: usize = 8;
        const ITERATIONS_PER_THREAD: usize = 50_000;

        let fast_path_count = Arc::new(AtomicUsize::new(0));
        let overflow_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));

        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let fast_count = Arc::clone(&fast_path_count);
                let overflow_cnt = Arc::clone(&overflow_count);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..ITERATIONS_PER_THREAD {
                        // 99% fast path, 1% overflow (realistic distribution)
                        let id = if i % 100 == 0 {
                            overflow_cnt.fetch_add(1, Ordering::Relaxed);
                            1025 + (i as u64 % 100) // Overflow range
                        } else {
                            fast_count.fetch_add(1, Ordering::Relaxed);
                            i as u64 % 256 // Fast path range (typical partition count)
                        };

                        let s = id_to_str(id);
                        assert_eq!(s, id.to_string());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let fast = fast_path_count.load(Ordering::Relaxed);
        let overflow = overflow_count.load(Ordering::Relaxed);

        eprintln!(
            "Mixed contention test: {} fast-path ops, {} overflow ops",
            fast, overflow
        );

        // Verify roughly 99/1 distribution
        assert!(fast > overflow * 90, "Fast path should dominate");
    }
}
