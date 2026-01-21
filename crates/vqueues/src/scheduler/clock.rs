// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::WallClock;
use restate_types::time::MillisSinceEpoch;

const MICROS_PER_SEC: u32 = 1_000_000;

/// A wrapper around the wall clock, used by the scheduler to get the current wall
/// clock time and provides an implementation of the `gardal::Clock` trait.
#[derive(Debug, Copy, Clone, Default)]
pub struct SchedulerClock;

impl SchedulerClock {
    /// Returns the current unix timestamp in milliseconds
    pub fn now_millis(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::now()
    }
}

impl gardal::Clock for SchedulerClock {
    #[inline]
    fn now(&self) -> f64 {
        let recent_us = WallClock::recent_us();
        if recent_us > 0 {
            // the input is in microseconds, so we scale it to seconds (fractional)
            recent_us as f64 / MICROS_PER_SEC as f64
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep is non-inlined to hint to the compiler that it's unlikely
            // the case.
            from_wall_clock()
        }
    }
}

// a hint to the compiler that this is unlikely to be called in the hot path.
#[cold]
#[inline(never)]
fn from_wall_clock() -> f64 {
    WallClock::now_us() as f64 / MICROS_PER_SEC as f64
}
