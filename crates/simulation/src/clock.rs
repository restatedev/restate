// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic clock for simulation testing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use restate_types::time::MillisSinceEpoch;

/// A deterministic clock for simulation testing.
///
/// This clock can be advanced manually, providing full control over time progression
/// in the simulation. The clock is thread-safe and can be shared across components.
#[derive(Debug, Clone)]
pub struct SimulationClock {
    inner: Arc<SimulationClockInner>,
}

#[derive(Debug)]
struct SimulationClockInner {
    /// Current time in milliseconds since the Restate epoch
    current_time: AtomicU64,
}

impl SimulationClock {
    /// Creates a new simulation clock starting at the given time.
    pub fn new(start_time: MillisSinceEpoch) -> Self {
        Self {
            inner: Arc::new(SimulationClockInner {
                current_time: AtomicU64::new(start_time.as_u64()),
            }),
        }
    }

    /// Creates a new simulation clock starting at the Restate epoch (time 0).
    pub fn at_epoch() -> Self {
        Self::new(MillisSinceEpoch::UNIX_EPOCH)
    }

    /// Returns the current time.
    pub fn now(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.inner.current_time.load(Ordering::Acquire))
    }

    /// Advances the clock by the specified number of milliseconds.
    /// Returns the new current time.
    pub fn advance_ms(&self, millis: u64) -> MillisSinceEpoch {
        let new_time = self.inner.current_time.fetch_add(millis, Ordering::AcqRel) + millis;
        MillisSinceEpoch::new(new_time)
    }

    /// Advances the clock to the specified time.
    /// Panics if the target time is before the current time.
    pub fn advance_to(&self, target: MillisSinceEpoch) {
        let current = self.now();
        assert!(
            target >= current,
            "Cannot move clock backwards: current={}, target={}",
            current.as_u64(),
            target.as_u64()
        );
        self.inner
            .current_time
            .store(target.as_u64(), Ordering::Release);
    }

    /// Sets the clock to a specific time, allowing backward movement.
    /// Use with caution - primarily for test setup.
    pub fn set(&self, time: MillisSinceEpoch) {
        self.inner
            .current_time
            .store(time.as_u64(), Ordering::Release);
    }
}

impl Default for SimulationClock {
    fn default() -> Self {
        Self::at_epoch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_advance() {
        let clock = SimulationClock::at_epoch();
        assert_eq!(clock.now(), MillisSinceEpoch::UNIX_EPOCH);

        clock.advance_ms(1000);
        assert_eq!(clock.now().as_u64(), 1000);

        clock.advance_ms(500);
        assert_eq!(clock.now().as_u64(), 1500);
    }

    #[test]
    fn test_clock_advance_to() {
        let clock = SimulationClock::at_epoch();

        clock.advance_to(MillisSinceEpoch::new(5000));
        assert_eq!(clock.now().as_u64(), 5000);
    }

    #[test]
    #[should_panic(expected = "Cannot move clock backwards")]
    fn test_clock_advance_to_backwards_panics() {
        let clock = SimulationClock::new(MillisSinceEpoch::new(1000));
        clock.advance_to(MillisSinceEpoch::new(500));
    }

    #[test]
    fn test_clock_clone_shares_state() {
        let clock1 = SimulationClock::at_epoch();
        let clock2 = clock1.clone();

        clock1.advance_ms(1000);
        assert_eq!(clock2.now().as_u64(), 1000);
    }
}
