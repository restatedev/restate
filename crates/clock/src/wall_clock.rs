// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::time::MillisSinceEpoch;

use super::Clock;

/// The cached timestamp of the most recent updated unix timestamp.
///
/// # Important
///
/// This requires the clock upkeep thread ([`ClockUpkeep`](crate::ClockUpkeep)) to
/// be running. If `WallClock::recent_ms()` is called before the upkeep thread is started,
/// it will return `MillisSinceEpoch(0)`.
///
/// The server's `main()` function starts the upkeep thread as early as possible to remove
/// the possibility where this could occur.
static RECENT_UNIX_TIMESTAMP_US: AtomicU64 = const { AtomicU64::new(0) };

/// Production implementation of [`Clock`] backed by system time.
///
/// `WallClock` provides different ways to read the current time:
///
/// - [`now_ms()`](WallClock::now_ms): Precise timestamp (in milliseconds) via `SystemTime::now()` syscall/vDSO.
/// - [`now_us()`](WallClock::now_us): Precise timestamp (in microseconds) via `SystemTime::now()` syscall/vDSO.
/// - [`recent_ms()`](WallClock::recent_ms): Cached timestamp (in milliseconds) from an atomic variable, ~100x faster.
/// - [`recent_us()`](WallClock::recent_us): Cached timestamp (in microseconds) from an atomic variable, ~100x faster.
///
/// # Cached Time
///
/// The cached timestamp is stored in a global atomic and refreshed every 500μs by
/// [`ClockUpkeep`](crate::ClockUpkeep). This provides sub-nanosecond read performance
/// at the cost of up to ~1ms staleness.
///
/// # Example
///
/// ```ignore
/// use restate_clock::{Clock, ClockUpkeep, WallClock};
///
/// // Start the upkeep thread (required for `recent_ms()` to work)
/// let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep");
///
/// // Fast path: read cached timestamp (~100x faster)
/// let cached = WallClock::recent_ms();
///
/// // Precise path: syscall/vDSO to get exact time
/// let precise = WallClock::now_ms();
///
/// // Can also use via the Clock trait
/// let clock = WallClock;
/// let timestamp = clock.recent();
/// ```
///
/// # Thread Safety
///
/// `WallClock` is `Copy`, `Clone`, and can be safely shared across threads. The cached
/// timestamp uses relaxed atomic ordering, which is sufficient since absolute precision
/// is not required for the cached value.
#[derive(Default, Copy, Clone)]
pub struct WallClock;

impl WallClock {
    /// Updates the cached recent timestamp.
    ///
    /// This is intended to be called exclusively from the [`ClockUpkeep`](crate::ClockUpkeep)
    /// background thread every 500μs.
    pub(crate) fn update_recent() {
        RECENT_UNIX_TIMESTAMP_US.store(Self::now_us(), Ordering::Relaxed);
    }

    /// Returns the current unix timestamp in milliseconds via `SystemTime::now()`.
    ///
    /// This method always makes a syscall/vDSO. For hot paths where ~1ms staleness is
    /// acceptable, prefer [`recent_ms()`](WallClock::recent_ms).
    #[inline]
    pub fn now_ms() -> MillisSinceEpoch {
        MillisSinceEpoch::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("duration since Unix epoch should be well-defined")
                .as_millis() as u64,
        )
    }

    /// Returns the current unix timestamp in microseconds via `SystemTime::now()`.
    ///
    /// This method always makes a syscall/vDSO. For hot paths where ~1ms staleness is
    /// acceptable, prefer [`recent_us()`](WallClock::recent_us).
    #[inline]
    pub fn now_us() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("duration since Unix epoch should be well-defined")
            .as_micros() as u64
    }

    /// Returns a cached unix timestamp (in milliseconds) that may be up to ~1ms stale.
    ///
    /// This method reads from a global atomic variable updated every 500μs by
    /// [`ClockUpkeep`](crate::ClockUpkeep), providing ~100x better performance than
    /// [`now_ms()`](WallClock::now_ms).
    ///
    /// # Requirements
    ///
    /// [`ClockUpkeep`](crate::ClockUpkeep) must be running for this to return valid
    /// timestamps. Returns `MillisSinceEpoch(0)` if called before upkeep starts.
    #[inline]
    pub fn recent_ms() -> MillisSinceEpoch {
        MillisSinceEpoch::new(RECENT_UNIX_TIMESTAMP_US.load(Ordering::Relaxed) / 1_000)
    }

    /// Returns a cached unix timestamp (in microseconds) that may be up to ~1ms stale.
    ///
    /// This method reads from a global atomic variable updated every 500μs by
    /// [`ClockUpkeep`](crate::ClockUpkeep), providing ~100x better performance than
    /// [`now_us()`](WallClock::now_us).
    ///
    /// # Requirements
    ///
    /// [`ClockUpkeep`](crate::ClockUpkeep) must be running for this to return valid
    /// timestamps. Returns `MillisSinceEpoch(0)` if called before upkeep starts.
    #[inline]
    pub fn recent_us() -> u64 {
        RECENT_UNIX_TIMESTAMP_US.load(Ordering::Relaxed)
    }
}

impl Clock for WallClock {
    #[inline]
    fn now(&self) -> MillisSinceEpoch {
        WallClock::now_ms()
    }

    #[inline]
    fn recent(&self) -> MillisSinceEpoch {
        WallClock::recent_ms()
    }
}
