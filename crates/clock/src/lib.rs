// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "test-util")]
mod mock_clock;
pub mod time;
mod upkeep;
mod wall_clock;

#[cfg(feature = "test-util")]
pub use mock_clock::MockClock;
pub use upkeep::ClockUpkeep;
pub use wall_clock::WallClock;

/// A trait for accessing physical/wall clock timestamps.
///
/// This trait provides two methods for reading the current time, offering a trade-off
/// between precision and performance:
///
/// - [`recent()`](Clock::recent): Returns a cached timestamp with ~100x better performance
///   but potentially up to ~1ms stale (refreshed every 500μs by [`ClockUpkeep`]).
/// - [`now()`](Clock::now): Returns a precise timestamp via a `SystemTime::now()` syscall/vDSO.
pub trait Clock {
    /// Returns a cached unix timestamp that may be up to ~1ms stale.
    ///
    /// This method reads from an atomic variable updated every 500μs by the
    /// [`ClockUpkeep`] background thread, avoiding syscall/vDSO overhead.
    ///
    /// # Performance
    ///
    /// ~100x faster than [`now()`](Clock::now) due to atomic read vs syscall/vDSO.
    ///
    /// # Requirements
    ///
    /// The [`ClockUpkeep`] thread must be running for this to return valid timestamps.
    /// If called before upkeep starts, returns `MillisSinceEpoch(0)`.
    fn recent(&self) -> crate::time::MillisSinceEpoch;

    /// Returns the current unix timestamp in milliseconds via `SystemTime::now()`.
    ///
    /// This method always makes a syscall/vDSO to get the precise current time.
    /// For hot paths where ~1ms staleness is acceptable, prefer [`recent()`](Clock::recent).
    fn now(&self) -> crate::time::MillisSinceEpoch;
}

impl<T: Clock> Clock for &T {
    #[inline]
    fn now(&self) -> crate::time::MillisSinceEpoch {
        T::now(self)
    }

    #[inline]
    fn recent(&self) -> crate::time::MillisSinceEpoch {
        T::recent(self)
    }
}

/// An anchor point for restate physical clock used in HLC timestamps.
///
/// RESTATE_EPOCH -> (2022-01-01 00:00:00 GMT)
const RESTATE_EPOCH: crate::time::MillisSinceEpoch =
    crate::time::MillisSinceEpoch::new(1_640_995_200_000);
