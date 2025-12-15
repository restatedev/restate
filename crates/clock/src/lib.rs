// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod time;
mod upkeep;
mod wall_clock;

pub use upkeep::ClockUpkeep;
pub use wall_clock::WallClock;

#[cfg(feature = "test-util")]
mod mock_clock;

#[cfg(feature = "test-util")]
pub use mock_clock::MockClock;

/// A trait for a physical/wall clock.
pub trait Clock {
    /// Returns the current unix timestamp in milliseconds.
    fn now(&self) -> crate::time::MillisSinceEpoch;
}

impl<T: Clock> Clock for &T {
    #[inline]
    fn now(&self) -> crate::time::MillisSinceEpoch {
        T::now(self)
    }
}

/// An anchor point for restate physical clock used in HLC timestamps.
///
/// RESTATE_EPOCH -> (2022-01-01 00:00:00 GMT)
const RESTATE_EPOCH: crate::time::MillisSinceEpoch =
    crate::time::MillisSinceEpoch::new(1_640_995_200_000);
