// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime};

use restate_encoding::{BilrostNewType, NetSerde};

use crate::WallClock;

/// Milliseconds since the unix epoch
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct MillisSinceEpoch(u64);

impl MillisSinceEpoch {
    pub const UNIX_EPOCH: Self = Self::new(0);
    pub const MAX: Self = Self::new(u64::MAX);

    pub const fn new(millis_since_epoch: u64) -> Self {
        Self(millis_since_epoch)
    }

    /// Returns the current unix timestamp in milliseconds.
    ///
    /// This method uses the cached [`WallClock::recent_ms()`] timestamp when available,
    /// providing ~100x better performance than a direct `SystemTime::now()` syscall.
    /// The cached value is refreshed every 500μs by [`ClockUpkeep`](crate::ClockUpkeep),
    /// so it may be up to ~1ms stale.
    ///
    /// # Fallback Behavior
    ///
    /// If [`ClockUpkeep`](crate::ClockUpkeep) has not been started (e.g., in tests or
    /// standalone tools), this falls back to `SystemTime::now()`. The fallback path
    /// is marked `#[cold]` to hint to the compiler that it's not the expected hot path.
    ///
    /// # Usage
    ///
    /// For production server binaries, ensure [`ClockUpkeep::start()`](crate::ClockUpkeep::start)
    /// is called early in initialization to enable the fast path.
    pub fn now() -> Self {
        let recent = WallClock::recent_ms();
        if recent.0 > 0 {
            recent
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep it non-inlined to hint to the compiler that it's unlikely
            // the case.
            Self::from_system_now()
        }
    }

    // a hint to the compiler that this is unlikely to be called in the hot path.
    #[cold]
    #[inline(never)]
    fn from_system_now() -> Self {
        WallClock::now_ms()
    }

    #[inline]
    pub fn after(duration: Duration) -> Self {
        Self::now() + duration
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// Note, this doesn't fail if the timestamp is higher than Timestamp::MAX instead
    /// it returns the default value (now). There are no practical cases where this can happen
    /// so it's decided to do this for API convenience.
    #[cfg(feature = "jiff")]
    pub fn into_timestamp(self) -> jiff::Timestamp {
        jiff::Timestamp::from_millisecond(self.0 as i64).unwrap_or_default()
    }

    /// Returns zero duration if self is in the future. Should not be used where monotonic
    /// clock/duration is expected.
    pub fn elapsed(&self) -> Duration {
        let now = Self::now();
        Duration::from_millis(now.0.saturating_sub(self.0))
    }

    /// Calculates the number of milliseconds between this timestamp and the earlier timestamp
    /// If the earlier timestamp is later than this timestamp, this will return zero.
    pub fn saturating_sub_ms(&self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl From<MillisSinceEpoch> for u64 {
    #[inline]
    fn from(value: MillisSinceEpoch) -> Self {
        value.0
    }
}

impl Add<Duration> for MillisSinceEpoch {
    type Output = MillisSinceEpoch;

    fn add(self, rhs: Duration) -> Self::Output {
        MillisSinceEpoch(self.0.saturating_add(
            u64::try_from(rhs.as_millis()).expect("millis since Unix epoch should fit in u64"),
        ))
    }
}

impl Sub<Duration> for MillisSinceEpoch {
    type Output = MillisSinceEpoch;

    fn sub(self, rhs: Duration) -> Self::Output {
        MillisSinceEpoch(self.0.saturating_sub(
            u64::try_from(rhs.as_millis()).expect("millis since Unix epoch should fit in u64"),
        ))
    }
}

impl From<u64> for MillisSinceEpoch {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<SystemTime> for MillisSinceEpoch {
    fn from(value: SystemTime) -> Self {
        MillisSinceEpoch::new(
            u64::try_from(
                value
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("duration since Unix epoch should be well-defined")
                    .as_millis(),
            )
            .expect("millis since Unix epoch should fit in u64"),
        )
    }
}

#[cfg(feature = "prost-types")]
/// # Panics
/// If timestamp is out of range (e.g. older than UNIX_EPOCH) this conversion will panic.
impl From<prost_types::Timestamp> for MillisSinceEpoch {
    fn from(value: prost_types::Timestamp) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        Self::from(std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH"))
    }
}

#[cfg(feature = "prost-types")]
impl From<MillisSinceEpoch> for prost_types::Timestamp {
    fn from(value: MillisSinceEpoch) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        Self::from(std::time::SystemTime::from(value))
    }
}

impl Display for MillisSinceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ms since epoch", self.0)
    }
}

impl From<MillisSinceEpoch> for SystemTime {
    fn from(value: MillisSinceEpoch) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::from_millis(value.as_u64()))
    }
}

/// Nanos since the unix epoch. Used internally to get rough latency measurements across nodes.
/// It's vulnerable to clock skews and sync issues, so use with care. That said, it's fairly
/// accurate when used on the same node. This roughly maps to std::time::Instant except that the
/// value is portable across nodes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct NanosSinceEpoch(u64);

impl NanosSinceEpoch {
    pub fn now() -> Self {
        let recent_us = WallClock::recent_us();
        if recent_us > 0 {
            // the input is in microseconds, so we scale it to nanoseconds
            Self(recent_us * 1_000)
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep is non-inlined to hint to the compiler that it's unlikely
            // the case.
            Self::from_system_now()
        }
    }

    // a hint to the compiler that this is unlikely to be called in the hot path.
    #[cold]
    #[inline(never)]
    fn from_system_now() -> Self {
        Self(WallClock::now_us() * 1_000)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Returns zero duration if self is in the future. Should not be used where monotonic
    /// clock/duration is expected.
    pub fn elapsed(&self) -> Duration {
        let now = Self::now();
        Duration::from_nanos(now.0.saturating_sub(self.0))
    }
}

impl Default for NanosSinceEpoch {
    fn default() -> Self {
        Self::now()
    }
}

impl From<u64> for NanosSinceEpoch {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<SystemTime> for NanosSinceEpoch {
    fn from(value: SystemTime) -> Self {
        Self(
            u64::try_from(
                value
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("duration since Unix epoch should be well-defined")
                    .as_nanos(),
            )
            .expect("nanos since Unix epoch should fit in u64"),
        )
    }
}

#[cfg(feature = "prost-types")]
/// # Panics
/// If timestamp is out of range (e.g. older than UNIX_EPOCH) this conversion will panic.
impl From<prost_types::Timestamp> for NanosSinceEpoch {
    fn from(value: prost_types::Timestamp) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        let ts = std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH");
        Self::from(ts)
    }
}

impl From<NanosSinceEpoch> for MillisSinceEpoch {
    fn from(value: NanosSinceEpoch) -> Self {
        Self(value.0 / 1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::SystemTime;

    #[test]
    fn millis_should_not_overflow() {
        let t: SystemTime = MillisSinceEpoch::new(u64::MAX).into();
        println!("{t:?}");
    }

    #[test]
    fn nanos_should_not_overflow() {
        // it's ~580 years from unix epoch until u64 wouldn't become sufficient to store nanos.
        let t = NanosSinceEpoch::now().as_u64();
        assert!(t < u64::MAX);
        println!("{t:?}");
    }

    #[test]
    fn elapsed_saturating_to_zero() {
        let future = SystemTime::now().add(Duration::from_secs(10));
        let ms_epoch = MillisSinceEpoch::from(future);
        let ns_epoch = NanosSinceEpoch::from(future);

        assert_eq!(ms_epoch.elapsed(), Duration::ZERO);
        assert_eq!(ns_epoch.elapsed(), Duration::ZERO);
    }
}
