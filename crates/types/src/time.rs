// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use std::ops::Add;
use std::time::{Duration, SystemTime};

/// Milliseconds since the unix epoch
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct MillisSinceEpoch(u64);

impl MillisSinceEpoch {
    pub const UNIX_EPOCH: MillisSinceEpoch = MillisSinceEpoch::new(0);
    pub const MAX: MillisSinceEpoch = MillisSinceEpoch::new(u64::MAX);

    pub const fn new(millis_since_epoch: u64) -> Self {
        MillisSinceEpoch(millis_since_epoch)
    }

    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn elapsed(&self) -> Duration {
        let now = Self::now();
        Duration::from_millis(now.0 - self.0)
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

/// # Panics
/// If timestamp is out of range (e.g. older than UNIX_EPOCH) this conversion will panic.
impl From<prost_types::Timestamp> for MillisSinceEpoch {
    fn from(value: prost_types::Timestamp) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        let ts = std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH");
        Self::from(ts)
    }
}

impl From<MillisSinceEpoch> for prost_types::Timestamp {
    fn from(value: MillisSinceEpoch) -> Self {
        // safest approach is to convert into SystemTime first, then calculate distance to
        // UNIX_EPOCH
        let ts = std::time::SystemTime::try_from(value).expect("Timestamp is after UNIX_EPOCH");
        Self::from(ts)
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
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct NanosSinceEpoch(u64);

impl NanosSinceEpoch {
    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn elapsed(&self) -> Duration {
        let now = Self::now();
        Duration::from_nanos(now.0 - self.0)
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::SystemTime;

    #[test]
    fn millis_should_not_overflow() {
        let t: SystemTime = MillisSinceEpoch::new(u64::MAX).into();
        println!("{:?}", t);
    }

    #[test]
    fn nanos_should_not_overflow() {
        // it's ~580 years from unix epoch until u64 wouldn't become sufficient to store nanos.
        let t = NanosSinceEpoch::now().as_u64();
        assert!(t < u64::MAX);
        println!("{:?}", t);
    }
}
