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
/// The resolution is in milliseconds.
///
/// Using this requires running the clock upkeep thread, if not, we'll always
/// return zero.
///
/// NOTE: This is only used if the feature `cached-time` is enabled.
static RECENT_UNIX_TIMESTAMP_MS: AtomicU64 = const { AtomicU64::new(0) };

#[derive(Default, Copy, Clone)]
pub struct WallClock;

impl WallClock {
    /// Updates the cached recent timestamp.
    /// This is intended to be called exclusively from the clock upkeep thread.
    pub(crate) fn update_recent() {
        RECENT_UNIX_TIMESTAMP_MS.store(Self::unix_now().as_u64(), Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn now() -> MillisSinceEpoch {
        if cfg!(feature = "cached-time") {
            MillisSinceEpoch::new(RECENT_UNIX_TIMESTAMP_MS.load(Ordering::Relaxed))
        } else {
            // In tests or binaries where the upkeep thread is not running, we don't
            // care about the performance of this call, so we can always call now()
            // but we keep is non-inlined to hint to the compiler that it's unlikely
            // the case.
            WallClock::unix_now()
        }
    }

    fn unix_now() -> MillisSinceEpoch {
        MillisSinceEpoch::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("duration since Unix epoch should be well-defined")
                .as_millis() as u64,
        )
    }
}

impl Clock for WallClock {
    fn now(&self) -> MillisSinceEpoch {
        WallClock::now()
    }
}
