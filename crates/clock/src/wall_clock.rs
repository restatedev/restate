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
static RECENT_UNIX_TIMESTAMP_MS: AtomicU64 = const { AtomicU64::new(0) };

#[derive(Default, Copy, Clone)]
pub struct WallClock;

impl WallClock {
    /// Updates the cached recent timestamp.
    /// This is intended to be called exclusively from the clock upkeep thread.
    pub(crate) fn update_recent() {
        RECENT_UNIX_TIMESTAMP_MS.store(Self::unix_now().as_u64(), Ordering::Relaxed);
    }

    pub fn now() -> MillisSinceEpoch {
        // In tests, we don't care about performance, so we can always call now.
        if cfg!(feature = "test-util") {
            Self::unix_now()
        } else {
            let recent = RECENT_UNIX_TIMESTAMP_MS.load(Ordering::Relaxed);
            // todo: consider removing if we can prove that now() will not be called
            // in places where the upkeep thread is not running.
            if recent == 0 {
                return refresh();
            }
            MillisSinceEpoch::new(recent)
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

// tldr; a poor's man `hint::unlikely`
#[cold]
#[inline(never)]
fn refresh() -> MillisSinceEpoch {
    eprintln!("Restate clock is not running, refreshing...");
    WallClock::update_recent();
    let recent = RECENT_UNIX_TIMESTAMP_MS.load(Ordering::Relaxed);
    assert!(recent > 0);
    MillisSinceEpoch::new(recent)
}

impl Clock for WallClock {
    fn now(&self) -> MillisSinceEpoch {
        WallClock::now()
    }
}
