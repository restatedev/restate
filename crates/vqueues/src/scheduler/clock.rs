// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;
use std::time::Duration;

use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;

struct Datum {
    // NOTE: temporary, will be replaced in subsequent commits
    millis_origin: MillisSinceEpoch,
    ts_origin: UniqueTimestamp,
    tokio_origin: tokio::time::Instant,
}

static DATUM: LazyLock<Datum> = LazyLock::new(|| {
    let millis_origin = MillisSinceEpoch::now();
    Datum {
        millis_origin,
        ts_origin: UniqueTimestamp::from_unix_millis(millis_origin)
            .expect("clock does not overflow"),
        tokio_origin: tokio::time::Instant::now(),
    }
});

/// A clock that tracks the physical clock of the scheduler that is synchronized
/// with the UniqueTimestamp physical clock.
#[derive(Debug, Copy, Clone, Default)]
pub struct SchedulerClock;

impl SchedulerClock {
    pub fn now_ts(&self) -> UniqueTimestamp {
        DATUM
            .ts_origin
            .add_millis(DATUM.tokio_origin.elapsed().as_millis() as u64)
            .expect("clock doesn't overflow")
    }

    pub fn now_millis(&self) -> MillisSinceEpoch {
        DATUM.millis_origin + DATUM.tokio_origin.elapsed()
    }

    /// Calculates a future tokio Instant from the given `MillisSinceEpoch`.
    ///
    /// Returns the clock datum/origin point if the input is in the past.
    pub fn millis_to_future_instant(&self, millis: MillisSinceEpoch) -> tokio::time::Instant {
        let delta = millis.saturating_sub_ms(DATUM.millis_origin);
        DATUM.tokio_origin + Duration::from_millis(delta)
    }
}

impl gardal::Clock for SchedulerClock {
    fn now(&self) -> f64 {
        // This calculates the new physical clock (ms since restate epoch)
        // by offsetting the origin ts with the monotonic clock elapsed time since
        // the clock was created.
        //
        // In a future change, this will be part of the global HLC clock that is updated
        // via a background thread for coarse time updates.
        self.now_ts().as_secs_f64()
    }
}
