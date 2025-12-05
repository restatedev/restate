// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_types::clock::UniqueTimestamp;

/// A clock that tracks the physical clock of the scheduler that is synchronized
/// with the UniqueTimestamp physical clock.
pub struct SchedulerClock {
    tokio_origin: tokio::time::Instant,
    ts_origin: UniqueTimestamp,
}

impl SchedulerClock {
    /// `now` is the current physical clock that corresponds to `Instant::now()` at the time
    /// of construction.
    pub fn new(now: UniqueTimestamp) -> Self {
        Self {
            tokio_origin: tokio::time::Instant::now(),
            ts_origin: now,
        }
    }

    #[allow(dead_code)]
    pub fn origin_instant(&self) -> tokio::time::Instant {
        self.tokio_origin
    }

    pub fn now_ts(&self) -> UniqueTimestamp {
        let delta = self.tokio_origin.elapsed().as_millis() as u64;
        self.ts_origin
            .add_millis(delta)
            .expect("clock doesn't overflow")
    }

    /// Calculates a future tokio Instant from the physical clock of `ts`.
    ///
    /// Returns the clock datum/origin point if the input `ts` is in the past.
    pub fn ts_to_future_instant(&self, ts: UniqueTimestamp) -> tokio::time::Instant {
        let delta = ts.milliseconds_since(self.ts_origin);
        self.tokio_origin + Duration::from_millis(delta)
    }
}
