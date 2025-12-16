// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::{Clock, RESTATE_EPOCH, WallClock};

/// Defines the frequency of updating the cached wall clock.
const UPKEEP_INTERVAL: Duration = const { Duration::from_micros(500) };

pub struct ClockUpkeep {
    running: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ClockUpkeep {
    /// Starts the clock upkeep thread, periodically updating the global physical clock and
    /// the coarse monotonic clock.
    ///
    /// The returned handle is a drop guard for the upkeep thread if it is successfully spawned.
    /// Dropping the handle will stop the thread, so the handle must be held.
    ///
    /// # Important
    ///
    /// This function must be called as early as possible during server/binary initialization, before
    /// any code attempts to read from [`WallClock::recent_ms()`](crate::WallClock::recent_ms).
    ///
    /// The server's `main()` function is responsible for starting the upkeep thread before
    /// any other initialization occurs.
    ///
    /// # Errors
    ///
    /// If there was an issue spawning the thread.
    pub fn start() -> Result<Self, std::io::Error> {
        // perform an immediate update
        WallClock::update_recent();

        let unix_millis = WallClock.now();
        assert!(
            unix_millis > RESTATE_EPOCH,
            "Restate requires a wall clock set after Sat Jan 01 2022 00:00:00 GMT+0000. Detected wall clock timestamp is {unix_millis}"
        );

        let running = Arc::new(AtomicBool::new(true));
        let handle: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name("rs-clock".to_owned())
            .spawn({
                let running = running.clone();
                move || {
                    while running.load(Ordering::Relaxed) {
                        std::thread::sleep(UPKEEP_INTERVAL);
                        WallClock::update_recent();
                    }
                }
            })?;
        Ok(Self {
            handle: Some(handle),
            running,
        })
    }
}

impl Drop for ClockUpkeep {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            let _result = handle
                .join()
                .map_err(|_| std::io::Error::other("failed to stop rs-clock thread"));
        }
    }
}
