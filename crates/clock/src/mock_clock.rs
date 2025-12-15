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
use std::sync::atomic::{AtomicU64, Ordering};

use crate::time::MillisSinceEpoch;
use crate::{Clock, RESTATE_EPOCH, WallClock};

#[derive(Clone)]
pub struct MockClock {
    storage: Arc<AtomicU64>,
}

impl Default for MockClock {
    fn default() -> Self {
        let clock = Self {
            storage: Arc::new(AtomicU64::default()),
        };
        clock.refresh_from_wall_clock();
        clock
    }
}

impl MockClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timestamp(timestamp: MillisSinceEpoch) -> Self {
        assert!(
            timestamp > RESTATE_EPOCH,
            "Restate requires a wall clock set after Sat Jan 01 2022 00:00:00 GMT+0000"
        );

        Self {
            storage: Arc::new(AtomicU64::new(timestamp.as_u64())),
        }
    }

    pub fn advance_ms(&self, ms: u64) {
        self.storage.fetch_add(ms, Ordering::SeqCst);
    }

    pub fn refresh_from_wall_clock(&self) {
        self.set(WallClock.now());
    }

    pub fn set(&self, timestamp: MillisSinceEpoch) {
        assert!(
            timestamp > RESTATE_EPOCH,
            "Restate requires a wall clock set after Sat Jan 01 2022 00:00:00 GMT+0000"
        );
        self.storage.store(timestamp.as_u64(), Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::new(self.storage.load(Ordering::SeqCst))
    }
}
