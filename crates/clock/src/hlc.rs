// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use crate::RESTATE_EPOCH;
use crate::unique_timestamp::{LC_BITS, LC_MAX, PHY_TIME_MAX};

use super::storage::HlcClockStorage;
use super::{Clock, Error, UniqueTimestamp};

#[derive(Clone)]
pub struct HlcClock<C: Clock, S: HlcClockStorage> {
    clock: C,
    state: S,
    /// The maximum allowed drift in milliseconds
    max_drift: Option<NonZeroUsize>,
}

impl<C: Clock, S: HlcClockStorage> HlcClock<C, S> {
    /// Creates a new HLC clock with the given parameters.
    ///
    /// The `max_drift` parameter is the maximum allowed drift in milliseconds. Note that
    /// setting `max_drift` to `None` will effectively disable the drift check allowing
    /// the clock to be updated by any physical clock value in the future.
    pub fn new(max_drift: Option<NonZeroUsize>, clock: C, state: S) -> Result<Self, Error> {
        let unix_now = clock.now();
        assert!(
            unix_now > RESTATE_EPOCH,
            "physical clock must be positive, are you sure your wall clock is set\
                after Sat Jan 01 2022 00:00:00 GMT+0000?"
        );
        let now = UniqueTimestamp::try_from_unix_millis(unix_now)?;

        state.store(now.as_u64());

        Ok(Self {
            clock,
            state,
            max_drift,
        })
    }

    /// Returns a copy of the last timestamp emitted by this HLC clock.
    ///
    /// Note that this does not advance the physical/logical clocks. To advance the
    /// clock, call [Self::next].
    pub fn snapshot(&self) -> UniqueTimestamp {
        UniqueTimestamp::new(self.state.load())
    }

    /// Generate a new unique timestamp for a local or send event.
    pub fn next(&self) -> UniqueTimestamp {
        // For performance, we rely on the initial check that happened in new() and assume that
        // the physical clock will not be adjusted after program start and end up smaller than RESTATE_EPOCH.
        let phy_clock_now = self.clock.recent().as_u64();
        debug_assert!(phy_clock_now > 0);
        let phy_clock_now = phy_clock_now - RESTATE_EPOCH.as_u64();
        // HLC clock _requires_ that the upkeep to be running, or that the clock provider
        // has other means of providing a recent timestamp.

        let result = self.update_state(move |(latest_phy, latest_lc)| {
            // Update the physical time and increment the logical count.
            if latest_phy >= phy_clock_now {
                // We don't check logical counter for overflow here intentionally for performance.
                // It's extremely unlikely that we will ever overflow the logical counter, unless
                // the physical clock has slowed down or stopped.
                //
                // On M2 MacBook Pro, running brrr_single example on a release build gives us 7x
                // safety factor. Running multi-threaded test give us even more safety due to the
                // contention on the shared atomic (~159x safety factor).
                Ok(UniqueTimestamp::from_parts_unchecked(
                    latest_phy,
                    latest_lc + 1,
                ))
            } else {
                Ok(UniqueTimestamp::from_parts_unchecked(phy_clock_now, 0))
            }
        });
        // SAFETY: The closure above returns Ok() in all branches.
        unsafe { result.unwrap_unchecked() }
    }

    /// Updates the HLC clock from a timestamp received from a remote node
    ///
    /// On success, this returns a timestamp that is guaranteed to be greater than both
    /// the remote timestamp and all previously returned local timestamps.
    pub fn try_update(&self, incoming: UniqueTimestamp) -> Result<UniqueTimestamp, Error> {
        let max_drift = self.max_drift;
        // For performance, we rely on the initial check that happened in new() and assume that
        // the physical clock will not be adjusted after program start and end up smaller than RESTATE_EPOCH.
        //
        // Important: We do not get the physical clock inside the closure because we do not want
        // to exert excessive pressure by performing syscalls or vDSO calls when the clock is
        // contended.
        let phy_clock_now = self.clock.recent().as_u64() - RESTATE_EPOCH.as_u64();
        debug_assert!(phy_clock_now > 0);
        let (incoming_phy, incoming_lc) = incoming.split();

        self.update_state(move |(my_latest_phy, my_latest_lc)| {
            // Physical clock is ahead of both the incoming timestamp and the current state.
            if phy_clock_now > incoming_phy && phy_clock_now > my_latest_phy {
                // Update the clock state.
                return Ok(UniqueTimestamp::from_parts_unchecked(phy_clock_now, 0));
            }

            match incoming_phy.cmp(&my_latest_phy) {
                // Incoming timestamp is ahead of my latest timestamp.
                std::cmp::Ordering::Greater => {
                    if let Some(max_drift) = max_drift {
                        // Note that this is guaranteed to be non-negative because we handle
                        // that case above.
                        let drift = incoming_phy.saturating_sub(phy_clock_now) as usize;
                        if drift > max_drift.get() {
                            return Err(Error::UnacceptableClockDrift {
                                actual: drift,
                                max_drift: max_drift.get(),
                            });
                        }
                    }
                    UniqueTimestamp::try_from_parts(incoming_phy, incoming_lc + 1)
                }
                // Incoming timestamp in the past
                std::cmp::Ordering::Less => {
                    // Our physical clock is ahead of the incoming timestamp, so it remains
                    // unchanged. We only need to update the logical counter.
                    //
                    // Basically, equivalent to calling Self::next();
                    Ok(UniqueTimestamp::from_parts_unchecked(
                        my_latest_phy,
                        my_latest_lc + 1,
                    ))
                }
                // Timestamps are equal, decide based on the logical counter.
                std::cmp::Ordering::Equal => {
                    // Checked because while we trust ourselves, we don't necessarily trust the
                    // incoming logical counter value. Albeit, it's very unlikely for this to
                    // overflow.
                    UniqueTimestamp::try_from_parts(
                        my_latest_phy,
                        my_latest_lc.max(incoming_lc) + 1,
                    )
                }
            }
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_state(&self, timestamp: UniqueTimestamp) {
        self.state.store(timestamp.as_u64());
    }

    /// Update from (physical, logical) tuple to (physical, logical) tuple
    fn update_state<F>(&self, update_fn: F) -> Result<UniqueTimestamp, Error>
    where
        F: Fn((u64, u64)) -> Result<UniqueTimestamp, Error>,
    {
        let mut current = self.state.load();
        loop {
            let phy = (current >> LC_BITS) & PHY_TIME_MAX;
            let lc = current & LC_MAX;
            let updated = update_fn((phy, lc))?;

            match self.state.compare_exchange_weak(current, updated.as_u64()) {
                Ok(_) => return Ok(updated),
                Err(changed) => current = changed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::storage::AtomicStorage;
    use crate::{Clock, MockClock};

    use super::*;

    const MAX_DRIFT_5000: Option<NonZeroUsize> = NonZeroUsize::new(5000);

    /// Helper to get the HLC physical clock (millis since RESTATE_EPOCH) from a MockClock
    fn phy_clock(mock: &MockClock) -> u64 {
        mock.now().as_u64() - RESTATE_EPOCH.as_u64()
    }

    /// Test that validates HLC correctness under concurrent updates.
    ///
    /// Properties verified:
    /// 1. Per-thread monotonicity: Each thread sees strictly increasing timestamps
    /// 2. Global uniqueness: No two calls to next() return the same timestamp
    /// 3. Causality: The final timestamp is greater than all timestamps returned
    #[test]
    fn concurrent_hlc_correctness() {
        use std::collections::HashSet;

        use crate::{ClockUpkeep, WallClock};

        const NUM_THREADS: usize = 10;
        const OPS_PER_THREAD: usize = 1000;

        // Start the clock upkeep thread so that WallClock::recent() works
        let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep");

        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            WallClock,
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();
        let start_ts = hlc.next();

        let mut handles = Vec::with_capacity(NUM_THREADS);
        for _ in 0..NUM_THREADS {
            let hlc = hlc.clone();
            handles.push(std::thread::spawn(move || {
                let mut thread_timestamps = Vec::with_capacity(OPS_PER_THREAD);
                let mut prev: UniqueTimestamp = start_ts;

                for _ in 0..OPS_PER_THREAD {
                    let ts = hlc.next();

                    // Property 1: Per-thread monotonicity
                    assert!(
                        ts > prev,
                        "Per-thread monotonicity violated: {ts:?} should be > {prev:?}"
                    );

                    thread_timestamps.push(ts);
                    prev = ts;
                    // In constrained CI environments, give more chances to other threads to
                    // make progress.
                    std::thread::yield_now();
                }

                thread_timestamps
            }));
        }

        // Collect results after all threads complete - no synchronization during the test
        let all_ts: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // Verify we got the expected number of timestamps
        assert_eq!(all_ts.len(), NUM_THREADS * OPS_PER_THREAD);

        // Property 2: Global uniqueness - no duplicate timestamps across all threads
        let unique_set: HashSet<u64> = all_ts.iter().map(|ts| ts.as_u64()).collect();
        assert_eq!(
            unique_set.len(),
            all_ts.len(),
            "Global uniqueness violated: {} unique timestamps out of {} total",
            unique_set.len(),
            all_ts.len()
        );

        // Property 3: Final timestamp is greater than all returned timestamps
        let final_timestamp = hlc.snapshot();
        let max_returned = all_ts.iter().max().unwrap();
        assert!(
            final_timestamp >= *max_returned,
            "Final timestamp {final_timestamp:?} should be >= max returned {max_returned:?}"
        );

        println!(
            "Concurrent test passed: {} unique timestamps generated across {} threads",
            all_ts.len(),
            NUM_THREADS
        );
        println!("Start: {start_ts:#?}");
        println!("Final: {final_timestamp:?}");
    }

    /// Test that validates HLC correctness under concurrent updates with mixed
    /// next() and try_update() calls.
    ///
    /// Properties verified:
    /// 1. Per-thread monotonicity: Each thread sees strictly increasing timestamps
    /// 2. Global uniqueness: No two calls return the same timestamp
    /// 3. Causality: The final timestamp is greater than all timestamps returned
    #[test]
    fn concurrent_hlc_correctness_mixed_operations() {
        use std::collections::HashSet;

        use crate::{ClockUpkeep, WallClock};

        const NUM_THREADS: usize = 10;
        const OPS_PER_THREAD: usize = 1000;

        // Start the clock upkeep thread so that WallClock::recent() works
        let _upkeep = ClockUpkeep::start().expect("failed to start clock upkeep");

        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            WallClock,
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();
        let start_ts = hlc.next();

        let mut handles = Vec::with_capacity(NUM_THREADS);
        for thread_id in 0..NUM_THREADS {
            let hlc = hlc.clone();
            handles.push(std::thread::spawn(move || {
                let mut thread_timestamps = Vec::with_capacity(OPS_PER_THREAD);
                let mut prev: UniqueTimestamp = start_ts;

                for i in 0..OPS_PER_THREAD {
                    // Alternate between next() and try_update() based on thread_id and iteration
                    let ts = if (thread_id + i) % 3 == 0 {
                        // Use try_update with a timestamp derived from prev
                        // This simulates receiving a message with a timestamp
                        hlc.try_update(prev).unwrap()
                    } else {
                        hlc.next()
                    };

                    // Property 1: Per-thread monotonicity
                    assert!(
                        ts > prev,
                        "Per-thread monotonicity violated: {ts:?} should be > {prev:?}"
                    );

                    thread_timestamps.push(ts);
                    prev = ts;
                    // In constrained CI environments, give more chances to other threads to
                    // make progress.
                    std::thread::yield_now();
                }

                thread_timestamps
            }));
        }

        // Collect results after all threads complete - no synchronization during the test
        let all_ts: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // Verify we got the expected number of timestamps
        assert_eq!(all_ts.len(), NUM_THREADS * OPS_PER_THREAD);

        // Property 2: Global uniqueness - no duplicate timestamps across all threads
        let unique_set: HashSet<u64> = all_ts.iter().map(|ts| ts.as_u64()).collect();
        assert_eq!(
            unique_set.len(),
            all_ts.len(),
            "Global uniqueness violated: {} unique timestamps out of {} total",
            unique_set.len(),
            all_ts.len()
        );

        // Property 3: Final timestamp is greater than all returned timestamps
        let final_timestamp = hlc.snapshot();
        let max_returned = all_ts.iter().max().unwrap();
        assert!(
            final_timestamp >= *max_returned,
            "Final timestamp {final_timestamp:?} should be >= max returned {max_returned:?}"
        );

        println!(
            "Concurrent mixed ops test passed: {} unique timestamps generated across {} threads",
            all_ts.len(),
            NUM_THREADS
        );
        println!("Start: {start_ts:#?}");
        println!("Final: {final_timestamp:?}");
    }

    #[test]
    fn next_physical_clock_ahead_resets_logical_counter() {
        // When the physical clock advances past the latest timestamp,
        // the logical counter should reset to 0.
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        // Set state to some timestamp with a non-zero logical counter
        let initial_phy = phy_clock(&mock_clock);
        hlc.set_state(UniqueTimestamp::try_from_parts(initial_phy, 10).unwrap());

        // Advance the physical clock
        mock_clock.advance_ms(100);

        let ts = hlc.next();
        // Should use the new physical time with logical counter = 0
        assert_eq!(ts.physical_raw(), initial_phy + 100);
        assert_eq!(ts.logical_raw(), 0);
    }

    #[test]
    fn next_physical_clock_behind_increments_logical_counter() {
        // When the physical clock hasn't advanced past the latest timestamp,
        // the logical counter should increment.
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        // Set state to a timestamp ahead of the physical clock
        let current_phy = phy_clock(&mock_clock);
        let future_phy = current_phy + 1000;
        hlc.set_state(UniqueTimestamp::try_from_parts(future_phy, 5).unwrap());

        let ts = hlc.next();
        // Should keep the same physical time and increment logical counter
        assert_eq!(ts.physical_raw(), future_phy);
        assert_eq!(ts.logical_raw(), 6);
    }

    #[test]
    fn next_physical_clock_equal_increments_logical_counter() {
        // When the physical clock equals the latest timestamp's physical component,
        // the logical counter should increment.
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 3).unwrap());

        let ts = hlc.next();
        // Should keep the same physical time and increment logical counter
        assert_eq!(ts.physical_raw(), current_phy);
        assert_eq!(ts.logical_raw(), 4);
    }

    #[test]
    fn next_always_monotonic() {
        // Repeated calls to next() should always return increasing timestamps.
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let mut prev = hlc.next();
        for _ in 0..100 {
            let current = hlc.next();
            assert!(
                current > prev,
                "current={current:?} should be > prev={prev:?}"
            );
            prev = current;
        }

        // Even when physical clock advances
        mock_clock.advance_ms(10);
        for _ in 0..100 {
            let current = hlc.next();
            assert!(
                current > prev,
                "current={current:?} should be > prev={prev:?}"
            );
            prev = current;
        }
    }

    // ===========================================
    // Tests for try_update() - Receive event
    // ===========================================

    #[test]
    fn try_update_physical_clock_ahead_of_both() {
        // When physical clock > incoming AND > latest, use physical clock with lc=0
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to a past timestamp
        let past_phy = current_phy - 500;
        hlc.set_state(UniqueTimestamp::try_from_parts(past_phy, 10).unwrap());

        // Incoming is also in the past
        let incoming_phy = current_phy - 600;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 20).unwrap();

        let ts = hlc.try_update(incoming).unwrap();
        // Should use current physical clock with logical counter = 0
        assert_eq!(ts.physical_raw(), current_phy);
        assert_eq!(ts.logical_raw(), 0);
    }

    #[test]
    fn try_update_incoming_ahead_of_latest() {
        // When incoming > latest (and physical clock is not ahead of incoming),
        // use incoming physical time with incoming logical counter + 1
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to current physical time
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 5).unwrap());

        // Incoming is ahead (within drift limit)
        let incoming_phy = current_phy + 100;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 10).unwrap();

        let ts = hlc.try_update(incoming).unwrap();
        // Should use incoming physical time with incoming logical counter + 1
        assert_eq!(ts.physical_raw(), incoming_phy);
        assert_eq!(ts.logical_raw(), 11);
    }

    #[test]
    fn try_update_latest_ahead_of_incoming() {
        // When latest > incoming, keep latest physical time with latest logical counter + 1
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state ahead of current physical time
        let latest_phy = current_phy + 500;
        hlc.set_state(UniqueTimestamp::try_from_parts(latest_phy, 7).unwrap());

        // Incoming is behind our state
        let incoming = UniqueTimestamp::try_from_parts(current_phy, 20).unwrap();

        let ts = hlc.try_update(incoming).unwrap();
        // Should keep latest physical time with latest logical counter + 1
        assert_eq!(ts.physical_raw(), latest_phy);
        assert_eq!(ts.logical_raw(), 8);
    }

    #[test]
    fn try_update_incoming_equals_latest_uses_max_logical() {
        // When incoming.physical == latest.physical, use max(incoming.lc, latest.lc) + 1
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state with lower logical counter
        let phy = current_phy + 100;
        hlc.set_state(UniqueTimestamp::try_from_parts(phy, 5).unwrap());

        // Incoming has same physical but higher logical counter
        let incoming = UniqueTimestamp::try_from_parts(phy, 15).unwrap();

        let ts = hlc.try_update(incoming).unwrap();
        // Should use max(5, 15) + 1 = 16
        assert_eq!(ts.physical_raw(), phy);
        assert_eq!(ts.logical_raw(), 16);

        // Now test the other way: incoming has lower logical counter
        hlc.set_state(UniqueTimestamp::try_from_parts(phy, 20).unwrap());
        let incoming2 = UniqueTimestamp::try_from_parts(phy, 10).unwrap();

        let ts2 = hlc.try_update(incoming2).unwrap();
        // Should use max(20, 10) + 1 = 21
        assert_eq!(ts2.physical_raw(), phy);
        assert_eq!(ts2.logical_raw(), 21);
    }

    #[test]
    fn try_update_drift_check_rejects_excessive_drift() {
        // When incoming is too far ahead of physical clock, should reject
        let mock_clock = MockClock::new();
        let max_drift = NonZeroUsize::new(1000).unwrap(); // 1 second
        let hlc = HlcClock::new(
            Some(max_drift),
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to current time
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 0).unwrap());

        // Incoming is way too far in the future
        let incoming_phy = current_phy + max_drift.get() as u64 + 500;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 0).unwrap();

        let result = hlc.try_update(incoming);
        assert!(matches!(result, Err(Error::UnacceptableClockDrift { .. })));
    }

    #[test]
    fn try_update_drift_check_accepts_within_limit() {
        // When incoming is within drift limit, should accept
        let mock_clock = MockClock::new();
        let max_drift = NonZeroUsize::new(1000).unwrap(); // 1 second
        let hlc = HlcClock::new(
            Some(max_drift),
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to current time
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 0).unwrap());

        // Incoming is just within drift limit
        let incoming_phy = current_phy + max_drift.get() as u64 - 1;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 0).unwrap();

        let result = hlc.try_update(incoming);
        assert!(result.is_ok());
    }

    #[test]
    fn try_update_drift_check_accepts_at_exact_limit() {
        // When incoming is exactly at the drift limit, should accept (limit is inclusive)
        let mock_clock = MockClock::new();
        let max_drift = NonZeroUsize::new(1000).unwrap(); // 1 second
        let hlc = HlcClock::new(
            Some(max_drift),
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to current time
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 0).unwrap());

        // Incoming is exactly at drift limit
        let incoming_phy = current_phy + max_drift.get() as u64;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 0).unwrap();

        let result = hlc.try_update(incoming);
        // The drift check uses `>` not `>=`, so exactly at the limit is accepted
        assert!(result.is_ok());
    }

    #[test]
    fn try_update_drift_check_disabled_when_none() {
        // When max_drift is None, drift check should be disabled
        let mock_clock = MockClock::new();
        let hlc =
            HlcClock::new(None, mock_clock.clone(), Arc::new(AtomicStorage::default())).unwrap();

        let current_phy = phy_clock(&mock_clock);

        // Set state to current time
        hlc.set_state(UniqueTimestamp::try_from_parts(current_phy, 0).unwrap());

        // Incoming is very far in the future
        let incoming_phy = current_phy + 1_000_000;
        let incoming = UniqueTimestamp::try_from_parts(incoming_phy, 0).unwrap();

        let result = hlc.try_update(incoming);
        assert!(result.is_ok());
    }

    #[test]
    fn try_update_always_monotonic() {
        // try_update should always return a timestamp greater than the previous
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);

        let mut prev = hlc.next();

        // Various incoming timestamps should still result in monotonic output
        let past_100 = current_phy - 100;
        let past_50 = current_phy - 50;
        let test_cases = [
            UniqueTimestamp::try_from_parts(past_100, 0).unwrap(), // past
            UniqueTimestamp::try_from_parts(current_phy, 0).unwrap(), // current
            UniqueTimestamp::try_from_parts(current_phy + 100, 0).unwrap(), // future (within drift)
            UniqueTimestamp::try_from_parts(past_50, 1000).unwrap(), // past with high lc
        ];

        for incoming in test_cases {
            let current = hlc.try_update(incoming).unwrap();
            assert!(
                current > prev,
                "current={current:?} should be > prev={prev:?}"
            );
            prev = current;
        }
    }

    #[test]
    fn try_update_result_greater_than_incoming() {
        // try_update should always return a timestamp greater than the incoming timestamp
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);
        let past_100 = current_phy - 100;

        let test_cases = [
            UniqueTimestamp::try_from_parts(past_100, 50).unwrap(),
            UniqueTimestamp::try_from_parts(current_phy, 100).unwrap(),
            UniqueTimestamp::try_from_parts(current_phy + 100, 200).unwrap(),
        ];

        for incoming in test_cases {
            let result = hlc.try_update(incoming).unwrap();
            assert!(
                result > incoming,
                "result={result:?} should be > incoming={incoming:?}"
            );
        }
    }

    // ===========================================
    // Tests for mixed operations
    // ===========================================

    #[test]
    fn mixed_next_and_try_update_monotonic() {
        // Interleaved next() and try_update() calls should maintain monotonicity
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let current_phy = phy_clock(&mock_clock);
        let mut prev = hlc.next();

        for i in 0..50 {
            let current = if i % 3 == 0 {
                // Advance physical clock occasionally
                mock_clock.advance_ms(10);
                hlc.next()
            } else if i % 3 == 1 {
                hlc.next()
            } else {
                // try_update with various timestamps
                let offset = (i as u64 * 7) % 200;
                let incoming_phy = current_phy + offset;
                let incoming = UniqueTimestamp::try_from_parts(incoming_phy, i as u64).unwrap();
                hlc.try_update(incoming).unwrap()
            };

            assert!(
                current > prev,
                "iteration {i}: current={current:?} should be > prev={prev:?}"
            );
            prev = current;
        }
    }

    #[test]
    fn snapshot_does_not_advance_clock() {
        // snapshot() should return the last emitted timestamp without advancing
        let mock_clock = MockClock::new();
        let hlc = HlcClock::new(
            MAX_DRIFT_5000,
            mock_clock.clone(),
            Arc::new(AtomicStorage::default()),
        )
        .unwrap();

        let ts1 = hlc.next();
        let snap1 = hlc.snapshot();
        let snap2 = hlc.snapshot();
        let snap3 = hlc.snapshot();

        assert_eq!(ts1, snap1);
        assert_eq!(snap1, snap2);
        assert_eq!(snap2, snap3);

        let ts2 = hlc.next();
        assert!(ts2 > snap3);
    }
}
