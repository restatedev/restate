// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use ahash::RandomState;
use priority_queue::PriorityQueue;
use tokio::time::Instant;

use restate_types::identifiers::PartitionKey;
use restate_types::time::MillisSinceEpoch;
use restate_worker_api::HotKeyEntry;

/// Length of the window over which the actions of a partition key are summed up.
const WINDOW: Duration = Duration::from_mins(1);
/// Number of partition keys tracked per window. A partition key that accounts for more than
/// `1 / CAPACITY` of a window's actions is always tracked; with at most `CAPACITY` partition keys
/// per window, counts are exact.
const CAPACITY: usize = 1024;

/// Counts the partition keys whose actions within a window reach a threshold, using the
/// Space-Saving algorithm: once full, a new partition key evicts the one with the lowest count and
/// inherits that count. A partition key is only reported hot if its actions since it was last
/// tracked reach the threshold, so every reported partition key is hot. A hot partition key can
/// only be missed if more than `CAPACITY` partition keys are active in a window and it was evicted
/// earlier in the window, while it had the lowest count.
#[derive(Debug)]
pub(super) struct HotKeyTracker {
    threshold: NonZeroU64,
    /// Partition key to its (count, inherited count). The count includes the inherited count, so
    /// the partition key's own actions are `count - inherited`. Lowest count first, and among
    /// equal counts the most inherited first, so that an eviction loses the fewest own actions.
    counts: PriorityQueue<PartitionKey, (Reverse<u64>, u64), RandomState>,
    window_started_at: Instant,
    /// Wall-clock time of `window_started_at`, as shown to users.
    window_started_at_ms: MillisSinceEpoch,
    /// Hot partition keys of the last completed window, in ascending order.
    last_window: Arc<[HotKeyEntry]>,
}

impl HotKeyTracker {
    pub(super) fn new(threshold: NonZeroU64) -> Self {
        Self {
            threshold,
            counts: PriorityQueue::with_capacity_and_hasher(CAPACITY, RandomState::new()),
            window_started_at: Instant::now(),
            window_started_at_ms: MillisSinceEpoch::now(),
            last_window: Arc::default(),
        }
    }

    pub(super) fn record(&mut self, partition_key: PartitionKey, actions: u64) {
        if self
            .counts
            .change_priority_by(&partition_key, |(Reverse(count), _)| *count += actions)
        {
            return;
        }

        let inherited = if self.counts.len() < CAPACITY {
            0
        } else {
            let (_, (Reverse(lowest), _)) = self.counts.pop().expect("tracker is full");
            lowest
        };
        self.counts
            .push(partition_key, (Reverse(inherited + actions), inherited));
    }

    /// Returns the number of hot partition keys if the current window has elapsed, and starts a
    /// new one. `now_ms` is the wall-clock time of `now`.
    pub(super) fn maybe_rotate(&mut self, now: Instant, now_ms: MillisSinceEpoch) -> Option<usize> {
        if now.duration_since(self.window_started_at) < WINDOW {
            return None;
        }

        let mut hot_keys: Vec<_> = self
            .counts
            .iter()
            .filter_map(|(partition_key, (Reverse(count), inherited))| {
                let actions = count - inherited;
                (actions >= self.threshold.get()).then_some(HotKeyEntry {
                    partition_key: *partition_key,
                    actions,
                    window_start: self.window_started_at_ms,
                    window_end: now_ms,
                })
            })
            .collect();
        hot_keys.sort_unstable_by_key(|entry| entry.partition_key);
        let count = hot_keys.len();
        self.last_window = hot_keys.into();

        self.counts.clear();
        self.window_started_at = now;
        self.window_started_at_ms = now_ms;
        Some(count)
    }

    /// The hot partition keys of the last completed window, in ascending order.
    pub(super) fn last_window(&self) -> &Arc<[HotKeyEntry]> {
        &self.last_window
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the (count, inherited count) of a partition key.
    fn count(tracker: &HotKeyTracker, partition_key: PartitionKey) -> Option<(u64, u64)> {
        tracker
            .counts
            .get_priority(&partition_key)
            .map(|(Reverse(count), inherited)| (*count, *inherited))
    }

    #[test]
    fn counts_partition_keys_reaching_threshold_per_window() {
        let mut tracker = HotKeyTracker::new(NonZeroU64::new(10).unwrap());
        let start = tracker.window_started_at;
        let start_ms = tracker.window_started_at_ms;
        let rotate = |tracker: &mut HotKeyTracker, elapsed: Duration| {
            tracker.maybe_rotate(start + elapsed, start_ms + elapsed)
        };

        // Actions add up within a window, and reaching the threshold exactly makes a key hot.
        tracker.record(1, 6);
        tracker.record(1, 4);
        tracker.record(2, 9);
        tracker.record(3, 25);
        assert_eq!(count(&tracker, 1), Some((10, 0)));

        assert_eq!(rotate(&mut tracker, WINDOW - Duration::from_secs(1)), None);
        assert!(tracker.last_window().is_empty());
        assert_eq!(rotate(&mut tracker, WINDOW), Some(2));
        let hot_key = |partition_key, actions| HotKeyEntry {
            partition_key,
            actions,
            window_start: start_ms,
            window_end: start_ms + WINDOW,
        };
        assert_eq!(
            *tracker.last_window(),
            [hot_key(1, 10), hot_key(3, 25)].into()
        );

        // Counts don't carry over, and the next window starts at the rotation.
        tracker.record(3, 5);
        assert_eq!(count(&tracker, 3), Some((5, 0)));
        assert_eq!(rotate(&mut tracker, WINDOW + Duration::from_secs(59)), None);
        assert_eq!(rotate(&mut tracker, 2 * WINDOW), Some(0));
        assert!(tracker.last_window().is_empty());
    }

    #[test]
    fn full_tracker_reports_only_guaranteed_hot_keys() {
        let mut tracker = HotKeyTracker::new(NonZeroU64::new(100).unwrap());
        let start = tracker.window_started_at;
        let hot = PartitionKey::MAX;
        let newcomer = PartitionKey::MAX - 1;

        tracker.record(hot, 60);
        for cold in 0..CAPACITY as PartitionKey - 1 {
            tracker.record(cold, 50);
        }
        assert_eq!(tracker.counts.len(), CAPACITY);

        // A newcomer evicts a partition key with the lowest count and inherits it.
        tracker.record(newcomer, 60);
        assert_eq!(count(&tracker, newcomer), Some((110, 50)));

        // Three times as many new partition keys as there is room for keep evicting the lowest
        // one. The hot key always outweighs the lowest count, so it is never evicted and stays
        // exact.
        for cold in CAPACITY as PartitionKey..4 * CAPACITY as PartitionKey {
            tracker.record(cold, 1);
        }
        tracker.record(hot, 40);
        assert_eq!(tracker.counts.len(), CAPACITY);
        assert_eq!(count(&tracker, hot), Some((100, 0)));
        assert_eq!(count(&tracker, newcomer), Some((110, 50)));

        // Only the hot key is reported: the newcomer reached the threshold only with the count it
        // inherited.
        assert_eq!(
            tracker.maybe_rotate(start + WINDOW, MillisSinceEpoch::now()),
            Some(1)
        );
        assert_eq!(tracker.last_window()[0].actions, 100);

        // Among the partition keys with the lowest count, the one with the most inherited count is
        // evicted first.
        for cold in 0..CAPACITY as PartitionKey {
            tracker.record(cold, 9);
        }
        tracker.record(newcomer, 1);
        assert_eq!(count(&tracker, newcomer), Some((10, 9)));
        for cold in 0..CAPACITY as PartitionKey {
            if count(&tracker, cold).is_some() {
                tracker.record(cold, 1);
            }
        }
        tracker.record(hot, 1);
        assert_eq!(count(&tracker, newcomer), None);
        assert_eq!(count(&tracker, hot), Some((11, 10)));
    }
}
