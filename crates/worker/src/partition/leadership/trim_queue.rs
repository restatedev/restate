// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove when trimming is fully implemented in PP
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{ShutdownError, TaskCenter, TaskId, cancellation_token};
use restate_storage_api::fsm_table::PartitionDurability;
use restate_types::config::Configuration;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::retries::with_jitter;
use restate_types::time::MillisSinceEpoch;

/// A task that trims the log by removing durable LSNs from the log.
pub struct LogTrimmer {
    bifrost: Bifrost,
    log_id: LogId,
    trim_queue: TrimQueue,
}
impl LogTrimmer {
    pub fn spawn(
        bifrost: Bifrost,
        log_id: LogId,
        trim_queue: TrimQueue,
    ) -> Result<TaskId, ShutdownError> {
        let task = Self {
            trim_queue,
            log_id,
            bifrost,
        };

        TaskCenter::spawn_child(
            restate_core::TaskKind::LogTrimmer,
            "log-trimmer",
            task.run(),
        )
    }

    async fn run(self) -> anyhow::Result<()> {
        let cancel = cancellation_token();
        // wait for 60-ish seconds before starting the first trim
        let mut interval = tokio::time::interval_at(
            Instant::now() + with_jitter(Duration::from_secs(60), 0.3),
            with_jitter(Duration::from_secs(30), 0.5),
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        debug!(log_id = %self.log_id, "Log trimmer task started");
        loop {
            // wait to pace down
            if cancel.run_until_cancelled(interval.tick()).await.is_none() {
                break;
            }

            if let Some(durability_point) = self.trim_queue.pop() {
                let Some(result) = cancel
                    .run_until_cancelled(self.trim_logs(&durability_point))
                    .await
                else {
                    // if cancelled, we push it back. it's safe to do because trims are
                    // idempotent.
                    self.trim_queue.push(&durability_point);
                    break;
                };

                if !result {
                    self.trim_queue.push(&durability_point);
                    // the next retry happens after the sleep.
                }
            }
        }
        debug!(log_id = %self.log_id, "Log trimmer task terminated");
        Ok(())
    }

    #[instrument(
        level = "error",
        skip_all,
        fields(
            log_id = %self.log_id,
        ),
    )]
    async fn trim_logs(&self, durability: &PartitionDurability) -> bool {
        let result = self
            .bifrost
            .admin()
            .trim(self.log_id, durability.durable_point)
            .await;
        if let Err(err) = result {
            warn!(
                "Could not trim the log (requested trim_point was {:?}). This can lead to increased disk usage: {err}",
                durability.durable_point,
            );
            false
        } else {
            info!(
                "Trimmed log {} to {:?}. This Lsn was reported durable at {}",
                self.log_id,
                durability.durable_point,
                durability.modification_time.into_timestamp().to_string()
            );
            true
        }
    }
}

/// A queue of potential new trim points for the log backing a partition.
///
/// Cheaply cloneable, state is shared between all clones.
#[derive(Clone, Default)]
pub struct TrimQueue {
    state: Arc<Mutex<State>>,
}

impl TrimQueue {
    /// Pushes a new trim point.
    ///
    /// Returns true if the trim point was higher than the max observed trim point.
    pub fn push(&self, durability: &PartitionDurability) -> bool {
        self.state.lock().push(
            durability,
            MillisSinceEpoch::now() - Configuration::pinned().worker.trim_delay_interval(),
        )
    }

    /// Pops the next trim point that has been observed at or older than `cutoff` timestamp.
    pub fn pop(&self) -> Option<PartitionDurability> {
        self.state
            .lock()
            .pop(MillisSinceEpoch::now() - Configuration::pinned().worker.trim_delay_interval())
    }

    /// Do we have pending trim points before and including the cutoff timestamp?
    pub fn has_pending_trim(&self, cutoff: MillisSinceEpoch) -> bool {
        self.state.lock().has_pending_trim(cutoff)
    }

    /// Compacts the queue after resetting the trim point to the new value.
    pub fn notify_trimmed(&self, trim_point: Lsn) {
        self.state.lock().notify_trimmed(trim_point);
    }
}

struct State {
    max_observed_durable_lsn: Lsn,
    known_trim_point: Lsn,
    trim_queue: BTreeMap<MillisSinceEpoch, Lsn>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            max_observed_durable_lsn: Lsn::INVALID,
            known_trim_point: Lsn::INVALID,
            trim_queue: BTreeMap::new(),
        }
    }
}

impl State {
    /// `cutoff` is the timestamp at which trim requests are created before they are considered
    /// due.
    pub fn push(&mut self, durability: &PartitionDurability, cutoff: MillisSinceEpoch) -> bool {
        let result = durability.durable_point > self.max_observed_durable_lsn;
        self.max_observed_durable_lsn = self.max_observed_durable_lsn.max(durability.durable_point);

        if durability.durable_point > self.known_trim_point {
            self.trim_queue
                .entry(durability.modification_time)
                .and_modify(|lsn| *lsn = (*lsn).max(durability.durable_point))
                .or_insert(durability.durable_point);
        }

        // Amortize compactions
        if self.trim_queue.range(..=cutoff).count() > 10 {
            self.compact(cutoff);
        }

        result
    }

    /// Compacts pending trim points before and including the cutoff timestamp.
    fn compact(&mut self, cutoff: MillisSinceEpoch) {
        if let Some(durability) = self.pop(cutoff) {
            self.trim_queue
                .insert(durability.modification_time, durability.durable_point);
        }
    }

    /// Do we have pending trim points before and including the cutoff timestamp?
    pub fn has_pending_trim(&mut self, cutoff: MillisSinceEpoch) -> bool {
        self.compact(cutoff);
        self.trim_queue.range(..=cutoff).count() > 0
    }

    pub fn pop(&mut self, cutoff: MillisSinceEpoch) -> Option<PartitionDurability> {
        // Why +1? Because split_off returns everything after and including the key (>= key)
        let mut trim_points = self
            .trim_queue
            .split_off(&MillisSinceEpoch::new(cutoff.as_u64() + 1));

        // we want to keep the future trim points in place.
        std::mem::swap(&mut trim_points, &mut self.trim_queue);

        // compact everything under the cutoff point
        // get the max lsn and its timestamp
        let (modification_time, durable_point) =
            trim_points.into_iter().max_by_key(|(_, lsn)| *lsn)?;

        (durable_point > self.known_trim_point).then_some(PartitionDurability {
            durable_point,
            modification_time,
        })
    }

    /// Compacts the queue after resetting the trim point to the new value.
    pub fn notify_trimmed(&mut self, trim_point: Lsn) {
        self.known_trim_point = self.known_trim_point.max(trim_point);
        self.trim_queue
            .retain(|_, lsn| *lsn > self.known_trim_point);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.trim_queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;

    #[test]
    fn test_pop_next_immediately() {
        let mut queue = State::default();
        let now = MillisSinceEpoch::now();
        let very_old = MillisSinceEpoch::new(MillisSinceEpoch::now().as_u64() - 20);
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(10),
                modification_time: now,
            },
            now,
        );

        assert_that!(queue.has_pending_trim(now), eq(true));

        assert_that!(
            queue.pop(now),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(10),
                modification_time: now,
            }))
        );

        assert_that!(queue.has_pending_trim(now), eq(false));
        assert_that!(queue.pop(now), none());

        // -- push multiple, but same modification time, higher lsn wins.
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(14),
                modification_time: very_old,
            },
            now,
        );

        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(12),
                modification_time: very_old,
            },
            now,
        );

        // only single pop needed, and max lsn is returned
        assert_that!(
            queue.pop(now),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(14),
                modification_time: very_old,
            }))
        );
        assert_that!(queue.pop(now), none());

        // -- push multiple, but different modification time, higher lsn still wins.
        let slightly_old = MillisSinceEpoch::new(now.as_u64() - 10);
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(14),
                modification_time: slightly_old,
            },
            now,
        );

        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(12),
                modification_time: now,
            },
            now,
        );

        // and one in the future
        let slightly_in_future = MillisSinceEpoch::new(now.as_u64() + 10);
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(16),
                modification_time: slightly_in_future,
            },
            now,
        );

        // only single pop needed, and max lsn is returned
        assert_that!(
            queue.pop(now),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(14),
                modification_time: slightly_old,
            }))
        );

        assert_that!(queue.pop(now), none());

        // poping 50ms in the future returns the one we already have in the future
        assert_that!(
            queue.pop(MillisSinceEpoch::new(now.as_u64() + 50)),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(16),
                modification_time: slightly_in_future,
            }))
        );
    }

    #[test]
    fn test_update_trim_point() {
        let mut queue = State::default();
        // millis since epoch is bigger if it's closer to now, small if it's far in the past.
        let now = MillisSinceEpoch::new(100);
        let slightly_old = MillisSinceEpoch::new(90);
        let very_old = MillisSinceEpoch::new(20);

        // future trim requests
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(10),
                modification_time: MillisSinceEpoch::new(110),
            },
            very_old,
        );
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(11),
                modification_time: MillisSinceEpoch::new(120),
            },
            very_old,
        );

        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(12),
                modification_time: MillisSinceEpoch::new(130),
            },
            very_old,
        );

        assert_that!(queue.len(), eq(3));
        queue.notify_trimmed(Lsn::from(11));
        assert_that!(queue.len(), eq(1));

        // nothing should happen at `now`, the first future trim request should be available at
        // 130.
        assert_that!(queue.pop(now), none());
        assert_that!(
            queue.pop(MillisSinceEpoch::new(130)),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(12),
                modification_time: MillisSinceEpoch::new(130),
            }))
        );

        queue.push(
            &PartitionDurability {
                // behind trim point
                durable_point: Lsn::from(9),
                modification_time: slightly_old,
            },
            now,
        );
        assert_that!(queue.len(), eq(0));

        // adding after trim point works just fine
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(15),
                modification_time: now,
            },
            now,
        );
        assert_that!(queue.len(), eq(1));
    }

    #[test]
    fn test_compaction() {
        // simulating a realistic scenario. A partition starts with knowledge about a durable LSN
        // slightly in the future.
        // Then we replay a large number of older durability points as follower.
        let mut queue = State::default();
        let now = MillisSinceEpoch::new(1000);
        queue.push(
            &PartitionDurability {
                durable_point: Lsn::from(2000),
                modification_time: MillisSinceEpoch::new(1500),
            },
            now,
        );

        assert_that!(queue.len(), eq(1));

        // 1000 trim points before/including now, and 710 in the future + 1 trim point that we
        // started with.
        // [1..=1000] => should translate into [1000]
        //
        // [1001..=1710, 2000] 709+1 = 710
        // [max-ts=1710, ts=1500]
        for i in 1..=1710 {
            queue.push(
                &PartitionDurability {
                    durable_point: Lsn::from(i),
                    modification_time: MillisSinceEpoch::new(i),
                },
                now,
            );
        }

        // We should see 711, but we have 720 because compaction is amortized at insertion time.
        // those will be compacted away on the next pop()
        //
        // The next pop should give us the max lsn that's due already, that's lsn=1000,
        assert_that!(queue.len(), eq(720));
        assert_that!(
            queue.pop(now),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(1000),
                modification_time: MillisSinceEpoch::new(1000),
            }))
        );
        assert_that!(queue.len(), eq(710));
        // now let's slide the time to 1200

        assert_that!(
            queue.pop(MillisSinceEpoch::new(1200)),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(1200),
                modification_time: MillisSinceEpoch::new(1200),
            }))
        );

        // 200 trim points were coealesced into one. remaining 510.
        assert_that!(queue.len(), eq(510));

        // at 1500, something interesting happens, we have the starting trim point requested at
        // t=1500 and it trims up to lsn=2000, so we expect that the pop at this time to eat up
        // everything up to lsn=2000 even that we have other requests with higher time and lower
        // lsn queued up. Those requests will still be available at the next pop if we didn't
        // update the trim point.
        assert_that!(
            queue.pop(MillisSinceEpoch::new(1500)),
            eq(Some(PartitionDurability {
                durable_point: Lsn::from(2000),
                modification_time: MillisSinceEpoch::new(1500),
            }))
        );

        // compaction of the remaining happens at the next pop()
        assert_that!(queue.len(), eq(210));

        // updating the trim point to simulate that we actually have trimmed.
        queue.notify_trimmed(Lsn::from(2000));
        assert_that!(queue.len(), eq(0));
    }
}
