// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

use slotmap::{SecondaryMap, SlotMap};
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_types::clock::UniqueTimestamp;

use crate::VQueuesMeta;
use crate::scheduler::vqueue_state::Eligibility;
use crate::vqueue_config::VQueueConfig;

use super::clock::SchedulerClock;
use super::vqueue_state::VQueueState;
use super::{ThrottleScope, VQueueHandle};

#[derive(Debug, Copy, Clone)]
struct WakeUp {
    ts: UniqueTimestamp,
    timer_key: delay_queue::Key,
}

#[derive(Debug, Copy, Clone)]
enum State {
    /// Needs a poll_head to update the state
    NeedsPoll,
    // Poll to pick next item. Next item is available now. It's in the ready ring.
    Ready,
    /// Can't dequeue items until wake_up point
    Throttled {
        wake_up: WakeUp,
        #[allow(dead_code)]
        scope: ThrottleScope,
    },
    /// Next item is scheduled (maybe this should be in head status?)
    Scheduled {
        wake_up: WakeUp,
    },
    BlockedOnCapacity,
}

#[derive(derive_more::Debug)]
pub struct EligibilityTracker {
    #[debug(skip)]
    delayed_eligibility: DelayQueue<VQueueHandle>,
    ready_ring: VecDeque<VQueueHandle>,
    #[debug(skip)]
    states: SecondaryMap<VQueueHandle, State>,
}

impl EligibilityTracker {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            delayed_eligibility: DelayQueue::with_capacity(capacity),
            ready_ring: VecDeque::with_capacity(capacity),
            states: SecondaryMap::with_capacity(capacity),
        }
    }

    pub fn insert_eligible(&mut self, handle: VQueueHandle) {
        self.states.insert(handle, State::NeedsPoll);
        self.ready_ring.push_back(handle);
    }

    pub fn poll_delayed(&mut self, cx: &mut std::task::Context<'_>) {
        while let Poll::Ready(Some(expired)) = self.delayed_eligibility.poll_expired(cx) {
            let timer_key = expired.key();
            let handle = expired.into_inner();
            if let Some(state) = self.states.get_mut(handle) {
                match state {
                    State::Throttled { wake_up, .. } if wake_up.timer_key == timer_key => {
                        debug_assert!(!self.ready_ring.contains(&handle));
                        *state = State::NeedsPoll;
                        self.ready_ring.push_back(handle);
                    }
                    State::Scheduled { wake_up } if wake_up.timer_key == timer_key => {
                        *state = State::NeedsPoll;
                        debug_assert!(!self.ready_ring.contains(&handle));
                        self.ready_ring.push_back(handle);
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn next_eligible<S>(
        &mut self,
        storage: &S,
        vqueues: &mut SlotMap<VQueueHandle, VQueueState<S>>,
        cache: VQueuesMeta<'_>,
    ) -> Result<Option<VQueueHandle>, StorageError>
    where
        S: VQueueStore,
        S::Item: std::fmt::Debug,
    {
        loop {
            // what is my current status
            let Some(handle) = self.ready_ring.front().copied() else {
                return Ok(None);
            };

            let Some(qstate) = vqueues.get_mut(handle) else {
                // the vqueue has been removed probably it's empty therefore, it's
                // safe to assume that it's not eligible.
                self.remove(handle);
                self.ready_ring.pop_front();
                continue;
            };

            let current_state = self
                .states
                .get_mut(handle)
                .expect("eligible must have state");

            let meta = cache.get_vqueue(&qstate.qid).unwrap();
            match current_state {
                State::NeedsPoll => {
                    qstate.poll_head(storage, meta)?;
                    let config = cache.config_pool().find(&qstate.qid.parent);
                    // update the state based on eligibility.
                    match qstate.check_eligibility(SchedulerClock.now_ts(), meta, config) {
                        Eligibility::Eligible => {
                            *current_state = State::Ready;
                            return Ok(Some(handle));
                        }
                        Eligibility::EligibleAt(ts) => {
                            let instant = SchedulerClock.ts_to_future_instant(ts);
                            let timer_key = self.delayed_eligibility.insert_at(handle, instant);
                            *current_state = State::Scheduled {
                                wake_up: WakeUp { ts, timer_key },
                            };
                            self.ready_ring.pop_front();
                            continue;
                        }
                        Eligibility::NotEligible => {
                            self.ready_ring.pop_front();
                            self.remove(handle);
                            continue;
                        }
                    }
                }
                State::Ready | State::BlockedOnCapacity => {
                    return Ok(Some(handle));
                }
                State::Throttled { .. } | State::Scheduled { .. } => {
                    self.ready_ring.pop_front();
                    continue;
                }
            }
        }
    }

    fn remove(&mut self, handle: VQueueHandle) {
        match self.states.remove(handle) {
            // cancel scheduled wake ups
            Some(State::Throttled { wake_up, .. }) => {
                self.delayed_eligibility.remove(&wake_up.timer_key);
            }
            Some(State::Scheduled { wake_up }) => {
                self.delayed_eligibility.remove(&wake_up.timer_key);
            }
            _ => {}
        }
    }

    /// Returns true if scheduler should be woken up
    pub fn refresh_membership<S>(
        &mut self,
        vqueue: &VQueueState<S>,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> bool
    where
        S: VQueueStore,
        S::Item: std::fmt::Debug,
    {
        let current_state = self.states.get(vqueue.handle).copied();
        let eligibility = vqueue.check_eligibility(SchedulerClock.now_ts(), meta, config);

        match (current_state, eligibility) {
            // --
            // Cases where we have NO state, we set the state and register membership in
            // ready_ring accordingly.
            // --
            //
            // should make it eligible, add to ready ring.
            (None, Eligibility::Eligible) => {
                self.states.insert(vqueue.handle, State::NeedsPoll);
                self.ready_ring.push_back(vqueue.handle);
                // should wake up
                true
            }
            // add state, but not ready ring.
            (None, Eligibility::EligibleAt(eligible_at)) => {
                self.states.insert(
                    vqueue.handle,
                    State::Scheduled {
                        wake_up: WakeUp {
                            ts: eligible_at,
                            timer_key: self.delayed_eligibility.insert_at(
                                vqueue.handle,
                                SchedulerClock.ts_to_future_instant(eligible_at),
                            ),
                        },
                    },
                );
                false
            }
            // it wasn't ready, and it's also not ready now, do nothing.
            (None, Eligibility::NotEligible) => false,
            // --
            // Cases where we have state, we deduce the action from the state and eligibility
            // and issue a corrective action
            // --
            (
                // needs poll will configure the state anyway.
                Some(State::NeedsPoll),
                _,
            ) => false,
            (
                Some(State::Ready | State::BlockedOnCapacity | State::Throttled { .. }),
                Eligibility::Eligible,
            ) => {
                // we are already good. Note that if we were throttled, we continue to be throttled
                // as throttling is only determined by the pop() operation and not calculated from
                // eligibility. Additionally, we only throttle after we become eligible anyway.
                false
            }
            // we were scheduled, but became available (now)
            (Some(State::Scheduled { wake_up }), Eligibility::Eligible) => {
                self.delayed_eligibility.remove(&wake_up.timer_key);
                self.ready_ring.push_back(vqueue.handle);
                self.states.insert(vqueue.handle, State::NeedsPoll);
                true
            }

            (
                Some(State::Ready | State::BlockedOnCapacity),
                Eligibility::EligibleAt(..) | Eligibility::NotEligible,
            ) => {
                // Switching to NeedsPoll to register the timer on the next poll
                self.states.insert(vqueue.handle, State::NeedsPoll);
                // no need to wake up because we were already ready
                false
            }
            // We were scheduled as we should, but make sure that the time point is still the same
            (Some(State::Scheduled { wake_up }), Eligibility::EligibleAt(eligible_at_ts)) => {
                if eligible_at_ts.cmp_physical(&wake_up.ts) != Ordering::Equal {
                    // Reschedule the timer as the new eligibility is further in the future
                    self.delayed_eligibility.reset_at(
                        &wake_up.timer_key,
                        SchedulerClock.ts_to_future_instant(eligible_at_ts),
                    );
                    self.states.insert(
                        vqueue.handle,
                        State::Scheduled {
                            wake_up: WakeUp {
                                ts: eligible_at_ts,
                                timer_key: wake_up.timer_key,
                            },
                        },
                    );
                }
                false
            }

            (Some(State::Throttled { wake_up, .. }), Eligibility::EligibleAt(eligible_at_ts)) => {
                // We move away from throttled only if the new time point is _after_ the
                // end of the throttling period.
                if eligible_at_ts > wake_up.ts {
                    // Reschedule the timer as the new eligibility is further in the future
                    self.delayed_eligibility.reset_at(
                        &wake_up.timer_key,
                        SchedulerClock.ts_to_future_instant(eligible_at_ts),
                    );
                    self.states.insert(
                        vqueue.handle,
                        State::Scheduled {
                            wake_up: WakeUp {
                                ts: eligible_at_ts,
                                timer_key: wake_up.timer_key,
                            },
                        },
                    );
                }
                false
            }

            // If we were throttled/scheduled and we are confident that are no longer eligible,
            // then we simply cancel the wake up timer
            (
                Some(State::Scheduled { wake_up } | State::Throttled { wake_up, .. }),
                Eligibility::NotEligible,
            ) => {
                self.delayed_eligibility.remove(&wake_up.timer_key);
                self.states.remove(vqueue.handle);
                false
            }
        }
    }

    pub fn rotate_one(&mut self) {
        self.ready_ring.rotate_left(1);
    }

    pub fn len(&self) -> usize {
        self.ready_ring.len()
    }

    pub fn front_needs_poll(&mut self) {
        if let Some(handle) = self.ready_ring.front()
            && let Some(state) = self.states.get_mut(*handle)
        {
            *state = State::NeedsPoll;
        }
    }

    pub fn front_blocked(&mut self) {
        if let Some(handle) = self.ready_ring.front()
            && let Some(state) = self.states.get_mut(*handle)
        {
            *state = State::BlockedOnCapacity;
        }
    }

    pub fn front_throttled(&mut self, delay: Duration, scope: ThrottleScope) {
        if let Some(handle) = self.ready_ring.front() {
            let now = SchedulerClock.now_ts();
            let wake_up_ts = now.add_millis(delay.as_millis() as u64).unwrap_or(now);

            let old = self.states.insert(
                *handle,
                State::Throttled {
                    scope,
                    wake_up: WakeUp {
                        ts: wake_up_ts,
                        timer_key: self.delayed_eligibility.insert(*handle, delay),
                    },
                },
            );
            trace!("{handle:?} is being throttled for {delay:?} in throttling scope {scope:?}");
            // cancel the previous timer
            if let Some(State::Throttled { wake_up, .. } | State::Scheduled { wake_up }) = old {
                self.delayed_eligibility.remove(&wake_up.timer_key);
            }
        }
    }

    pub fn compact_memory(&mut self) {
        self.delayed_eligibility.compact();
    }
}
