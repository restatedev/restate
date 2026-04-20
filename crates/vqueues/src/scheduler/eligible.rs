// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

use slotmap::secondary::Entry;
use slotmap::{SecondaryMap, SlotMap};
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::time::MillisSinceEpoch;

use super::clock::SchedulerClock;
use super::resource_manager::ResourceKind;
use super::vqueue_state::VQueueState;
use super::{SchedulingStatus, ThrottleScope, VQueueHandle};
use crate::scheduler::vqueue_state::Eligibility;

#[derive(Debug, Copy, Clone)]
pub(super) struct WakeUp {
    ts: MillisSinceEpoch,
    timer_key: delay_queue::Key,
}

#[derive(Debug, Clone)]
pub(super) enum State {
    /// Needs a `poll_eligibility` to update the state.
    /// A vqueue in this state must be already added to the ready ring.
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
    // Not in the ready ring.
    BlockedOn(ResourceKind),
}

#[derive(derive_more::Debug)]
pub(crate) struct EligibilityTracker {
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

    pub fn get_status<S: VQueueStore>(&self, qstate: &VQueueState<S>) -> SchedulingStatus {
        match self.states.get(qstate.handle) {
            None | Some(State::NeedsPoll) | Some(State::Ready) => {
                SchedulingStatus::from(qstate.check_eligibility())
            }
            Some(State::Throttled { wake_up, scope }) => SchedulingStatus::Throttled {
                until: wake_up.ts,
                scope: *scope,
            },
            Some(State::Scheduled { wake_up }) => SchedulingStatus::Scheduled {
                at: wake_up.ts.into(),
            },
            Some(State::BlockedOn(resource)) => SchedulingStatus::BlockedOn(resource.clone()),
        }
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

    pub fn next_eligible<S: VQueueStore>(
        &mut self,
        storage: &S,
        vqueues: &mut SlotMap<VQueueHandle, VQueueState<S>>,
    ) -> Result<Option<VQueueHandle>, StorageError> {
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

            let Some(current_state) = self.states.get_mut(handle) else {
                // The vqueue is not eligible anymore. This can happen if the vqueue became dormant
                // due to items being removed externally (killed, etc.)
                self.ready_ring.pop_front();
                continue;
            };

            match current_state {
                State::NeedsPoll => {
                    // update the state based on eligibility.
                    match qstate.poll_eligibility(storage)?.as_compact() {
                        Eligibility::Eligible => {
                            *current_state = State::Ready;
                            return Ok(Some(handle));
                        }
                        Eligibility::EligibleAt(ts) => {
                            let ts = ts.as_unix_millis();
                            let duration = ts.duration_since(SchedulerClock.now_millis());
                            let timer_key = self.delayed_eligibility.insert(handle, duration);
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
                State::BlockedOn(_) => {
                    self.ready_ring.pop_front();
                }
                State::Ready => {
                    return Ok(Some(handle));
                }
                State::Throttled { .. } | State::Scheduled { .. } => {
                    self.ready_ring.pop_front();
                }
            }
        }
    }

    pub fn remove(&mut self, handle: VQueueHandle) {
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

    // Places the vqueue back on the ready ring only if it was not already there.
    // Sets the state to NeedsPoll
    pub fn ensure_queue_needs_polling(&mut self, vqueue: VQueueHandle) -> bool {
        if let Some(state) = self.states.entry(vqueue) {
            match state {
                Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                    State::BlockedOn(_resource) => {
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue);
                        true
                    }
                    State::NeedsPoll => false,
                    State::Ready => {
                        // it's already in the ready ring, but we need to adjust its state
                        occupied_entry.insert(State::NeedsPoll);
                        // It's already in the ready ring, we return false since we didn't add it
                        false
                    }
                    State::Throttled { wake_up, .. } => {
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue);
                        true
                    }
                    State::Scheduled { wake_up } => {
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue);
                        true
                    }
                },
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(State::NeedsPoll);
                    self.ready_ring.push_back(vqueue);
                    true
                }
            }
        } else {
            // The vqueue handle was removed from cache. We are operating on stale
            // information. Best to ignore it.
            false
        }
    }

    pub fn wake_up_queues(&mut self, vqueues: impl IntoIterator<Item = VQueueHandle>) -> bool {
        vqueues.into_iter().fold(false, |wake_up, vqueue| {
            wake_up | self.wake_up_queue(vqueue)
        })
    }

    /// returns true if the scheduler should be woken up
    pub fn wake_up_queue(&mut self, vqueue: VQueueHandle) -> bool {
        if let Some(state) = self.states.entry(vqueue) {
            match state {
                Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                    State::BlockedOn(_resource) => {
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue);
                        true
                    }
                    _ => {
                        // do nothing.
                        false
                    }
                },
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(State::NeedsPoll);
                    self.ready_ring.push_back(vqueue);
                    true
                }
            }
        } else {
            // The vqueue handle was removed from cache. We are operating on stale
            // information. Best to ignore it.
            false
        }
    }

    /// Returns true if scheduler should be woken up
    ///
    /// If the vqueue is blocked on a resource, this function will not touch it.
    pub fn refresh_membership<S: VQueueStore>(&mut self, vqueue: &VQueueState<S>) -> bool {
        let Some(current_state) = self.states.entry(vqueue.handle) else {
            // the vqueue handle was removed from the original slot map.
            return false;
        };

        match current_state {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(State::NeedsPoll);
                self.ready_ring.push_back(vqueue.handle);
                true
            }
            Entry::Occupied(mut occupied_entry) => {
                let eligibility = vqueue.check_eligibility().as_compact();
                match (occupied_entry.get(), eligibility) {
                    (State::NeedsPoll, _) => {
                        // do nothing.
                        false
                    }
                    (State::Ready, _) => {
                        occupied_entry.insert(State::NeedsPoll);
                        false
                    }
                    (State::Throttled { .. }, Eligibility::Eligible) => {
                        // in throttling, we leave it as is. The new head would latch on.
                        false
                    }
                    (State::Throttled { wake_up, .. }, Eligibility::EligibleAt(eligible_at_ts)) => {
                        // We move away from throttled only if the new time point is _after_ the
                        // end of the throttling period.
                        let eligible_at_ts = eligible_at_ts.as_unix_millis();
                        if eligible_at_ts > wake_up.ts {
                            // Reschedule the timer as the new eligibility is further in the future
                            self.delayed_eligibility.reset(
                                &wake_up.timer_key,
                                eligible_at_ts.duration_since(SchedulerClock.now_millis()),
                            );
                            occupied_entry.insert(State::Scheduled {
                                wake_up: WakeUp {
                                    ts: eligible_at_ts,
                                    timer_key: wake_up.timer_key,
                                },
                            });
                        }
                        // We didn't add it to the ready ring
                        false
                    }
                    (State::Scheduled { wake_up }, Eligibility::NotEligible) => {
                        // We should not really have a scenario where we land here. But if this
                        // happens, we err on the safe side and switch to polling.
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue.handle);
                        true
                    }
                    (State::Throttled { wake_up, .. }, Eligibility::NotEligible) => {
                        // We should not really have a scenario where we land here. But if this
                        // happens, we err on the safe side and switch to polling.
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.ready_ring.push_back(vqueue.handle);
                        true
                    }
                    (State::Scheduled { wake_up }, Eligibility::Eligible) => {
                        // wake up now
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        self.states.insert(vqueue.handle, State::NeedsPoll);
                        self.ready_ring.push_back(vqueue.handle);
                        true
                    }
                    (State::Scheduled { wake_up }, Eligibility::EligibleAt(eligible_at_ts)) => {
                        let eligible_at_ts = eligible_at_ts.as_unix_millis();
                        // maybe change the wake up time
                        if eligible_at_ts != wake_up.ts {
                            self.delayed_eligibility.reset(
                                &wake_up.timer_key,
                                eligible_at_ts.duration_since(SchedulerClock.now_millis()),
                            );

                            occupied_entry.insert(State::Scheduled {
                                wake_up: WakeUp {
                                    ts: eligible_at_ts,
                                    timer_key: wake_up.timer_key,
                                },
                            });
                        }
                        false
                    }
                    (State::BlockedOn(_), _) => {
                        // don't touch it.
                        false
                    }
                }
            }
        }
    }

    pub fn find_blocking_resource(&self, handle: VQueueHandle) -> Option<&ResourceKind> {
        let current_state = self.states.get(handle)?;

        if let State::BlockedOn(resource) = current_state {
            Some(resource)
        } else {
            None
        }
    }

    /// Places the vqueue back on the ready ring only if it was blocked on resources
    pub fn mark_queue_unblocked(&mut self, handle: VQueueHandle) -> Option<ResourceKind> {
        let current_state = self.states.get_mut(handle)?;

        if matches!(current_state, State::BlockedOn(_)) {
            let blocked_on = std::mem::replace(current_state, State::NeedsPoll);
            self.ready_ring.push_back(handle);
            let State::BlockedOn(resource) = blocked_on else {
                unreachable!();
            };

            Some(resource)
        } else {
            None
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

    pub fn front_blocked(&mut self, resource: ResourceKind) {
        if let Some(handle) = self.ready_ring.front()
            && let Some(state) = self.states.get_mut(*handle)
        {
            *state = State::BlockedOn(resource);
        }
    }

    pub fn front_throttled(&mut self, delay: Duration, scope: ThrottleScope) {
        if let Some(handle) = self.ready_ring.pop_front() {
            let wake_up_ts = SchedulerClock.now_millis() + delay;

            let old = self.states.insert(
                handle,
                State::Throttled {
                    scope,
                    wake_up: WakeUp {
                        ts: wake_up_ts,
                        timer_key: self.delayed_eligibility.insert(handle, delay),
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
