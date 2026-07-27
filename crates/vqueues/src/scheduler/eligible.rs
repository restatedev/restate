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
use std::num::NonZeroU32;
use std::task::Poll;
use std::time::Duration;

use slotmap::SecondaryMap;
use slotmap::secondary::Entry;
use tokio_util::time::{DelayQueue, delay_queue};

use restate_limiter::RuleHandle;
use restate_platform::hash::HashMap;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_types::time::MillisSinceEpoch;
use restate_types::{Scope, ServiceName};
use restate_util_string::ReString;
use restate_worker_api::{ResourceKind, SchedulingStatus};

use super::clock::SchedulerClock;
use super::vqueue_state::VQueueState;
use crate::VQueuesMeta;
use crate::cache::VQueueHandle;
use crate::scheduler::vqueue_state::Eligibility;

/// Tokio's `DelayQueue` timer wheel panics with `invalid deadline` if an entry
/// is scheduled more than `(1 << 36) - 1` ms (~2.1 years) into the future.
///
/// When a vqueue head is scheduled beyond that horizon, we park the timer at
/// this cap instead of the real delay. This should be safe because when the
/// vqueue is dequeued from the ready ring, we'll notice that it's still not
/// eligible and just reschedule it. Now, if this is ever to happen,
/// I applaud your server's uptime.
const MAX_PARK: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The scheduler's fairness key: a vqueue belongs to its invocation SCOPE when
/// one is attached (Restate's flow-control scopes — the operator-facing fairness
/// key, weighted via `restate rules set <scope> --weight N`), and falls back to
/// its service otherwise. Service groups always run at the default weight 1;
/// only scope groups have configurable weights.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchedulingGroup {
    Scope(Scope),
    Service(ServiceName),
}

impl SchedulingGroup {
    /// Derives the scheduling group of a queue from its metadata.
    pub fn of(meta: &VQueueMeta) -> Self {
        match meta.scope() {
            Some(scope) => Self::Scope(scope.clone()),
            None => Self::Service(meta.service_name().cloned().unwrap_or_else(unlinked_group)),
        }
    }
}

/// Resolves a scheduling group's weight. Consulted whenever a group is
/// (re-)created, so rule changes take effect as soon as a group cycles.
/// Shared (Arc) between the eligibility tracker and the resource manager's
/// grouped waiter lists.
pub type WeightResolver = std::sync::Arc<dyn Fn(&SchedulingGroup) -> NonZeroU32 + Send + Sync>;

/// Resolves a service LANE's weight within a scheduling group — the second
/// level of the waiter list's stride scheduling. Consulted at lane creation
/// (so, like group weights, changes apply as a lane cycles). Default weight 1
/// gives equal round-robin across a group's services; L1 rules
/// (`<scope>/<Service>` with a `scheduling_weight`) configure unequal shares.
pub type LaneWeightResolver =
    std::sync::Arc<dyn Fn(&SchedulingGroup, &ServiceName) -> NonZeroU32 + Send + Sync>;

/// Group name used for vqueues that are not linked to any service.
pub(in crate::scheduler) fn unlinked_group() -> ServiceName {
    ServiceName::new("")
}

/// Weighted-round-robin quota shared by the eligibility ring's [`ServiceGroup`]
/// and the resource manager's `GroupedWaiters`: a group receives `weight` slots
/// per cycle before yielding its turn.
#[derive(Debug)]
pub(in crate::scheduler) struct WrrQuota {
    weight: NonZeroU32,
    used: u32,
}

impl WrrQuota {
    pub(in crate::scheduler) fn new(weight: NonZeroU32) -> Self {
        Self { weight, used: 0 }
    }

    /// Consumes one slot; returns true when the quota is exhausted and the
    /// group ring should rotate.
    pub(in crate::scheduler) fn consume_slot(&mut self) -> bool {
        self.used += 1;
        if self.used >= self.weight.get() {
            self.used = 0;
            true
        } else {
            false
        }
    }
}

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
    /// Next item is scheduled (maybe this should be in head status?)
    Scheduled {
        wake_up: WakeUp,
    },
    // Not in the ready ring.
    BlockedOn(ResourceKind),
}

/// Per-service group in the two-level weighted round-robin.
///
/// The scheduler cycles through groups (outer ring); within a group, vqueues cycle in
/// the inner ring exactly like the flat DRR. A group with weight N receives N
/// dispatch slots before rotating to the next group.
#[derive(Debug)]
struct ServiceGroup {
    quota: WrrQuota,
    ring: VecDeque<VQueueHandle>,
}

impl ServiceGroup {
    fn new(weight: NonZeroU32, handle: VQueueHandle) -> Self {
        let mut ring = VecDeque::with_capacity(4);
        ring.push_back(handle);
        Self {
            quota: WrrQuota::new(weight),
            ring,
        }
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct EligibilityTracker {
    #[debug(skip)]
    delayed_eligibility: DelayQueue<VQueueHandle>,
    /// Outer WRR ring: rotation order over service groups. Contains exactly
    /// the keys of `groups`, each once.
    group_ring: VecDeque<SchedulingGroup>,
    #[debug(skip)]
    groups: HashMap<SchedulingGroup, ServiceGroup>,
    /// Which group a handle belongs to. Set when the scheduler first learns about a
    /// queue and kept for the lifetime of the cache slot (a queue's scope/service
    /// never changes), so wake-up paths that only carry the handle can find the group.
    #[debug(skip)]
    handle_group: SecondaryMap<VQueueHandle, SchedulingGroup>,
    #[debug(skip)]
    states: SecondaryMap<VQueueHandle, State>,
    #[debug(skip)]
    weight_resolver: WeightResolver,
}

impl EligibilityTracker {
    pub fn with_capacity(capacity: usize, weight_resolver: WeightResolver) -> Self {
        Self {
            delayed_eligibility: DelayQueue::with_capacity(capacity),
            group_ring: VecDeque::with_capacity(16),
            groups: HashMap::default(),
            handle_group: SecondaryMap::with_capacity(capacity),
            states: SecondaryMap::with_capacity(capacity),
            weight_resolver,
        }
    }

    /// Adds the handle to its service group's ring, creating the group (and resolving
    /// its weight) if needed. O(1); the weight resolver only runs on group creation.
    fn push_to_group(&mut self, handle: VQueueHandle) {
        let group_name = self
            .handle_group
            .get(handle)
            .cloned()
            .unwrap_or_else(|| SchedulingGroup::Service(unlinked_group()));
        if let Some(group) = self.groups.get_mut(&group_name) {
            group.ring.push_back(handle);
        } else {
            let weight = (self.weight_resolver)(&group_name);
            self.groups
                .insert(group_name.clone(), ServiceGroup::new(weight, handle));
            self.group_ring.push_back(group_name);
        }
    }

    /// Removes the front group if its ring is empty. The group being removed is always
    /// at the front of the group ring, so this is O(1).
    fn drop_front_group_if_empty(&mut self) {
        let Some(front) = self.group_ring.front() else {
            return;
        };
        if self
            .groups
            .get(front)
            .is_some_and(|group| group.ring.is_empty())
        {
            let name = self.group_ring.pop_front().expect("front exists");
            self.groups.remove(&name);
        }
    }

    /// The handle currently at the dispatch position: front of the front group's ring.
    fn front_handle(&self) -> Option<VQueueHandle> {
        let front_group = self.group_ring.front()?;
        self.groups.get(front_group)?.ring.front().copied()
    }

    pub fn insert_eligible(&mut self, handle: VQueueHandle, group: SchedulingGroup) {
        self.states.insert(handle, State::NeedsPoll);
        self.handle_group.insert(handle, group);
        self.push_to_group(handle);
    }

    /// Records (or refreshes) the group a handle belongs to. Used by paths that have
    /// access to the vqueue metadata.
    fn record_group(&mut self, handle: VQueueHandle, meta: &VQueueMeta) {
        if !self.handle_group.contains_key(handle) {
            self.handle_group.insert(handle, SchedulingGroup::of(meta));
        }
    }

    pub fn get_status<S, R>(
        &self,
        handle: VQueueHandle,
        meta: &VQueueMeta,
        qstate: &VQueueState<S>,
        resolve_rule: &R,
    ) -> SchedulingStatus
    where
        S: VQueueStore,
        R: Fn(RuleHandle) -> Option<ReString>,
    {
        match self.states.get(handle) {
            None | Some(State::NeedsPoll) | Some(State::Ready) => {
                super::status_from_detailed_eligibility(qstate.check_eligibility(meta))
            }
            Some(State::Scheduled { wake_up }) => SchedulingStatus::Scheduled {
                at: wake_up.ts.into(),
            },
            Some(State::BlockedOn(resource)) => {
                // Resolve the rule handle to a pattern string once, at status-
                // construction time, so external consumers don't need the store.
                SchedulingStatus::BlockedOn(resource.to_blocked_resource(resolve_rule))
            }
        }
    }

    pub fn poll_delayed(&mut self, cx: &mut std::task::Context<'_>) {
        while let Poll::Ready(Some(expired)) = self.delayed_eligibility.poll_expired(cx) {
            let timer_key = expired.key();
            let handle = expired.into_inner();
            if let Some(state) = self.states.get_mut(handle) {
                match state {
                    State::Scheduled { wake_up } if wake_up.timer_key == timer_key => {
                        *state = State::NeedsPoll;
                        debug_assert!(
                            self.groups
                                .values()
                                .all(|group| !group.ring.contains(&handle)),
                            "scheduled handle must not already be in a ring"
                        );
                        self.push_to_group(handle);
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn next_eligible<S: VQueueStore>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        metas: VQueuesMeta<'_>,
        storage: &S,
        vqueues: &mut SecondaryMap<VQueueHandle, VQueueState<S>>,
    ) -> Result<Option<VQueueHandle>, StorageError> {
        // Each iteration either scans+rotates a group or drops an emptied one,
        // each bounded by num_groups, so 2 * num_groups bounds the scan.
        let num_groups = self.group_ring.len();
        let scan_bound = if num_groups <= 1 {
            num_groups
        } else {
            num_groups.saturating_mul(2)
        };
        'groups: for _ in 0..scan_bound {
            let Some(group_name) = self.group_ring.front().cloned() else {
                return Ok(None);
            };
            let group = self.groups.get(&group_name).expect("ring/groups in sync");

            // avoid rescanning this group's ring multiple rounds
            let n = group.ring.len();
            for _ in 0..n {
                if self.group_ring.front() != Some(&group_name) {
                    // the group drained and was dropped during the scan; the new front
                    // group gets its own full scan without an outer rotation
                    continue 'groups;
                }
                // what is my current status
                let Some(handle) = self.front_handle() else {
                    // group drained during the scan
                    break;
                };

                let Some(qstate) = vqueues.get_mut(handle) else {
                    // the vqueue has been removed probably it's empty therefore, it's
                    // safe to assume that it's not eligible.
                    self.remove(handle);
                    self.pop_front_handle();
                    continue;
                };

                let Some(current_state) = self.states.get_mut(handle) else {
                    // The vqueue is not eligible anymore. This can happen if the vqueue became dormant
                    // due to items being removed externally (killed, etc.)
                    self.pop_front_handle();
                    continue;
                };

                let slot = metas.get(handle).unwrap();

                match current_state {
                    State::NeedsPoll => {
                        // update the state based on eligibility.
                        match qstate.poll_eligibility(cx, slot, storage) {
                            Poll::Ready(Ok(Eligibility::Eligible)) => {
                                *current_state = State::Ready;
                                return Ok(Some(handle));
                            }
                            Poll::Ready(Ok(Eligibility::EligibleAt(ts))) => {
                                let ts = ts.as_unix_millis();
                                // Cap the parked delay. See `MAX_PARK`.
                                let duration =
                                    ts.duration_since(SchedulerClock.now_millis()).min(MAX_PARK);
                                let timer_key = self.delayed_eligibility.insert(handle, duration);
                                *self.states.get_mut(handle).expect("state exists") =
                                    State::Scheduled {
                                        wake_up: WakeUp { ts, timer_key },
                                    };
                                self.pop_front_handle();
                                continue;
                            }
                            Poll::Ready(Ok(Eligibility::NotEligible)) => {
                                self.pop_front_handle();
                                self.remove(handle);
                                continue;
                            }
                            Poll::Ready(Err(err)) => {
                                return Err(err);
                            }
                            Poll::Pending => {
                                self.rotate_one();
                                continue;
                            }
                        }
                    }
                    State::BlockedOn(_) | State::Scheduled { .. } => {
                        self.pop_front_handle();
                    }
                    State::Ready => {
                        return Ok(Some(handle));
                    }
                }
            }

            // No eligible handle in this group; move on to the next group.
            self.drop_front_group_if_empty();
            if self.group_ring.len() <= 1 {
                continue 'groups;
            }
            self.group_ring.rotate_left(1);
        }

        Ok(None)
    }

    /// Pops the handle at the dispatch position (front of the front group's ring) and
    /// removes the group if it became empty. O(1).
    fn pop_front_handle(&mut self) {
        if let Some(front_group) = self.group_ring.front()
            && let Some(group) = self.groups.get_mut(front_group)
        {
            group.ring.pop_front();
            self.drop_front_group_if_empty();
        }
    }

    pub fn remove(&mut self, handle: VQueueHandle) {
        // cancel scheduled wake ups
        if let Some(State::Scheduled { wake_up }) = self.states.remove(handle) {
            self.delayed_eligibility.remove(&wake_up.timer_key);
        }
        // The handle is lazily purged from its group's ring on the next
        // `next_eligible` scan (missing state => pop). `handle_group` is
        // kept: the queue's service never changes for the cache slot's lifetime.
    }

    // Places the vqueue back on the ready ring only if it was not already there.
    // Sets the state to NeedsPoll
    pub fn ensure_queue_needs_polling(&mut self, vqueue: VQueueHandle) -> bool {
        if let Some(state) = self.states.entry(vqueue) {
            match state {
                Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                    State::BlockedOn(_resource) => {
                        occupied_entry.insert(State::NeedsPoll);
                        self.push_to_group(vqueue);
                        true
                    }
                    State::NeedsPoll => false,
                    State::Ready => {
                        // it's already in the ready ring, but we need to adjust its state
                        occupied_entry.insert(State::NeedsPoll);
                        // It's already in the ready ring, we return false since we didn't add it
                        false
                    }
                    State::Scheduled { wake_up } => {
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.push_to_group(vqueue);
                        true
                    }
                },
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(State::NeedsPoll);
                    self.push_to_group(vqueue);
                    true
                }
            }
        } else {
            // The vqueue handle was removed from cache. We are operating on stale
            // information. Best to ignore it.
            false
        }
    }

    pub fn wake_up_queues(&mut self, vqueues: impl IntoIterator<Item = VQueueHandle>) {
        vqueues
            .into_iter()
            .for_each(|vqueue| self.wake_up_queue(vqueue))
    }

    /// returns true if the scheduler should be woken up
    pub fn wake_up_queue(&mut self, vqueue: VQueueHandle) {
        let Some(state) = self.states.entry(vqueue) else {
            // The vqueue handle was removed from cache. We are operating on stale
            // information. Best to ignore it.
            return;
        };

        match state {
            Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                State::BlockedOn(_resource) => {
                    occupied_entry.insert(State::NeedsPoll);
                    self.push_to_group(vqueue);
                }
                _ => {
                    // do nothing.
                }
            },
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(State::NeedsPoll);
                self.push_to_group(vqueue);
            }
        }
    }

    /// Returns true if scheduler should be woken up
    ///
    /// If the vqueue is blocked on a resource, this function will not touch it.
    pub fn refresh_membership<S: VQueueStore>(
        &mut self,
        handle: VQueueHandle,
        meta: &VQueueMeta,
        vqueue: &VQueueState<S>,
    ) {
        self.record_group(handle, meta);

        let Some(current_state) = self.states.entry(handle) else {
            // the vqueue handle was removed from the original slot map.
            return;
        };

        match current_state {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(State::NeedsPoll);
                self.push_to_group(handle);
            }
            Entry::Occupied(mut occupied_entry) => {
                let eligibility = vqueue.check_eligibility(meta).as_compact();
                match (occupied_entry.get(), eligibility) {
                    (State::NeedsPoll, _) => {
                        // do nothing.
                    }
                    (State::Ready, _) => {
                        occupied_entry.insert(State::NeedsPoll);
                    }
                    (State::Scheduled { wake_up }, Eligibility::NotEligible) => {
                        // We should not really have a scenario where we land here. But if this
                        // happens, we err on the safe side and switch to polling.
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        occupied_entry.insert(State::NeedsPoll);
                        self.push_to_group(handle);
                    }
                    (State::Scheduled { wake_up }, Eligibility::Eligible) => {
                        // wake up now
                        self.delayed_eligibility.remove(&wake_up.timer_key);
                        self.states.insert(handle, State::NeedsPoll);
                        self.push_to_group(handle);
                    }
                    (State::Scheduled { wake_up }, Eligibility::EligibleAt(eligible_at_ts)) => {
                        let eligible_at_ts = eligible_at_ts.as_unix_millis();
                        // maybe change the wake up time
                        if eligible_at_ts != wake_up.ts {
                            self.delayed_eligibility.reset(
                                &wake_up.timer_key,
                                // Cap the parked delay. See `MAX_PARK`.
                                eligible_at_ts
                                    .duration_since(SchedulerClock.now_millis())
                                    .min(MAX_PARK),
                            );

                            occupied_entry.insert(State::Scheduled {
                                wake_up: WakeUp {
                                    ts: eligible_at_ts,
                                    timer_key: wake_up.timer_key,
                                },
                            });
                        }
                    }
                    (State::BlockedOn(_), _) => {
                        // don't touch it.
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
            self.push_to_group(handle);
            let State::BlockedOn(resource) = blocked_on else {
                unreachable!();
            };

            Some(resource)
        } else {
            None
        }
    }

    /// Rotates the current group's inner ring (DRR "needs credit" step). Group quota is
    /// not consumed since nothing was dispatched.
    pub fn rotate_one(&mut self) {
        if let Some(front_group) = self.group_ring.front()
            && let Some(group) = self.groups.get_mut(front_group)
        {
            group.ring.rotate_left(1);
        }
    }

    pub fn len(&self) -> usize {
        // Only used for periodic reporting and tests; group count is small (bounded by
        // the number of services), so summing is cheap.
        self.groups.values().map(|group| group.ring.len()).sum()
    }

    /// Called after a successful dispatch (Run/Yield): flips the dispatched queue back
    /// to NeedsPoll and consumes one unit of the group's WRR quota, rotating the group
    /// ring when the quota is exhausted.
    pub fn front_needs_poll(&mut self) {
        let Some(handle) = self.front_handle() else {
            return;
        };
        if let Some(state) = self.states.get_mut(handle) {
            *state = State::NeedsPoll;
        }
        if let Some(front_group) = self.group_ring.front()
            && let Some(group) = self.groups.get_mut(front_group)
            && group.quota.consume_slot()
            && self.group_ring.len() > 1
        {
            self.group_ring.rotate_left(1);
        }
    }

    pub fn front_blocked(&mut self, resource: ResourceKind) {
        let Some(handle) = self.front_handle() else {
            return;
        };
        if let Some(state) = self.states.get_mut(handle) {
            *state = State::BlockedOn(resource);
        }
        // A blocked queue leaves the ring until a resource release wakes it up.
        self.pop_front_handle();
    }

    pub fn compact_memory(&mut self) {
        self.delayed_eligibility.compact();
    }

    /// Verifies internal data-structure invariants. Test-only.
    #[cfg(test)]
    pub(crate) fn assert_invariants(&self) {
        use std::collections::HashSet;
        // group_ring contains exactly the keys of groups, each once
        let ring_set: HashSet<&SchedulingGroup> = self.group_ring.iter().collect();
        assert_eq!(
            ring_set.len(),
            self.group_ring.len(),
            "group_ring has duplicates"
        );
        assert_eq!(
            ring_set.len(),
            self.groups.len(),
            "group_ring and groups out of sync"
        );
        for name in self.groups.keys() {
            assert!(ring_set.contains(name), "group {name:?} missing from ring");
        }
        // no handle appears in two different rings; every ringed handle has a group mapping
        let mut seen: HashSet<VQueueHandle> = HashSet::new();
        for group in self.groups.values() {
            for &handle in &group.ring {
                assert!(seen.insert(handle), "handle {handle:?} in multiple rings");
                assert!(
                    self.handle_group.contains_key(handle),
                    "ringed handle {handle:?} missing handle_group mapping"
                );
            }
        }
    }
}
