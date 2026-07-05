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

use slotmap::SecondaryMap;

use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{SchedulingGroup, WeightResolver, WrrQuota};

/// Two-level weighted round-robin waiter list.
///
/// A plain FIFO waiter list reintroduces the starvation the two-level WRR
/// eligibility ring eliminates: when a resource (e.g. invoker concurrency) is
/// exhausted, *arrival order* decides who acquires next, so a service with
/// thousands of queued keys pushes every other service to the back regardless
/// of scheduling weights. This structure applies the same service-group WRR to
/// the waiter list itself: the head is the front of the front group's queue,
/// and a group's quota (its scheduling weight) controls how many acquisitions
/// it gets before the group ring rotates.
///
/// Invariants: `ring` contains exactly the keys of `groups`; a handle appears
/// at most once (tracked in `membership`); `total` == sum of group queue lens.
pub(in crate::scheduler) struct GroupedWaiters {
    ring: VecDeque<SchedulingGroup>,
    groups: hashbrown::HashMap<SchedulingGroup, GroupQueue>,
    /// Which group each waiting handle sits in. SecondaryMap (direct slotmap
    /// index) rather than a hash map: `VQueueHandle` is a slotmap key, matching
    /// the `EligibilityTracker::handle_group` idiom and avoiding per-op hashing.
    membership: SecondaryMap<VQueueHandle, SchedulingGroup>,
    total: usize,
    weight_resolver: WeightResolver,
}

struct GroupQueue {
    quota: WrrQuota,
    queue: VecDeque<VQueueHandle>,
}

impl GroupedWaiters {
    pub fn new(weight_resolver: WeightResolver) -> Self {
        Self {
            ring: VecDeque::new(),
            groups: hashbrown::HashMap::new(),
            membership: SecondaryMap::new(),
            total: 0,
            weight_resolver,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    pub fn len(&self) -> usize {
        self.total
    }

    /// The handle at the current WRR head (front of the front group). Only used
    /// by tests; permit claims are not head-gated.
    #[cfg(test)]
    pub fn front(&self) -> Option<VQueueHandle> {
        let group = self.ring.front()?;
        self.groups.get(group)?.queue.front().copied()
    }

    /// Adds a waiter to its scheduling group's queue (no-op if already waiting).
    pub fn push_back(&mut self, handle: VQueueHandle, group: &SchedulingGroup) {
        // single membership probe: entry() combines the duplicate check + insert
        let Some(entry) = self.membership.entry(handle) else {
            return;
        };
        let slotmap::secondary::Entry::Vacant(vacant) = entry else {
            return;
        };
        let group = group.clone();
        if let Some(gq) = self.groups.get_mut(&group) {
            gq.queue.push_back(handle);
            vacant.insert(group);
        } else {
            let weight = (self.weight_resolver)(&group);
            let mut queue = VecDeque::with_capacity(4);
            queue.push_back(handle);
            self.groups.insert(
                group.clone(),
                GroupQueue {
                    quota: WrrQuota::new(weight),
                    queue,
                },
            );
            self.ring.push_back(group.clone());
            vacant.insert(group);
        }
        self.total += 1;
    }

    /// Pops the current head (front of the front group), consuming one unit of
    /// the group's quota. Rotates the group ring when the quota is exhausted
    /// and drops the group when its queue empties.
    pub fn pop_front(&mut self) -> Option<VQueueHandle> {
        let group_name = self.ring.front()?.clone();
        let gq = self.groups.get_mut(&group_name)?;
        let handle = gq.queue.pop_front()?;
        self.membership.remove(handle);
        self.total -= 1;

        let quota_exhausted = gq.quota.consume_slot();

        if gq.queue.is_empty() {
            self.groups.remove(&group_name);
            self.ring.pop_front();
        } else if quota_exhausted && self.ring.len() > 1 {
            self.ring.rotate_left(1);
        }

        Some(handle)
    }

    /// Removes a waiter wherever it sits (external cancellation). O(group len).
    pub fn remove(&mut self, handle: VQueueHandle) {
        let Some(group_name) = self.membership.remove(handle) else {
            return;
        };
        if let Some(gq) = self.groups.get_mut(&group_name) {
            gq.queue.retain(|h| *h != handle);
            self.total -= 1;
            if gq.queue.is_empty() {
                self.groups.remove(&group_name);
                self.ring.retain(|g| g != &group_name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use slotmap::SlotMap;

    use restate_types::ServiceName;

    use super::*;

    /// Test groups: derive the weight from a `-w<N>` suffix on the group's
    /// service name so weighted scenarios don't need a live rule map.
    fn resolver() -> WeightResolver {
        Arc::new(|group: &SchedulingGroup| {
            let SchedulingGroup::Service(service_name) = group else {
                return NonZeroU32::MIN;
            };
            let name: &str = service_name.as_ref();
            name.rsplit_once("-w")
                .and_then(|(_, w)| w.parse::<u32>().ok())
                .and_then(NonZeroU32::new)
                .unwrap_or(NonZeroU32::MIN)
        })
    }

    fn group(name: &str) -> SchedulingGroup {
        SchedulingGroup::Service(ServiceName::new(name))
    }

    fn handles(n: usize) -> Vec<VQueueHandle> {
        let mut map = SlotMap::<VQueueHandle, ()>::with_key();
        (0..n).map(|_| map.insert(())).collect()
    }

    #[test]
    fn fifo_within_single_group() {
        let hs = handles(3);
        let mut w = GroupedWaiters::new(resolver());
        let g = group("svc");
        for h in &hs {
            w.push_back(*h, &g);
        }
        assert_eq!(w.len(), 3);
        assert_eq!(w.pop_front(), Some(hs[0]));
        assert_eq!(w.pop_front(), Some(hs[1]));
        assert_eq!(w.pop_front(), Some(hs[2]));
        assert!(w.is_empty());
    }

    #[test]
    fn weighted_interleave_between_groups() {
        // payment-w1 has 20 waiters queued FIRST; batcher-w10 has 4 queued after.
        // A FIFO would grant all 20 payment permits before any batcher permit;
        // the grouped list must serve batchers within the first WRR cycle.
        let payment_handles = handles(24);
        let (pay, batch) = payment_handles.split_at(20);
        let mut w = GroupedWaiters::new(resolver());
        let payment = group("payment-w1");
        let batcher = group("batcher-w10");
        for h in pay {
            w.push_back(*h, &payment);
        }
        for h in batch {
            w.push_back(*h, &batcher);
        }

        let mut first_batcher_position = None;
        for i in 0.. {
            let Some(h) = w.pop_front() else { break };
            if batch.contains(&h) && first_batcher_position.is_none() {
                first_batcher_position = Some(i);
            }
        }
        // payment (weight 1) yields after 1 grant; batcher group is next
        assert_eq!(first_batcher_position, Some(1));
    }

    #[test]
    fn duplicate_push_is_noop_and_remove_clears() {
        let hs = handles(2);
        let mut w = GroupedWaiters::new(resolver());
        let g = group("svc");
        w.push_back(hs[0], &g);
        w.push_back(hs[0], &g);
        assert_eq!(w.len(), 1);
        w.push_back(hs[1], &g);
        w.remove(hs[0]);
        assert_eq!(w.len(), 1);
        assert_eq!(w.front(), Some(hs[1]));
        w.remove(hs[1]);
        assert!(w.is_empty());
        assert!(w.ring.is_empty());
        assert!(w.groups.is_empty());
    }

    /// Removing the only waiter of a NON-front group must drop that group from
    /// both `groups` and `ring` (exercises the retain-based removal branch).
    #[test]
    fn remove_last_member_of_non_front_group() {
        let hs = handles(3);
        let mut w = GroupedWaiters::new(resolver());
        let a = group("a");
        let b = group("b");
        w.push_back(hs[0], &a); // front group
        w.push_back(hs[1], &a);
        w.push_back(hs[2], &b); // non-front group, single member
        w.remove(hs[2]);
        assert_eq!(w.len(), 2);
        assert!(!w.ring.contains(&b), "ring must drop the emptied group");
        assert!(
            !w.groups.contains_key(&b),
            "groups must drop the emptied group"
        );
        // remaining group still drains normally
        assert_eq!(w.pop_front(), Some(hs[0]));
        assert_eq!(w.pop_front(), Some(hs[1]));
        assert!(w.is_empty());
    }

    /// Weight changes apply on group re-creation (after full drain), not
    /// retroactively — same semantics as the eligibility ring.
    #[test]
    fn weight_change_applies_after_group_recreation() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let hs = handles(6);
        let weight = Arc::new(AtomicU32::new(1));
        let w_ref = Arc::clone(&weight);
        let dyn_resolver: WeightResolver = Arc::new(move |_: &SchedulingGroup| {
            NonZeroU32::new(w_ref.load(Ordering::Relaxed)).unwrap()
        });
        let mut w = GroupedWaiters::new(dyn_resolver);
        let a = group("dyn");
        let b = group("other");

        // group created at weight 1: after 1 grant it must yield to `other`
        w.push_back(hs[0], &a);
        w.push_back(hs[1], &a);
        w.push_back(hs[2], &b);
        assert_eq!(w.pop_front(), Some(hs[0]));
        assert_eq!(
            w.pop_front(),
            Some(hs[2]),
            "weight-1 group yields after one grant"
        );
        assert_eq!(w.pop_front(), Some(hs[1]));
        assert!(w.is_empty());

        // bump the weight; the re-created group must get 2 grants before yielding
        weight.store(2, Ordering::Relaxed);
        w.push_back(hs[3], &a);
        w.push_back(hs[4], &a);
        w.push_back(hs[5], &b);
        assert_eq!(w.pop_front(), Some(hs[3]));
        assert_eq!(
            w.pop_front(),
            Some(hs[4]),
            "weight-2 group gets both grants first"
        );
        assert_eq!(w.pop_front(), Some(hs[5]));
    }

    /// All-weight-1 with many groups: pop order must be a clean round-robin
    /// cycle across services, not FIFO within a cycle.
    #[test]
    fn equal_weights_round_robin_across_many_groups() {
        const GROUPS: usize = 5;
        const PER_GROUP: usize = 10;
        let hs = handles(GROUPS * PER_GROUP);
        let names: Vec<SchedulingGroup> = (0..GROUPS).map(|g| group(&format!("svc{g}"))).collect();
        let mut w = GroupedWaiters::new(resolver());
        // enqueue all of group 0 first, then group 1, etc. (worst case for FIFO)
        for (g, name) in names.iter().enumerate() {
            for q in 0..PER_GROUP {
                w.push_back(hs[g * PER_GROUP + q], name);
            }
        }
        // every consecutive window of GROUPS pops must contain one handle from
        // each group
        let mut popped = Vec::new();
        while let Some(h) = w.pop_front() {
            popped.push(h);
        }
        assert_eq!(popped.len(), GROUPS * PER_GROUP);
        for (cycle, window) in popped.chunks(GROUPS).enumerate() {
            let mut seen = [false; GROUPS];
            for h in window {
                let idx = hs.iter().position(|x| x == h).unwrap() / PER_GROUP;
                assert!(
                    !seen[idx],
                    "cycle {cycle}: group {idx} served twice within one round"
                );
                seen[idx] = true;
            }
        }
    }

    #[test]
    fn work_conserving_when_group_drains() {
        let hs = handles(6);
        let mut w = GroupedWaiters::new(resolver());
        let a = group("a-w10");
        let b = group("b-w1");
        w.push_back(hs[0], &a);
        for h in &hs[1..] {
            w.push_back(*h, &b);
        }
        // a's single waiter drains; every remaining grant goes to b
        let order: Vec<_> = std::iter::from_fn(|| w.pop_front()).collect();
        assert_eq!(order.len(), 6);
        assert_eq!(order[0], hs[0]);
    }
}
