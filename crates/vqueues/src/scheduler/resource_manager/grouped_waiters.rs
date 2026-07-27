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

use restate_types::ServiceName;

use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{LaneWeightResolver, SchedulingGroup, WeightResolver, WrrQuota};

/// Stride quantum: a lane of weight `w` advances its pass by `STRIDE1 / w` per
/// grant, so heavier lanes are selected proportionally more often and the
/// interleave is smooth (w2:w1 serves `A B A A B A…`; a lane never gets more
/// than its weight in consecutive grants while siblings have work).
const STRIDE1: u64 = 1 << 20;

/// Pass values are rebased (min subtracted from every lane) once the minimum
/// crosses this threshold, so `pass + stride` can never overflow. With
/// stride ≤ 2^20 per grant this allows ~2^42 grants between rebases.
const PASS_REBASE_THRESHOLD: u64 = u64::MAX / 2;

/// Two-level weighted round-robin waiter list with per-service stride lanes.
///
/// 1. **Across groups**: the head is the front of the front group's queue;
///    a group's [`WrrQuota`] controls how many acquisitions it gets before
///    the ring rotates, keeping cross-scope ratios exact.
/// 2. **Within a group**: waiters are held in per-service lanes scheduled by
///    stride scheduling. Each grant serves the lane with the lowest
///    `(pass, seq)`, so lane shares are weight-determined and FIFO order is
///    preserved within a lane.
///
/// Lane weights come from the [`LaneWeightResolver`] (default 1 ⇒ equal
/// round-robin). A new or refilled lane joins at the current minimum pass
/// so it can neither monopolize nor starve.
pub(in crate::scheduler) struct GroupedWaiters {
    ring: VecDeque<SchedulingGroup>,
    groups: hashbrown::HashMap<SchedulingGroup, GroupQueue>,
    /// Which (group, lane) each waiting handle sits in. SecondaryMap (direct
    /// slotmap index) rather than a hash map: `VQueueHandle` is a slotmap key,
    /// matching the `EligibilityTracker::handle_group` idiom.
    membership: SecondaryMap<VQueueHandle, (SchedulingGroup, ServiceName)>,
    total: usize,
    weight_resolver: WeightResolver,
    lane_weight_resolver: LaneWeightResolver,
}

struct GroupQueue {
    quota: WrrQuota,
    lanes: hashbrown::HashMap<ServiceName, Lane>,
    /// Monotonic lane-creation counter, the deterministic tie-break for equal
    /// passes. Never reset while the group lives.
    lane_seq: u64,
}

struct Lane {
    queue: VecDeque<VQueueHandle>,
    /// Stride pass: the lane with the lowest `(pass, seq)` serves next.
    pass: u64,
    stride: u64,
    seq: u64,
}

impl GroupQueue {
    /// The lane that serves the group's next grant: minimum `(pass, seq)`.
    /// Lanes are few (≤ number of services in the scope), so a linear scan
    /// beats maintaining an ordered index. Deterministic: `seq` is unique.
    fn min_lane(&self) -> Option<&ServiceName> {
        self.lanes
            .iter()
            .min_by_key(|(_, lane)| (lane.pass, lane.seq))
            .map(|(name, _)| name)
    }

    fn min_pass(&self) -> u64 {
        self.lanes.values().map(|l| l.pass).min().unwrap_or(0)
    }

    /// Subtract the minimum pass from every lane once it grows past the
    /// threshold. Relative order (and therefore the schedule) is unchanged.
    fn maybe_rebase(&mut self) {
        let min = self.min_pass();
        if min > PASS_REBASE_THRESHOLD {
            for lane in self.lanes.values_mut() {
                lane.pass -= min;
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }
}

impl GroupedWaiters {
    pub fn new(weight_resolver: WeightResolver, lane_weight_resolver: LaneWeightResolver) -> Self {
        Self {
            ring: VecDeque::new(),
            groups: hashbrown::HashMap::new(),
            membership: SecondaryMap::new(),
            total: 0,
            weight_resolver,
            lane_weight_resolver,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    pub fn len(&self) -> usize {
        self.total
    }

    /// The handle at the current head (front of the front group's poorest
    /// lane). Only used by tests; permit claims are not head-gated.
    #[cfg(test)]
    pub fn front(&self) -> Option<VQueueHandle> {
        let gq = self.groups.get(self.ring.front()?)?;
        let lane = gq.min_lane()?.clone();
        gq.lanes.get(&lane)?.queue.front().copied()
    }

    /// Adds a waiter to its service lane within its scheduling group (no-op if
    /// already waiting).
    pub fn push_back(&mut self, handle: VQueueHandle, group: &SchedulingGroup, service: &ServiceName) {
        self.push(handle, group, service, false)
    }

    /// Re-adds a waiter at the front of its service lane, refunding the
    /// lane's pass bump. Used for the wake-then-steal race, where another
    /// dispatched queue claimed the freed permit before this waiter did.
    pub fn push_front(&mut self, handle: VQueueHandle, group: &SchedulingGroup, service: &ServiceName) {
        self.push(handle, group, service, true)
    }

    fn push(
        &mut self,
        handle: VQueueHandle,
        group: &SchedulingGroup,
        service: &ServiceName,
        front: bool,
    ) {
        // single membership probe: entry() combines the duplicate check + insert
        let Some(entry) = self.membership.entry(handle) else {
            return;
        };
        let slotmap::secondary::Entry::Vacant(vacant) = entry else {
            return;
        };

        let gq = if let Some(gq) = self.groups.get_mut(group) {
            gq
        } else {
            let weight = (self.weight_resolver)(group);
            self.groups.insert(
                group.clone(),
                GroupQueue {
                    quota: WrrQuota::new(weight),
                    lanes: hashbrown::HashMap::new(),
                    lane_seq: 0,
                },
            );
            self.ring.push_back(group.clone());
            self.groups.get_mut(group).expect("just inserted")
        };

        if let Some(lane) = gq.lanes.get_mut(service) {
            if front {
                lane.queue.push_front(handle);
                // refund the stride charged by the pop that woke this waiter —
                // the lane never received the grant
                lane.pass = lane.pass.saturating_sub(lane.stride);
            } else {
                lane.queue.push_back(handle);
            }
        } else {
            // New (or refilled) lane: join at the current minimum pass with a
            // fresh tie-break seq, served right after the poorest existing lane.
            let weight = (self.lane_weight_resolver)(group, service);
            let stride = (STRIDE1 / u64::from(weight.get())).max(1);
            let pass = gq.min_pass();
            let seq = gq.lane_seq;
            gq.lane_seq += 1;
            let mut queue = VecDeque::with_capacity(4);
            queue.push_back(handle);
            gq.lanes.insert(
                service.clone(),
                Lane {
                    queue,
                    pass,
                    stride,
                    seq,
                },
            );
        }
        vacant.insert((group.clone(), service.clone()));
        self.total += 1;
    }

    /// Pops the current head: the front of the poorest lane of the front
    /// group. Charges the lane one stride, consumes one unit of the group's
    /// quota, rotates the group ring when the quota is exhausted, and drops
    /// emptied lanes/groups.
    pub fn pop_front(&mut self) -> Option<VQueueHandle> {
        let group_name = self.ring.front()?.clone();
        let gq = self.groups.get_mut(&group_name)?;

        let lane_name = gq.min_lane()?.clone();
        let lane = gq.lanes.get_mut(&lane_name)?;
        let handle = lane.queue.pop_front()?;
        lane.pass = lane.pass.saturating_add(lane.stride);
        if lane.queue.is_empty() {
            // an emptied lane is dropped; if it refills it rejoins at min pass
            gq.lanes.remove(&lane_name);
        } else {
            gq.maybe_rebase();
        }

        self.membership.remove(handle);
        self.total -= 1;

        let quota_exhausted = gq.quota.consume_slot();

        if gq.is_empty() {
            self.groups.remove(&group_name);
            self.ring.pop_front();
        } else if quota_exhausted && self.ring.len() > 1 {
            self.ring.rotate_left(1);
        }

        Some(handle)
    }

    /// Removes a waiter wherever it sits (external cancellation). O(lane len)
    /// — the membership map pins down the exact lane, so a minority service's
    /// removal never scans a flooded sibling's queue.
    pub fn remove(&mut self, handle: VQueueHandle) {
        let Some((group_name, lane_name)) = self.membership.remove(handle) else {
            return;
        };
        let Some(gq) = self.groups.get_mut(&group_name) else {
            return;
        };
        if let Some(lane) = gq.lanes.get_mut(&lane_name) {
            lane.queue.retain(|h| *h != handle);
            self.total -= 1;
            if lane.queue.is_empty() {
                gq.lanes.remove(&lane_name);
            }
            if gq.is_empty() {
                self.groups.remove(&group_name);
                self.ring.retain(|g| g != &group_name);
            }
        }
    }

    /// Debug/test invariant check: membership, totals, ring and lane structure
    /// must agree.
    #[cfg(test)]
    fn assert_invariants(&self) {
        let mut count = 0;
        for group in &self.ring {
            let gq = self.groups.get(group).expect("ring entry has a group");
            assert!(!gq.is_empty(), "no empty group may stay in the ring");
            for (lane_name, lane) in &gq.lanes {
                assert!(!lane.queue.is_empty(), "no empty lane may be retained");
                for h in &lane.queue {
                    let (g, s) = self.membership.get(*h).expect("waiter has membership");
                    assert_eq!(g, group);
                    assert_eq!(s, lane_name);
                    count += 1;
                }
            }
        }
        assert_eq!(self.groups.len(), self.ring.len(), "groups ⊆ ring exactly");
        assert_eq!(count, self.total, "total == sum of lane lens");
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use slotmap::SlotMap;

    use restate_types::ServiceName;

    use super::*;

    /// Test groups: derive the group weight from a `-w<N>` suffix on the
    /// group's service name so weighted scenarios don't need a live rule map.
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

    /// Test lanes: derive the lane weight from a `-lw<N>` suffix on the lane's
    /// service name (default 1).
    fn lane_resolver() -> LaneWeightResolver {
        Arc::new(|_: &SchedulingGroup, service: &ServiceName| {
            let name: &str = service.as_ref();
            name.rsplit_once("-lw")
                .and_then(|(_, w)| w.parse::<u32>().ok())
                .and_then(NonZeroU32::new)
                .unwrap_or(NonZeroU32::MIN)
        })
    }

    fn waiters() -> GroupedWaiters {
        GroupedWaiters::new(resolver(), lane_resolver())
    }

    fn group(name: &str) -> SchedulingGroup {
        SchedulingGroup::Service(ServiceName::new(name))
    }

    fn svc(name: &str) -> ServiceName {
        ServiceName::new(name)
    }

    fn handles(n: usize) -> Vec<VQueueHandle> {
        let mut map = SlotMap::<VQueueHandle, ()>::with_key();
        (0..n).map(|_| map.insert(())).collect()
    }

    #[test]
    fn fifo_within_single_service() {
        let hs = handles(3);
        let mut w = waiters();
        let g = group("svc");
        let s = svc("svc");
        for h in &hs {
            w.push_back(*h, &g, &s);
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
        let mut w = waiters();
        let payment = group("payment-w1");
        let batcher = group("batcher-w10");
        for h in pay {
            w.push_back(*h, &payment, &svc("payment-w1"));
        }
        for h in batch {
            w.push_back(*h, &batcher, &svc("batcher-w10"));
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
        let mut w = waiters();
        let g = group("svc");
        let s = svc("svc");
        w.push_back(hs[0], &g, &s);
        w.push_back(hs[0], &g, &s);
        assert_eq!(w.len(), 1);
        w.push_back(hs[1], &g, &s);
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
        let mut w = waiters();
        let a = group("a");
        let b = group("b");
        w.push_back(hs[0], &a, &svc("a"));
        w.push_back(hs[1], &a, &svc("a"));
        w.push_back(hs[2], &b, &svc("b"));
        w.remove(hs[2]);
        assert_eq!(w.len(), 2);
        assert!(!w.ring.contains(&b), "ring must drop the emptied group");
        assert!(
            !w.groups.contains_key(&b),
            "groups must drop the emptied group"
        );
        w.assert_invariants();
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
        let mut w = GroupedWaiters::new(dyn_resolver, lane_resolver());
        let a = group("dyn");
        let b = group("other");

        // group created at weight 1: after 1 grant it must yield to `other`
        w.push_back(hs[0], &a, &svc("dyn"));
        w.push_back(hs[1], &a, &svc("dyn"));
        w.push_back(hs[2], &b, &svc("other"));
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
        w.push_back(hs[3], &a, &svc("dyn"));
        w.push_back(hs[4], &a, &svc("dyn"));
        w.push_back(hs[5], &b, &svc("other"));
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
        let mut w = waiters();
        // enqueue all of group 0 first, then group 1, etc. (worst case for FIFO)
        for (g, name) in names.iter().enumerate() {
            let SchedulingGroup::Service(sname) = name else {
                unreachable!()
            };
            let sname = sname.clone();
            for q in 0..PER_GROUP {
                w.push_back(hs[g * PER_GROUP + q], name, &sname);
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
        let mut w = waiters();
        let a = group("a-w10");
        let b = group("b-w1");
        w.push_back(hs[0], &a, &svc("a-w10"));
        for h in &hs[1..] {
            w.push_back(*h, &b, &svc("b-w1"));
        }
        // a's single waiter drains; every remaining grant goes to b
        let order: Vec<_> = std::iter::from_fn(|| w.pop_front()).collect();
        assert_eq!(order.len(), 6);
        assert_eq!(order[0], hs[0]);
    }

    // ───────────────────────── stride-lane tests ─────────────────────────

    /// THE production pathology: 4,000 keyed-parent handles queue FIRST in the
    /// scope group, 21 callee handles after. FIFO would grant all 4,000 before
    /// any callee; per-service stride lanes must serve the callee's first
    /// handle on the group's SECOND grant, then strictly interleave 1:1.
    #[test]
    fn equal_weights_interleave_across_lanes() {
        let hs = handles(4021);
        let (parents, callees) = hs.split_at(4000);
        let mut w = waiters();
        let g = group("payment-w10");
        let workflow = svc("PaymentWorkflow");
        let callee = svc("SubService");
        for h in parents {
            w.push_back(*h, &g, &workflow);
        }
        for h in callees {
            w.push_back(*h, &g, &callee);
        }
        w.assert_invariants();

        let mut order = Vec::new();
        while let Some(h) = w.pop_front() {
            order.push(h);
        }
        assert_eq!(order.len(), 4021);
        let first_callee = order.iter().position(|h| callees.contains(h)).unwrap();
        assert_eq!(first_callee, 1, "first callee served on the second grant");
        // strict 1:1 interleave while both lanes are non-empty (first 42 pops)
        for (i, h) in order.iter().take(42).enumerate() {
            let is_callee = callees.contains(h);
            assert_eq!(
                is_callee,
                i % 2 == 1,
                "position {i}: expected strict parent/callee alternation"
            );
        }
    }

    /// Weighted lanes must interleave smoothly: with weights 5:1:1, every
    /// 7-grant window carries exactly 5/1/1 and the heavy lane never
    /// exceeds its weight in consecutive grants.
    #[test]
    fn weighted_lanes_interleave_smoothly() {
        let hs = handles(70);
        let mut w = waiters();
        let g = group("payment-w10");
        let heavy = svc("Workflow-lw5");
        let e1 = svc("Emitter-lw1");
        let e2 = svc("Sub-lw1");
        // 50 heavy, 10 each light, heavy queued first (worst case)
        for h in &hs[..50] {
            w.push_back(*h, &g, &heavy);
        }
        for h in &hs[50..60] {
            w.push_back(*h, &g, &e1);
        }
        for h in &hs[60..70] {
            w.push_back(*h, &g, &e2);
        }

        let mut order = Vec::new();
        while let Some(h) = w.pop_front() {
            order.push(h);
        }
        let lane_of = |h: &VQueueHandle| -> usize {
            let idx = hs.iter().position(|x| x == h).unwrap();
            if idx < 50 {
                0
            } else if idx < 60 {
                1
            } else {
                2
            }
        };
        // exact ratio per full cycle while all lanes are non-empty:
        // 10 light handles * 7 grants/cycle = first 70 pops cover it, but the
        // light lanes drain after 10 cycles → check the first 10 windows of 7
        for (win, chunk) in order.chunks(7).take(10).enumerate() {
            let heavy_count = chunk.iter().filter(|h| lane_of(h) == 0).count();
            assert_eq!(heavy_count, 5, "window {win}: heavy lane must get 5 of 7");
        }
        // smoothness: the heavy lane never exceeds its weight in a row
        let mut run = 0;
        for h in order.iter().take(70) {
            if lane_of(h) == 0 {
                run += 1;
                assert!(
                    run <= 5,
                    "heavy lane must not exceed its weight in consecutive grants (got {run})"
                );
            } else {
                run = 0;
            }
        }
    }

    /// A drained-then-refilled lane joins at the current MIN pass with a new
    /// seq: neither monopolizing (pass 0) nor starving (stale-high pass).
    #[test]
    fn join_at_min_pass() {
        let hs = handles(24);
        let mut w = waiters();
        let g = group("payment-w10");
        let a = svc("A");
        let b = svc("B");
        // A has 1 handle, B has 10 → A drains on grant 1, B accumulates pass
        w.push_back(hs[0], &g, &a);
        for h in &hs[1..11] {
            w.push_back(*h, &g, &b);
        }
        assert_eq!(w.pop_front(), Some(hs[0]), "A first (lower seq at same pass)");
        for _ in 0..5 {
            w.pop_front(); // B advances, pass grows
        }
        // A refills: must join at B's CURRENT pass (min), not 0 — so strict
        // alternation resumes instead of A monopolizing
        for h in &hs[11..21] {
            w.push_back(*h, &g, &a);
        }
        let mut a_grants = 0;
        let mut b_grants = 0;
        for _ in 0..6 {
            let h = w.pop_front().unwrap();
            let idx = hs.iter().position(|x| x == &h).unwrap();
            if (11..21).contains(&idx) {
                a_grants += 1;
            } else {
                b_grants += 1;
            }
        }
        assert_eq!(a_grants, 3, "refilled lane alternates, does not monopolize");
        assert_eq!(b_grants, 3);
    }

    /// Rebase must preserve relative order and never overflow. The rebase
    /// fires on a pop whose lane survives (an emptied lane is dropped before
    /// the check), so B carries two handles here.
    #[test]
    fn pass_rebase_no_overflow() {
        let hs = handles(3);
        let mut w = waiters();
        let g = group("payment-w10");
        let a = svc("A");
        let b = svc("B");
        w.push_back(hs[0], &g, &a);
        w.push_back(hs[1], &g, &b);
        w.push_back(hs[2], &g, &b);
        // force passes past the threshold: A slightly higher than B
        {
            let gq = w.groups.get_mut(&g).unwrap();
            gq.lanes.get_mut(&a).unwrap().pass = PASS_REBASE_THRESHOLD + 15;
            gq.lanes.get_mut(&b).unwrap().pass = PASS_REBASE_THRESHOLD + 10;
        }
        // B (lower pass) serves first; its lane survives → rebase fires
        assert_eq!(w.pop_front(), Some(hs[1]), "lower pass serves first");
        {
            let gq = w.groups.get(&g).unwrap();
            assert!(
                gq.lanes.values().all(|l| l.pass < PASS_REBASE_THRESHOLD),
                "rebase must pull passes back under the threshold"
            );
        }
        // relative order preserved: A (now ~0) before B (now ~stride)
        assert_eq!(w.pop_front(), Some(hs[0]));
        assert_eq!(w.pop_front(), Some(hs[2]));
        assert!(w.is_empty());
    }

    /// push_front (the steal re-park) restores the handle at its lane's front
    /// AND refunds the stride, so the lane is not charged for an unreceived
    /// grant.
    #[test]
    fn push_front_refunds_stride() {
        let hs = handles(4);
        let mut w = waiters();
        let g = group("payment-w10");
        let a = svc("A");
        let b = svc("B");
        w.push_back(hs[0], &g, &a);
        w.push_back(hs[1], &g, &a);
        w.push_back(hs[2], &g, &b);

        // A is chosen (seq order), charged one stride
        let woken = w.pop_front().unwrap();
        assert_eq!(woken, hs[0]);
        let pass_after_pop = w.groups.get(&g).unwrap().lanes.get(&a).unwrap().pass;
        assert!(pass_after_pop > 0);

        // steal: the woken waiter lost the race and re-parks at its lane front
        w.push_front(woken, &g, &a);
        let gq = w.groups.get(&g).unwrap();
        assert_eq!(
            gq.lanes.get(&a).unwrap().pass,
            pass_after_pop - gq.lanes.get(&a).unwrap().stride,
            "stride refunded"
        );
        assert_eq!(
            gq.lanes.get(&a).unwrap().queue.front(),
            Some(&woken),
            "woken waiter back at its lane front"
        );
        w.assert_invariants();
        // and it is served next (pass refunded to the minimum)
        assert_eq!(w.pop_front(), Some(hs[0]));
    }

    /// Cross-group ratios must stay EXACT with multi-lane groups: the outer
    /// quota consumes one unit per grant regardless of which lane served.
    #[test]
    fn cross_scope_ratio_exact_with_lanes() {
        let hs = handles(90);
        let mut w = waiters();
        let payment = group("payment-w10");
        let indexing = group("indexing-w5");
        // payment: two lanes (30 + 30); indexing: one lane (30)
        for h in &hs[..30] {
            w.push_back(*h, &payment, &svc("Workflow"));
        }
        for h in &hs[30..60] {
            w.push_back(*h, &payment, &svc("Emitter"));
        }
        for h in &hs[60..90] {
            w.push_back(*h, &indexing, &svc("Batcher"));
        }
        // one full WRR cycle = 15 grants: exactly 10 payment + 5 indexing,
        // at every cycle boundary while both groups have work
        let mut order = Vec::new();
        while let Some(h) = w.pop_front() {
            order.push(h);
        }
        for (cycle, chunk) in order.chunks(15).take(4).enumerate() {
            let payment_grants = chunk
                .iter()
                .filter(|h| hs.iter().position(|x| &x == h).unwrap() < 60)
                .count();
            assert_eq!(
                payment_grants, 10,
                "cycle {cycle}: payment must get exactly 10 of 15 grants"
            );
        }
    }

    #[test]
    fn remove_cascades_consistently() {
        let hs = handles(5);
        let mut w = waiters();
        let g = group("payment-w10");
        let a = svc("A");
        let b = svc("B");
        w.push_back(hs[0], &g, &a);
        w.push_back(hs[1], &g, &a);
        w.push_back(hs[2], &g, &b);
        w.assert_invariants();

        // removing B's only member drops the lane but keeps the group
        w.remove(hs[2]);
        w.assert_invariants();
        assert_eq!(w.len(), 2);
        assert!(w.groups.get(&g).unwrap().lanes.get(&b).is_none());

        // removing the rest drops the group and the ring entry
        w.remove(hs[0]);
        w.remove(hs[1]);
        assert!(w.is_empty());
        assert!(w.ring.is_empty());
        assert!(w.groups.is_empty());
        w.assert_invariants();
    }

    /// Identical push/pop sequences must produce identical grant orders —
    /// ordering state lives in VecDeques and (pass, seq) tuples only, never in
    /// HashMap iteration order.
    #[test]
    fn deterministic_grant_order() {
        let run = || {
            let hs = handles(30);
            let mut w = waiters();
            let g1 = group("payment-w2");
            let g2 = group("indexing-w1");
            for (i, h) in hs.iter().enumerate() {
                match i % 3 {
                    0 => w.push_back(*h, &g1, &svc("Workflow")),
                    1 => w.push_back(*h, &g1, &svc("Emitter")),
                    _ => w.push_back(*h, &g2, &svc("Batcher")),
                }
            }
            let mut order = Vec::new();
            while let Some(h) = w.pop_front() {
                // record positions, not handles (handles differ across runs)
                order.push(hs.iter().position(|x| x == &h).unwrap());
            }
            order
        };
        assert_eq!(run(), run(), "grant order must be deterministic");
    }
}
