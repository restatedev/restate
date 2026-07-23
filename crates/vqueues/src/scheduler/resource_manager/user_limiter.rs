// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::doc_overindented_list_items)]
//! # Background
//!
//! For each vqueue that has scope+limit-key, we track its usage via a set of counters in [`Usage`].
//! Counters are organized in a trie-like structure, scope->level1->level2. In the case of
//! concurrency limiting, they track the number of concurrently running invocations (invocations
//! in the same scope+limit-key in the run stage of inbox across all vqueues within the same scope).
//!
//! Permit acquisition is a two-phase process: first, capacity is checked at all applicable levels
//! without side effects. If all levels have capacity, the permit is staged in a
//! [`ProvisionalPermit`] and later secured — this is the only point where counters are
//! incremented and locks are acquired (see [`ProvisionalPermit::secure`]).
//!
//! When a permit is released, we decrement the corresponding usage counters on the corresponding
//! (scope, scope.level1, scope.level1.level2) counters, but we also need to figure out if we have
//! queues that are waiting for capacity and that can be woken up as a result of this release.
//!
//! Consequently, we need to keep track of which vqueues can be woken up when a counter is released,
//! to do that, let's look at an example.
//!
//! Consider the following scenario:
//! Rules:
//!   -  scope1/*/tenant1 = 10 (10 limit for concurrent invocations) for l2=tenant1
//!   -  * = 100   each scope gets a maximum of 100 concurrent invocations
//!
//! We have the following running invocations already:
//!   - x10 invocations with scope1/foo/tenant1 (scope=scope1, limit-key=foo/tenant1)
//!        counters: scope1, scope1.foo, scope1.foo.tenant1
//!   - x9 invocations with scope1/bar/tenant1 (scope=scope1, limit-key=bar/tenant1)
//!        counters: scope1, scope1.bar, scope1.bar.tenant1
//!   - x1 invocation with scope1/bar (scope=scope1, limit-key=bar)
//!        counters: scope1, scope1.bar
//!   - x80 invocations with scope1   (scope=scope1, limit-key=None)
//!        counters: scope1
//!
//! This results in the following state of counters:
//!   - scope1.foo.tenant1 = 10
//!   - scope1.foo = 10
//!   - scope1.bar.tenant1 = 9
//!   - scope1.bar = 10        (remember that 1 invocation has scope/l1 defined with scope1/bar)
//!   - scope1 = 100           (sum of all: 10 + 9 + 1 + 80)
//!
//! Now, let's walk through a journey of some vqueues trying to acquire new permits.
//!  - inv1: scope1  (scope=scope1, limit-key=None)
//!      matching rules:
//!          * = 100
//!      matching counters:
//!          scope1 = 100
//!      status: BLOCKED on Level=Scope.
//!
//!  - inv2: scope1/asoli (scope=scope1, limit-key=asoli)
//!      matching rules:
//!          * = 100
//!      matching counters:
//!          scope1 = 100     (BLOCKED, on rule *=100)
//!          scope1.asoli = 0 (allowed, no rule match means unlimited)
//!
//!  - inv3: scope1/foo (scope=scope1, limit-key=foo)
//!      matching rules:
//!          * = 100
//!      matching counters:
//!          scope1 = 100     (BLOCKED, on rule *=100)
//!          scope1.foo = 10  (allowed, no rule match means unlimited)
//!
//!  - inv4: scope1/bar/tenant1 (scope=scope1, limit-key=bar/tenant1)
//!          * = 100
//!          scope1/*/tenant1 = 10
//!      matching counters:
//!          scope1 = 100      (BLOCKED, on rule *=100)
//!          scope1.bar = 10   (allowed, no rule match means unlimited)
//!          scope1.bar.tenant1 = 9  (allowed, on rule scope1/*/tenant1)
//!
//!  - inv5: scope1/foo/tenant1 (scope=scope1, limit-key=foo/tenant1)
//!          * = 100
//!          scope1/*/tenant1 = 10
//!      matching counters:
//!          scope1 = 100      (BLOCKED, on rule *=100)
//!          scope1.foo = 10   (allowed, no rule match means unlimited)
//!          scope1.foo.tenant1 = 10  (BLOCKED, on rule scope1/*/tenant1)
//!
//!
//! This illustrates the following:
//!   1. An invocation/vqueue can be blocked on multiple levels at the same time.
//!   2. Parent counters are inclusive of their children counters but can be bigger because
//!      some invocations are invoked directly on their level (e.g. scope1/bar)
//!   3. Multiple invocations/vqueues can be blocked on the same rule but different concrete
//!      counters. For instance, inv5 and inv4 at L3-level rule scope1/*/tenant1 but if an
//!      permit was released (e.g. scope1/foo/tenant1), that would only impact inv5's potential
//!      eligibility but not inv4's.
//!   4. A counter at a specific level matches a single rule, or no rule (unlimited). It cannot
//!      match multiple rules following the rules of precedence defined in `restate-limiter`'s
//!      rule module. This means that if a rule has been "deleted", or "inserted", some counters
//!      may match different rules than before and therefore different limits. This also may impact
//!      the eligibility of vqueues that belong to these counters.
//!   5. Because a rule may match many counters, one cannot simply say that a vqueue is blocked
//!      behind a rule. Instead, the combination of the rule _and_ the concrete counter(s) are
//!      what determines whether a vqueue is eligible or not. A change in any of them would result
//!      in a _potential_ impact on the eligibility.
//!
//! ## Design
//!
//! ### Transactional acquisition
//!
//! The check phase in `poll_acquire_permit` is side-effect free — it only stages resources in
//! a [`ProvisionalPermit`]. All state mutations (lock acquisition, counter increments) happen
//! atomically in [`ProvisionalPermit::secure`]. If any check fails, the provisional is dropped
//! without side effects, and the vqueue is registered behind the first blocking resource.
//!
//! ### Waiter tracking
//!
//! Each blocked vqueue is queued behind a single counter — the narrowest (deepest) level where
//! it's blocked. Waiter lists are embedded directly in the trie nodes ([`ScopeNode`],
//! [`L1Node`], [`L2Leaf`]) alongside usage counters.
//!
//! The routing info (scope, limit_key, blocked_level) is carried in
//! `ResourceKind::LimitKeyConcurrency` and held by the `EligibilityTracker`. The `UserLimiter`
//! does not maintain its own membership map — removal uses the routing info passed back from
//! the eligibility tracker.
//!
//! ### Correctness of narrowest-counter queuing
//!
//! A permit release always decrements ALL ancestor counters. If the narrowest blocked counter
//! decreases, all its ancestors also decrease. The vqueue is woken, re-evaluates, and either
//! proceeds or re-queues at a different narrowest-blocked counter (dynamic migration).
//!
//! ### Wake strategy
//!
//! On release, we pop at most 1 vqueue from each affected counter's waiter list (up to 3
//! wakeups per release — one per level). Waking proceeds deepest level first so that vqueues
//! blocked on the most restrictive counter get priority, since the release directly addresses
//! their constraint.
//!
//! ### Rule updates
//!
//! When a rule changes, we only wake waiters at the changed rule's level — not descendants.
//! Waiters at deeper levels are blocked on something more specific; the shallower change doesn't
//! help them. When eventually re-evaluated, they see the current rules. Over-limit scenarios
//! (rule becomes more restrictive while permits are outstanding) use soft enforcement: existing
//! permits drain naturally, new ones are denied until usage drops below the new limit.
//!
//! ### Trie pruning
//!
//! After each release, empty trie nodes are pruned bottom-up: L2 leaves, then L1 nodes, then
//! scope nodes are removed when they have zero usage, no waiters, and no children. This prevents
//! unbounded trie growth from transient keys.
//!
//! Note: usage and waiter counts are related but not strictly coupled. For example, usage can
//! transiently be zero while waiters still exist if many releases happen before woken vqueues
//! are re-evaluated.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use arrayvec::ArrayVec;
use hashbrown::HashMap;
use tokio::time::Instant;

use restate_limiter::{
    Level, Limit, LimitKey, Pattern, RuleHandle, RulePattern, Rules, StructuredLimits,
};
use restate_types::Scope;
use restate_types::identifiers::PartitionKey;
use restate_util_string::{ReString, RestrictedValue};
use restate_worker_api::UserLimitCounterEntry;
use restate_worker_api::resources::{RuleUpdate, UserLimits};

use crate::scheduler::VQueueHandle;
use crate::scheduler::resource_manager::gradient2::{Gradient2Controller, UpdateOutcome};

#[derive(Clone, Copy, Debug)]
pub enum LimitKind {
    Concurrency,
}

#[derive(Default, Copy, Clone, Debug)]
pub struct Usage {
    /// Usage value for [`LimitKind::Concurrency`]
    concurrency: u32,
}

impl Usage {
    fn increment(&mut self, limit_kind: LimitKind) {
        match limit_kind {
            LimitKind::Concurrency => self.concurrency += 1,
        }
    }

    fn decrement(&mut self, limit_kind: LimitKind) {
        match limit_kind {
            LimitKind::Concurrency => self.concurrency = self.concurrency.saturating_sub(1),
        }
    }

    fn is_zero(&self) -> bool {
        self.concurrency == 0
    }
}

pub struct UserLimiter {
    state: State,
    rules: Rules<ReString, UserLimits>,
    /// One Gradient2 controller per adaptive rule. A controller exists only
    /// while the rule has `adaptive_concurrency` set and no static
    /// `concurrency` (static takes precedence).
    adaptive: HashMap<RuleHandle, Gradient2Controller>,
    /// Metric label distinguishing this partition's series (many partitions
    /// share one process-global metrics registry).
    partition_label: String,
}

impl UserLimiter {
    pub fn create(partition_label: String) -> Self {
        Self {
            rules: Rules::default(),
            state: Default::default(),
            adaptive: HashMap::new(),
            partition_label,
        }
    }

    /// Checks capacity for the given scope + limit key across all hierarchy levels.
    ///
    /// Returns a [`CapacityResult`] that the caller can inspect, log, or store.
    /// Use [`CapacityResult::has_capacity`] or [`CapacityResult::narrowest_blocked`] to
    /// determine the outcome.
    /// Acquire-path entry: feeds the sojourn observation to the adaptive
    /// controllers, then delegates to the capacity check.
    pub(super) fn check_concurrency_capacity_observing(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        sojourn: Duration,
    ) -> CapacityResult {
        if !self.adaptive.is_empty() {
            let targets: ArrayVec<RuleHandle, { Level::COUNT }> = {
                let limits = self.rules.lookup(scope.as_str(), limit_key);
                [Level::Level2, Level::Level1, Level::Scope]
                    .into_iter()
                    .filter_map(|level| match limits.limit_at(level) {
                        Limit::Defined(handle, _) if self.adaptive.contains_key(handle) => {
                            Some(*handle)
                        }
                        _ => None,
                    })
                    .collect()
            };
            let now = Instant::now();
            for handle in targets {
                if let Some(controller) = self.adaptive.get_mut(&handle) {
                    controller.on_sojourn(sojourn, now);
                }
            }
        }
        self.check_concurrency_capacity(scope, limit_key)
    }

    /// Capacity check with the adaptive overlay applied (no sojourn feed —
    /// used by the observing acquire-path entry above and by tests).
    pub(super) fn check_concurrency_capacity(
        &self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
    ) -> CapacityResult {
        let limits = self.rules.lookup(scope.as_str(), limit_key);
        let mut result =
            self.state
                .check_capacity(scope, limit_key, LimitKind::Concurrency, &limits);
        // Adaptive overlay: a controller only exists for rules without a
        // static concurrency, so overwriting the (then-None) value is safe.
        if !self.adaptive.is_empty() {
            for level in &mut result.levels {
                if let Some(handle) = level.rule_handle
                    && let Some(controller) = self.adaptive.get(&handle)
                {
                    level.limit_value = Some(controller.current_limit());
                }
            }
        }
        result
    }

    /// Increments usage counters at all levels along the path (scope → l1 → l2).
    pub(super) fn increment_all(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        limit_kind: LimitKind,
    ) {
        self.state.increment_all(scope, limit_key, limit_kind);
    }

    /// Decrements usage counters and wakes up to 1 vqueue per affected counter level.
    ///
    /// Returns up to 3 vqueue handles that should be woken via `EligibilityTracker`.
    pub(super) fn release_concurrency(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
    ) -> ArrayVec<VQueueHandle, { Level::COUNT }> {
        self.state
            .decrement_and_wake(scope, limit_key, LimitKind::Concurrency)
    }

    /// Releases a permit and, when a hold-time sample is attached, feeds it
    /// to the deepest adaptive controller matching the permit's rule chain.
    /// On a limit increase, up to `new_limit - usage` waiters at that rule's
    /// level are woken (targeted wake-K — never a full drain).
    pub(super) fn on_permit_released(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        held_for: Option<Duration>,
    ) -> Vec<VQueueHandle> {
        let mut woken: Vec<VQueueHandle> = self
            .release_concurrency(scope, limit_key)
            .into_iter()
            .collect();

        // Zero adaptive rules (the common case): skip the trie walk entirely —
        // this branch runs on every completion, mirroring the acquire-path guard.
        let Some(hold) = held_for else {
            return woken;
        };
        if self.adaptive.is_empty() {
            return woken;
        }
        // Feed the sample to every adaptive rule along the (scope, limit_key)
        // chain: each level learns its own latency independently. Collect
        // first so the `rules` borrow ends before mutation.
        let targets: ArrayVec<(RuleHandle, Level), { Level::COUNT }> = {
            let limits = self.rules.lookup(scope.as_str(), limit_key);
            [Level::Level2, Level::Level1, Level::Scope]
                .into_iter()
                .filter_map(|level| match limits.limit_at(level) {
                    Limit::Defined(handle, _) if self.adaptive.contains_key(handle) => {
                        Some((*handle, level))
                    }
                    _ => None,
                })
                .collect()
        };
        let now = Instant::now();
        for (handle, level) in targets {
            // usage_at reads POST-release; +1 counts this permit so the sample
            // carries the in-flight level at completion (Netflix semantics).
            let in_flight = self.state.usage_at(scope, limit_key, level) + 1;
            let controller = self
                .adaptive
                .get_mut(&handle)
                .expect("target selected from map");
            let outcome = controller.on_sample(hold, in_flight, now);
            if outcome == UpdateOutcome::Increase {
                // Post-release usage is `in_flight - 1` (this permit is gone).
                let headroom = controller
                    .current_limit()
                    .get()
                    .saturating_sub(in_flight - 1);
                if headroom > 0 {
                    self.state
                        .pop_waiters(scope, limit_key, level, headroom as usize, &mut woken);
                }
            }
        }
        woken
    }

    /// Adds a vqueue to the waiter list at the specified trie node.
    pub(super) fn add_to_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        blocked_level: Level,
    ) {
        self.state
            .add_to_waiters(handle, scope, limit_key, blocked_level);
    }

    /// Removes a vqueue from the waiter list using caller-provided routing info.
    pub(super) fn remove_from_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        blocked_level: Level,
    ) {
        self.state
            .remove_from_waiters(handle, scope, limit_key, blocked_level);
    }

    /// Applies a batch of rule updates (add, remove, or update) in order and returns
    /// the union of vqueues that need re-evaluation.
    ///
    /// This uses soft enforcement: existing permits that exceed new lower limits are not revoked;
    /// the system simply stops issuing new permits until usage drains below the new limit.
    pub(super) fn apply_rule_updates(
        &mut self,
        updates: impl IntoIterator<Item = RuleUpdate>,
    ) -> Vec<VQueueHandle> {
        // allocate a fairly large sized vec to avoid frequent re-allocations when adding the woken
        // vqueues
        let mut woken = Vec::with_capacity(1024);
        for update in updates {
            // Apply the mutation to the rules store, capturing the pattern for the
            // subsequent waiter-drain pass.
            let pattern = match update {
                RuleUpdate::Upsert { pattern, limit } => {
                    let wants_adaptive =
                        limit.concurrency.is_none() && limit.adaptive_concurrency.is_some();
                    let adaptive_cfg = limit.adaptive_concurrency.clone();
                    let handle = self.rules.upsert_rule(pattern.clone(), limit);
                    if wants_adaptive {
                        let cfg = adaptive_cfg.expect("checked above");
                        // Identical re-upserts (every helm redeploy re-runs the
                        // registration job) must NOT reset the learned state.
                        let unchanged = self
                            .adaptive
                            .get(&handle)
                            .is_some_and(|c| c.config_matches(&cfg));
                        if !unchanged {
                            self.adaptive.insert(
                                handle,
                                Gradient2Controller::from_config(
                                    &cfg,
                                    pattern.to_string(),
                                    self.partition_label.clone(),
                                    Instant::now(),
                                ),
                            );
                        }
                    } else if let Some(c) = self.adaptive.remove(&handle) {
                        c.zero_gauges();
                    }
                    pattern
                }
                RuleUpdate::Remove { pattern } => {
                    if let Some((handle, _)) = self.rules.remove_rule(&pattern)
                        && let Some(c) = self.adaptive.remove(&handle)
                    {
                        c.zero_gauges();
                    }
                    pattern
                }
            };

            // Walk the trie guided by the pattern and drain affected waiter lists
            self.state.drain_affected_waiters(&pattern, &mut woken);
        }
        woken
    }

    /// Resolves a rule handle to its pattern. Returns `None` if the handle is stale
    /// (rule was removed since the handle was captured).
    #[allow(dead_code)]
    pub fn resolve_rule(&self, handle: RuleHandle) -> Option<&RulePattern<ReString>> {
        self.rules.get_pattern(handle)
    }

    /// Walks the entire counter trie (scope → L1 → L2) and emits one
    /// [`UserLimitCounterEntry`] per non-empty node, tagging each with the
    /// rule currently governing it (if any) and its waiter depth.
    ///
    /// The emitted rows are what the `sys_user_limits` DataFusion table
    /// surfaces; callers stamp the `partition_key` from the owning partition
    /// before handing the rows back.
    pub fn scan_counters(&self, partition_key: PartitionKey) -> Vec<UserLimitCounterEntry> {
        let mut out = Vec::new();
        for (scope, scope_node) in &self.state.scopes {
            let scope_name = scope.as_str().to_owned();

            // Scope-level row
            let scope_limits = self.rules.lookup(scope.as_str(), &LimitKey::None);
            let (scope_limit, scope_rule) =
                limit_and_pattern(scope_limits.limit_at(Level::Scope), &self.rules);
            out.push(UserLimitCounterEntry {
                partition_key,
                scope: scope_name.clone(),
                l1: None,
                l2: None,
                level: Level::Scope,
                usage: scope_node.value.concurrency,
                concurrency_limit: scope_limit,
                rule_pattern: scope_rule,
                num_waiters: scope_node.waiters.len() as u64,
            });

            for (l1_key, l1_node) in &scope_node.l1 {
                let l1_limit_key = LimitKey::L1(l1_key.clone());
                let l1_limits = self.rules.lookup(scope.as_str(), &l1_limit_key);
                let (l1_limit, l1_rule) =
                    limit_and_pattern(l1_limits.limit_at(Level::Level1), &self.rules);
                out.push(UserLimitCounterEntry {
                    partition_key,
                    scope: scope_name.clone(),
                    l1: Some(l1_key.as_str().to_owned()),
                    l2: None,
                    level: Level::Level1,
                    usage: l1_node.value.concurrency,
                    concurrency_limit: l1_limit,
                    rule_pattern: l1_rule,
                    num_waiters: l1_node.waiters.len() as u64,
                });

                for (l2_key, l2_leaf) in &l1_node.l2 {
                    let l2_limit_key = LimitKey::L2(l1_key.clone(), l2_key.clone());
                    let l2_limits = self.rules.lookup(scope.as_str(), &l2_limit_key);
                    let (l2_limit, l2_rule) =
                        limit_and_pattern(l2_limits.limit_at(Level::Level2), &self.rules);
                    out.push(UserLimitCounterEntry {
                        partition_key,
                        scope: scope_name.clone(),
                        l1: Some(l1_key.as_str().to_owned()),
                        l2: Some(l2_key.as_str().to_owned()),
                        level: Level::Level2,
                        usage: l2_leaf.value.concurrency,
                        concurrency_limit: l2_limit,
                        rule_pattern: l2_rule,
                        num_waiters: l2_leaf.waiters.len() as u64,
                    });
                }
            }
        }
        out
    }
}

/// Resolve a `Limit<&UserLimits>` into its concrete (limit_value, pattern_display) pair.
fn limit_and_pattern(
    limit: &Limit<&UserLimits>,
    rules: &Rules<ReString, UserLimits>,
) -> (Option<u32>, Option<String>) {
    match limit {
        Limit::Undefined => (None, None),
        Limit::Defined(handle, user_limits) => {
            let value = user_limits.concurrency.map(NonZeroU32::get);
            let pattern = rules.get_pattern(*handle).map(ToString::to_string);
            (value, pattern)
        }
    }
}

#[derive(Debug, Default)]
struct State {
    scopes: HashMap<Scope, ScopeNode>,
}

/// Scope-level trie node with usage counter, waiter list, and L1 children.
#[derive(Debug, Default)]
struct ScopeNode {
    value: Usage,
    waiters: VecDeque<VQueueHandle>,
    l1: HashMap<RestrictedValue<ReString>, L1Node>,
}

/// L1-level trie node with usage counter, waiter list, and L2 children.
#[derive(Debug, Default)]
struct L1Node {
    value: Usage,
    waiters: VecDeque<VQueueHandle>,
    l2: HashMap<RestrictedValue<ReString>, L2Leaf>,
}

impl ScopeNode {
    /// Returns true if this node has no usage, no waiters, and no children.
    fn is_unused(&self) -> bool {
        self.value.is_zero() && self.waiters.is_empty() && self.l1.is_empty()
    }
}

impl L1Node {
    /// Returns true if this node has no usage, no waiters, and no children.
    fn is_unused(&self) -> bool {
        self.value.is_zero() && self.waiters.is_empty() && self.l2.is_empty()
    }
}

/// L2-level trie leaf with usage counter and waiter list.
#[derive(Debug, Default)]
struct L2Leaf {
    value: Usage,
    waiters: VecDeque<VQueueHandle>,
}

impl L2Leaf {
    fn is_unused(&self) -> bool {
        self.value.is_zero() && self.waiters.is_empty()
    }
}

impl State {
    fn increment_all(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        limit_kind: LimitKind,
    ) {
        let scope_node = self.scopes.entry_ref(scope).or_default();
        scope_node.value.increment(limit_kind);
        match limit_key {
            LimitKey::None => {}
            LimitKey::L1(l1) => {
                scope_node
                    .l1
                    .entry_ref(l1)
                    .or_default()
                    .value
                    .increment(limit_kind);
            }
            LimitKey::L2(l1, l2) => {
                let l1_node = scope_node.l1.entry_ref(l1).or_default();
                l1_node.value.increment(limit_kind);
                l1_node
                    .l2
                    .entry_ref(l2)
                    .or_default()
                    .value
                    .increment(limit_kind);
            }
        }
    }

    /// Decrements counters along the path and pops at most 1 waiter per affected level.
    ///
    /// Wakes deepest level first so that vqueues blocked on the most restrictive (narrowest)
    /// counter get priority — they are most likely to succeed since the release directly
    /// addresses their constraint.
    ///
    /// After waking, prunes empty trie nodes bottom-up to avoid unbounded growth.
    fn decrement_and_wake(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        limit_kind: LimitKind,
    ) -> ArrayVec<VQueueHandle, { Level::COUNT }> {
        let mut woken = ArrayVec::new();

        let Some(scope_node) = self.scopes.get_mut(scope) else {
            return woken;
        };

        // Decrement all affected counters top-down (natural traversal order)
        scope_node.value.decrement(limit_kind);
        match limit_key {
            LimitKey::None => {}
            LimitKey::L1(l1) => {
                if let Some(l1_node) = scope_node.l1.get_mut(l1) {
                    l1_node.value.decrement(limit_kind);
                }
            }
            LimitKey::L2(l1, l2) => {
                if let Some(l1_node) = scope_node.l1.get_mut(l1) {
                    l1_node.value.decrement(limit_kind);
                    if let Some(l2_leaf) = l1_node.l2.get_mut(l2) {
                        l2_leaf.value.decrement(limit_kind);
                    } else {
                        debug_assert!(
                            false,
                            "missing Level2 node for released key: scope='{}', l1='{}', l2='{}'",
                            scope, l1, l2,
                        );
                    }
                } else {
                    debug_assert!(
                        false,
                        "missing Level1 node for released key: scope='{}', l1='{}'",
                        scope, l1,
                    );
                }
            }
        }

        // Wake deepest level first — vqueues blocked on narrower counters have
        // more restrictive limits and are most likely to benefit from this release.
        match limit_key {
            LimitKey::None => {}
            LimitKey::L2(l1, l2) => {
                if let Some(l1_node) = scope_node.l1.get_mut(l1) {
                    if let Some(l2_leaf) = l1_node.l2.get_mut(l2)
                        && let Some(h) = l2_leaf.waiters.pop_front()
                    {
                        woken.push(h);
                    }
                    if let Some(h) = l1_node.waiters.pop_front() {
                        woken.push(h);
                    }
                }
            }
            LimitKey::L1(l1) => {
                if let Some(l1_node) = scope_node.l1.get_mut(l1)
                    && let Some(h) = l1_node.waiters.pop_front()
                {
                    woken.push(h);
                }
            }
        }
        if let Some(h) = scope_node.waiters.pop_front() {
            woken.push(h);
        }

        // Prune empty nodes bottom-up
        match limit_key {
            LimitKey::None => {}
            LimitKey::L1(l1) => {
                if scope_node.l1.get(l1).is_some_and(|n| n.is_unused()) {
                    scope_node.l1.remove(l1);
                }
            }
            LimitKey::L2(l1, l2) => {
                if let Some(l1_node) = scope_node.l1.get_mut(l1)
                    && l1_node.l2.get(l2).is_some_and(|n| n.is_unused())
                {
                    l1_node.l2.remove(l2);
                }
                if scope_node.l1.get(l1).is_some_and(|n| n.is_unused()) {
                    scope_node.l1.remove(l1);
                }
            }
        }
        if scope_node.is_unused() {
            self.scopes.remove(scope);
        }

        woken
    }

    /// Current concurrency usage at the given level along the
    /// (scope, limit_key) chain. 0 when the node does not exist.
    fn usage_at(&self, scope: &Scope, limit_key: &LimitKey<ReString>, level: Level) -> u32 {
        let Some(scope_node) = self.scopes.get(scope) else {
            return 0;
        };
        match level {
            Level::Scope => scope_node.value.concurrency,
            Level::Level1 => match limit_key {
                LimitKey::L1(l1) | LimitKey::L2(l1, _) => {
                    scope_node.l1.get(l1).map_or(0, |n| n.value.concurrency)
                }
                LimitKey::None => 0,
            },
            Level::Level2 => match limit_key {
                LimitKey::L2(l1, l2) => scope_node
                    .l1
                    .get(l1)
                    .and_then(|n| n.l2.get(l2))
                    .map_or(0, |n| n.value.concurrency),
                _ => 0,
            },
        }
    }

    /// Pops up to `k` waiters from the node at the given level of the
    /// (scope, limit_key) chain (targeted wake-K on adaptive limit
    /// increases — deliberately NOT a full drain).
    fn pop_waiters(
        &mut self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        level: Level,
        k: usize,
        out: &mut Vec<VQueueHandle>,
    ) {
        let Some(scope_node) = self.scopes.get_mut(scope) else {
            return;
        };
        let waiters = match level {
            Level::Scope => Some(&mut scope_node.waiters),
            Level::Level1 => match limit_key {
                LimitKey::L1(l1) | LimitKey::L2(l1, _) => {
                    scope_node.l1.get_mut(l1).map(|n| &mut n.waiters)
                }
                LimitKey::None => None,
            },
            Level::Level2 => match limit_key {
                LimitKey::L2(l1, l2) => scope_node
                    .l1
                    .get_mut(l1)
                    .and_then(|n| n.l2.get_mut(l2))
                    .map(|n| &mut n.waiters),
                _ => None,
            },
        };
        if let Some(waiters) = waiters {
            for _ in 0..k {
                match waiters.pop_front() {
                    Some(h) => out.push(h),
                    None => break,
                }
            }
        }
    }

    fn add_to_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        blocked_level: Level,
    ) {
        let scope_node = self.scopes.entry_ref(scope).or_default();
        match blocked_level {
            Level::Scope => {
                scope_node.waiters.push_back(handle);
            }
            Level::Level1 => {
                let l1_key = limit_key
                    .level1()
                    .expect("L1 key required for Level1 block");
                scope_node
                    .l1
                    .entry_ref(l1_key)
                    .or_default()
                    .waiters
                    .push_back(handle);
            }
            Level::Level2 => {
                let l1_key = limit_key
                    .level1()
                    .expect("L1 key required for Level2 block");
                let l2_key = limit_key
                    .level2()
                    .expect("L2 key required for Level2 block");
                scope_node
                    .l1
                    .entry_ref(l1_key)
                    .or_default()
                    .l2
                    .entry_ref(l2_key)
                    .or_default()
                    .waiters
                    .push_back(handle);
            }
        }
    }

    fn remove_from_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        blocked_level: Level,
    ) {
        let Some(scope_node) = self.scopes.get_mut(scope) else {
            return;
        };
        match blocked_level {
            Level::Scope => {
                scope_node.waiters.retain(|h| *h != handle);
            }
            Level::Level1 => {
                if let Some(l1_key) = limit_key.level1()
                    && let Some(l1_node) = scope_node.l1.get_mut(l1_key)
                {
                    l1_node.waiters.retain(|h| *h != handle);
                }
            }
            Level::Level2 => {
                if let Some(l1_key) = limit_key.level1()
                    && let Some(l1_node) = scope_node.l1.get_mut(l1_key)
                    && let Some(l2_key) = limit_key.level2()
                    && let Some(l2_leaf) = l1_node.l2.get_mut(l2_key)
                {
                    l2_leaf.waiters.retain(|h| *h != handle);
                }
            }
        }
    }

    /// Walks the trie guided by a rule pattern and drains waiter lists from all matching
    /// nodes at the pattern's target level.
    ///
    /// Uses `Pattern::Exact` for O(1) lookups and `Pattern::Wildcard` for iterating all
    /// entries at that level. Only drains waiters at the rule's own level, not descendants.
    fn drain_affected_waiters(
        &mut self,
        pattern: &RulePattern<ReString>,
        woken: &mut Vec<VQueueHandle>,
    ) {
        match pattern {
            RulePattern::Scope(scope_pat) => {
                for_each_matching_scope(&mut self.scopes, scope_pat, |scope_node| {
                    woken.extend(scope_node.waiters.drain(..));
                });
            }
            RulePattern::L1 {
                scope: scope_pat,
                l1: l1_pat,
            } => {
                for_each_matching_scope(&mut self.scopes, scope_pat, |scope_node| {
                    for_each_matching(&mut scope_node.l1, l1_pat, |l1_node| {
                        woken.extend(l1_node.waiters.drain(..));
                    });
                });
            }
            RulePattern::L2 {
                scope: scope_pat,
                l1: l1_pat,
                l2: l2_pat,
            } => {
                for_each_matching_scope(&mut self.scopes, scope_pat, |scope_node| {
                    for_each_matching(&mut scope_node.l1, l1_pat, |l1_node| {
                        for_each_matching(&mut l1_node.l2, l2_pat, |l2_leaf| {
                            woken.extend(l2_leaf.waiters.drain(..));
                        });
                    });
                });
            }
        }
    }

    fn check_capacity(
        &self,
        scope: &Scope,
        limit_key: &LimitKey<ReString>,
        limit_kind: LimitKind,
        limits: &StructuredLimits<&UserLimits>,
    ) -> CapacityResult {
        match limit_key {
            LimitKey::None => self.check_scope(limit_kind, scope, limits),
            LimitKey::L1(l1) => self.check_l1(limit_kind, scope, l1, limits),
            LimitKey::L2(l1, l2) => self.check_l1_l2(limit_kind, scope, l1, l2, limits),
        }
    }

    fn check_scope(
        &self,
        limit_kind: LimitKind,
        scope: &Scope,
        limits: &StructuredLimits<&UserLimits>,
    ) -> CapacityResult {
        let current = self.scopes.get(scope).map(|n| &n.value);
        let mut levels = ArrayVec::new();
        levels.push(make_level_status(
            Level::Scope,
            current,
            limits.limit_at(Level::Scope),
            limit_kind,
        ));
        CapacityResult { levels }
    }

    fn check_l1(
        &self,
        limit_kind: LimitKind,
        scope: &Scope,
        l1: &RestrictedValue<ReString>,
        limits: &StructuredLimits<&UserLimits>,
    ) -> CapacityResult {
        let (scope_current, l1_current) = match self.scopes.get(scope) {
            Some(sn) => (Some(&sn.value), sn.l1.get(l1).map(|n| &n.value)),
            None => (None, None),
        };

        let mut levels = ArrayVec::new();
        levels.push(make_level_status(
            Level::Scope,
            scope_current,
            limits.limit_at(Level::Scope),
            limit_kind,
        ));
        levels.push(make_level_status(
            Level::Level1,
            l1_current,
            limits.limit_at(Level::Level1),
            limit_kind,
        ));
        CapacityResult { levels }
    }

    fn check_l1_l2(
        &self,
        limit_kind: LimitKind,
        scope: &Scope,
        l1: &RestrictedValue<ReString>,
        l2: &RestrictedValue<ReString>,
        limits: &StructuredLimits<&UserLimits>,
    ) -> CapacityResult {
        let (scope_current, l1_current, l2_current) = match self.scopes.get(scope) {
            Some(sn) => {
                let l1n = sn.l1.get(l1);
                (
                    Some(&sn.value),
                    l1n.map(|n| &n.value),
                    l1n.and_then(|n| n.l2.get(l2)).map(|leaf| &leaf.value),
                )
            }
            None => (None, None, None),
        };

        let mut levels = ArrayVec::new();
        levels.push(make_level_status(
            Level::Scope,
            scope_current,
            limits.limit_at(Level::Scope),
            limit_kind,
        ));
        levels.push(make_level_status(
            Level::Level1,
            l1_current,
            limits.limit_at(Level::Level1),
            limit_kind,
        ));
        levels.push(make_level_status(
            Level::Level2,
            l2_current,
            limits.limit_at(Level::Level2),
            limit_kind,
        ));
        CapacityResult { levels }
    }
}

/// Applies `f` to each scope node matching the scope pattern.
fn for_each_matching_scope(
    scopes: &mut HashMap<Scope, ScopeNode>,
    pat: &Pattern<ReString>,
    mut f: impl FnMut(&mut ScopeNode),
) {
    match pat {
        Pattern::Exact(val) => {
            if let Some(node) = scopes.get_mut(val.as_str()) {
                f(node);
            }
        }
        Pattern::Wildcard => {
            for node in scopes.values_mut() {
                f(node);
            }
        }
    }
}

/// Applies `f` to each entry in a `HashMap<RestrictedValue<ReString>, V>` matching the pattern.
fn for_each_matching<V>(
    map: &mut HashMap<RestrictedValue<ReString>, V>,
    pat: &Pattern<ReString>,
    mut f: impl FnMut(&mut V),
) {
    match pat {
        Pattern::Exact(val) => {
            if let Some(node) = map.get_mut(val) {
                f(node);
            }
        }
        Pattern::Wildcard => {
            for node in map.values_mut() {
                f(node);
            }
        }
    }
}

/// Result of a capacity check across all applicable hierarchy levels.
#[derive(Clone, Debug)]
pub struct CapacityResult {
    /// Status for each applicable level (stack-allocated, up to 3).
    levels: ArrayVec<LevelStatus, { Level::COUNT }>,
}

#[allow(dead_code)]
impl CapacityResult {
    /// Returns `true` if all levels have capacity available.
    pub fn has_capacity(&self) -> bool {
        self.levels.iter().all(|l| l.has_capacity())
    }

    /// Returns the narrowest (deepest) blocked level and its rule handle.
    /// Returns `None` if all levels have capacity.
    pub fn narrowest_blocked(&self) -> Option<(Level, Option<RuleHandle>)> {
        self.levels
            .iter()
            .rev()
            .find(|l| !l.has_capacity())
            .map(|l| (l.level, l.rule_handle))
    }

    /// Iterates over per-level status entries.
    pub fn iter(&self) -> impl Iterator<Item = &LevelStatus> {
        self.levels.iter()
    }
}

impl CapacityResult {
    /// Creates a display wrapper that resolves rule handles to their patterns
    /// via the provided [`UserLimiter`].
    pub fn display<'a>(&'a self, limiter: &'a UserLimiter) -> CapacityResultDisplay<'a> {
        CapacityResultDisplay {
            result: self,
            limiter,
        }
    }
}

/// Display wrapper for [`CapacityResult`] that resolves rule handles to patterns.
///
/// Created via [`CapacityResult::display`].
pub struct CapacityResultDisplay<'a> {
    result: &'a CapacityResult,
    limiter: &'a UserLimiter,
}

impl fmt::Display for CapacityResultDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for status in &self.result.levels {
            if !first {
                write!(f, ", ")?;
            }
            first = false;

            write!(f, "{}={}/", status.level, status.usage)?;
            match status.limit_value {
                None => write!(f, "unlimited")?,
                Some(limit) => {
                    write!(f, "{}", limit)?;
                    // Resolve the rule pattern for context
                    if let Some(handle) = status.rule_handle {
                        match self.limiter.resolve_rule(handle) {
                            Some(pattern) => write!(f, " [{}]", pattern)?,
                            None => write!(f, " [rule removed]")?,
                        }
                    }
                    let tag = if status.has_capacity() {
                        "ok"
                    } else {
                        "BLOCKED"
                    };
                    write!(f, " ({})", tag)?;
                }
            }
        }
        Ok(())
    }
}

/// Status of a single level's capacity.
#[derive(Clone, Debug)]
pub struct LevelStatus {
    /// The hierarchy level (Scope, Level1, or Level2).
    pub level: Level,
    /// Current usage at this level.
    pub usage: u32,
    /// The configured limit, or `None` if unlimited.
    pub limit_value: Option<NonZeroU32>,
    /// Handle to the rule defining the limit. `None` if unlimited.
    /// Resolve via [`UserLimiter::resolve_rule`] for display.
    pub rule_handle: Option<RuleHandle>,
}

#[allow(dead_code)]
impl LevelStatus {
    /// Returns `true` if this level has capacity available.
    pub fn has_capacity(&self) -> bool {
        self.limit_value
            .map(|limit| self.usage < limit.get())
            .unwrap_or(true)
    }

    /// Returns the available capacity, or `None` if unlimited.
    pub fn available(&self) -> Option<u32> {
        self.limit_value
            .map(|limit| limit.get().saturating_sub(self.usage))
    }
}

/// Constructs a [`LevelStatus`] from the trie usage and the resolved limit.
fn make_level_status(
    level: Level,
    current: Option<&Usage>,
    limit: &Limit<&UserLimits>,
    limit_kind: LimitKind,
) -> LevelStatus {
    let usage = current.map_or(0, |u| match limit_kind {
        LimitKind::Concurrency => u.concurrency,
    });
    let (limit_value, rule_handle) = match limit {
        Limit::Undefined => (None, None),
        Limit::Defined(handle, user_limits) => {
            let value = match limit_kind {
                LimitKind::Concurrency => user_limits.concurrency,
            };
            (value, Some(*handle))
        }
    };
    LevelStatus {
        level,
        usage,
        limit_value,
        rule_handle,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use restate_limiter::{LimitKey, RulePattern};
    use restate_util_string::{ReString, RestateString};

    use super::*;

    fn scope(s: &str) -> Scope {
        Scope::try_new(s).unwrap()
    }

    fn limit_key(s: &str) -> LimitKey<ReString> {
        s.parse().unwrap()
    }

    fn rule(s: &str) -> RulePattern<ReString> {
        s.parse().unwrap()
    }

    fn limits(concurrency: u32) -> UserLimits {
        UserLimits::new(NonZeroU32::new(concurrency))
    }

    /// Creates a UserLimiter with the given rules.
    fn limiter_with_rules(specs: &[(&str, u32)]) -> UserLimiter {
        let rules = Rules::from_rules(specs.iter().map(|(pat, limit)| {
            (
                pat.parse::<RulePattern<ReString>>().unwrap(),
                limits(*limit),
            )
        }));
        UserLimiter {
            rules,
            state: Default::default(),
            adaptive: HashMap::new(),
            partition_label: "test".to_string(),
        }
    }

    // Fake VQueueHandle for testing. The slotmap key needs to be created via a SlotMap.
    fn make_handles(n: usize) -> (slotmap::SlotMap<VQueueHandle, ()>, Vec<VQueueHandle>) {
        let mut sm = slotmap::SlotMap::with_key();
        let handles: Vec<_> = (0..n).map(|_| sm.insert(())).collect();
        (sm, handles)
    }

    #[test]
    fn check_capacity_unlimited() {
        let limiter = limiter_with_rules(&[]);
        let result = limiter.check_concurrency_capacity(&scope("s1"), &LimitKey::None);
        assert!(result.has_capacity());
    }

    #[test]
    fn check_capacity_with_headroom() {
        let mut limiter = limiter_with_rules(&[("*", 10)]);
        for _ in 0..5 {
            limiter.increment_all(&scope("s1"), &LimitKey::None, LimitKind::Concurrency);
        }
        let result = limiter.check_concurrency_capacity(&scope("s1"), &LimitKey::None);
        assert!(result.has_capacity());
    }

    #[test]
    fn check_capacity_blocked_at_scope() {
        let mut limiter = limiter_with_rules(&[("*", 2)]);
        let s = scope("s1");
        limiter.increment_all(&s, &LimitKey::None, LimitKind::Concurrency);
        limiter.increment_all(&s, &LimitKey::None, LimitKind::Concurrency);

        let result = limiter.check_concurrency_capacity(&s, &LimitKey::None);
        let (level, rule_handle) = result.narrowest_blocked().unwrap();
        assert_eq!(level, Level::Scope);
        let rule = limiter.resolve_rule(rule_handle.unwrap()).unwrap();
        assert_eq!(rule.to_string(), "*");
    }

    #[test]
    fn check_capacity_blocked_at_l2_narrowest() {
        // scope limit = 100, L2 limit = 2
        let mut limiter = limiter_with_rules(&[("*", 100), ("s1/*/t1", 2)]);
        let s = scope("s1");
        let lk = limit_key("foo/t1");

        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);

        // s1=2 (under 100), s1.foo=2 (unlimited), s1.foo.t1=2 (at limit)
        let result = limiter.check_concurrency_capacity(&s, &lk);
        let (level, rule_handle) = result.narrowest_blocked().unwrap();
        assert_eq!(level, Level::Level2);
        let rule = limiter.resolve_rule(rule_handle.unwrap()).unwrap();
        assert_eq!(rule.to_string(), "s1/*/t1");
    }

    #[test]
    fn check_capacity_blocked_on_both_scope_and_l2_returns_narrowest() {
        let mut limiter = limiter_with_rules(&[("*", 2), ("s1/*/t1", 2)]);
        let s = scope("s1");
        let lk = limit_key("foo/t1");

        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);

        // Both scope and L2 are at limit; narrowest is L2
        let result = limiter.check_concurrency_capacity(&s, &lk);
        let (level, _) = result.narrowest_blocked().unwrap();
        assert_eq!(level, Level::Level2);
    }

    #[test]
    fn increment_and_decrement_all_levels() {
        let mut limiter = limiter_with_rules(&[]);
        let s = scope("s1");
        let lk = limit_key("foo/bar");

        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);

        assert_eq!(limiter.state.scopes[&s].value.concurrency, 2);
        let l1 = lk.level1().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1].value.concurrency, 2);
        let l2 = lk.level2().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1].l2[l2].value.concurrency, 2);

        // Decrement via release (also pops waiters, but there are none)
        let woken = limiter.release_concurrency(&s, &lk);
        assert!(woken.is_empty());
        assert_eq!(limiter.state.scopes[&s].value.concurrency, 1);
        assert_eq!(limiter.state.scopes[&s].l1[l1].value.concurrency, 1);
        assert_eq!(limiter.state.scopes[&s].l1[l1].l2[l2].value.concurrency, 1);
    }

    #[test]
    fn release_prunes_empty_trie_nodes() {
        let mut limiter = limiter_with_rules(&[]);
        let s = scope("s1");
        let lk = limit_key("foo/bar");

        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        assert!(limiter.state.scopes.contains_key(&s));

        // Release the single permit — all counters go to zero, nodes should be pruned
        limiter.release_concurrency(&s, &lk);
        assert!(
            !limiter.state.scopes.contains_key(&s),
            "scope node should be pruned when fully empty"
        );

        // Partial pruning: scope still has usage from a different path
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        limiter.increment_all(&s, &LimitKey::None, LimitKind::Concurrency);

        // Release the L2 path — L2 and L1 should be pruned, but scope stays (has usage=1)
        limiter.release_concurrency(&s, &lk);
        assert!(limiter.state.scopes.contains_key(&s));
        let l1 = lk.level1().unwrap();
        assert!(
            !limiter.state.scopes[&s].l1.contains_key(l1),
            "l1 node should be pruned when its subtree is empty"
        );
    }

    #[test]
    fn add_and_remove_waiters() {
        let mut limiter = limiter_with_rules(&[]);
        let (_sm, handles) = make_handles(3);
        let s = scope("s1");
        let lk = limit_key("foo/bar");

        // Add waiters at different levels
        limiter.add_to_waiters(handles[0], &s, &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[1], &s, &lk, Level::Level1);
        limiter.add_to_waiters(handles[2], &s, &lk, Level::Level2);

        // Verify they're there
        assert_eq!(limiter.state.scopes[&s].waiters.len(), 1);
        let l1 = lk.level1().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1].waiters.len(), 1);
        let l2 = lk.level2().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1].l2[l2].waiters.len(), 1);

        // Remove the L1 waiter
        limiter.remove_from_waiters(handles[1], &s, &lk, Level::Level1);
        assert_eq!(limiter.state.scopes[&s].l1[l1].waiters.len(), 0);
        // Others untouched
        assert_eq!(limiter.state.scopes[&s].waiters.len(), 1);
        assert_eq!(limiter.state.scopes[&s].l1[l1].l2[l2].waiters.len(), 1);
    }

    #[test]
    fn release_wakes_one_per_level() {
        let mut limiter = limiter_with_rules(&[("*", 2)]);
        let (_sm, handles) = make_handles(6);
        let s = scope("s1");
        let lk = limit_key("foo/bar");

        // Fill up counters
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);
        limiter.increment_all(&s, &lk, LimitKind::Concurrency);

        // Park 2 waiters at each level
        limiter.add_to_waiters(handles[0], &s, &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[1], &s, &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[2], &s, &lk, Level::Level1);
        limiter.add_to_waiters(handles[3], &s, &lk, Level::Level1);
        limiter.add_to_waiters(handles[4], &s, &lk, Level::Level2);
        limiter.add_to_waiters(handles[5], &s, &lk, Level::Level2);

        // Release 1 permit for the full L2 path
        let woken = limiter.release_concurrency(&s, &lk);
        // Should wake exactly 1 from each level, deepest first
        assert_eq!(woken.len(), 3);
        assert_eq!(woken[0], handles[4]); // l2 head (deepest, highest priority)
        assert_eq!(woken[1], handles[2]); // l1 head
        assert_eq!(woken[2], handles[0]); // scope head (shallowest, lowest priority)

        // Each level still has 1 remaining waiter
        assert_eq!(limiter.state.scopes[&s].waiters.len(), 1);
        let l1k = lk.level1().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1k].waiters.len(), 1);
        let l2k = lk.level2().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1k].l2[l2k].waiters.len(), 1);
    }

    #[test]
    fn rule_update_upsert_drains_affected_waiters() {
        let mut limiter = limiter_with_rules(&[("*", 100)]);
        let (_sm, handles) = make_handles(3);
        let s = scope("s1");

        // Park waiters at scope level
        limiter.add_to_waiters(handles[0], &s, &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[1], &s, &LimitKey::None, Level::Scope);

        // Also park one at L1 level under a different counter
        let lk = limit_key("foo");
        limiter.add_to_waiters(handles[2], &s, &lk, Level::Level1);

        // Upsert a scope-level rule: should drain scope waiters only
        let woken = limiter.apply_rule_updates(vec![RuleUpdate::Upsert {
            pattern: rule("s1"),
            limit: limits(200),
        }]);
        assert_eq!(woken.len(), 2);
        assert!(woken.contains(&handles[0]));
        assert!(woken.contains(&handles[1]));

        // L1 waiter was NOT drained (scope-level rule doesn't affect L1 waiters)
        let l1k = lk.level1().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1k].waiters.len(), 1);
    }

    #[test]
    fn rule_update_remove_drains_affected_waiters() {
        let mut limiter = limiter_with_rules(&[("*", 100), ("s1/*/t1", 10)]);
        let (_sm, handles) = make_handles(4);
        let s = scope("s1");

        // Park waiters at L2 nodes matching the pattern s1/*/t1
        let lk1 = limit_key("foo/t1");
        let lk2 = limit_key("bar/t1");
        limiter.add_to_waiters(handles[0], &s, &lk1, Level::Level2);
        limiter.add_to_waiters(handles[1], &s, &lk2, Level::Level2);

        // Park a waiter at L2 for a different l2 key (should NOT be drained)
        let lk3 = limit_key("foo/t2");
        limiter.add_to_waiters(handles[2], &s, &lk3, Level::Level2);

        // Park a waiter at scope (should NOT be drained)
        limiter.add_to_waiters(handles[3], &s, &LimitKey::None, Level::Scope);

        // Remove the L2 rule: should drain L2 waiters matching s1/*/t1
        let woken = limiter.apply_rule_updates(vec![RuleUpdate::Remove {
            pattern: rule("s1/*/t1"),
        }]);
        assert_eq!(woken.len(), 2);
        assert!(woken.contains(&handles[0]));
        assert!(woken.contains(&handles[1]));

        // t2 waiter still there
        let l1k = lk3.level1().unwrap();
        let l2k = lk3.level2().unwrap();
        assert_eq!(limiter.state.scopes[&s].l1[l1k].l2[l2k].waiters.len(), 1);

        // scope waiter still there
        assert_eq!(limiter.state.scopes[&s].waiters.len(), 1);
    }

    #[test]
    fn rule_update_wildcard_scope_drains_all_scopes() {
        let mut limiter = limiter_with_rules(&[("*", 100)]);
        let (_sm, handles) = make_handles(3);

        // Park waiters at different scope nodes
        limiter.add_to_waiters(handles[0], &scope("s1"), &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[1], &scope("s2"), &LimitKey::None, Level::Scope);
        limiter.add_to_waiters(handles[2], &scope("s3"), &LimitKey::None, Level::Scope);

        // Update wildcard scope rule: should drain ALL scope waiters
        let woken = limiter.apply_rule_updates(vec![RuleUpdate::Upsert {
            pattern: rule("*"),
            limit: limits(200),
        }]);
        assert_eq!(woken.len(), 3);
    }

    // ---- adaptive controller integration ----------------------------------

    use restate_limiter::AdaptiveConcurrency;
    use std::time::Duration;

    fn adaptive_cfg(min: u32, max: u32) -> AdaptiveConcurrency {
        AdaptiveConcurrency {
            min: NonZeroU32::new(min),
            max: NonZeroU32::new(max),
            tolerance_permille: None,
            smoothing_permille: None,
        }
    }

    fn upsert(limiter: &mut UserLimiter, pattern: &str, limit: UserLimits) {
        limiter.apply_rule_updates([RuleUpdate::Upsert {
            pattern: pattern.parse().unwrap(),
            limit,
        }]);
    }

    /// Effective scope-level limit as the acquire path sees it (post-overlay).
    fn effective_scope_limit(limiter: &UserLimiter, s: &str) -> Option<u32> {
        limiter
            .check_concurrency_capacity(&scope(s), &LimitKey::None)
            .iter()
            .next()
            .and_then(|l| l.limit_value)
            .map(NonZeroU32::get)
    }

    #[tokio::test(start_paused = true)]
    async fn adaptive_rule_creates_controller_and_overlays_limit() {
        let mut limiter = limiter_with_rules(&[]);
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 300))),
        );
        // cold start: min + (max-min)/4 = 78 replaces the (None) static limit
        assert_eq!(effective_scope_limit(&limiter, "payment"), Some(78));

        // removal drops the controller and the limit becomes unlimited again
        limiter.apply_rule_updates([RuleUpdate::Remove {
            pattern: "payment".parse().unwrap(),
        }]);
        assert!(limiter.adaptive.is_empty());
        assert_eq!(effective_scope_limit(&limiter, "payment"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn static_concurrency_takes_precedence_over_adaptive() {
        let mut limiter = limiter_with_rules(&[]);
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(NonZeroU32::new(50))
                .with_adaptive_concurrency(Some(adaptive_cfg(4, 300))),
        );
        // static set -> no controller, static limit enforced
        assert!(limiter.adaptive.is_empty());
        assert_eq!(effective_scope_limit(&limiter, "payment"), Some(50));
    }

    #[tokio::test(start_paused = true)]
    async fn identical_reupsert_preserves_learned_state() {
        let mut limiter = limiter_with_rules(&[]);
        let rule = UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 300)));
        upsert(&mut limiter, "payment", rule.clone());

        // learn something: healthy fast samples grow the limit. Keep ~60
        // permits in flight so the app-limited guard (in_flight >= limit/2)
        // sees real demand.
        for _ in 0..60 {
            limiter.increment_all(&scope("payment"), &LimitKey::None, LimitKind::Concurrency);
        }
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(1050)).await;
            limiter.increment_all(&scope("payment"), &LimitKey::None, LimitKind::Concurrency);
            limiter.on_permit_released(
                &scope("payment"),
                &LimitKey::None,
                Some(Duration::from_millis(100)),
            );
        }
        let learned = effective_scope_limit(&limiter, "payment").unwrap();
        assert!(learned > 78, "should have grown, got {learned}");

        // identical re-upsert (helm redeploy) must NOT reset to cold start
        upsert(&mut limiter, "payment", rule);
        assert_eq!(effective_scope_limit(&limiter, "payment"), Some(learned));

        // changed config DOES reset
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(8, 300))),
        );
        assert_eq!(
            effective_scope_limit(&limiter, "payment"),
            Some(8 + (300 - 8) / 4)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sample_feeds_all_matching_adaptive_levels() {
        let mut limiter = limiter_with_rules(&[]);
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 300))),
        );
        upsert(
            &mut limiter,
            "payment/sepa",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 100))),
        );
        // release with an L1 key: BOTH controllers must receive the sample
        // (umbrella stays a true aggregate guardrail — no cross-starvation).
        // Keep ~60 in flight so both levels clear their app-limited guards.
        for _ in 0..60 {
            limiter.increment_all(
                &scope("payment"),
                &limit_key("sepa"),
                LimitKind::Concurrency,
            );
        }
        for _ in 0..30 {
            tokio::time::advance(Duration::from_millis(1050)).await;
            limiter.increment_all(
                &scope("payment"),
                &limit_key("sepa"),
                LimitKind::Concurrency,
            );
            limiter.on_permit_released(
                &scope("payment"),
                &limit_key("sepa"),
                Some(Duration::from_millis(100)),
            );
        }
        let umbrella = effective_scope_limit(&limiter, "payment").unwrap();
        let sub = limiter
            .check_concurrency_capacity(&scope("payment"), &limit_key("sepa"))
            .iter()
            .find(|l| l.level == Level::Level1)
            .and_then(|l| l.limit_value)
            .map(NonZeroU32::get)
            .unwrap();
        assert!(
            umbrella > 78,
            "umbrella must learn from child samples: {umbrella}"
        );
        assert!(sub > 4 + (100 - 4) / 4, "sub-bulkhead must learn: {sub}");
    }

    #[tokio::test(start_paused = true)]
    async fn adaptive_limit_blocks_admission_at_learned_value() {
        let mut limiter = limiter_with_rules(&[]);
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 300))),
        );
        let limit = effective_scope_limit(&limiter, "payment").unwrap();
        for _ in 0..limit {
            assert!(
                limiter
                    .check_concurrency_capacity(&scope("payment"), &LimitKey::None)
                    .has_capacity()
            );
            limiter.increment_all(&scope("payment"), &LimitKey::None, LimitKind::Concurrency);
        }
        // at the learned limit: admission must block (overlay drives the decision)
        let result = limiter.check_concurrency_capacity(&scope("payment"), &LimitKey::None);
        assert!(!result.has_capacity());
        assert!(result.narrowest_blocked().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn limit_increase_wakes_up_to_headroom_waiters() {
        let mut limiter = limiter_with_rules(&[]);
        upsert(
            &mut limiter,
            "payment",
            UserLimits::new(None).with_adaptive_concurrency(Some(adaptive_cfg(4, 300))),
        );
        let (_slotmap, handles) = make_handles(20);
        for h in &handles {
            limiter.add_to_waiters(*h, &scope("payment"), &LimitKey::None, Level::Scope);
        }
        // drive healthy samples until an Increase fires; the woken set must be
        // bounded by the headroom (targeted wake-K, not a full drain of 20)
        let mut woken_by_increase = 0usize;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(1050)).await;
            limiter.increment_all(&scope("payment"), &LimitKey::None, LimitKind::Concurrency);
            // keep in_flight high enough to pass the app-limited guard
            for _ in 0..60 {
                limiter.increment_all(&scope("payment"), &LimitKey::None, LimitKind::Concurrency);
            }
            let woken = limiter.on_permit_released(
                &scope("payment"),
                &LimitKey::None,
                Some(Duration::from_millis(100)),
            );
            woken_by_increase = woken.len();
        }
        assert!(
            woken_by_increase < 20,
            "must not drain all waiters: {woken_by_increase}"
        );
    }
}
