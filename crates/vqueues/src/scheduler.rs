// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::pin::Pin;

use std::future::poll_fn;
use std::task::Poll;

use restate_limiter::{Pattern, RulePattern, RuleUpdate};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::scheduler::{RunAction, SchedulerAction, YieldAction};
use restate_storage_api::vqueue_table::{EntryKey, EntryMetadata, ScanVQueueTable, VQueueStore};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;
use restate_worker_api::resources::ReservedResources;
use restate_worker_api::{SchedulingStatus, UserLimitCounterEntry, VQueueSchedulerStatus};

use crate::cache::VQueueHandle;
use crate::metric_definitions::publish_scheduler_decision_metrics;
use crate::{VQueueEvent, VQueuesMeta};
use crate::{VQueuesMetaCache, cache};

use self::drr::DRRScheduler;
use self::resource_manager::PermitBuilder;
use self::vqueue_state::DetailedEligibility;

mod clock;
mod drr;
mod eligible;
mod queue;
mod resource_manager;
mod vqueue_state;

// Re-exports
pub use eligible::{SchedulingGroup, WeightResolver};
pub use resource_manager::ResourceManager;

/// Live map of scope name → scheduling weight, fed from scope-level exact rules
/// (`restate rules set <scope> --weight N`). Shared between the scheduler service
/// (which maintains it from rule updates) and the [`WeightResolver`] closures
/// used by the eligibility ring and the grouped waiter lists.
pub type ScopeWeights = std::sync::Arc<std::sync::RwLock<hashbrown::HashMap<String, NonZeroU32>>>;

/// Builds the weight resolver over a shared scope-weights map: scope groups get
/// their rule weight (default 1); service groups always run at weight 1 —
/// configurable weights are scope-keyed only.
pub fn scope_weight_resolver(weights: ScopeWeights) -> WeightResolver {
    std::sync::Arc::new(move |group: &SchedulingGroup| match group {
        SchedulingGroup::Scope(scope) => weights
            .read()
            .expect("scope weights lock poisoned")
            .get(scope.as_str())
            .copied()
            .unwrap_or(NonZeroU32::MIN),
        SchedulingGroup::Service(_) => NonZeroU32::MIN,
    })
}

/// Folds rule updates into the scope-weights map. Only scope-level EXACT
/// patterns carry scheduling weights (wildcards and L1/L2 limit-key patterns
/// are concurrency-only); an upsert without a weight resets to the default.
pub fn apply_rule_updates_to_scope_weights(weights: &ScopeWeights, updates: &[RuleUpdate]) {
    let mut map = weights.write().expect("scope weights lock poisoned");
    for update in updates {
        match update {
            RuleUpdate::Upsert { pattern, limit } => {
                if let RulePattern::Scope(Pattern::Exact(scope)) = pattern {
                    match limit.scheduling_weight {
                        Some(weight) => {
                            map.insert(scope.as_str().to_owned(), weight);
                        }
                        None => {
                            map.remove(scope.as_str());
                        }
                    }
                }
            }
            RuleUpdate::Remove { pattern } => {
                if let RulePattern::Scope(Pattern::Exact(scope)) = pattern {
                    map.remove(scope.as_str());
                }
            }
        }
    }
}

type UnconfirmedAssignments = hashbrown::HashMap<EntryKey, (PermitBuilder, EntryMetadata)>;

fn status_from_detailed_eligibility(value: DetailedEligibility) -> SchedulingStatus {
    match value {
        DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
            SchedulingStatus::Ready
        }
        DetailedEligibility::Scheduled(ts) => SchedulingStatus::Scheduled { at: ts },
        DetailedEligibility::Empty => SchedulingStatus::Empty,
    }
}

#[derive(Default, Debug)]
pub struct Decisions {
    // Vqueue ids are ordered by partition key, this enables us to group
    // vqueues within the same partition key together when scanning through
    // the scheduler's decisions.
    pub qids: BTreeMap<VQueueId, Vec<SchedulerAction>>,
    /// running items
    pub num_run: u32,
    /// Items in run queue that need to go back to the inbox stage
    pub num_yield: u32,
}

impl Decisions {
    pub fn push(&mut self, qid: &VQueueId, action: impl Into<SchedulerAction>) {
        let action = action.into();
        match action {
            SchedulerAction::Unknown => unreachable!(),
            SchedulerAction::Yield(_) => self.num_yield += 1,
            SchedulerAction::Run(_) => self.num_run += 1,
        }

        if let Some(actions) = self.qids.get_mut(qid) {
            actions.push(action);
        } else {
            self.qids.insert(qid.clone(), vec![action]);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.qids.is_empty()
    }

    /// The number of vqueues in this decision
    pub fn num_queues(&self) -> usize {
        self.qids.len()
    }

    #[cfg(test)]
    pub fn num_run(&self) -> usize {
        self.num_run as usize
    }

    #[cfg(test)]
    pub fn num_yield(&self) -> usize {
        self.num_yield as usize
    }

    /// Total number of items in all queues
    pub fn total_items(&self) -> usize {
        self.num_run as usize + self.num_yield as usize
    }

    pub fn report_metrics(&self) {
        publish_scheduler_decision_metrics(self.num_run, self.num_yield);
    }
}

enum State<S: VQueueStore> {
    Active(Pin<Box<DRRScheduler<S>>>),
    Disabled,
}

pub struct SchedulerService<S: VQueueStore> {
    state: State<S>,
    /// Scope → weight map kept in sync from rule updates; read by the
    /// weight-resolver closures inside the DRR scheduler.
    scope_weights: ScopeWeights,
}

impl<S: VQueueStore> SchedulerService<S> {
    pub fn new_disabled() -> Self
    where
        S: ScanVQueueTable,
    {
        Self {
            state: State::<S>::Disabled,
            scope_weights: ScopeWeights::default(),
        }
    }

    pub async fn create(
        resource_manager: ResourceManager,
        storage: S,
        vqueues_cache: &VQueuesMetaCache,
        weight_resolver: WeightResolver,
        scope_weights: ScopeWeights,
    ) -> Result<Self, StorageError>
    where
        S: ScanVQueueTable,
    {
        // We maintain a clone of the vqueue cache that's snapshotted at the time of creation
        // of the scheduler. The clone is then kept in-sync by applying the events emitted
        // by the partition processor.
        let state = State::Active(Box::pin(DRRScheduler::new(
            // this assumes a worst case of 2 assignment segments per queue.
            // This limits the total number of commands we send via propose_many to bifrost.
            //
            // Note that propose_many will error out if the number of commands is greater than the
            // channel's max capacity.
            NonZeroU32::new(25).unwrap(),
            // currently constant but we can make it configurable if needed
            NonZeroU32::new(1000).unwrap(),
            resource_manager,
            storage,
            vqueues_cache.view(),
            weight_resolver,
        )));
        Ok(Self {
            state,
            scope_weights,
        })
    }

    pub fn on_inbox_event(&mut self, metas: VQueuesMeta<'_>, event: VQueueEvent) {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler.as_mut().on_inbox_event(metas, event);
        }
    }

    /// Forward a batch of rule-book updates to the embedded resource
    /// manager, keeping the scope-weight map in sync first. No-op when the
    /// scheduler is disabled (followers).
    pub fn on_rules_updated(&self, updates: Box<[RuleUpdate]>) {
        apply_rule_updates_to_scope_weights(&self.scope_weights, &updates);
        if let State::Active(ref drr_scheduler) = self.state {
            drr_scheduler.on_rules_updated(updates);
        }
    }

    /// Return reserved resources (concurrency permit + memory lease) for a given
    /// item hash if it was assigned by the scheduler.
    ///
    /// Resources will not be returned if the unconfirmed assignment was rejected or removed.
    pub fn confirm_run_attempt(
        &mut self,
        handle: VQueueHandle,
        slot: &cache::Slot,
        key: &EntryKey,
    ) -> Option<ReservedResources> {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler
                .as_mut()
                .confirm_run_attempt(handle, slot, key)
        } else {
            None
        }
    }

    pub async fn schedule_next(
        &mut self,
        metas: VQueuesMeta<'_>,
    ) -> Result<Decisions, StorageError> {
        poll_fn(|cx| self.poll_schedule_next(metas, cx)).await
    }

    pub fn poll_schedule_next(
        &mut self,
        metas: VQueuesMeta<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Decisions, StorageError>> {
        match self.state {
            // if scheduler is disabled, we always return pending.
            State::Disabled => Poll::Pending,
            State::Active(ref mut drr_scheduler) => {
                drr_scheduler.as_mut().poll_schedule_next(metas, cx)
            }
        }
    }

    /// Returns the scheduling status for a specific vqueue.
    #[cfg(test)]
    pub fn get_status(
        &self,
        metas: VQueuesMeta<'_>,
        handle: VQueueHandle,
    ) -> VQueueSchedulerStatus {
        match self.state {
            State::Disabled => VQueueSchedulerStatus::default(),
            State::Active(ref drr_scheduler) => drr_scheduler.get_status(metas, handle),
        }
    }

    /// Returns an iterator over the scheduling status of all tracked vqueues.
    ///
    /// Returns `None` if the scheduler is disabled.
    pub fn iter_status(
        &self,
        metas: VQueuesMeta<'_>,
    ) -> Option<impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)>> {
        match self.state {
            State::Disabled => None,
            State::Active(ref drr_scheduler) => Some(drr_scheduler.iter_status(metas)),
        }
    }

    /// Returns a snapshot of every user-limit counter currently tracked by this
    /// partition's scheduler, stamped with `partition_key` for DataFusion routing.
    ///
    /// Returns an empty vec when the scheduler is disabled.
    pub fn scan_user_limit_counters(
        &self,
        partition_key: PartitionKey,
    ) -> Vec<UserLimitCounterEntry> {
        match self.state {
            State::Disabled => Vec::new(),
            State::Active(ref drr_scheduler) => {
                drr_scheduler.scan_user_limit_counters(partition_key)
            }
        }
    }
}

#[cfg(test)]
mod scope_weight_tests {
    use std::num::NonZeroU32;

    use restate_limiter::UserLimits;
    use restate_types::Scope;
    use restate_util_string::ReString;

    use super::*;

    fn scope_pattern(name: &'static str) -> RulePattern<ReString> {
        name.parse().expect("valid pattern")
    }

    fn weight(n: u32) -> Option<NonZeroU32> {
        NonZeroU32::new(n)
    }

    /// Scope-level EXACT rules feed the weight map; wildcard and limit-key
    /// (L1/L2) patterns are concurrency-only and must be ignored; an upsert
    /// without a weight resets to the default; removes clear.
    #[test]
    fn rule_updates_feed_scope_weights() {
        let weights = ScopeWeights::default();
        let resolver = scope_weight_resolver(weights.clone());
        let payment = SchedulingGroup::Scope(Scope::try_non_interned("payment").unwrap());
        let other = SchedulingGroup::Scope(Scope::try_non_interned("other").unwrap());

        // default weight before any rule
        assert_eq!(resolver(&payment), NonZeroU32::MIN);

        apply_rule_updates_to_scope_weights(
            &weights,
            &[
                RuleUpdate::Upsert {
                    pattern: scope_pattern("payment"),
                    limit: UserLimits::new(NonZeroU32::new(100)).with_scheduling_weight(weight(10)),
                },
                // wildcard scope: ignored for weights
                RuleUpdate::Upsert {
                    pattern: scope_pattern("*"),
                    limit: UserLimits::new(None).with_scheduling_weight(weight(7)),
                },
                // limit-key level: ignored for weights
                RuleUpdate::Upsert {
                    pattern: scope_pattern("payment/sepa"),
                    limit: UserLimits::new(NonZeroU32::new(50)).with_scheduling_weight(weight(9)),
                },
            ],
        );
        assert_eq!(resolver(&payment), weight(10).unwrap());
        assert_eq!(resolver(&other), NonZeroU32::MIN, "wildcard must not leak");

        // upsert WITHOUT a weight resets to the default
        apply_rule_updates_to_scope_weights(
            &weights,
            &[RuleUpdate::Upsert {
                pattern: scope_pattern("payment"),
                limit: UserLimits::new(NonZeroU32::new(100)),
            }],
        );
        assert_eq!(resolver(&payment), NonZeroU32::MIN);

        // re-set then remove clears
        apply_rule_updates_to_scope_weights(
            &weights,
            &[RuleUpdate::Upsert {
                pattern: scope_pattern("payment"),
                limit: UserLimits::new(None).with_scheduling_weight(weight(3)),
            }],
        );
        assert_eq!(resolver(&payment), weight(3).unwrap());
        apply_rule_updates_to_scope_weights(
            &weights,
            &[RuleUpdate::Remove {
                pattern: scope_pattern("payment"),
            }],
        );
        assert_eq!(resolver(&payment), NonZeroU32::MIN);
    }

    /// Service groups never resolve to configurable weights.
    #[test]
    fn service_groups_are_always_weight_one() {
        let weights = ScopeWeights::default();
        weights
            .write()
            .unwrap()
            .insert("SomeService".to_owned(), weight(10).unwrap());
        let resolver = scope_weight_resolver(weights);
        let group = SchedulingGroup::Service(restate_types::ServiceName::new("SomeService"));
        assert_eq!(resolver(&group), NonZeroU32::MIN);
    }
}

#[cfg(test)]
mod scope_weight_lifecycle_tests {
    use std::num::NonZeroU32;

    use slotmap::SlotMap;

    use restate_limiter::UserLimits;
    use restate_types::Scope;
    use restate_util_string::ReString;

    use super::*;
    use crate::scheduler::resource_manager::test_grouped_waiters;

    /// The WeightResolver docstring claims weights are "consulted whenever a
    /// group is (re-)created, so rule changes take effect as soon as a group
    /// cycles" — pin BOTH halves: an EXISTING group keeps its creation-time
    /// weight after a rule update, and a FRESH group picks the new weight up
    /// immediately.
    #[test]
    fn rule_update_applies_on_group_recreation_only() {
        let weights = ScopeWeights::default();
        let resolver = scope_weight_resolver(weights.clone());
        let mut waiters = test_grouped_waiters(resolver);

        let mut map = SlotMap::<VQueueHandle, ()>::with_key();
        let handles: Vec<_> = (0..6).map(|_| map.insert(())).collect();
        let payment = SchedulingGroup::Scope(Scope::try_non_interned("payment").unwrap());
        let other = SchedulingGroup::Service(restate_types::ServiceName::new("other"));

        // group created at default weight 1
        waiters.push_back(handles[0], &payment);
        waiters.push_back(handles[1], &payment);
        waiters.push_back(handles[2], &other);

        // rule update arrives while the group EXISTS: weight 10
        let pattern: restate_limiter::RulePattern<ReString> = "payment".parse().unwrap();
        apply_rule_updates_to_scope_weights(
            &weights,
            &[RuleUpdate::Upsert {
                pattern,
                limit: UserLimits::new(NonZeroU32::new(100))
                    .with_scheduling_weight(NonZeroU32::new(10)),
            }],
        );

        // existing group still behaves as weight 1: yields after ONE grant
        assert_eq!(waiters.pop_front(), Some(handles[0]));
        assert_eq!(
            waiters.pop_front(),
            Some(handles[2]),
            "existing payment group must keep its creation-time weight (1) until recreated"
        );
        assert_eq!(waiters.pop_front(), Some(handles[1]));
        assert!(waiters.is_empty());

        // fresh group (created after the update) picks up weight 10 immediately
        waiters.push_back(handles[3], &payment);
        waiters.push_back(handles[4], &payment);
        waiters.push_back(handles[5], &other);
        assert_eq!(waiters.pop_front(), Some(handles[3]));
        assert_eq!(
            waiters.pop_front(),
            Some(handles[4]),
            "fresh payment group runs at the new weight (10): both grants before yielding"
        );
        assert_eq!(waiters.pop_front(), Some(handles[5]));
    }
}
