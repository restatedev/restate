// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use tokio::sync::mpsc;
use tracing::trace;

use restate_platform::hash::HashMap;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::NotificationId;
use restate_types::sharding::KeyRange;
use restate_types::{LimitKey, Scope};
use restate_util_string::ReString;
use restate_worker_api::invoker::{
    FencedEffect, SuspendIdleTarget, invocation_reader::InvocationReader,
};

use crate::InvocationStateMachine;

/// Tree of [InvocationStateMachine] held by the [Service].
///
/// Also maintains the index of invocations that can be suspended eagerly (see
/// [`SuspendableMark`](crate::invocation_state_machine::SuspendableMark)). The index is derived from the
/// state machines' suspendable marks: entries are dropped in [`Self::remove_invocation`] and
/// restored from the mark in [`Self::register_invocation`]. In-place changes of the mark go
/// through [`Self::mark_suspendable`], [`Self::clear_suspendable`] and [`Self::eager_suspend`].
#[derive(Debug)]
pub(super) struct InvocationStateMachineManager<SR> {
    output_tx: mpsc::Sender<FencedEffect>,
    invocation_state_machines: HashMap<InvocationId, InvocationStateMachine>,
    suspendable: SuspendableIndex,
    _partition_key_range: KeyRange,
    storage_reader: SR,
}

impl<SR> InvocationStateMachineManager<SR>
where
    SR: InvocationReader + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(super) fn new(
        partition_key_range: KeyRange,
        storage_reader: SR,
        sender: mpsc::Sender<FencedEffect>,
    ) -> Self {
        Self {
            output_tx: sender,
            invocation_state_machines: Default::default(),
            suspendable: Default::default(),
            _partition_key_range: partition_key_range,
            storage_reader,
        }
    }

    #[inline]
    pub(super) fn storage_reader(&self) -> &SR {
        &self.storage_reader
    }

    #[inline]
    pub(super) fn partition_sender(&self) -> &mpsc::Sender<FencedEffect> {
        &self.output_tx
    }

    /// Mutable access to a registered state machine.
    ///
    /// Callers must not change the suspendable mark through this reference, use [`Self::mark_suspendable`]
    /// and [`Self::clear_suspendable`] instead.
    #[inline]
    pub(super) fn resolve_invocation(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<FencedEffect>, &mut InvocationStateMachine)> {
        self.invocation_state_machines
            .get_mut(invocation_id)
            .map(|ism| (&self.output_tx, ism))
    }

    /// Returns `true` if the in-flight state machine for `invocation_id` exists
    /// but represents a *different* (newer) attempt than `fencing_token`.
    ///
    /// This fences output from an aborted task that is still draining out of the
    /// task→invoker channel after the invocation has already been restarted: such
    /// output must neither mutate the new attempt's state machine nor be emitted
    /// as an effect stamped with the new attempt's epoch.
    #[inline]
    pub(super) fn is_stale_fencing_token(
        &self,
        invocation_id: &InvocationId,
        fencing_token: restate_types::invocation::FencingToken,
    ) -> bool {
        self.invocation_state_machines
            .get(invocation_id)
            .is_some_and(|ism| ism.fencing_token != fencing_token)
    }

    #[inline]
    pub(super) fn handle_for_invocation<R>(
        &mut self,
        invocation_id: &InvocationId,
        f: impl FnOnce(&mpsc::Sender<FencedEffect>, &mut InvocationStateMachine) -> R,
    ) -> Option<R> {
        if let Some((tx, ism)) = self.resolve_invocation(invocation_id) {
            Some(f(tx, ism))
        } else {
            trace!("No state machine found for invocation {invocation_id}");
            None
        }
    }

    /// Removes the state machine and its suspendable-index entries. If the state machine is
    /// registered again, its entries are restored from its suspendable mark.
    #[inline]
    pub(super) fn remove_invocation(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<FencedEffect>, &SR, InvocationStateMachine)> {
        let ism = self.invocation_state_machines.remove(invocation_id)?;
        if let Some(seq) = ism.suspendable_seq() {
            self.suspendable.remove(&ism, seq, *invocation_id);
        }
        Some((&self.output_tx, &self.storage_reader, ism))
    }

    #[inline]
    pub(super) fn remove_all(&mut self) -> HashMap<InvocationId, InvocationStateMachine> {
        self.suspendable.clear();
        std::mem::take(&mut self.invocation_state_machines)
    }

    #[inline]
    pub(super) fn register_invocation(&mut self, id: InvocationId, ism: InvocationStateMachine) {
        if let Some(old) = self.invocation_state_machines.remove(&id)
            && let Some(seq) = old.suspendable_seq()
        {
            self.suspendable.remove(&old, seq, id);
        }
        if let Some(seq) = ism.suspendable_seq() {
            self.suspendable.insert(&ism, seq, id);
        }
        self.invocation_state_machines.insert(id, ism);
    }

    /// Marks the invocation as suspendable, after the SDK reported that user
    /// code is blocked awaiting `awaiting`. Returns `true` if the invocation is (still) marked.
    pub(super) fn mark_suspendable(
        &mut self,
        invocation_id: &InvocationId,
        awaiting: Box<[NotificationId]>,
    ) -> bool {
        let Some(ism) = self.invocation_state_machines.get_mut(invocation_id) else {
            return false;
        };
        let previous_seq = ism.suspendable_seq();
        let Some(seq) = ism.try_mark_suspendable(self.suspendable.next_seq, awaiting) else {
            debug_assert!(
                previous_seq.is_none(),
                "a marked invocation must stay eligible"
            );
            return false;
        };
        if previous_seq.is_none() {
            self.suspendable.next_seq += 1;
            self.suspendable.insert(ism, seq, *invocation_id);
        }
        true
    }

    /// Clears the suspendable mark of the invocation, e.g. because the SDK made progress.
    pub(super) fn clear_suspendable(&mut self, invocation_id: &InvocationId) {
        if let Some(ism) = self.invocation_state_machines.get_mut(invocation_id)
            && let Some(seq) = ism.clear_suspendable_mark()
        {
            self.suspendable.remove(ism, seq, *invocation_id);
        }
    }

    /// Picks the invocation that has been marked suspendable for the longest time among those whose
    /// suspension releases `target`, and closes its notification channel so that it suspends.
    pub(super) fn eager_suspend(&mut self, target: &SuspendIdleTarget) -> Option<InvocationId> {
        let key = SuspendableKey::from_target(target);
        loop {
            let (seq, invocation_id) = self.suspendable.first(&key)?;
            let ism = self
                .invocation_state_machines
                .get_mut(&invocation_id)
                .filter(|ism| ism.suspendable_seq() == Some(seq));
            debug_assert!(ism.is_some(), "stale entry in the suspendable index");
            let Some(ism) = ism else {
                self.suspendable.remove_from(&key, seq, invocation_id);
                continue;
            };
            ism.clear_suspendable_mark();
            self.suspendable.remove(ism, seq, invocation_id);
            let suspended = ism.notify_eager_suspend();
            debug_assert!(suspended, "a marked invocation must have an open channel");
            if suspended {
                return Some(invocation_id);
            }
        }
    }
}

/// Key under which suspendable invocations are indexed, see [`SuspendableIndex`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SuspendableKey {
    /// Every invocation of this invoker holds a node invoker concurrency slot.
    Node,
    /// Invocations whose limit-key permit lies in this scope, under this limit-key prefix.
    Limit(Scope, LimitKey<ReString>),
}

impl SuspendableKey {
    fn from_target(target: &SuspendIdleTarget) -> Self {
        match target {
            SuspendIdleTarget::InvokerConcurrency => Self::Node,
            SuspendIdleTarget::LimitKey { scope, limit_key } => {
                Self::Limit(scope.clone(), limit_key.clone())
            }
        }
    }

    /// The keys an invocation is indexed under: the node key, plus one key per prefix of its
    /// limit-key permit, because releasing a permit frees capacity on every level of its path.
    fn for_invocation(ism: &InvocationStateMachine) -> [Option<Self>; 4] {
        let mut keys = [Some(Self::Node), None, None, None];
        if let Some((scope, limit_key)) = ism.limit_key_permit() {
            keys[1] = Some(Self::Limit(scope.clone(), LimitKey::None));
            if let Some(l1) = limit_key.level1() {
                keys[2] = Some(Self::Limit(scope.clone(), LimitKey::L1(l1.clone())));
            }
            if limit_key.level2().is_some() {
                keys[3] = Some(Self::Limit(scope.clone(), limit_key.clone()));
            }
        }
        keys
    }
}

/// Index of the invocations marked suspendable, ordered by the time they were
/// marked (oldest first) under each [`SuspendableKey`].
#[derive(Debug, Default)]
struct SuspendableIndex {
    next_seq: u64,
    sets: HashMap<SuspendableKey, BTreeSet<(u64, InvocationId)>>,
}

impl SuspendableIndex {
    fn insert(&mut self, ism: &InvocationStateMachine, seq: u64, invocation_id: InvocationId) {
        for key in SuspendableKey::for_invocation(ism).into_iter().flatten() {
            self.sets
                .entry(key)
                .or_default()
                .insert((seq, invocation_id));
        }
    }

    fn remove(&mut self, ism: &InvocationStateMachine, seq: u64, invocation_id: InvocationId) {
        for key in SuspendableKey::for_invocation(ism).into_iter().flatten() {
            self.remove_from(&key, seq, invocation_id);
        }
    }

    fn remove_from(&mut self, key: &SuspendableKey, seq: u64, invocation_id: InvocationId) {
        if let Some(set) = self.sets.get_mut(key) {
            set.remove(&(seq, invocation_id));
            if set.is_empty() {
                self.sets.remove(key);
            }
        }
    }

    fn first(&self, key: &SuspendableKey) -> Option<(u64, InvocationId)> {
        self.sets.get(key)?.first().copied()
    }

    fn clear(&mut self) {
        self.sets.clear();
    }
}
