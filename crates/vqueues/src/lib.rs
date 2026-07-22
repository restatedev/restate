// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cache;
pub mod context;
mod metric_definitions;
pub mod migrations;
pub mod scheduler;
mod util;

use std::debug_assert_matches;
use std::ops::Add;
use std::time::Duration;

// Re-exports
pub use cache::{VQueueHandle, VQueuesMeta, VQueuesMetaCache};
pub use metric_definitions::describe_metrics;
pub use restate_worker_api::{ResourceKind, SchedulingStatus, VQueueSchedulerStatus};
pub use scheduler::{ResourceManager, SchedulerService};
pub use util::*;

use smallvec::SmallVec;
use tracing::debug;

use restate_clock::RoughTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_limiter::LimitKey;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata,
};
use restate_storage_api::lock_table::{LockState, WriteLockTable};
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryStatusHeader, EntryValue, ReadVQueueTable, Stage, Status,
    WriteVQueueTable, metadata,
};
use restate_storage_api::{StorageError, lock_table};
use restate_types::ServiceName;
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey};
use restate_types::invocation::{InvocationTarget, InvocationTargetType, VirtualObjectHandlerType};
use restate_types::sharding::WithPartitionKey;
use restate_types::vqueues::{EntryId, Seq, VQueueId};
use restate_types::{LockName, Scope};
use restate_util_string::{ReString, ToReString};
use restate_worker_api::invoker::YieldReason;

// Token bucket used for throttling over all vqueues
type GlobalTokenBucket<C = gardal::TokioClock> =
    gardal::TokenBucket<gardal::PaddedAtomicSharedStorage, C>;

#[derive(Debug)]
pub enum EventDetails {
    /// The vqueue was manually paused
    QueuePaused,
    /// The vqueue was resumed
    QueueResumed,
    RemovedFromInbox(EntryKey),
    EnqueuedToInbox {
        key: EntryKey,
        value: EntryValue,
    },
    LockReleased {
        scope: Option<Scope>,
        lock_name: LockName,
    },
}

#[derive(Debug)]
pub struct VQueueEvent {
    pub queue: VQueueHandle,
    pub updates: SmallVec<[EventDetails; 1]>,
}

impl VQueueEvent {
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    pub const fn new(queue: VQueueHandle) -> Self {
        Self {
            queue,
            updates: SmallVec::new_const(),
        }
    }

    pub fn push(&mut self, details: EventDetails) {
        self.updates.push(details);
    }
}

/// VQueue Mutations
///
/// given an operation, describe the storage changes that need to be made and emit effects
/// to allow the scheduler to cache.
pub struct VQueue<'a, A, S> {
    storage: &'a mut S,
    cache: &'a mut VQueuesMetaCache,
    handle: VQueueHandle,
    // action collector is only available if we have a scheduler to notify
    action_collector: Option<&'a mut Vec<A>>,
}

impl VQueue<'_, (), ()> {
    /// Determines the vqueue id from the invocation id, invocation target, and limit key.
    #[inline]
    pub fn infer_vqueue_id_from_invocation(
        partition_key: PartitionKey,
        invocation_target: &InvocationTarget,
        limit_key: &LimitKey<ReString>,
    ) -> VQueueId {
        util::infer_vqueue_id_from_invocation(partition_key, invocation_target, limit_key)
    }
}

impl<'a, A, S> VQueue<'a, A, S>
where
    A: From<VQueueEvent> + 'static,
    S: WriteVQueueTable + ReadVQueueTable + WriteLockTable,
{
    pub fn handle(&self) -> VQueueHandle {
        self.handle
    }

    pub fn meta(&self) -> &VQueueMeta {
        self.cache.get(self.handle).unwrap().meta()
    }

    /// Get access to the vqueue if it exists, otherwise this returns None.
    pub async fn get(
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
    ) -> Result<Option<Self>, StorageError> {
        let Some(cache_key) = cache.load(storage, qid).await? else {
            return Ok(None);
        };

        Ok(Some(Self {
            storage,
            handle: cache_key,
            cache,
            action_collector,
        }))
    }

    pub async fn vqueue_from_invocation_target(
        at: UniqueTimestamp,
        qid: &VQueueId,
        invocation_target: &InvocationTarget,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        limit_key: &LimitKey<ReString>,
    ) -> Result<Self, StorageError> {
        let cache_key = match cache.load(storage, qid).await? {
            Some(key) => key,
            None => {
                let link = if let Some(lock_name) = invocation_target.lock_name() {
                    VQueueLink::Lock(lock_name)
                } else {
                    VQueueLink::Service(ServiceName::new(invocation_target.service_name()))
                };

                let meta = VQueueMeta::new(
                    at,
                    invocation_target.scope().cloned(),
                    limit_key.clone(),
                    link,
                );
                storage.create_vqueue(qid, &meta);
                cache.insert(qid.clone(), meta)
            }
        };

        Ok(Self {
            storage,
            handle: cache_key,
            cache,
            action_collector,
        })
    }

    // Used for migrations
    pub async fn get_or_insert_with(
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        insert_fn: impl FnOnce() -> VQueueMeta,
    ) -> Result<Self, StorageError> {
        let cache_key = match cache.load(storage, qid).await? {
            Some(key) => key,
            None => {
                let meta = insert_fn();
                storage.create_vqueue(qid, &meta);
                cache.insert(qid.clone(), meta)
            }
        };

        Ok(Self {
            storage,
            handle: cache_key,
            cache,
            action_collector: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub async fn get_or_create_vqueue(
        at: UniqueTimestamp,
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        service_name: &ServiceName,
        scope: &Option<Scope>,
        limit_key: &LimitKey<ReString>,
        lock_name: &Option<LockName>,
    ) -> Result<Self, StorageError> {
        let cache_key = match cache.load(storage, qid).await? {
            None => {
                let link = if let Some(lock_name) = lock_name {
                    VQueueLink::Lock(lock_name.clone())
                } else {
                    VQueueLink::Service(service_name.clone())
                };
                // Note: we don't send an event (i.e. vqueue created) because we only care
                // about notifying the scheduler only when the queue becomes active.
                let limit_key = limit_key.clone();
                let meta = VQueueMeta::new(at, scope.clone(), limit_key, link);
                storage.create_vqueue(qid, &meta);
                cache.insert(qid.clone(), meta)
            }
            Some(key) => key,
        };

        Ok(Self {
            storage,
            handle: cache_key,
            cache,
            action_collector,
        })
    }

    /// Enqueues a new item into the vqueue.
    ///
    /// -> Inbox
    pub fn enqueue_new(
        &mut self,
        created_at: UniqueTimestamp,
        seq: impl Into<Seq>,
        run_at: Option<MillisSinceEpoch>,
        entry_id: impl Into<EntryId>,
        metadata: impl Into<EntryMetadata>,
    ) {
        let meta = self.cache.get_mut(self.handle).unwrap();

        let created_at_unix = created_at.to_unix_millis();
        let (run_at, status) = match run_at {
            // Future: ceil to the next whole second so we never fire early.
            Some(t) if t > created_at_unix => {
                (RoughTimestamp::from_unix_millis_ceil(t), Status::Scheduled)
            }
            // Already past: floor is safe — it's eligible immediately regardless.
            Some(t) => (RoughTimestamp::from_unix_millis_clamped(t), Status::New),
            // Unset: treat as "run ASAP" using the record's creation timestamp, floored.
            None => (
                RoughTimestamp::from_unix_millis_clamped(created_at_unix),
                Status::New,
            ),
        };

        let entry_id = entry_id.into();
        let metadata = metadata.into();

        let key = EntryKey::new(false, run_at, seq, entry_id);
        let stats = EntryStatistics::new(created_at, run_at);

        let update = metadata::Update::new(
            created_at,
            metadata::Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(&stats, None),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
        }

        // We need to add the entry into the inbox vqueue.
        let value = EntryValue {
            status,
            stats: stats.clone(),
            metadata: metadata.clone(),
        };

        debug!(
            entry = %entry_id.display(meta.vqueue_id().partition_key()),
            qid = %meta.vqueue_id(),
            "->{}, status: {status}, run_at: {run_at}",
            Stage::Inbox,
        );

        self.storage
            .put_vqueue_inbox(meta.vqueue_id(), Stage::Inbox, &key, &value);

        self.storage.put_vqueue_entry_status(
            meta.vqueue_id(),
            Stage::Inbox,
            &key,
            &metadata,
            stats,
            status,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let mut event = VQueueEvent::new(self.handle);
            event.push(EventDetails::EnqueuedToInbox { key, value });

            collector.push(A::from(event));
        }
    }

    /// Moves a vqueue item from [`Stage::Inbox`] to [`Stage::Run`] and returns the modified
    /// [`EntryKey`] to identify the updated vqueue item.
    ///
    /// Inbox -> Run
    ///
    /// This move **must** be driven by applying a scheduler's decision.
    /// It's the caller's responsibility to ensure the vqueue exists and the item is
    /// in the the Inbox stage.
    ///
    /// Otherwise, this will panic.
    pub fn run_entry(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        wait_stats: WaitStats,
    ) -> EntryKey {
        let vqueue_id = header.vqueue_id();
        let partition_key = vqueue_id.partition_key();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Inbox));

        // Remove from inbox and move to ready
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Inbox, header.entry_key());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                // Use the stats before updating the entry to measure stage exit durations and
                // first-run wait correctly. Pass the head's `wait_stats` so the
                // vqueue-level EMAs can sample per-bucket blocking time.
                metrics: Self::build_move_metrics(header.stats(), Some(wait_stats)),
            },
        );

        meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        let stats = Self::mark_run_attempt(at, header.stats(), wait_stats);

        // Do we need to hold the lock or is this running an entry that doesn't need a lock (or)
        // already has the lock held?
        let modified_key = if !header.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            // acquire lock
            let lock_state = LockState {
                acquired_at: at,
                acquired_by: lock_table::AcquiredBy::from_entry_id(
                    partition_key,
                    header.entry_id(),
                ),
            };

            self.storage
                .acquire_lock(meta.meta().scope(), lock_name, &lock_state);

            header.entry_key().acquire_lock()
        } else {
            *header.entry_key()
        };

        let new_status = if !header.has_started() {
            Status::Started
        } else {
            header.status()
        };

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{}->{}, status: {}->{new_status}",
            header.stage(),
            Stage::Running,
            header.status(),
        );

        self.storage.put_vqueue_inbox(
            vqueue_id,
            Stage::Running,
            &modified_key,
            &EntryValue {
                status: new_status,
                stats: stats.clone(),
                // We pick metadata from EntryStatusHeader since it could have been updated
                // while we were parked, or after the previous run.
                metadata: header.metadata().clone(),
            },
        );

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Running,
            &modified_key,
            header.metadata(),
            stats,
            new_status,
        );

        modified_key
    }

    // Left intentionally for future reference
    // pub fn wake_up<T>(
    //     &mut self,
    //     at: UniqueTimestamp,
    //     header: &impl EntryStatusHeader,
    //     run_at: Option<RoughTimestamp>,
    //     updated_state: &T,
    // ) where
    //     T: EntryStatusExtra + bilrost::Message + bilrost::encoding::RawMessage,
    //     (): bilrost::encoding::EmptyState<(), T>,
    // {
    // }

    /// Wake up moves the inbox entry from a parked stage (Paused/Suspended) back
    /// into the inbox stage.
    ///
    /// Paused/Suspended -> Inbox
    pub fn wake_up(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        run_at: Option<RoughTimestamp>,
        updated_metadata: Option<EntryMetadata>,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Paused | Stage::Suspended));

        // Delete the old inbox entry
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(header.stats(), None),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);
        // Update vqueue meta in storage
        self.storage.update_vqueue(vqueue_id, &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(vqueue_id);
        }

        // We can be asked to wake up but not run immediately (or get a lower run_at for priority
        // boosting). If that's the case, we mutate the entry key to reflect that.
        let modified_key = header.entry_key().set_run_at(run_at);

        let stats = Self::mark_transition(at, header.stats());

        let maybe_new_metadata = updated_metadata.unwrap_or_else(|| header.metadata().clone());

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{}->{}, status: {}",
            header.stage(),
            Stage::Inbox,
            header.status(),
        );

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Inbox,
            &modified_key,
            &maybe_new_metadata,
            stats.clone(),
            header.status(),
        );

        let value = EntryValue {
            stats,
            status: header.status(),
            metadata: maybe_new_metadata,
        };

        // We add the entry back into the inbox stage
        self.storage
            .put_vqueue_inbox(vqueue_id, Stage::Inbox, &modified_key, &value);

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);

            event.push(EventDetails::EnqueuedToInbox {
                key: modified_key,
                value,
            });
            collector.push(A::from(event));
        }
    }

    /// Reschedules a waiting entry to a new `run_at`, leaving its stage, [`Status`] and statistics
    /// untouched. Only the entry key (its `run_at`) changes.
    ///
    /// `run_at` may be set in the past (`< now`): the entry becomes immediately eligible *and*
    /// jumps ahead of entries with a larger `run_at` (the [`EntryKey`] ordering is by `run_at`),
    /// so this doubles as a priority signal.
    ///
    /// Rescheduling makes sense for an entry that is *waiting* with a `run_at`:
    /// - `Inbox`: changes scheduling eligibility right away. Emits the inbox reconciliation pair
    ///   (`RemovedFromInbox` + `EnqueuedToInbox`) so the scheduler drops the stale key and
    ///   re-evaluates eligibility at the new `run_at`.
    /// - `Suspended` / `Paused`: changes the `run_at` the entry will carry once it is woken back
    ///   into the inbox (see [`Self::wake_up`]). The scheduler does not track parked entries, so no
    ///   event is emitted.
    ///
    /// This is *not* a stage transition: the entry stays in its current stage, so (unlike
    /// [`Self::yield_entry`]) the vqueue's aggregate metadata (stage counts, active flag, dwell-time
    /// EMAs) and the entry's stats are left untouched.
    ///
    /// It is a no-op (with a debug log) for any other stage (`Running`, where the entry is
    /// executing, and the terminal stages) and a no-op if `run_at` already equals the entry's
    /// current `run_at`.
    ///
    /// One can optionally provide a deployment to which the entry is pinned (updating it's metadata
    /// accordingly).
    pub fn reschedule(
        &mut self,
        header: &impl EntryStatusHeader,
        run_at: RoughTimestamp,
        pinned_deployment: Option<DeploymentId>,
    ) {
        let stage = header.stage();
        if !matches!(stage, Stage::Inbox | Stage::Suspended | Stage::Paused) {
            // Running entries are executing and terminal entries are done: there is nothing to
            // reschedule. Be lenient and ignore rather than crash the caller.
            debug!(
                entry = %header.display_entry_id(),
                qid = %header.vqueue_id(),
                "Skipping reschedule of entry in stage {stage}",
            );
            return;
        }

        let vqueue_id = header.vqueue_id();
        assert_eq!(
            vqueue_id,
            self.cache.get_mut(self.handle).unwrap().vqueue_id()
        );

        // Nothing to do if the entry is already scheduled at the requested time.
        if header.entry_key().run_at() == run_at {
            return;
        }

        // Delete the entry at its old key, then re-insert it at the new key in the *same* stage.
        self.storage
            .delete_vqueue_inbox(vqueue_id, stage, header.entry_key());

        let modified_key = header.entry_key().set_run_at(Some(run_at));
        let mut metadata = header.metadata().clone();
        let stats = header.stats().clone();

        if let Some(deployment_id) = pinned_deployment {
            metadata.deployment = Some(deployment_id.to_restring());
        }

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{0}->{0}, status: {1} (reschedule)",
            stage,
            header.status(),
        );

        // Update the entry state to track the new entry key
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            stage,
            &modified_key,
            &metadata,
            stats.clone(),
            header.status(),
        );

        let value = EntryValue {
            stats,
            status: header.status(),
            metadata,
        };

        // Re-insert the entry into its stage at the new key
        self.storage
            .put_vqueue_inbox(vqueue_id, stage, &modified_key, &value);

        // Only inbox entries live in the scheduler's ring, so only they need reconciliation events;
        // parked (Suspended/Paused) entries are not tracked by the scheduler.
        if matches!(stage, Stage::Inbox)
            && let Some(collector) = self.action_collector.as_deref_mut()
        {
            let mut event = VQueueEvent::new(self.handle);

            // Confirm the removal of the stale key so the scheduler drops the old (future) wake-up,
            // then enqueue the re-keyed entry.
            //
            // Important: Rescheduling an inbox entry can lead to a race with a decision that was
            // proposed but not yet applied. Delaying the inbox entry might invalidate a run
            // decision as the entry is not yet eligible for running. We need to be careful about
            // applying the SchedulerActions and filter actions out that no longer apply. Currently,
            // we do this by comparing the EntryKey and assume that a change constitutes an
            // invalidation of the action.
            //
            // TODO(perf): the scheduler processes `RemovedFromInbox` before `EnqueuedToInbox`, so if
            //  this entry is the vqueue's only inbox entry the queue momentarily looks dormant
            //  between the two and may be evicted from / re-added to the scheduler's ring. A dedicated
            //  re-key event (carrying both keys) would let the scheduler update the key in place and
            //  avoid that transient flip.
            event.push(EventDetails::RemovedFromInbox(*header.entry_key()));
            event.push(EventDetails::EnqueuedToInbox {
                key: modified_key,
                value,
            });

            collector.push(A::from(event));
        }
    }

    /// Suspend an entry
    /// ? -> Suspended
    /// Returns `true` if the entry was found in the previous stage and parked correctly, `false` otherwise.
    pub fn pause_entry(&mut self, at: UniqueTimestamp, header: &impl EntryStatusHeader)
    // add new state
    {
        self.park_entry(at, header, Stage::Paused)
    }

    /// Suspend an entry
    pub fn suspend_entry(&mut self, at: UniqueTimestamp, header: &impl EntryStatusHeader) {
        self.park_entry(at, header, Stage::Suspended)
    }

    /// Private helper for park/suspend
    ///
    /// # Panics
    /// Panics if the entry is not found in the previous stage.
    fn park_entry(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        next_stage: Stage,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(next_stage, Stage::Paused | Stage::Suspended));

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{}->{}, status: {}",
            header.stage(),
            next_stage,
            header.status(),
        );

        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage,
                metrics: Self::build_move_metrics(header.stats(), None),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before && !is_active_now {
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
        }

        let stats = match next_stage {
            Stage::Paused => Self::mark_pause(at, header.stats()),
            Stage::Suspended => Self::mark_suspension(at, header.stats()),
            _ => unreachable!(),
        };

        self.storage.put_vqueue_inbox(
            vqueue_id,
            next_stage,
            header.entry_key(),
            &EntryValue {
                stats: stats.clone(),
                metadata: header.metadata().clone(),
                // When pausing, we keep the last status as is. This is to provide
                // the ability to present the status prior to pausing to the user.
                status: header.status(),
            },
        );

        self.storage.put_vqueue_entry_status(
            vqueue_id,
            next_stage,
            header.entry_key(),
            header.metadata(),
            stats,
            header.status(),
        );

        if let Some(collector) = self.action_collector.as_deref_mut()
            && matches!(header.stage(), Stage::Inbox)
        {
            let mut event = VQueueEvent::new(self.handle);
            event.push(EventDetails::RemovedFromInbox(*header.entry_key()));
            collector.push(A::from(event));
        }
    }

    /// Movement of a running entry back to the inbox stage happens on failover of pp.
    /// or entries being retried.
    ///
    /// ? -> Inbox
    pub fn yield_entry(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        run_at: Option<RoughTimestamp>,
        reason: YieldReason,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());

        // Remove from running and move to waiting
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(header.stats(), None),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
        }

        // We can be asked to wake up but not run immediately (or get a lower run_at for priority
        // boosting). If that's the case, we mutate the entry key to reflect that.
        let modified_key = header.entry_key().set_run_at(run_at);

        let (status, metadata, is_error) = match reason {
            YieldReason::Unknown
            | YieldReason::InvokerLoadShedding
            | YieldReason::PartitionLeaderChange => {
                // The reason could be coming from a future version of restate.
                // It's okay to have an unknown reason, we'll continue to yield as usual.
                (Status::Yielded, header.metadata().clone(), false)
            }
            YieldReason::ExhaustedMemoryBudget { needed_memory } => {
                let current_metadata = header.metadata().clone();
                (
                    Status::Yielded,
                    EntryMetadata {
                        needed_memory: Some(needed_memory),
                        ..current_metadata
                    },
                    false,
                )
            }
            YieldReason::TransientError {
                retry_attempts,
                retry_count_since_last_stored_command,
            } => {
                let current_metadata = header.metadata().clone();
                (
                    Status::BackingOff,
                    EntryMetadata {
                        retry_attempts,
                        retry_count_since_last_stored_command,
                        ..current_metadata
                    },
                    true,
                )
            }
        };

        let stats = Self::mark_yield(at, header.stats().clone(), is_error);

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{}->{} due to '{}', status: {}->{status}",
            header.stage(),
            Stage::Inbox,
            reason,
            header.status(),
        );

        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Inbox,
            &modified_key,
            &metadata,
            stats.clone(),
            status,
        );

        let value = EntryValue {
            stats,
            status,
            metadata,
        };

        // We add the entry back into the inbox stage
        self.storage
            .put_vqueue_inbox(vqueue_id, Stage::Inbox, &modified_key, &value);

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);

            if matches!(header.stage(), Stage::Inbox) {
                // Inbox -> Inbox
                // The scheduler is moving the item from inbox, so we need to confirm its
                // decision. We only do that if prev_stage is Inbox because it's the only
                // stage that needs confirmation.
                event.push(EventDetails::RemovedFromInbox(*header.entry_key()));
            }

            // Enqueue the replaced entry now
            event.push(EventDetails::EnqueuedToInbox {
                key: modified_key,
                value,
            });

            collector.push(A::from(event));
        }
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    pub fn end(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        new_status: Status,
        delete_after: Duration,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());

        // Remove from the current stage
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        debug!(
            entry = %header.display_entry_id(),
            qid = %header.vqueue_id(),
            "{}->{}, status: {}->{new_status}",
            header.stage(),
            Stage::Finished,
            header.status(),
        );

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Finished,
                metrics: Self::build_move_metrics(header.stats(), None),
            },
        );

        let modified_key = if header.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            self.storage.release_lock(meta.meta().scope(), lock_name);
            header.entry_key().release_lock()
        } else {
            *header.entry_key()
        };

        let delete_at = at.to_unix_millis() + delete_after;
        let modified_key = modified_key.set_run_at(Some(RoughTimestamp::from(delete_at)));

        let stats = Self::mark_transition(at, header.stats());

        let value = EntryValue {
            stats: stats.clone(),
            metadata: header.metadata().clone(),
            status: new_status,
        };

        // Move the entry to Finished stage
        self.storage
            .put_vqueue_inbox(vqueue_id, Stage::Finished, &modified_key, &value);

        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Finished,
            &modified_key,
            header.metadata(),
            stats,
            new_status,
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before && !is_active_now {
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);
            // Release the lock if this entry has been holding a lock already
            if header.has_lock()
                && let Some(lock) = meta.meta().lock_name()
            {
                event.push(EventDetails::LockReleased {
                    scope: meta.meta().scope().clone(),
                    lock_name: lock.clone(),
                });
            }

            if matches!(header.stage(), Stage::Inbox) {
                event.push(EventDetails::RemovedFromInbox(*header.entry_key()));
            }
            if !event.is_empty() {
                collector.push(A::from(event));
            }
        }

        if delete_after.is_zero() {
            // Delete immediately!
            self.delete(at, vqueue_id, header.entry_id(), &modified_key);
        }
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// It's the caller's responsibility to ensure that the entry is in the `Finished` stage
    /// before calling this method.
    pub fn delete(
        &mut self,
        at: UniqueTimestamp,
        vqueue_id: &VQueueId,
        entry_id: &EntryId,
        entry_key: &EntryKey,
    ) {
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());

        debug!(
            entry = %entry_id.display(vqueue_id.partition_key()),
            qid = %vqueue_id,
            "{}->X",
            Stage::Finished,
        );

        let update = metadata::Update::new(
            at,
            metadata::Action::RemoveEntry {
                stage: Stage::Finished,
            },
        );

        self.storage
            .delete_vqueue_entry_status(vqueue_id.partition_key(), entry_id);
        // delete the entry's input
        self.storage
            .delete_vqueue_input_payload(vqueue_id, entry_key.seq(), entry_id);
        // delete the inbox entry
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Finished, entry_key);
        // update cache
        let _ = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);
    }

    /// A specialized version of run designed for inline execution of an entry.
    ///
    /// This drives the transition from inbox -> finished directly and correctly notifies
    /// the scheduler as if it was a regular invocation but takes a few shortcuts
    /// since there is no actual time spent that can be tracked.
    pub fn run_then_finish(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        wait_stats: WaitStats,
        status: Status,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.handle).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Inbox));

        // Remove from inbox and move to ready
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Inbox, header.entry_key());

        // Fake run, for the same of completeness
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                // Pass wait_stats so the vqueue-level EMAs sample state
                // mutations too — the head truly did wait for those resources,
                // regardless of whether it's an invocation or a state mutation.
                metrics: Self::build_move_metrics(header.stats(), Some(wait_stats)),
            },
        );

        meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);
        let stats = Self::mark_run_attempt(at, header.stats(), wait_stats);

        // Move to finish
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Finished,
                metrics: Self::build_move_metrics(&stats, None),
            },
        );

        let stats = Self::mark_transition(at, &stats);

        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        if was_active_before && !is_active_now {
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
        }

        // Move the entry to Finished stage
        // for future: Use this to set the deletion time.
        let modified_key = header.entry_key().set_run_at(Some(RoughTimestamp::MAX));
        assert!(!modified_key.has_lock());

        self.storage.put_vqueue_inbox(
            vqueue_id,
            Stage::Finished,
            &modified_key,
            &EntryValue {
                stats: stats.clone(),
                metadata: header.metadata().clone(),
                status,
            },
        );

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Finished,
            &modified_key,
            header.metadata(),
            stats,
            status,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);
            // In the case of state mutation, we execute it inline and we can
            // ask the scheduler to drop any pending resources for this entry.
            event.push(EventDetails::RemovedFromInbox(*header.entry_key()));

            collector.push(A::from(event));
        }

        // -- DELETION --
        // We currently fake the transition from finished -> deleted by emitting another transition
        // immediately after moving to Stage::Finish. In future changes, this will be separated
        // into separate step.
        //
        // The end result would be that a finished vqueue item would expire after some time
        // and be deleted from the vqueue (or moved to archival key-prefix).
        let update = metadata::Update::new(
            at,
            metadata::Action::RemoveEntry {
                stage: Stage::Finished,
            },
        );

        self.storage
            .delete_vqueue_entry_status(vqueue_id.partition_key(), header.entry_id());
        // delete the entry's input
        self.storage
            .delete_vqueue_input_payload(vqueue_id, header.seq(), header.entry_id());
        // delete the inbox entry
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Finished, &modified_key);
        // update cache
        let _ = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);
    }

    /// Marks this vqueue as paused
    pub fn pause_queue(&mut self, at: UniqueTimestamp) {
        let meta = self.cache.get_mut(self.handle).unwrap();

        if meta.meta().queue_is_paused() {
            // queue is already paused
            return;
        }

        debug!(qid = %meta.vqueue_id(), "Pausing vqueue");
        let update = metadata::Update::new(at, metadata::Action::PauseVQueue {});

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before && !is_active_now {
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);
            event.push(EventDetails::QueuePaused);
            collector.push(A::from(event));
        }
    }

    /// Marks this vqueue as resumed
    pub fn resume_queue(&mut self, at: UniqueTimestamp) {
        let meta = self.cache.get_mut(self.handle).unwrap();

        if !meta.meta().queue_is_paused() {
            // queue is not paused
            return;
        }
        debug!(qid = %meta.vqueue_id(), "Resuming vqueue");
        let update = metadata::Update::new(at, metadata::Action::ResumeVQueue {});

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
        }

        if is_active_now && let Some(collector) = self.action_collector.as_deref_mut() {
            let mut event = VQueueEvent::new(self.handle);
            event.push(EventDetails::QueueResumed);
            collector.push(A::from(event));
        }
    }

    pub fn update_entry_metadata(
        &mut self,
        header: &impl EntryStatusHeader,
        metadata: &EntryMetadata,
    ) {
        debug!(
            "update_entry_metadata: {} {metadata:?}, stage: {}",
            header.display_entry_id(),
            header.stage(),
        );

        self.storage.put_vqueue_entry_status(
            header.vqueue_id(),
            header.stage(),
            header.entry_key(),
            metadata,
            header.stats().clone(),
            header.status(),
        );
    }

    #[inline]
    fn build_move_metrics(
        stats: &EntryStatistics,
        scheduler_wait_stats: Option<WaitStats>,
    ) -> metadata::MoveMetrics {
        metadata::MoveMetrics {
            last_transition_at: stats.transitioned_at,
            has_started: stats.num_attempts > 0,
            first_runnable_at: stats.first_runnable_at,
            scheduler_wait_stats,
        }
    }

    #[inline]
    fn mark_transition(at: UniqueTimestamp, stats: &EntryStatistics) -> EntryStatistics {
        EntryStatistics {
            transitioned_at: at,
            ..stats.clone()
        }
    }

    #[inline]
    fn mark_run_attempt(
        at: UniqueTimestamp,
        stats: &EntryStatistics,
        wait_stats: WaitStats,
    ) -> EntryStatistics {
        EntryStatistics {
            num_attempts: stats.num_attempts.saturating_add(1),
            first_attempt_at: stats.first_attempt_at.or(Some(at)),
            latest_attempt_at: Some(at),
            transitioned_at: at,
            latest_attempt_wait_stats: wait_stats,
            total_wait_stats: stats.total_wait_stats.add(wait_stats),
            ..stats.clone()
        }
    }

    #[inline]
    fn mark_pause(at: UniqueTimestamp, stats: &EntryStatistics) -> EntryStatistics {
        EntryStatistics {
            num_paused: stats.num_paused.saturating_add(1),
            transitioned_at: at,
            ..stats.clone()
        }
    }

    #[inline]
    fn mark_suspension(at: UniqueTimestamp, stats: &EntryStatistics) -> EntryStatistics {
        EntryStatistics {
            num_suspensions: stats.num_suspensions.saturating_add(1),
            transitioned_at: at,
            ..stats.clone()
        }
    }

    #[inline]
    fn mark_yield(
        at: UniqueTimestamp,
        mut stats: EntryStatistics,
        is_error: bool,
    ) -> EntryStatistics {
        stats.transitioned_at = at;
        if is_error {
            stats.num_errors = stats.num_errors.saturating_add(1);
        } else {
            stats.num_yields = stats.num_yields.saturating_add(1);
        }
        stats
    }

    /// Backfills the vqueue with an existing running invocation
    fn migrate_invoked_invocation(
        &mut self,
        invocation_id: &InvocationId,
        invoked: &InFlightInvocationMetadata,
    ) {
        let meta = self.cache.get_mut(self.handle).unwrap();

        // We use a special value (0) for all running invocations under the following assumptions:
        // - We don't allow two invocations with the same ID to co-exist (prior to vqueues)
        // - Any new invocation with the same ID will be created with Lsn > 0 after migration.
        let seq = 0;
        let entry_id = EntryId::from(invocation_id);
        let stage = Stage::Running;
        let status = Status::Started;

        let partition_key = invocation_id.partition_key();
        let created_at =
            UniqueTimestamp::from_unix_millis_unchecked(invoked.timestamps.creation_time());
        let modified_at =
            UniqueTimestamp::from_unix_millis_unchecked(invoked.timestamps.modification_time());
        let started_running_at = UniqueTimestamp::from_unix_millis_unchecked(
            invoked
                .timestamps
                .running_transition_time()
                .unwrap_or_else(|| invoked.timestamps.modification_time()),
        );

        let created_at_unix = created_at.to_unix_millis();

        // following the same logic as enqueue_new
        let first_run_at = match invoked.execution_time {
            Some(t) if t > created_at_unix => RoughTimestamp::from_unix_millis_ceil(t),
            Some(t) => RoughTimestamp::from_unix_millis_clamped(t),
            None => RoughTimestamp::from_unix_millis_clamped(created_at_unix),
        };

        let mut metadata = EntryMetadata::default();
        if let Some(ref pinned) = invoked.pinned_deployment {
            metadata.deployment = Some(pinned.deployment_id.to_restring());
        }

        // Prepare Lock
        // We only use has_lock = true for exclusive handlers of virtual objects
        let has_lock = matches!(
            invoked.invocation_target.invocation_target_ty(),
            InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        );

        if has_lock {
            let lock_state = LockState {
                acquired_at: started_running_at,
                acquired_by: lock_table::AcquiredBy::from_entry_id(partition_key, &entry_id),
            };
            let lock_name = meta.meta().lock_name().expect("vo must have a lock link");
            self.storage.acquire_lock(&None, lock_name, &lock_state);
        }

        let key = EntryKey::new(has_lock, first_run_at, seq, entry_id);
        let mut stats = EntryStatistics::new(created_at, first_run_at);
        stats.transitioned_at = modified_at;
        stats.latest_attempt_at = Some(modified_at);
        // We fallback to assuming that this has only been attempted once because we no source of
        // truth for this information.
        // This needs to be > 0 because this is how we denote that an entry "has_started" in stats.
        stats.num_attempts = 1;

        // This will mess up the last_start_at of the vqueue metadata but it will be corrected
        // on the next real invocation start.
        let update = metadata::Update::new(
            modified_at,
            metadata::Action::Move {
                // We deliberately don't set prev_stage here because we are "adding" this entry
                // to the vqueue and not moving it from another stage.
                prev_stage: None,
                next_stage: stage,
                metrics: Self::build_move_metrics(&stats, None),
            },
        );

        // Update vqueue meta cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
        }

        // We need to add the entry into the inbox vqueue.
        let value = EntryValue {
            status,
            stats: stats.clone(),
            metadata: metadata.clone(),
        };

        self.storage
            .put_vqueue_inbox(meta.vqueue_id(), stage, &key, &value);

        self.storage.put_vqueue_entry_status(
            meta.vqueue_id(),
            stage,
            &key,
            &metadata,
            stats,
            status,
        );
    }

    /// Backfills the vqueue with an existing completed invocation
    fn migrate_completed_invocation(
        &mut self,
        invocation_id: &InvocationId,
        completed: &CompletedInvocation,
    ) {
        let meta = self.cache.get_mut(self.handle).unwrap();

        // We use a special value (0) for invocations under the following assumptions:
        // - We don't allow two invocations with the same ID to co-exist (prior to vqueues)
        // - Any new invocation with the same ID will be created with Lsn > 0 after migration.
        let seq = 0;
        let entry_id = EntryId::from(invocation_id);
        let stage = Stage::Finished;

        // How do we know if the invocation was "killed" or cancelled?
        let status = match completed.response_result {
            restate_types::invocation::ResponseResult::Success(_) => Status::Succeeded,
            restate_types::invocation::ResponseResult::Failure(ref e) => {
                if e.code() == restate_types::errors::codes::ABORTED {
                    // Special handling for cancel/kill. Definitely not ideal, but the current
                    // design leaves me with no other options.
                    //
                    // This takes the same approach as what the UI does to determine if the
                    // invocation was killed or cancelled.
                    match e.message.as_ref() {
                        "cancelled" | "Cancelled" | "canceled" | "Canceled" => Status::Cancelled,
                        "killed" | "Killed" => Status::Killed,
                        _ => Status::Failed,
                    }
                } else {
                    Status::Failed
                }
            }
        };

        let created_at =
            UniqueTimestamp::from_unix_millis_unchecked(completed.timestamps.creation_time());
        let modified_at =
            UniqueTimestamp::from_unix_millis_unchecked(completed.timestamps.modification_time());
        let completed_at = UniqueTimestamp::from_unix_millis_unchecked(
            completed
                .timestamps
                .completed_transition_time()
                .unwrap_or_else(|| completed.timestamps.modification_time()),
        );

        let created_at_unix = created_at.to_unix_millis();

        // following the same logic as enqueue_new
        let first_run_at = match completed.execution_time {
            Some(t) if t > created_at_unix => RoughTimestamp::from_unix_millis_ceil(t),
            Some(t) => RoughTimestamp::from_unix_millis_clamped(t),
            None => RoughTimestamp::from_unix_millis_clamped(created_at_unix),
        };

        let mut metadata = EntryMetadata::default();
        if let Some(ref pinned) = completed.pinned_deployment {
            metadata.deployment = Some(pinned.deployment_id.to_restring());
        }

        // Completed invocations cannot hold locks
        // The run_at of the entry_key for completed invocations is set to be the cleanup time.
        // of completion.
        let delete_at = completed_at.to_unix_millis() + completed.completion_retention_duration;
        let key = EntryKey::new(false, delete_at, seq, entry_id);

        let mut stats = EntryStatistics::new(created_at, first_run_at);
        // We fallback to assuming that this has only been attempted once because we no source of
        // truth for this information.
        // This needs to be > 0 because this is how we denote that an entry "has_started" in stats.
        stats.num_attempts = 1;
        stats.transitioned_at = modified_at;
        stats.latest_attempt_at = completed
            .timestamps
            .running_transition_time()
            .map(UniqueTimestamp::from_unix_millis_unchecked);

        // This will mess up the last_finish_at of the vqueue metadata but it will be corrected
        // on the next real invocation completion.
        let update = metadata::Update::new(
            completed_at,
            metadata::Action::Move {
                // We deliberately don't set prev_stage here because we are "adding" this entry
                // to the vqueue and not moving it from another stage.
                prev_stage: None,
                next_stage: stage,
                metrics: Self::build_move_metrics(&stats, None),
            },
        );

        // Update vqueue meta cache
        meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        // We need to add the entry into the inbox vqueue.
        let value = EntryValue {
            status,
            stats: stats.clone(),
            metadata: metadata.clone(),
        };

        self.storage
            .put_vqueue_inbox(meta.vqueue_id(), stage, &key, &value);

        self.storage.put_vqueue_entry_status(
            meta.vqueue_id(),
            stage,
            &key,
            &metadata,
            stats,
            status,
        );
    }

    /// Backfills the vqueue with an existing parked (suspended or paused) invocations
    fn migrate_parked_invocation(
        &mut self,
        invocation_id: &InvocationId,
        parked: &InFlightInvocationMetadata,
        stage: Stage,
    ) {
        debug_assert_matches!(stage, Stage::Paused | Stage::Suspended);
        let meta = self.cache.get_mut(self.handle).unwrap();

        // We use a special value (0) for all running invocations under the following assumptions:
        // - We don't allow two invocations with the same ID to co-exist (prior to vqueues)
        // - Any new invocation with the same ID will be created with Lsn > 0 after migration.
        let seq = 0;
        let entry_id = EntryId::from(invocation_id);
        let status = Status::Started;

        let partition_key = invocation_id.partition_key();
        let created_at =
            UniqueTimestamp::from_unix_millis_unchecked(parked.timestamps.creation_time());

        let modified_at =
            UniqueTimestamp::from_unix_millis_unchecked(parked.timestamps.modification_time());

        let may_have_ran_at = UniqueTimestamp::from_unix_millis_unchecked(
            parked
                .timestamps
                .running_transition_time()
                .unwrap_or_else(|| parked.timestamps.modification_time()),
        );

        let created_at_unix = created_at.to_unix_millis();

        // following the same logic as enqueue_new
        let first_run_at = match parked.execution_time {
            Some(t) if t > created_at_unix => RoughTimestamp::from_unix_millis_ceil(t),
            Some(t) => RoughTimestamp::from_unix_millis_clamped(t),
            None => RoughTimestamp::from_unix_millis_clamped(created_at_unix),
        };

        let mut metadata = EntryMetadata::default();
        if let Some(ref pinned) = parked.pinned_deployment {
            metadata.deployment = Some(pinned.deployment_id.to_restring());
        }

        // Prepare Lock
        // We only use has_lock = true for exclusive handlers of virtual objects
        let has_lock = matches!(
            parked.invocation_target.invocation_target_ty(),
            InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        );

        if has_lock {
            let lock_state = LockState {
                // We try to use the last run timestamp if we have it, otherwise we fallback to the
                // modification time.
                acquired_at: may_have_ran_at,
                acquired_by: lock_table::AcquiredBy::from_entry_id(partition_key, &entry_id),
            };
            let lock_name = meta.meta().lock_name().expect("vo must have a lock link");
            self.storage.acquire_lock(&None, lock_name, &lock_state);
        }

        // Order parked invocations by their original eligibile run time.
        let key = EntryKey::new(has_lock, first_run_at, seq, entry_id);
        let mut stats = EntryStatistics::new(created_at, first_run_at);
        stats.transitioned_at = modified_at;
        stats.latest_attempt_at = Some(modified_at);
        // Without vqueues, we cannot pause inboxed or invocations that didn't go through the
        // Invoked state. Therefore, we'll assume that we have performed at least one attempt.
        stats.num_attempts = 1;
        if matches!(stage, Stage::Paused) {
            stats.num_paused = 1;
        } else {
            stats.num_suspensions = 1;
        }

        // This will mess up the last_start_at of the vqueue metadata but it will be corrected
        // on the next real invocation start.
        let update = metadata::Update::new(
            modified_at,
            metadata::Action::Move {
                // We deliberately don't set prev_stage here because we are "adding" this entry
                // to the vqueue and not moving it from another stage.
                prev_stage: None,
                next_stage: stage,
                metrics: Self::build_move_metrics(&stats, None),
            },
        );

        // Update vqueue meta cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if !was_active_before && is_active_now {
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
        }

        // We need to add the entry into the inbox vqueue.
        let value = EntryValue {
            status,
            stats: stats.clone(),
            metadata: metadata.clone(),
        };

        self.storage
            .put_vqueue_inbox(meta.vqueue_id(), stage, &key, &value);

        self.storage.put_vqueue_entry_status(
            meta.vqueue_id(),
            stage,
            &key,
            &metadata,
            stats,
            status,
        );
    }
}
