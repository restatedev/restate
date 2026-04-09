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
mod metric_definitions;
pub mod scheduler;
mod util;

// Re-exports
pub use cache::{VQueuesMeta, VQueuesMetaCache};
pub use metric_definitions::describe_metrics;
pub use scheduler::{
    ResourceManager, SchedulerService, SchedulingStatus, ThrottleScope, VQueueSchedulerStatus,
};
use tracing::trace;
pub use util::*;

use restate_limiter::LimitKey;
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::{AcquiredBy, LockState, WriteLockTable};
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::scheduler::{RunAction, YieldAction};
use restate_storage_api::vqueue_table::{
    AsEntryStateHeader, EntryId, EntryKey, EntryMetadata, EntryStatistics, EntryValue,
    ReadVQueueTable, RunAt, Seq, Stage, WriteVQueueTable, metadata,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, StateMutationId};
use restate_types::invocation::InvocationTarget;
use restate_types::vqueue::VQueueId;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use self::cache::VQueueCacheKey;

// Token bucket used for throttling over all vqueues
type GlobalTokenBucket<C = gardal::TokioClock> =
    gardal::TokenBucket<gardal::PaddedAtomicSharedStorage, C>;

#[derive(Debug, Clone)]
pub enum EventDetails {
    MetadataUpdated(metadata::Update),
    // Queue has been added to the list of active
    VQueueBecameActive(VQueueMeta),
    // Queue has been marked as dormant
    VQueueBecameDormant,
    /// Entry is being enqueued for the first time
    Enqueued {
        key: EntryKey,
        value: EntryValue,
    },
    /// Scheduler decision has been confirmed
    DecisionConfirmed {
        key: EntryKey,
    },
    /// Entry has been removed from the inbox
    Removed {
        key: EntryKey,
    },
    LockReleased {
        scope: Option<Scope>,
        lock_name: LockName,
    },
}

#[derive(Debug, Clone)]
pub struct VQueueEvent {
    pub qid: VQueueId,
    pub updates: Vec<EventDetails>,
}

impl VQueueEvent {
    pub const fn new(qid: VQueueId) -> Self {
        Self {
            qid,
            updates: Vec::new(),
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
    cache_key: VQueueCacheKey,
    // action collector is only available if we have a scheduler to notify
    action_collector: Option<&'a mut Vec<A>>,
}

impl<'a, A, S> VQueue<'a, A, S>
where
    A: From<VQueueEvent> + 'static,
    S: WriteVQueueTable + ReadVQueueTable + WriteLockTable,
{
    /// Determines the vqueue id from the invocation id, invocation target, and limit key.
    #[inline]
    pub fn infer_vqueue_id_from_invocation(
        partition_key: PartitionKey,
        invocation_target: &InvocationTarget,
        limit_key: &LimitKey<ReString>,
    ) -> VQueueId {
        util::infer_vqueue_id_from_invocation(partition_key, invocation_target, limit_key)
    }
    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    ///
    /// Returns true if the entry was found and was ended correctly, false otherwise.
    pub async fn end_by_id(
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        at: UniqueTimestamp,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<bool, StorageError> {
        // find the entry
        let entry_state = storage.get_entry_state_header(partition_key, id).await?;

        let Some(entry_state) = entry_state else {
            return Ok(false);
        };

        let inbox = Self::get(entry_state.vqueue_id(), storage, cache, action_collector).await?;
        let Some(mut inbox) = inbox else {
            return Ok(false);
        };

        inbox.end(at, entry_state.stage(), entry_state.current_entry_key())
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
            cache_key,
            cache,
            action_collector,
        }))
    }

    pub async fn vqueue_from_invocation_target(
        partition_key: PartitionKey,
        invocation_target: &InvocationTarget,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        limit_key: &LimitKey<ReString>,
    ) -> Result<Self, StorageError> {
        let qid =
            Self::infer_vqueue_id_from_invocation(partition_key, invocation_target, limit_key);
        let cache_key = match cache.load(storage, &qid).await? {
            Some(key) => key,
            None => {
                let meta = VQueueMeta::new(
                    invocation_target.scope(),
                    limit_key.clone(),
                    invocation_target.lock_name(),
                );
                storage.create_vqueue(&qid, &meta);
                cache.insert(qid.clone(), meta)
            }
        };

        Ok(Self {
            storage,
            cache_key,
            cache,
            action_collector,
        })
    }

    pub async fn get_or_create_vqueue(
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        scope: &Option<Scope>,
        limit_key: &LimitKey<ReString>,
        lock_name: &Option<LockName>,
    ) -> Result<Self, StorageError> {
        let cache_key = match cache.load(storage, qid).await? {
            None => {
                // Note: we don't send an event (i.e. vqueue created) because we only care
                // about notifying the scheduler only when the queue becomes active.
                let limit_key = limit_key.to_cheap_cloneable();
                let meta = VQueueMeta::new(scope.clone(), limit_key, lock_name.clone());
                storage.create_vqueue(qid, &meta);
                cache.insert(qid.clone(), meta)
            }
            Some(key) => key,
        };

        Ok(Self {
            storage,
            cache_key,
            cache,
            action_collector,
        })
    }

    /// Enqueues a new item into the vqueue.
    ///
    /// -> Inbox
    pub fn enqueue_new<E>(
        &mut self,
        created_at: UniqueTimestamp,
        seq: impl Into<Seq>,
        run_at: RunAt,
        id: impl Into<EntryId>,
        metadata: impl Into<EntryMetadata>,
        item: Option<E>,
    ) -> Result<(), StorageError>
    where
        E: bilrost::Message,
    {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        let mut event = VQueueEvent::new(meta.vqueue_id().clone());

        let entry_id = id.into();
        let key = EntryKey::new(false, run_at, seq.into());
        let value = EntryValue {
            id: entry_id.clone(),
            original_run_at: run_at,
            stats: EntryStatistics::new(created_at, run_at),
            metadata: metadata.into(),
        };

        // todo: Perform enqueue validations:
        // - Can admit? (bounds check, is_sealed, etc.)
        let update = metadata::Update::new(
            created_at,
            metadata::Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        // We need to add the entry into the inbox vqueue.
        // todo: add to the general vqueue prefix (that includes all entries regardless of stage)
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &key, &value);

        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &entry_id, Stage::Inbox, &key, ());

        // store the vqueue item for later usage
        if let Some(item) = item {
            self.storage
                .put_item(meta.vqueue_id(), created_at, &entry_id, item);
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::Enqueued { key, value });
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Enqueued { key, value });
        } else {
            unreachable!("Cannot enqueue an item on a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            collector.push(A::from(event));
        }

        Ok(())
    }

    /// Moves a vqueue item from [`Stage::Inbox`] to [`Stage::Run`] and returns the modified
    /// [`Option<EntryKey>`] to identify the updated vqueue item.
    ///
    /// Inbox -> Run
    ///
    /// This move **must** be driven by applying a scheduler's decision.
    ///
    /// The returned [`Option<EntryKey>`] is `None` if the vqueue item was not found in the inbox.
    pub fn attempt_to_run(
        &mut self,
        at: UniqueTimestamp,
        action: &RunAction,
    ) -> Result<Option<(EntryKey, EntryValue)>, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        // Remove from inbox and move to ready
        let Some(mut value) =
            self.storage
                .pop_inbox_entry(meta.vqueue_id(), Stage::Inbox, &action.key)?
        else {
            // We don't do work if the inbox entry was not found
            return Ok(None);
        };

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let mut modified_key = action.key;

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Run,
                // Use the stats before updating the entry to measure stage exit durations and
                // first-run wait correctly.
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        Self::mark_run_attempt(&mut value.stats, at);

        // Update entry metadata if we were asked so.
        // if let Some(new_metadata) = action.new_metadata {
        //     value.metadata = new_metadata;
        // }

        // Do we need to hold the lock or is this running an entry that doesn't need a lock (or)
        // already has the lock held?
        if !action.key.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            let acquired_by = match &value.id {
                EntryId::Unknown => AcquiredBy::Other(ReString::from_static("unknown")),
                EntryId::Invocation(uuid) => AcquiredBy::InvocationId(InvocationId::from_parts(
                    meta.vqueue_id().partition_key(),
                    InvocationUuid::from_bytes(*uuid),
                )),
                EntryId::StateMutation(remainder) => {
                    AcquiredBy::StateMutation(StateMutationId::from_partition_key_and_bytes(
                        meta.vqueue_id().partition_key(),
                        *remainder,
                    ))
                }
            };

            // acquire lock
            let lock_state = LockState {
                acquired_at: at,
                acquired_by,
            };

            modified_key.acquire_lock();
            self.storage
                .acquire_lock(meta.meta().scope(), lock_name, &lock_state);
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::DecisionConfirmed { key: action.key });
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::DecisionConfirmed { key: action.key });
        } else {
            unreachable!("Cannot run an item on a dormant vqueue");
        }

        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Run, &modified_key, &value);

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_state(
            meta.vqueue_id(),
            &value.id,
            Stage::Run,
            &modified_key,
            (),
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(Some((modified_key, value)))
    }

    /// Wake up moves the inbox entry from (parked) back into the inbox stage.
    ///
    /// Park -> Inbox
    ///
    /// Returns true if the entry was found in the parked inbox and resumed correctly, false otherwise.
    pub fn wake_up(
        &mut self,
        at: UniqueTimestamp,
        key: &EntryKey,
        run_at: Option<RunAt>,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        let Some(mut value) = self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), Stage::Park, key)?
        else {
            // We don't do work if the inbox entry was not found
            return Ok(false);
        };

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Park),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        let mut modified_key = *key;
        if let Some(run_at) = run_at {
            modified_key.set_run_at(run_at);
        } else if value.stats.num_attempts > 0 {
            // we need to do this to ensure that inbox entries of started entries follow the
            // creation time and not their visible_at time.
            modified_key.set_run_at(value.original_run_at);
        }

        Self::mark_transition(&mut value.stats, at);

        // We add the entry back into the waiting inbox
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &modified_key, &value);

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_state(
            meta.vqueue_id(),
            &value.id,
            Stage::Inbox,
            &modified_key,
            (),
        );

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::Enqueued {
                key: modified_key,
                value,
            });
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Enqueued {
                key: modified_key,
                value,
            });
        } else {
            unreachable!("Cannot enqueue an item on a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// Park an entry
    /// ? -> Park
    /// Returns `true` if the entry was found in the previous stage and parked correctly, `false` otherwise.
    pub fn park(
        &mut self,
        at: UniqueTimestamp,
        key: &EntryKey,
        prev_stage: Stage,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        let Some(mut value) = self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), prev_stage, key)?
        else {
            return Ok(false);
        };

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(prev_stage),
                next_stage: Stage::Park,
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            event.push(EventDetails::Removed { key: *key });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Removed { key: *key });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        Self::mark_park(&mut value.stats, at);

        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Park, key, &value);
        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &value.id, Stage::Park, key, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// Movement of a running entry back to the waiting inbox happens on failover of pp.
    ///
    /// ? -> Inbox
    ///
    /// Returns `true` if the entry was found in running inbox and yielded correctly, `false` otherwise.
    pub fn yield_entry(
        &mut self,
        at: UniqueTimestamp,
        action: &YieldAction,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        // Remove from running and move to waiting
        let Some(mut value) =
            self.storage
                .pop_inbox_entry(meta.vqueue_id(), action.prev_stage, &action.key)?
        else {
            return Ok(false);
        };

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(action.prev_stage),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            unreachable!("Yielding should never toggle vqueue's active state");
        }

        let mut modified_key = action.key;

        if let Some(run_at) = action.run_at {
            modified_key.set_run_at(run_at);
        }

        Self::mark_transition(&mut value.stats, at);
        if matches!(action.prev_stage, Stage::Run) {
            value.stats.num_yields = value.stats.num_yields.saturating_add(1);
        }

        // We add the entry back into the waiting inbox
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &modified_key, &value);

        self.storage.put_vqueue_entry_state(
            meta.vqueue_id(),
            &value.id,
            Stage::Inbox,
            &modified_key,
            (),
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let mut event = VQueueEvent::new(meta.vqueue_id().clone());
            event.push(EventDetails::MetadataUpdated(update));
            if matches!(action.prev_stage, Stage::Inbox) {
                // The scheduler is moving the item from inbox, so we need to confirm its
                // decision. We only do that if prev_stage is Inbox because it's the only
                // stage that needs confirmation.
                event.push(EventDetails::DecisionConfirmed { key: action.key });
            }
            event.push(EventDetails::Enqueued {
                key: modified_key,
                value,
            });
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    ///
    /// Returns `true` if the entry was found in `prev_stage` inbox and was ended correctly, `false` otherwise.
    pub fn end(
        &mut self,
        at: UniqueTimestamp,
        prev_stage: Stage,
        key: &EntryKey,
        // todo: add a way to specify the "deletion time" for this item.
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        // Remove from the current stage
        let Some(mut value) = self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), prev_stage, key)?
        else {
            trace!(
                "[end] vqueue {} has no entry key {key} in stage {prev_stage}",
                meta.vqueue_id()
            );
            return Ok(false);
        };

        trace!(
            vqueue = %meta.vqueue_id(),
            key = %key,
            prev_stage = %prev_stage,
            "[end] ending vqueue entry: {}", value.id.display(meta.vqueue_id().partition_key())
        );

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(prev_stage),
                next_stage: Stage::Finished,
                metrics: Self::build_move_metrics(&value.stats),
            },
        );

        let mut modified_key = *key;

        if key.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            self.storage.release_lock(meta.meta().scope(), lock_name);
            event.push(EventDetails::LockReleased {
                scope: meta.meta().scope().clone(),
                lock_name: lock_name.clone(),
            });
            modified_key.release_lock();
        }

        // Move the entry to Finished stage
        // for future: Use this to set the deletion time.
        modified_key.set_run_at(RunAt::MAX);

        Self::mark_transition(&mut value.stats, at);
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Finished, &modified_key, &value);

        self.storage.put_vqueue_entry_state(
            meta.vqueue_id(),
            &value.id,
            Stage::Finished,
            &modified_key,
            (),
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            event.push(EventDetails::Removed { key: *key });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Removed { key: *key });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        // We currently fake the transition from finished -> deleted by emitting another transition
        // immediately after moving to Stage::Finish. In future changes, this will be separated
        // into separate step.
        //
        // The end result would be that a finished vqueue item would expire after some time
        // and be deleted from the vqueue (or moved to archival key-prefix).
        let update = metadata::Update::new(
            at,
            metadata::Action::RemoveEntries {
                stage: Stage::Finished,
                count: 1,
            },
        );

        self.storage
            .delete_vqueue_entry_state(meta.vqueue_id().partition_key(), &value.id);
        // todo(asoli): Here we delete the item even if we never had it. This is currently needed
        // only for state mutation, but we pay the tombstone cost for all types.
        self.storage
            .delete_item(meta.vqueue_id(), value.stats.created_at, &value.id);
        self.storage
            .delete_inbox_entry(meta.vqueue_id(), Stage::Finished, &modified_key);
        // update cache
        let _ = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        Ok(true)
    }

    #[inline]
    fn build_move_metrics(stats: &EntryStatistics) -> metadata::MoveMetrics {
        metadata::MoveMetrics {
            last_transition_at: stats.transitioned_at,
            has_started: stats.num_attempts > 0,
            first_runnable_at: stats.first_runnable_at,
        }
    }

    #[inline]
    fn mark_transition(stats: &mut EntryStatistics, at: UniqueTimestamp) {
        stats.transitioned_at = at;
    }

    #[inline]
    fn mark_run_attempt(stats: &mut EntryStatistics, at: UniqueTimestamp) {
        stats.num_attempts = stats.num_attempts.saturating_add(1);
        if stats.first_attempt_at.is_none() {
            stats.first_attempt_at = Some(at);
        }
        stats.latest_attempt_at = Some(at);
        Self::mark_transition(stats, at);
    }

    #[inline]
    fn mark_park(stats: &mut EntryStatistics, at: UniqueTimestamp) {
        stats.num_parks = stats.num_parks.saturating_add(1);
        Self::mark_transition(stats, at);
    }
}
