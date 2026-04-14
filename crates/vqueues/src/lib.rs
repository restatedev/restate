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

use restate_clock::RoughTimestamp;
use restate_limiter::LimitKey;
use restate_storage_api::lock_table::{LockState, WriteLockTable};
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryStatusHeader, EntryValue, ReadVQueueTable, Stage, Status,
    WriteVQueueTable, metadata,
};
use restate_storage_api::{StorageError, lock_table};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::invocation::InvocationTarget;
use restate_types::vqueues::{EntryId, Seq, VQueueId};
use restate_types::{LockName, Scope, ServiceName};
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
        drop_pending_resources: bool,
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
        status: Status,
    ) -> Result<bool, StorageError> {
        // find the entry
        let header = storage.get_vqueue_entry_status(partition_key, id).await?;

        let Some(entry_state) = header else {
            return Ok(false);
        };

        let inbox = Self::get(entry_state.vqueue_id(), storage, cache, action_collector).await?;
        let Some(mut inbox) = inbox else {
            return Ok(false);
        };

        inbox.end(at, &entry_state, status);
        Ok(true)
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
        at: UniqueTimestamp,
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
                let link = if let Some(lock_name) = invocation_target.lock_name() {
                    VQueueLink::Lock(lock_name)
                } else {
                    VQueueLink::Service(ServiceName::new(invocation_target.service_name()))
                };

                let meta = VQueueMeta::new(at, invocation_target.scope(), limit_key.clone(), link);
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
                let limit_key = limit_key.to_cheap_cloneable();
                let meta = VQueueMeta::new(at, scope.clone(), limit_key, link);
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
    pub fn enqueue_new(
        &mut self,
        created_at: UniqueTimestamp,
        seq: impl Into<Seq>,
        run_at: impl Into<RoughTimestamp>,
        entry_id: impl Into<EntryId>,
        metadata: impl Into<EntryMetadata>,
    ) {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        let mut event = VQueueEvent::new(meta.vqueue_id().clone());

        let run_at = run_at.into();
        let entry_id = entry_id.into();
        let metadata = metadata.into();

        let key = EntryKey::new(false, run_at, seq, entry_id);
        let stats = EntryStatistics::new(created_at, run_at);
        let status = if stats.first_runnable_at > created_at.to_unix_millis() {
            Status::Scheduled
        } else {
            Status::New
        };

        let update = metadata::Update::new(
            created_at,
            metadata::Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(&stats),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        // We need to add the entry into the inbox vqueue.
        let value = EntryValue {
            status,
            stats: stats.clone(),
            metadata: metadata.clone(),
        };

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
        wait_stats: &WaitStats,
    ) -> EntryKey {
        let vqueue_id = header.vqueue_id();
        let partition_key = vqueue_id.partition_key();
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Inbox));

        // Remove from inbox and move to ready
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Inbox, header.entry_key());

        let mut event = VQueueEvent::new(vqueue_id.clone());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                // Use the stats before updating the entry to measure stage exit durations and
                // first-run wait correctly.
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(vqueue_id);
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::DecisionConfirmed {
                key: *header.entry_key(),
                drop_pending_resources: false,
            });
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::DecisionConfirmed {
                key: *header.entry_key(),
                drop_pending_resources: false,
            });
        } else {
            unreachable!("Cannot run an item on a dormant vqueue");
        }

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

        self.storage.put_vqueue_inbox(
            vqueue_id,
            Stage::Running,
            &modified_key,
            &EntryValue {
                status: Status::Running,
                stats: stats.clone(),
                // We pick metadata from EntryStateHeader since it could have been updated
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
            Status::Running,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        modified_key
    }

    // Left intentionally for future reference
    // pub fn wake_up<T>(
    //     &mut self,
    //     at: UniqueTimestamp,
    //     header: &impl EntryStateHeader,
    //     run_at: Option<RoughTimestamp>,
    //     updated_state: &T,
    // ) where
    //     T: EntryState + bilrost::Message + bilrost::encoding::RawMessage,
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
        new_status: Status,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Paused | Stage::Suspended));

        // Delete the old inbox entry
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let mut event = VQueueEvent::new(vqueue_id.clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(vqueue_id, &update);

        // We can be asked to wake up but not run immediately (or get a lower run_at for priority
        // boosting). If that's the case, we mutate the entry key to reflect that.
        let modified_key = header.entry_key().set_run_at(run_at);

        let stats = Self::mark_transition(at, header.stats());

        let maybe_new_metadata = updated_metadata.unwrap_or_else(|| header.metadata().clone());

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Inbox,
            &modified_key,
            &maybe_new_metadata,
            stats.clone(),
            new_status,
        );

        let value = EntryValue {
            stats,
            status: new_status,
            metadata: maybe_new_metadata,
        };

        // We add the entry back into the waiting inbox
        self.storage
            .put_vqueue_inbox(vqueue_id, Stage::Inbox, &modified_key, &value);

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(vqueue_id);
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
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(next_stage, Stage::Paused | Stage::Suspended));

        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let mut event = VQueueEvent::new(vqueue_id.clone());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            // todo: This only makes sense if previous stage was inbox
            event.push(EventDetails::Removed {
                key: *header.entry_key(),
            });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            // todo: This only makes sense if previous stage was inbox
            event.push(EventDetails::Removed {
                key: *header.entry_key(),
            });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        let new_status = match next_stage {
            Stage::Paused => Status::Paused,
            Stage::Suspended => Status::Suspended,
            _ => unreachable!(),
        };

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
                status: new_status,
            },
        );

        self.storage.put_vqueue_entry_status(
            vqueue_id,
            next_stage,
            header.entry_key(),
            header.metadata(),
            stats,
            new_status,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }
    }

    /// Movement of a running entry back to the waiting inbox happens on failover of pp.
    /// or entries being retried.
    ///
    /// ? -> Inbox
    pub fn yield_entry(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        run_at: Option<RoughTimestamp>,
        updated_metadata: Option<EntryMetadata>,
        new_status: Status,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());

        trace!(
            vqueue = %vqueue_id,
            key = ?header.entry_key(),
            prev_stage = %header.stage(),
            "[yield] vqueue entry: {}", header.display_entry_id()
        );
        // Remove from running and move to waiting
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Inbox,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        if was_active_before != is_active_now {
            unreachable!("Yielding should never toggle vqueue's active state");
        }

        // We can be asked to wake up but not run immediately (or get a lower run_at for priority
        // boosting). If that's the case, we mutate the entry key to reflect that.
        let modified_key = header.entry_key().set_run_at(run_at);

        let stats = Self::mark_yield(at, header.stats());

        let maybe_new_metadata = updated_metadata.unwrap_or_else(|| header.metadata().clone());
        let value = EntryValue {
            stats: stats.clone(),
            status: new_status,
            metadata: maybe_new_metadata,
        };
        // We add the entry back into the waiting inbox
        self.storage
            .put_vqueue_inbox(vqueue_id, Stage::Inbox, &modified_key, &value);

        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Inbox,
            &modified_key,
            header.metadata(),
            stats.clone(),
            new_status,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let mut event = VQueueEvent::new(meta.vqueue_id().clone());
            event.push(EventDetails::MetadataUpdated(update));
            if matches!(header.stage(), Stage::Inbox) {
                // The scheduler is moving the item from inbox, so we need to confirm its
                // decision. We only do that if prev_stage is Inbox because it's the only
                // stage that needs confirmation.
                event.push(EventDetails::DecisionConfirmed {
                    key: *header.entry_key(),
                    drop_pending_resources: false,
                });
            }
            trace!(
                "Informing the scheduler about this item being enqueued to Inbox: {modified_key:?}"
            );
            event.push(EventDetails::Enqueued {
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
        // todo: add a paramter to specify the "scrub time" for this item.
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());

        // Remove from the current stage
        self.storage
            .delete_vqueue_inbox(vqueue_id, header.stage(), header.entry_key());

        trace!(
            vqueue = %vqueue_id,
            key = ?header.entry_key(),
            prev_stage = %header.stage(),
            "[end] ending vqueue entry: {}", header.display_entry_id()
        );

        let mut event = VQueueEvent::new(vqueue_id.clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(header.stage()),
                next_stage: Stage::Finished,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        let modified_key = if header.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            self.storage.release_lock(meta.meta().scope(), lock_name);
            event.push(EventDetails::LockReleased {
                scope: meta.meta().scope().clone(),
                lock_name: lock_name.clone(),
            });
            header.entry_key().release_lock()
        } else {
            *header.entry_key()
        };

        // Move the entry to Finished stage
        // for future: Use this to set the deletion time.
        let modified_key = modified_key.set_run_at(Some(RoughTimestamp::MAX));

        let stats = Self::mark_transition(at, header.stats());

        let value = EntryValue {
            stats: stats.clone(),
            metadata: header.metadata().clone(),
            status: new_status,
        };

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

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            // todo: when does this make sense?
            // if we were in inbox only.
            // validate that
            event.push(EventDetails::Removed {
                key: *header.entry_key(),
            });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            // todo: when does this make sense?
            // if we were in inbox only.
            event.push(EventDetails::Removed {
                key: *header.entry_key(),
            });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
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

    /// A specialized version of run designed for inline execution of an entry.
    ///
    /// This drives the transition from inbox -> finished directly and correctly notifies
    /// the scheduler as if it was a regular invocation but takes a few shortcuts
    /// since there is no actual time spent that can be tracked.
    pub fn run_then_finish(
        &mut self,
        at: UniqueTimestamp,
        header: &impl EntryStatusHeader,
        wait_stats: &WaitStats,
    ) {
        let vqueue_id = header.vqueue_id();
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        assert_eq!(vqueue_id, meta.vqueue_id());
        assert!(matches!(header.stage(), Stage::Inbox));

        // Remove from inbox and move to ready
        self.storage
            .delete_vqueue_inbox(vqueue_id, Stage::Inbox, header.entry_key());

        let mut event = VQueueEvent::new(vqueue_id.clone());

        // fake run, for the same of completeness
        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);
        let stats = Self::mark_run_attempt(at, header.stats(), wait_stats);

        event.push(EventDetails::MetadataUpdated(update));
        event.push(EventDetails::DecisionConfirmed {
            key: *header.entry_key(),
            // In the case of state mutation, we execute it inline and we can
            // ask the scheduler to drop any pending resources for this entry.
            drop_pending_resources: true,
        });

        // move to finished
        if let Some(lock_name) = meta.meta().lock_name() {
            // We only release the lock in scheduler's memory.
            // We don't release it in storage because we didn't acquire it in storage.
            event.push(EventDetails::LockReleased {
                scope: meta.meta().scope().clone(),
                lock_name: lock_name.clone(),
            });
        }

        let update = metadata::Update::new(
            at,
            metadata::Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Finished,
                metrics: Self::build_move_metrics(header.stats()),
            },
        );

        let (was_active_before, is_active_now) = meta.apply_update(&update);
        self.storage.update_vqueue(vqueue_id, &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
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
                status: Status::Succeeded,
                metadata: header.metadata().clone(),
            },
        );

        // Update the entry state so we can track the new entry key and stage
        self.storage.put_vqueue_entry_status(
            vqueue_id,
            Stage::Finished,
            &modified_key,
            header.metadata(),
            stats,
            Status::Succeeded,
        );

        if let Some(collector) = self.action_collector.as_deref_mut() {
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

    #[inline]
    fn build_move_metrics(stats: &EntryStatistics) -> metadata::MoveMetrics {
        metadata::MoveMetrics {
            last_transition_at: stats.transitioned_at,
            has_started: stats.num_attempts > 0,
            first_runnable_at: stats.first_runnable_at,
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
        // todo: use wait stats for cumulative per-entry wait times.
        _wait_stats: &WaitStats,
    ) -> EntryStatistics {
        EntryStatistics {
            num_attempts: stats.num_attempts.saturating_add(1),
            first_attempt_at: stats.first_attempt_at.or(Some(at)),
            latest_attempt_at: Some(at),
            transitioned_at: at,
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
    fn mark_yield(at: UniqueTimestamp, stats: &EntryStatistics) -> EntryStatistics {
        EntryStatistics {
            num_yields: stats.num_yields.saturating_add(1),
            transitioned_at: at,
            ..stats.clone()
        }
    }
}
